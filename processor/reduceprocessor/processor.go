package reduceprocessor

import (
	"context"
	"strings"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/processor"
	"go.uber.org/zap"

	"github.com/hashicorp/golang-lru/v2/expirable"

	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/pdatautil"
	"github.com/open-telemetry/opentelemetry-collector-contrib/processor/reduceprocessor/internal/metadata"

	"github.com/cespare/xxhash/v2"
)

type cacheKey [16]byte

type mergeState struct {
	resource  pcommon.Resource
	scope     pcommon.InstrumentationScope
	logRecord plog.LogRecord
	count     int
	firstSeen time.Time
	lastSeen  time.Time
}

func newMergeState(r pcommon.Resource, s pcommon.InstrumentationScope, lr plog.LogRecord) *mergeState {
	return &mergeState{
		resource:  r,
		scope:     s,
		logRecord: lr,
		count:     1,
		firstSeen: time.Now(),
		lastSeen:  time.Now(),
	}
}

type reduceProcessor struct {
	telemetryBuilder *metadata.TelemetryBuilder
	nextConsumer     consumer.Logs
	logger           *zap.Logger
	cache            *expirable.LRU[cacheKey, *mergeState]
	config           *Config
}

var _ consumer.Logs = (*reduceProcessor)(nil)
var _ processor.Logs = (*reduceProcessor)(nil)

func (p *reduceProcessor) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: true}
}

func (p *reduceProcessor) Start(context.Context, component.Host) error {
	return nil
}

func (p *reduceProcessor) Shutdown(ctx context.Context) error {
	p.Flush(ctx)
	return nil
}

func (p *reduceProcessor) ConsumeLogs(ctx context.Context, ld plog.Logs) error {
	ld.ResourceLogs().RemoveIf(func(rl plog.ResourceLogs) bool {
		// get resource attributes
		resourceAttrs := rl.Resource().Attributes()

		rl.ScopeLogs().RemoveIf(func(sl plog.ScopeLogs) bool {
			// increment number of received log records, this doesn't mean they will be merged
			p.telemetryBuilder.ReduceProcessorReceived.Add(ctx, int64(sl.LogRecords().Len()))

			// get scope attributes
			scopeAttrs := sl.Scope().Attributes()

			sl.LogRecords().RemoveIf(func(lr plog.LogRecord) bool {
				// generate hash for log record using resource, scope and log record attributes
				// returns whether the log record should be kept or not
				// true means we can aggregate the log record
				// false means we can't aggregate the log record
				hash, keep := p.generateHash(resourceAttrs, scopeAttrs, lr)
				if !keep {
					return false
				}

				// increment number of merged log records
				p.telemetryBuilder.ReduceProcessorMerged.Add(ctx, 1)

				// try to get log merge state from the cache
				state, ok := p.cache.Get(hash)
				if ok {
					// check if the state has reached the maximum number of log records to merge
					if state.count >= p.config.MaxMergeCount {
						// remove it from the cache to force it to be evicted and sent to the next consumer
						p.cache.Remove(hash)

						// crete a new merge state
						state = newMergeState(rl.Resource(), sl.Scope(), lr)
					} else {
						// increment merge state's count and update last seen time
						state.count += 1
						state.lastSeen = time.Now()
					}

					// merge resource, scope and log record attributes
					p.mergeAttributes(state.resource.Attributes(), rl.Resource().Attributes())
					p.mergeAttributes(state.scope.Attributes(), sl.Scope().Attributes())
					p.mergeAttributes(state.logRecord.Attributes(), lr.Attributes())
				} else {
					// state was not found in the cache, create a new state
					state = newMergeState(rl.Resource(), sl.Scope(), lr)
				}
				// add state to the cache, replaces existing state if it was found
				p.cache.Add(hash, state)

				// remove log record as it has been merged
				return true
			})
			return sl.LogRecords().Len() == 0
		})
		return rl.ScopeLogs().Len() == 0
	})

	// pass any remaining log records to the next consumer
	if ld.LogRecordCount() > 0 {
		return p.nextConsumer.ConsumeLogs(ctx, ld)
	}
	return nil
}

func (p *reduceProcessor) mergeAttributes(existingAttrs pcommon.Map, additionalAttrs pcommon.Map) {
	// ensure existing attributes has enough capacity to hold additional attributes
	// TODO: this is not efficient, we should determine number of unique attributes and ensure capacity based on that
	existingAttrs.EnsureCapacity(existingAttrs.Len() + additionalAttrs.Len())

	// loop over new attributes and apply merge strategy
	additionalAttrs.Range(func(attrName string, attrValue pcommon.Value) bool {
		// get merge strategy using attribute name
		mergeStrategy, ok := p.config.MergeStrategies[attrName]
		if !ok {
			// use default merge strategy if no strategy is defined for the attribute
			mergeStrategy = p.config.DefaultMergeStrategy
		}

		switch mergeStrategy {
		case First:
			// add attribute if it doesn't exist
			_, ok := existingAttrs.Get(attrName)
			if !ok {
				attrValue.CopyTo(existingAttrs.PutEmpty(attrName))
			}
		case Last:
			// overwrite existing attribute if present
			attrValue.CopyTo(existingAttrs.PutEmpty(attrName))
		case Array:
			// append value to existing value if it exists
			existingValue, ok := existingAttrs.Get(attrName)
			if ok {
				// if existing value is a slice, append to it
				// otherwise, create a new slice and append both values
				// NOTE: not sure how this will deal with different data types :/
				var slice pcommon.Slice
				if existingValue.Type() == pcommon.ValueTypeSlice {
					slice = existingValue.Slice()
					slice.EnsureCapacity(slice.Len() + 1)
				} else {
					slice = pcommon.NewSlice()
					slice.EnsureCapacity(2)
					existingValue.CopyTo(slice.AppendEmpty())
				}
				attrValue.CopyTo(slice.AppendEmpty())
				slice.CopyTo(existingAttrs.PutEmptySlice(attrName))
			} else {
				// add new attribute as it doesn't exist yet
				attrValue.CopyTo(existingAttrs.PutEmpty(attrName))
			}
		case Concat:
			// concatenate value with existing value if it exists
			existingValue, ok := existingAttrs.Get(attrName)
			if ok {
				// concatenate existing value with new value using configured delimiter
				strValue := strings.Join([]string{existingValue.AsString(), attrValue.AsString()}, p.config.ConcatDelimiter)
				existingAttrs.PutStr(attrName, strValue)
			} else {
				// add new attribute as it doesn't exist yet
				attrValue.CopyTo(existingAttrs.PutEmpty(attrName))
			}
		}
		return true
	})
}

func (p *reduceProcessor) toLogs(state *mergeState) plog.Logs {
	logs := plog.NewLogs()

	rl := logs.ResourceLogs().AppendEmpty()
	state.resource.CopyTo(rl.Resource())

	sl := rl.ScopeLogs().AppendEmpty()
	state.scope.CopyTo(sl.Scope())

	lr := sl.LogRecords().AppendEmpty()
	state.logRecord.CopyTo(lr)

	// add merge count, first seen and last seen attributes if configured
	if p.config.MergeCountAttribute != "" {
		lr.Attributes().PutInt(p.config.MergeCountAttribute, int64(state.count))
	}
	if p.config.FirstSeenAttribute != "" {
		lr.Attributes().PutStr(p.config.FirstSeenAttribute, lr.Timestamp().String())
	}
	if p.config.LastSeenAttribute != "" {
		lr.Attributes().PutStr(p.config.LastSeenAttribute, lr.Timestamp().String())
	}
	return logs
}

func (p *reduceProcessor) onEvict(key cacheKey, state *mergeState) {
	// send merged log record to next consumer
	logs := p.toLogs(state)
	if err := p.nextConsumer.ConsumeLogs(context.Background(), logs); err != nil {
		p.logger.Error("failed to send logs to next consumer", zap.Error(err))
	}

	// increment number of output log records
	p.telemetryBuilder.ReduceProcessorOutput.Add(context.Background(), 1)
}

func (p *reduceProcessor) Flush(context.Context) error {
	p.cache.Purge()
	return nil
}

func (p *reduceProcessor) generateHash(resourceAttrs pcommon.Map, scopeAttrs pcommon.Map, lr plog.LogRecord) (cacheKey, bool) {
	// create a map to hold group by attributes
	groupByAttrs := pcommon.NewMap()
	groupByAttrs.EnsureCapacity(len(p.config.GroupBy))

	// loop over group by attributes and try to find them in log record, scope and resource
	for _, attrName := range p.config.GroupBy {
		// try to find each attribute in log record, scope and resource
		// done in reverse order so that log record attributes take precedence
		// over scope attributes and scope attributes take precedence over resource attributes
		attr, ok := lr.Attributes().Get(attrName)
		if ok {
			attr.CopyTo(groupByAttrs.PutEmpty(attrName))
			continue
		}
		if attr, ok = scopeAttrs.Get(attrName); ok {
			attr.CopyTo(groupByAttrs.PutEmpty(attrName))
			continue
		}
		if attr, ok = resourceAttrs.Get(attrName); ok {
			attr.CopyTo(groupByAttrs.PutEmpty(attrName))
		}
	}

	var key cacheKey
	if groupByAttrs.Len() == 0 {
		return key, false
	}

	// generate hashes for group by attrs, body and severity
	groupByAttrsHash := pdatautil.MapHash(groupByAttrs)
	bodyHash := pdatautil.ValueHash(lr.Body())
	severityHash := pdatautil.ValueHash(pcommon.NewValueStr(lr.SeverityText()))

	// generate hash for log record
	hash := xxhash.New()
	hash.Write(groupByAttrsHash[:])
	hash.Write(bodyHash[:])
	hash.Write(severityHash[:])

	copy(key[:], hash.Sum(nil))
	return key, true
}
