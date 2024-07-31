package reduceprocessor

import (
	"context"
	"strings"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/processor"

	"github.com/hashicorp/golang-lru/v2/expirable"

	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/pdatautil"
	"github.com/open-telemetry/opentelemetry-collector-contrib/processor/reduceprocessor/internal/metadata"

	"github.com/cespare/xxhash/v2"
)

type mergeState struct {
	resource  pcommon.Resource
	scope     pcommon.InstrumentationScope
	logRecord plog.LogRecord
}

func newMergeState(r pcommon.Resource, s pcommon.InstrumentationScope, lr plog.LogRecord) mergeState {
	return mergeState{
		resource:  r,
		scope:     s,
		logRecord: lr,
	}
}

func (state mergeState) toLogs() plog.Logs {
	logs := plog.NewLogs()

	rl := logs.ResourceLogs().AppendEmpty()
	state.resource.CopyTo(rl.Resource())

	sl := rl.ScopeLogs().AppendEmpty()
	state.scope.CopyTo(sl.Scope())

	lr := sl.LogRecords().AppendEmpty()
	state.logRecord.CopyTo(lr)

	return logs
}

type reduceProcessor struct {
	telemetryBuilder *metadata.TelemetryBuilder
	nextConsumer     consumer.Logs
	cache            *expirable.LRU[[16]byte, mergeState]
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
	for i := 0; i < ld.ResourceLogs().Len(); i++ {
		rl := ld.ResourceLogs().At(i)

		// generate hash for resource attributes
		resourceAttrsHash := pdatautil.MapHash(rl.Resource().Attributes())

		for j := 0; j < rl.ScopeLogs().Len(); j++ {
			sl := rl.ScopeLogs().At(j)

			// increment number of received log records
			p.telemetryBuilder.ReduceProcessorReceived.Add(ctx, int64(sl.LogRecords().Len()))

			// generate hash for scope attributes
			scopeAttrsHash := pdatautil.MapHash(sl.Scope().Attributes())

			for k := 0; k < sl.LogRecords().Len(); k++ {
				lr := sl.LogRecords().At(k)

				// generate hash for log record
				hash := p.generateHash(resourceAttrsHash, scopeAttrsHash, lr)

				// try to get log state from the cache
				state, ok := p.cache.Get(hash)
				if ok {
					// state was found in the cache, merge log record with existing state
					p.mergeLogRecord(state, lr)

					// increment number of merged log records
					p.telemetryBuilder.ReduceProcessorMerged.Add(ctx, 1)
				} else {
					// state was not found in the cache, create a new state
					state = newMergeState(rl.Resource(), sl.Scope(), lr)
				}
				// add state to the cache, replaces existing state if it was found
				p.cache.Add(hash, state)
			}
		}
	}
	// reutrn nil to indicate logs were processed successfully
	// we're actually not doing anything with the logs right now
	return nil
}

func (p *reduceProcessor) mergeLogRecord(state mergeState, lr plog.LogRecord) {
	// create new attributes map and ensure it has enough capacity to hold attributes from both
	// the existing log record and the new log record
	attrs := pcommon.NewMap()
	attrs.EnsureCapacity(state.logRecord.Attributes().Len() + lr.Attributes().Len())

	// copy existing attributes
	state.logRecord.Attributes().CopyTo(attrs)

	// loop over new record's attributes and apply merge strategy
	lr.Attributes().Range(func(attrName string, attrValue pcommon.Value) bool {
		mergeStrategy, ok := p.config.MergeStrategies[attrName]
		if !ok {
			// use default merge strategy if no strategy is defined for the attribute
			mergeStrategy = p.config.DefaultMergeStrategy
		}

		switch mergeStrategy {
		case First:
			// add attribute if it doesn't exist
			_, ok := state.logRecord.Attributes().Get(attrName)
			if !ok {
				attrValue.CopyTo(attrs.PutEmpty(attrName))
			}
		case Last:
			// overwrite existing attribute if present
			attrValue.CopyTo(attrs.PutEmpty(attrName))
		case Array:
			// append value to existing value if it exists
			existingValue, ok := state.logRecord.Attributes().Get(attrName)
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
				slice.CopyTo(attrs.PutEmptySlice(attrName))
			} else {
				// add new attribute as it doesn't exist yet
				attrValue.CopyTo(attrs.PutEmpty(attrName))
			}
		case Concat:
			// concatenate value with existing value if it exists
			existingValue, ok := state.logRecord.Attributes().Get(attrName)
			if ok {
				// concatenate existing value with new value using configured delimiter
				strValue := strings.Join([]string{existingValue.AsString(), attrValue.AsString()}, p.config.ConcatDelimiter)
				attrs.PutStr(attrName, strValue)
			} else {
				// add new attribute as it doesn't exist yet
				attrValue.CopyTo(attrs.PutEmpty(attrName))
			}
		}
		return true
	})

	// replace attributes in log record
	attrs.CopyTo(state.logRecord.Attributes())
}

func (p *reduceProcessor) onEvict(key [16]byte, value mergeState) {
	lr := value.toLogs()
	p.nextConsumer.ConsumeLogs(context.Background(), lr)

	// increment number of output log records
	p.telemetryBuilder.ReduceProcessorOutput.Add(context.Background(), 1)
}

func (p *reduceProcessor) Flush(context.Context) error {
	p.cache.Purge()
	return nil
}

func (p *reduceProcessor) generateHash(resourceHash [16]byte, scopeHash [16]byte, lr plog.LogRecord) [16]byte {
	// get all group by attributes from log record
	groupByAttrs := pcommon.NewMap()
	groupByAttrs.EnsureCapacity(len(p.config.GroupBy))
	for _, attrName := range p.config.GroupBy {
		attr, ok := lr.Attributes().Get(attrName)
		if ok {
			attr.CopyTo(groupByAttrs.PutEmpty(attrName))
		}
	}

	// generate hash for group key
	groupByAttrsHash := pdatautil.MapHash(groupByAttrs)

	// generate hash for log record
	hash := xxhash.New()
	hash.Write(resourceHash[:])
	hash.Write(scopeHash[:])
	hash.Write(groupByAttrsHash[:])
	hash.Write([]byte(lr.Body().AsString()))
	hash.Write([]byte(lr.SeverityText()))

	var key [16]byte
	copy(key[:], hash.Sum(nil))
	return key
}
