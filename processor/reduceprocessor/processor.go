package reduceprocessor

import (
	"context"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/processor"
	"go.uber.org/zap"

	"github.com/hashicorp/golang-lru/v2/expirable"
	"github.com/open-telemetry/opentelemetry-collector-contrib/processor/reduceprocessor/internal/metadata"
)

type reduceProcessor struct {
	telemetryBuilder *metadata.TelemetryBuilder
	nextConsumer     consumer.Logs
	logger           *zap.Logger
	cache            *expirable.LRU[cacheKey, *cacheEntry]
	config           *Config
}

var _ processor.Logs = (*reduceProcessor)(nil)

func newReduceProcessor(_ context.Context, settings processor.Settings, nextConsumer consumer.Logs, config *Config) (*reduceProcessor, error) {
	telemetryBuilder, err := metadata.NewTelemetryBuilder(settings.TelemetrySettings)
	if err != nil {
		return nil, err
	}

	return &reduceProcessor{
		telemetryBuilder: telemetryBuilder,
		nextConsumer:     nextConsumer,
		logger:           settings.Logger,
		config:           config,
		cache:            newCache(telemetryBuilder, settings.Logger, nextConsumer, config),
	}, nil
}

func (p *reduceProcessor) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: true}
}

func (p *reduceProcessor) Start(context.Context, component.Host) error {
	return nil
}

func (p *reduceProcessor) Shutdown(_ context.Context) error {
	p.cache.Purge()
	return nil
}

func (p *reduceProcessor) ConsumeLogs(ctx context.Context, ld plog.Logs) error {
	ld.ResourceLogs().RemoveIf(func(rl plog.ResourceLogs) bool {
		// cache copy of resource attributes
		resource := rl.Resource()

		rl.ScopeLogs().RemoveIf(func(sl plog.ScopeLogs) bool {
			// cache copy of scope attributes
			scope := sl.Scope()

			// increment number of received log records
			p.telemetryBuilder.ReduceProcessorReceived.Add(ctx, int64(sl.LogRecords().Len()))

			sl.LogRecords().RemoveIf(func(logRecord plog.LogRecord) bool {
				// create cache key using resource, scope and log record
				// returns whether we can aggregate the log record or not
				cacheKey, keep := newCacheKey(p.config.GroupBy, resource, scope, logRecord)
				if !keep {
					// cannot aggregate, don't remove log record
					return false
				}

				// increment number of aggregated log records
				p.telemetryBuilder.ReduceProcessorAggregated.Add(ctx, 1)

				// try to get existing entry from cache
				cacheEntry, ok := p.cache.Get(cacheKey)
				if !ok {
					// not found, create a new entry
					cacheEntry = newCacheEntry(resource, scope, logRecord)
				} else {
					// check if the existing entry is still valid
					if cacheEntry.isValid(p.config.MaxReduceCount, p.config.MaxReduceTimeout) {
						// not valid, remove it from the cache which triggers onEvict and sends it to the next consumer
						p.cache.Remove(cacheKey)

						// crete a new entry
						cacheEntry = newCacheEntry(resource, scope, logRecord)
					} else {
						// valid, merge log record with existing entry
						cacheEntry.merge(p.config.MergeStrategies, resource, scope, logRecord)
					}
				}

				// add entry to the cache, replaces existing entry if present
				p.cache.Add(cacheKey, cacheEntry)

				// remove log record as it has been aggregated
				return true
			})

			// remove if no log records left
			return sl.LogRecords().Len() == 0
		})

		// remove if no scope logs left
		return rl.ScopeLogs().Len() == 0
	})

	// pass any remaining unaggregated log records to the next consumer
	if ld.LogRecordCount() > 0 {
		return p.nextConsumer.ConsumeLogs(ctx, ld)
	}
	return nil
}
