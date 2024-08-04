package reduceprocessor

import (
	"context"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/processor"

	"github.com/hashicorp/golang-lru/v2/expirable"

	"github.com/open-telemetry/opentelemetry-collector-contrib/processor/reduceprocessor/internal/metadata"
)

// createDefaultConfig creates the default configuration for the processor.
func createDefaultConfig() component.Config {
	return &Config{
		GroupBy:              []string{},
		ReduceTimeout:        time.Second * 10,
		MaxReduceTimeout:     time.Second * 60,
		MaxReduceCount:       100,
		CacheSize:            10_000,
		MergeStrategies:      map[string]MergeStrategy{},
		ReduceCountAttribute: "",
		FirstSeenAttribute:   "",
		LastSeenAttribute:    "",
	}
}

// newReduceLogProcessor returns a processor that modifies attributes of a
// log record. To construct the attributes processors, the use of the factory
// methods are required in order to validate the inputs.
func newReduceLogProcessor(_ context.Context, set processor.Settings, cfg *Config, nextConsumer consumer.Logs) (*reduceProcessor, error) {
	telemetryBuilder, err := metadata.NewTelemetryBuilder(set.TelemetrySettings)
	if err != nil {
		return nil, err
	}

	p := newReduceProcessor(telemetryBuilder, nextConsumer, set.Logger, cfg)
	cache := expirable.NewLRU[cacheKey, *reduceState](cfg.CacheSize, p.onEvict, cfg.ReduceTimeout)
	p.cache = cache

	return p, nil
}

// NewFactory returns a new factory for the Attributes processor.
func NewFactory() processor.Factory {
	return processor.NewFactory(
		metadata.Type,
		createDefaultConfig,
		processor.WithLogs(createLogsProcessor, metadata.LogsStability),
	)
}

func createLogsProcessor(
	ctx context.Context,
	set processor.Settings,
	cfg component.Config,
	nextConsumer consumer.Logs,
) (processor.Logs, error) {
	oCfg := cfg.(*Config)
	rp, err := newReduceLogProcessor(ctx, set, oCfg, nextConsumer)
	if err != nil {
		return nil, err
	}
	return rp, nil
}
