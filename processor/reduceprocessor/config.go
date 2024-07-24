package reduceprocessor

import (
	"time"

	"go.opentelemetry.io/collector/component"
)

type Config struct {
	// FlushInterval is the time-to-live for each entry in the cache. Default is 30 seconds.
	FlushInterval time.Duration `mapstructure:"flush_interval"`
	// MaxEntries is the maximum number of entries that can be stored in the cache. Default is 1000.
	MaxEntries int `mapstructure:"max_entries"`
	// FieldNames is the list of field names to be used to identify unique log records.
	FieldNames []string `mapstructure:"field_names"`
}

var _ component.Config = (*Config)(nil)

// Validate checks if the processor configuration is valid
func (cfg *Config) Validate() error {
	// TODO: Add validation logic
	return nil
}
