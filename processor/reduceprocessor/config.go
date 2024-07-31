package reduceprocessor

import (
	"time"

	"go.opentelemetry.io/collector/component"
)

type MergeStrategy int

const (
	First MergeStrategy = iota
	Last
	Array
	Concat
)

type Config struct {
	// FlushInterval is the time-to-live for each entry in the cache. Default is 30 seconds.
	FlushInterval time.Duration `mapstructure:"flush_interval"`
	// MaxEntries is the maximum number of entries that can be stored in the cache. Default is 1000.
	MaxEntries int `mapstructure:"max_entries"`
	// GroupBy is the list of attribute names to be used to identify unique log records.
	GroupBy []string `mapstructure:"group_by"`
	// MergeStrategies is a map of attribute names and merge strategy's to use when merging log records. DefaultMergeStrategy is used when no strategy is defined for an attribute.
	MergeStrategies map[string]MergeStrategy `mapstructure:"merge_strategy"`
	// DefaultMergeStrategy is the strategy to use when no merge strategy is defined for an attribute. Default is "first".
	DefaultMergeStrategy MergeStrategy `mapstructure:"default_merge_strategy"`
	// ConcatDelimiter is the delimiter to use when merging attributes with the "concat" strategy. Default is ",".
	ConcatDelimiter string `mapstructure:"concat_delimiter"`
}

var _ component.Config = (*Config)(nil)

// Validate checks if the processor configuration is valid
func (cfg *Config) Validate() error {
	if len(cfg.GroupBy) == 0 {
		return errors.New("group_by must contain at least one attribute name")
	}
	return nil
}
