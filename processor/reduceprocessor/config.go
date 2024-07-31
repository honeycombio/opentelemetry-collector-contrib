package reduceprocessor

import (
	"errors"
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
	// WaitFor is the amount of time to wait after the last log record was received before passing it onto the next component in the pipeline. Default is 10s.
	WaitFor time.Duration `mapstructure:"wait_for"`
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
	// MergeCountAttribute is the attribute name used to store the use number of merged log records on the aggregated log record'. Default is "".
	MergeCountAttribute string `mapstructure:"merge_count_attribute"`
	// MaxMergeCount is the maximum number of log records that can be merged into a single aggregated log record. Default is 100.
	MaxMergeCount int `mapstructure:"max_merge_count"`
}

var _ component.Config = (*Config)(nil)

// Validate checks if the processor configuration is valid
func (cfg *Config) Validate() error {
	if len(cfg.GroupBy) == 0 {
		return errors.New("group_by must contain at least one attribute name")
	}
	return nil
}
