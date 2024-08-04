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
	// GroupBy is the list of attribute names used to group log records together.
	GroupBy []string `mapstructure:"group_by"`

	// ReduceTimeout is the amount of time to wait after the last log record before considering the log record is complete and ready to sent to the next consumer. Default is 10s.
	ReduceTimeout time.Duration `mapstructure:"reduce_timeout"`

	// MaxReduceTimeout is the maximum amount of time an aggregated log record can be stored in the cache before being sent to the next consumer. Default is 60s.
	MaxReduceTimeout time.Duration `mapstructure:"max_reduce_timeout"`

	// MaxReduceCount is the maximum number of log records that can be aggregated together. Once this limit is reached, the aggregated log record is sent to the next consumer and a new aggregated log record is created. Default is 100.
	MaxReduceCount int `mapstructure:"max_reduce_count"`

	// CacheSize is the maximum number of entries that can be stored in the cache. Default is 10000.
	CacheSize int `mapstructure:"cache_size"`

	// MergeStrategies is a map of attribute names to their merge strategies. If an attribute is not found in the map, the default merge strategy of First is used.
	MergeStrategies map[string]MergeStrategy `mapstructure:"merge_strategies"`

	// ReduceCountAttribute is the attribute name used to store the count of log records on the aggregated log record'. If empty, the count is not stored. Default is "".
	ReduceCountAttribute string `mapstructure:"reduce_count_attribute"`

	// FirstSeenAttribute is the attribute name used to store the first seen time of the aggregated log record. If empty, the first seen time is not stored. Default is "".
	FirstSeenAttribute string `mapstructure:"first_seen_attribute"`

	// LastSeenAttribute is the attribute name used to store the last seen time of the aggregated log record. If empty, the last seen time is not stored. Default is "".
	LastSeenAttribute string `mapstructure:"last_seen_attribute"`
}

var _ component.Config = (*Config)(nil)

// Validate checks if the processor configuration is valid
func (cfg *Config) Validate() error {
	if len(cfg.GroupBy) == 0 {
		return errors.New("group_by must contain at least one attribute name")
	}
	return nil
}
