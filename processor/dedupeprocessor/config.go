// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package dedupeprocessor // import "github.com/open-telemetry/opentelemetry-collector-contrib/processor/dedupeprocessor"

import (
	"time"

	"go.opentelemetry.io/collector/component"
)

type Config struct {
	// CacheTTL is the time-to-live for each entry in the cache. Default is 30 seconds.
	CacheTTL time.Duration `mapstructure:"cache_ttl"`
	// MaxEntries is the maximum number of entries that can be stored in the cache. Default is 1000.
	MaxEntries int `mapstructure:"max_entries"`
	// IgnoreAttributes is a list of attributes that should be ignored when calculating the hash for each log record.
	IgnoreAttributes []string `mapstructure:"ignore_attributes"`
}

var _ component.Config = (*Config)(nil)

// Validate checks if the processor configuration is valid
func (cfg *Config) Validate() error {
	// TODO: Add validation logic
	return nil
}
