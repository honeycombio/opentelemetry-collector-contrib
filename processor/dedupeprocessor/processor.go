// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package dedupeprocessor // import "github.com/open-telemetry/opentelemetry-collector-contrib/processor/dedupeprocessor"

import (
	"context"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"

	"github.com/hashicorp/golang-lru/v2/expirable"

	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/pdatautil"
	"github.com/open-telemetry/opentelemetry-collector-contrib/processor/dedupeprocessor/internal/metadata"

	"github.com/cespare/xxhash/v2"
)

type dedupeProcessor struct {
	telemetryBuilder *metadata.TelemetryBuilder
	cache            *expirable.LRU[[16]byte, bool]
	config           *Config
}

func (p *dedupeProcessor) processLogs(ctx context.Context, ld plog.Logs) (plog.Logs, error) {
	ld.ResourceLogs().RemoveIf(func(rl plog.ResourceLogs) bool {
		resourceAttrs := p.filterAttributes(rl.Resource().Attributes())
		resourceAttrsHash := pdatautil.MapHash(resourceAttrs)

		rl.ScopeLogs().RemoveIf(func(sl plog.ScopeLogs) bool {
			scopeAttrs := p.filterAttributes(sl.Scope().Attributes())
			scopeAttrsHash := pdatautil.MapHash(scopeAttrs)

			// record the number of log records that were received
			p.telemetryBuilder.DedupeProcessorReceived.Add(ctx, int64(sl.LogRecords().Len()))

			sl.LogRecords().RemoveIf(func(lr plog.LogRecord) bool {
				hash := p.generateHash(resourceAttrsHash, scopeAttrsHash, lr)
				if p.cache.Contains(hash) {
					// log record was already in the cache, drop it
					p.telemetryBuilder.DedupeProcessorDropped.Add(ctx, int64(1))
					return true
				}
				// log record was added to the cache, keep it
				p.cache.Add(hash, true)
				p.telemetryBuilder.DedupeProcessorOutput.Add(ctx, int64(1))
				return false
			})
			return sl.LogRecords().Len() == 0
		})
		return rl.ScopeLogs().Len() == 0
	})
	return ld, nil
}

func (p *dedupeProcessor) filterAttributes(attrs pcommon.Map) pcommon.Map {
	filteredAttrs := pcommon.NewMap()
	filteredAttrs.EnsureCapacity(attrs.Len())
	attrs.CopyTo(filteredAttrs)

	// remove ignored attributes
	// we do this after copying to avoid looping over the ignored attributes
	// for every attribute in the original map
	for _, attr := range p.config.IgnoreAttributes {
		filteredAttrs.Remove(attr)
	}

	return filteredAttrs
}

func (p *dedupeProcessor) generateHash(resourceHash [16]byte, scopeHash [16]byte, lr plog.LogRecord) [16]byte {
	logAttrs := p.filterAttributes(lr.Attributes())
	logAttrsHash := pdatautil.MapHash(logAttrs)

	hash := xxhash.New()
	hash.Write(resourceHash[:])
	hash.Write(scopeHash[:])
	hash.Write(logAttrsHash[:])
	hash.Write([]byte(lr.Body().AsString()))
	hash.Write([]byte(lr.SeverityText()))

	var key [16]byte
	copy(key[:], hash.Sum(nil))
	return key
}
