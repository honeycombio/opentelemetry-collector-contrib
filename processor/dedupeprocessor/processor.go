// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package dedupeprocessor // import "github.com/open-telemetry/opentelemetry-collector-contrib/processor/dedupeprocessor"

import (
	"context"

	"go.opentelemetry.io/collector/pdata/plog"

	"github.com/hashicorp/golang-lru/v2/expirable"

	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/pdatautil"
	"github.com/open-telemetry/opentelemetry-collector-contrib/processor/dedupeprocessor/internal/metadata"

	"github.com/cespare/xxhash/v2"
)

type dedupeProcessor struct {
	telemetryBuilder *metadata.TelemetryBuilder
	cache            *expirable.LRU[[16]byte, bool]
}

func (a *dedupeProcessor) processLogs(ctx context.Context, ld plog.Logs) (plog.Logs, error) {
	ld.ResourceLogs().RemoveIf(func(rl plog.ResourceLogs) bool {
		resourceAttrsHash := pdatautil.MapHash(rl.Resource().Attributes())
		rl.ScopeLogs().RemoveIf(func(sl plog.ScopeLogs) bool {
			scopeAttrsHash := pdatautil.MapHash(sl.Scope().Attributes())
			sl.LogRecords().RemoveIf(func(lr plog.LogRecord) bool {
				// record the number of log records that were received
				a.telemetryBuilder.DedupeProcessorReceived.Add(ctx, int64(sl.LogRecords().Len()))

				hash := generateHash(resourceAttrsHash, scopeAttrsHash, lr)
				if a.cache.Contains(hash) {
					// log record was already in the cache, drop it
					a.telemetryBuilder.DedupeProcessorDropped.Add(ctx, int64(1))
					return true
				}
				// log record was added to the cache, keep it
				a.cache.Add(hash, true)
				a.telemetryBuilder.DedupeProcessorOutput.Add(ctx, int64(1))
				return false
			})
			return sl.LogRecords().Len() == 0
		})
		return rl.ScopeLogs().Len() == 0
	})
	return ld, nil
}

func generateHash(resourceHash [16]byte, scopeHash [16]byte, lr plog.LogRecord) [16]byte {
	logAttrsHash := pdatautil.MapHash(lr.Attributes())

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
