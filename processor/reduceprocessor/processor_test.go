package reduceprocessor

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/processor/processortest"

	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/golden"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/pdatatest/plogtest"
)

func TestProcessLogsDeduplicate(t *testing.T) {
	testCases := []struct {
		name               string
		inputFile          string
		expectedFile       string
		mergeStrategies    map[string]MergeStrategy
		skippedAggregation int
	}{
		{
			name:         "different record attrs",
			inputFile:    "attrs.yaml",
			expectedFile: "attrs-expected.yaml",
		},
		{
			name:         "merge strategy first",
			inputFile:    "merge.yaml",
			expectedFile: "merge-first-expected.yaml",
			mergeStrategies: map[string]MergeStrategy{
				"location": First,
			},
		},
		{
			name:         "merge strategy last",
			inputFile:    "merge.yaml",
			expectedFile: "merge-last-expected.yaml",
			mergeStrategies: map[string]MergeStrategy{
				"location": Last,
			},
		},
		{
			name:         "merge strategy append",
			inputFile:    "merge.yaml",
			expectedFile: "merge-array-expected.yaml",
			mergeStrategies: map[string]MergeStrategy{
				"location": Array,
			},
		},
		{
			name:         "merge strategy append",
			inputFile:    "merge.yaml",
			expectedFile: "merge-concat-expected.yaml",
			mergeStrategies: map[string]MergeStrategy{
				"location": Concat,
			},
		},
		{
			name:               "skip aggregation when no group by attributes match",
			inputFile:          "skip-aggregation.yaml",
			expectedFile:       "skip-aggregation-expected.yaml",
			skippedAggregation: 1,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			factory := NewFactory()
			cfg := factory.CreateDefaultConfig()
			oCfg := cfg.(*Config)
			oCfg.GroupBy = []string{"partition_id"}
			oCfg.WaitFor = time.Second * 1
			oCfg.MergeStrategies = tc.mergeStrategies

			sink := new(consumertest.LogsSink)
			p, err := factory.CreateLogsProcessor(context.Background(), processortest.NewNopSettings(), oCfg, sink)
			require.NoError(t, err)

			input, err := golden.ReadLogs(filepath.Join("testdata", tc.inputFile))
			require.NoError(t, err)
			expected, err := golden.ReadLogs(filepath.Join("testdata", tc.expectedFile))
			require.NoError(t, err)

			assert.NoError(t, p.ConsumeLogs(context.Background(), input))

			// check aggregated logs are not emitted immediately
			// non-aggregated logs are emitted
			actual := sink.AllLogs()
			require.Len(t, actual, tc.skippedAggregation)

			// flush the cache to evit all entries, causing logs to be emitted
			p.(*reduceProcessor).cache.Purge()

			// check logs are emitted after flush interval
			actual = sink.AllLogs()
			require.Len(t, actual, 1)

			assert.NoError(t, plogtest.CompareLogs(expected, actual[0]))
		})
	}
}
