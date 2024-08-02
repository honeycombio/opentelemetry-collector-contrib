# Dedupe Processor

*Status*: Experimental

The dedupe processor deduplicates log records by dropping records that match recently seen log records.
A hash is created for a log record using it's attributes (resource, scope and record) along with it's log level (severity) and body.
The hash is stored in a LRU with a configurable expiry.
Log records that produce the same hash are dropped.

### Configuration Options

| Name | Description | Required | Default Value |
| - | - | - | - |
| cache_ttl | The TTL for log record hashes to live in the cache expressed as a `time.Duration`. | No | `30s` (30 seconds) |
| max_entries | The maximum number of entries for the cache. | No | 1000 |
| ignore_attributes | A list of attribute name to ignore when calculating a cache entry used to deduplicate log records. | No | `none` |

### Example configuration

The following is the minimal configuration of the processor and uses default values:

```yaml
dedupe:
```

The following is an example of configuring the processor with custom values for each option:

```yaml
dedupe:
  cache_ttl: 30s
  max_entries: 1000
  ignore_attribute:
    - host.name
```
