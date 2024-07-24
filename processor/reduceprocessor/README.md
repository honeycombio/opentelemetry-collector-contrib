# Reduce Processor

*Status*: Experimental

// TODO

### Configuration Options

| Name        | Description                                                                            | Default Value |
| ----------- | -------------------------------------------------------------------------------------- | ------------- |
| ttl         | The TTL for log record hashes to live in the cache for expressed as a `time.Duration`. | 30 seconds    |
| max_entries | The maximum number of entries for the LRU cache.                                       | 1000          |

### Example configuration

The following is an example of configuring the processor:

```yaml
reduce:
  ttl: 1m # 1 minute
  max_entries: 10_000
```
