# Reduce Processor

*Status*: Experimental

// TODO

### Configuration Options

| Name | Description | Required | Default Value | 
| - | - | - | - |
| group_by | A list of attribute names to be used to identify log records that should be grouped and merged together. | Yes | `none` |
| wait_for | The amount of time to wait after the last log record was received before passing it onto the next component in the pipeline. | No | `10s` (10 seconds) |
| max_entries | The maximum number of entries for the LRU cache. | No | `10000` |
| default_merge_strategy | The default merge strategy to use when a custom one has not been defined. | No | `Last` |
| merge_strategies | A map of attribute names and merge strategy to use when merging attributes. | No | `none` |
| concat_delimiter | The default delimitor to use when concategnating attribute values together. | No | `,` |

### Example configuration

The following is the minimal configuration of the processor:

```yaml
reduce:
  group_by:
    - "host.name"
```

The following is a complete configuration of the processor:

```yaml
reduce:
  group_by:
    - "host.name"
  wait_for: 10s
  max_entries: 1000
  default_merge_strategy: first
  merge_strategies:
    "some-attribute": first
    "another-attribute": last
  concat-delimiter: ","
```
