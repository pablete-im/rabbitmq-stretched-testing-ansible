# Stream Support in run-test-omq.sh

This document describes the AMQP 1.0 stream support and stream filtering capabilities available in `run-test-omq.sh`.

## Overview

OMQ supports RabbitMQ streams via AMQP 1.0, including the advanced stream filtering feature introduced in RabbitMQ 4.1. This allows publishers to tag messages with application properties and consumers to filter which messages they receive based on those properties.

## Basic Stream Support

### YAML Configuration

```yaml
name: stream-baseline
description: "Basic stream test"
type: amqp
duration: 300
publishers: 1
consumers: 1
message_size: 1000
confirm: 100
qos: 200

# Stream configuration
queue_type: stream
offset: next  # Optional - defaults to 'next' if not specified
```

**Default Values:**
- **`offset`**: If not specified, defaults to `next` (start consuming from the next message)
- **Stream filtering**: If not specified, no filtering is applied (all messages consumed)

### Offset Specifications

The `offset` parameter controls where the consumer starts reading from the stream:

- `first` - Start from the beginning of the stream
- `last` - Start from the end of the stream
- `next` - Start from the next message (**default if not specified**)
- `offset N` - Start from offset N
- `timestamp` - Start from a specific timestamp

**Default behavior:** If `offset` is not specified in the YAML, the script automatically sets `--stream-offset next`.

### OMQ Command Generated

```bash
omq amqp-amqp \
  --uri amqp://admin:password@localhost:5672/ \
  --publishers 1 --consumers 1 \
  --time 300s --size 1000 \
  --publish-to /queues/stream-baseline \
  --consume-from /queues/stream-baseline \
  --queues stream \
  --max-in-flight 100 \
  --consumer-credits 200 \
  --stream-offset next
```

## AMQP 1.0 Stream Filtering (RabbitMQ 4.1+)

Stream filtering allows consumers to receive only a subset of messages based on application properties, reducing network traffic and improving performance.

### How It Works

1. **Publisher** sends messages with different application property values
2. **Consumer** specifies a filter expression to match only specific values
3. **RabbitMQ** filters messages server-side before sending to consumer

### YAML Configuration

```yaml
name: stream-filter
description: "Stream test with AMQP 1.0 filtering"
type: amqp
duration: 60
publishers: 1
pub_rate: 100
consumers: 1
message_size: 1000
confirm: 50
qos: 100

# Stream configuration
queue_type: stream
offset: next

# AMQP 1.0 Stream Filtering
stream_filter_key: key                       # Property name
stream_filter_values: foo,bar,baz            # Publisher: mix of values
stream_filter_match_unfiltered: "&p:ba"      # Consumer: filter expression
```

### Filter Expression Syntax

| Expression | Meaning | Example |
|------------|---------|---------|
| `value` | Exact match | `foo` matches only "foo" |
| `&p:prefix` | Prefix match | `&p:ba` matches "bar", "baz", "banana" |
| `&s:suffix` | Suffix match | `&s:ar` matches "bar", "car" |
| `&c:substring` | Contains match | `&c:ab` matches "rabbit", "cab" |

### OMQ Commands Generated

**Publisher:**
```bash
omq amqp-amqp \
  --publisher-uri amqp://admin:password@localhost:5672/ \
  --publishers 1 --consumers 0 \
  --time 60s --size 1000 --rate 100 \
  --publish-to /queues/stream-filter \
  --queues stream \
  --max-in-flight 50 \
  --amqp-app-property key=foo,bar,baz  # Mix of values
```

**Consumer:**
```bash
omq amqp-amqp \
  --consumer-uri amqp://admin:password@localhost:5672/ \
  --publishers 0 --consumers 1 \
  --time 60s \
  --consume-from /queues/stream-filter \
  --queues predeclared \
  --consumer-credits 100 \
  --stream-offset next \
  --amqp-app-property-filter key=&p:ba  # Only bar and baz
```

### Expected Behavior

With the configuration above:
- Publisher sends ~100 msg/s with mixed values (foo, bar, baz)
- Distribution: ~33% foo, ~33% bar, ~33% baz
- Consumer filter: `&p:ba` (starts with "ba")
- Consumer receives: ~66% of messages (bar + baz)
- Network efficiency: 33% reduction in traffic

## Example Scenarios

### 1. Basic Stream Test

```bash
./perf-tests/run-test-omq.sh stream-baseline \
  --hosts 10.85.10.234,10.85.10.235,10.85.10.236 \
  --user admin --password mypass
```

### 2. Stream with Filtering

```bash
./perf-tests/run-test-omq.sh stream-filter \
  --hosts 10.85.10.234,10.85.10.235,10.85.10.236 \
  --user admin --password mypass
```

### 3. Federated Stream Test

```bash
./perf-tests/run-test-omq.sh stream-filter \
  --pub-hosts 10.85.10.234 \
  --con-hosts 10.85.10.235 \
  --user admin --password mypass
```

## Multi-Protocol Stream Support

Streams can be used with different protocol combinations:

```bash
# MQTT publisher → AMQP 1.0 consumer (stream)
./perf-tests/run-test-omq.sh stream-baseline \
  --hosts localhost --password test123 \
  --protocol mqtt-amqp

# STOMP publisher → AMQP 1.0 consumer (stream)
./perf-tests/run-test-omq.sh stream-baseline \
  --hosts localhost --password test123 \
  --protocol stomp-amqp
```

**Note**: Stream filtering is an AMQP 1.0 feature, so it works best with `amqp-amqp` or `amqp091-amqp` protocols.

## Monitoring

Stream metrics are collected via OMQ's Prometheus endpoint and displayed in real-time:

```
id: stream-filter, time 1.001 s, sent: 100 msg/s, confirmed: 100 msg/s, received: 66 msg/s
id: stream-filter, time 2.001 s, sent: 100 msg/s, confirmed: 100 msg/s, received: 67 msg/s
```

Notice `received` is ~66% of `sent` due to filtering.

## Limitations

1. **RabbitMQ 4.1+** required for stream filtering
2. **AMQP 1.0** protocol recommended for filtering (some features may not work with other protocols)
3. **Single filter per consumer** - you can only filter on one property at a time
4. **Predeclared queues** - in federation mode, producer creates the stream, consumer uses predeclared

## References

- [OMQ Stream Filtering Documentation](https://github.com/rabbitmq/omq/blob/main/README.md#amqp-10-stream-filter-support)
- [RabbitMQ AMQP 1.0 Support](https://www.rabbitmq.com/docs/amqp)
- [RabbitMQ Streams](https://www.rabbitmq.com/docs/streams)
