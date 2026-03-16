# RabbitMQ Performance Test Scripts

This directory contains two performance testing scripts for different AMQP protocols:

## 📋 **Available Scripts**

### 1. `run-test.sh` - AMQP 0.9.1 Testing (Recommended for Production)
- **Protocol:** AMQP 0.9.1
- **Tool:** perf-test (Java-based)
- **Features:** 
  - ✅ Full TLS support with Java truststores
  - ✅ Stream testing support
  - ✅ Federation testing
  - ✅ Mature and stable
  - ✅ All RabbitMQ features supported

### 2. `run-test-omq.sh` - Multi-Protocol Testing (Experimental)
- **Protocols:** AMQP 1.0, AMQP 0.9.1, STOMP, MQTT (and combinations)
- **Tool:** OMQ (Go-based)
- **Features:**
  - ✅ **Multi-protocol combinations** (16 combinations available)
  - ✅ Cross-protocol testing (e.g., MQTT → AMQP 1.0)
  - ✅ Federation testing (sequential: producer first, then consumer)
  - 📊 **Enhanced metrics** - Prometheus-based latency collection with perf-test-style output
  - ⚠️  **TLS certificate verification issues**
  - ❌ No stream testing support
  - 🧪 Experimental - use for specific multi-protocol testing

## 🚀 **Usage Examples**

### AMQP 0.9.1 (Recommended)
```bash
# Basic test
./perf-tests/run-test.sh baseline --hosts 10.85.10.234

# With TLS
./perf-tests/run-test.sh baseline --hosts pve-schwab-rmq01.acc.broadcom.net --truststore ../pki/rmqtruststore.jks --truststore-pass VMware1!

# Stream testing
./perf-tests/run-test.sh streams --hosts 10.85.10.234

# Federation test
./perf-tests/run-test.sh federation-test --pub-hosts 10.85.10.234 --con-hosts 10.85.10.235
```

### Multi-Protocol (Experimental)
```bash
# Basic AMQP 1.0 test (default)
./perf-tests/run-test-omq.sh baseline --hosts 10.85.10.234

# Multi-protocol: MQTT publisher → AMQP 1.0 consumer
./perf-tests/run-test-omq.sh baseline --hosts 10.85.10.234 --protocol mqtt-amqp

# Cross-protocol: STOMP publisher → AMQP 0.9.1 consumer
./perf-tests/run-test-omq.sh baseline --hosts 10.85.10.234 --protocol stomp-amqp091

# Multi-protocol federation test
./perf-tests/run-test-omq.sh federation-test --pub-hosts 10.85.10.234 --con-hosts 10.85.10.235 --protocol mqtt-amqp

# TLS test with CA certificate (may have certificate verification issues)
./perf-tests/run-test-omq.sh baseline --hosts pve-schwab-rmq01 --ca-cert /path/to/ca.crt --protocol amqp-amqp

# TLS test with certificate verification disabled (insecure but useful for testing)
./perf-tests/run-test-omq.sh baseline --hosts pve-schwab-rmq01.acc.broadcom.net --tls-skip-verify --protocol mqtt-amqp

# Extract CA from Java truststore first, then use it:
./perf-tests/setup-omq-tls.sh --truststore ../pki/rmqtruststore.jks --truststore-pass VMware1!
./perf-tests/run-test-omq.sh baseline --hosts pve-schwab-rmq01 --ca-cert ./perf-tests/omq-ca-bundle.crt --protocol stomp-amqp091
```

## 📊 **Enhanced Metrics Collection (OMQ)**

`run-test-omq.sh` now includes automatic Prometheus metrics collection that provides perf-test-style output:

```
id: baseline, time 1.001 s, sent: 8100 msg/s, confirmed: 8000 msg/s, nacked: 0 msg/s, received: 8000 msg/s, min/median/90th/95th/99th/max consumer latency: 1200/1234/2345/3456/4567/3400 µs, confirm latency: 650/123/234/345/456/2500 µs
```

**Key Features:**
- 📊 **Real-time metrics** - Updates every second during test execution
- 🎯 **Complete latency data** - Min/median/90th/95th/99th/max from Prometheus + log output
- 📈 **Rate calculations** - Messages per second for sent, confirmed, nacked, received
- 🔄 **Automatic collection** - Uses OMQ's built-in Prometheus endpoint (port 8080) + log parsing
- 📁 **Merged output** - Combines formatted metrics with original OMQ output

**Metric Mapping:**
- `sent` = OMQ published rate
- `confirmed` = OMQ confirmed rate  
- `received` = OMQ consumed rate (handles `omq_messages_consumed_total{priority="0"}`)
- `consumer latency` = OMQ end-to-end latency (min/median/90th/95th/99th/max µs)
- `confirm latency` = OMQ publishing latency (min/median/90th/95th/99th/max µs)

**New OMQ Features Supported:**
- 🔒 **`--tls-skip-verify`** - Skip TLS certificate verification (insecure but useful for testing)
- ⚡ **`--amqp-send-settled`** - Automatic unsettled mode when no `confirm` configured in scenario
- 🎯 **`--amqp-consume-settled`** - Automatic settled mode when `autoack: true` in scenario

## ⚠️ **Known Issues**

### AMQP 1.0 (OMQ) Limitations:
1. **TLS Certificate Verification Bug:** OMQ has issues with TLS certificate verification, showing errors like `not vhost:/`
2. **No Stream Support:** Cannot test RabbitMQ Streams
3. **Limited Features:** Some AMQP 0.9.1 features not available (multi-ack-every, qos)

### Workarounds for OMQ TLS Issues:
1. **Use `--tls-skip-verify`** for testing (insecure but functional)
2. **Use IP addresses** instead of hostnames if certificate includes them
3. **Test without TLS** first to verify functionality
4. **Use run-test.sh** for production TLS testing

### New OMQ Parameter Handling:
- **`confirm`**: When configured in scenario, OMQ confirms each message independently (value ignored)
- **`autoack`**: When `true` in scenario, OMQ uses settled consume mode
- **Multi-ack and QoS**: Not supported, warnings shown but test continues

### Federation Testing Changes:
- **Sequential execution**: Producer runs first (creates queues), then consumer (predeclared queues)
- **No Kubernetes sync**: Removed `--expected-instances` for standalone operation
- **Management URIs**: Automatic `--management-uri` configuration for queue operations
- **Auto metrics ports**: OMQ automatically assigns ports (8080 for producer, 8081 for consumer)
- **Merged output**: Combines metrics from both processes, filters zero rates

## 🛠️ **Helper Scripts**

- `setup-omq-tls.sh` - Configure TLS certificates for OMQ
- `fix-tls-hostnames.sh` - Diagnose and fix TLS hostname issues
- `omq-metrics-collector.sh` - Collect and format OMQ Prometheus metrics (used internally)

## 📊 **When to Use Which Script**

| Use Case | Script | Reason |
|----------|--------|---------|
| **Production Testing** | `run-test.sh` | Stable, full TLS support |
| **Stream Testing** | `run-test.sh` | Only option available |
| **TLS Testing** | `run-test.sh` | Reliable certificate handling |
| **AMQP 1.0 Specific Features** | `run-test-omq.sh` | Protocol-specific testing |
| **Multi-Protocol Testing** | `run-test-omq.sh` | 16 protocol combinations |
| **Cross-Protocol Testing** | `run-test-omq.sh` | MQTT→AMQP, STOMP→MQTT, etc. |
| **Non-TLS Multi-Protocol** | `run-test-omq.sh` | Works well without TLS |

## 🎯 **Recommendation**

For most use cases, **use `run-test.sh`** (AMQP 0.9.1) as it's more mature and stable. Only use `run-test-omq.sh` when you specifically need AMQP 1.0 protocol features or multi-protocol testing.