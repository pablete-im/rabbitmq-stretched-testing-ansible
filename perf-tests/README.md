# RabbitMQ Performance Testing

Performance test framework for validating RabbitMQ cluster reliability and replication under load.

Uses [rabbitmq-perf-test](https://github.com/rabbitmq/rabbitmq-perf-test) (AMQP queues) and [rabbitmq-stream-perf-test](https://github.com/rabbitmq/rabbitmq-stream-perf-test) (streams).

## Prerequisites

- Java 11+ on the machine running tests
- Network access to RabbitMQ nodes on ports 5672 (AMQP) and 5552 (streams)

## Setup

Install the perf-test tools:

```bash
ansible-playbook playbooks/install_perftest.yml
```

This downloads the JAR files to `perf-tests/tools/` and creates wrapper scripts.

## Running Tests

```bash
# List available scenarios
./perf-tests/run-test.sh

# Run a scenario against AZ-Cluster-1
./perf-tests/run-test.sh baseline --host 192.168.20.200

# Run against TX-Cluster-1
./perf-tests/run-test.sh baseline --host 192.168.20.206

# Add a label to distinguish runs
./perf-tests/run-test.sh baseline --host 192.168.20.200 --label "az-cluster-1-3ms"

# Test federation (publish to AZ-Cluster-1, consume from TX-Cluster-1)
./perf-tests/run-test.sh federation-test \
  --pub-host 192.168.20.200 \
  --con-host 192.168.20.206

# Pass extra args directly to perf-test
./perf-tests/run-test.sh baseline --host 192.168.20.200 -- --queue "my-test-queue"
```

Set `RMQ_PASSWORD` to avoid the password prompt:

```bash
export RMQ_PASSWORD="your-admin-password"
```

## Scenarios

| Scenario | Type | Description |
|----------|------|-------------|
| `baseline` | AMQP | 1 pub, 1 con, 1KB messages on quorum queue |
| `high-throughput` | AMQP | 5 pubs, 5 cons, small messages, max throughput |
| `large-messages` | AMQP | 64KB payloads, throttled rate |
| `latency-focus` | AMQP | Controlled 1k msg/s rate for accurate latency |
| `classic-queue` | AMQP | Non-replicated classic queue for comparison |
| `streams` | Stream | Stream with fan-out to 3 consumers |
| `federation-test` | AMQP | Cross-cluster federation replication |

## Comparing Results

```bash
# Show all results
./perf-tests/compare-results.sh

# Filter by scenario name
./perf-tests/compare-results.sh baseline

# Show last 5 results
./perf-tests/compare-results.sh --last 5
```

Results are saved to `perf-tests/results/` with timestamped filenames.

## Suggested Test Plan

### 1. Baseline Comparison

Establish performance baselines for each queue type:

```bash
./perf-tests/run-test.sh classic-queue --host 192.168.20.200 --label "az-cl-1"
./perf-tests/run-test.sh baseline --host 192.168.20.200 --label "az-cl-1-quorum"
./perf-tests/run-test.sh streams --host 192.168.20.200 --label "az-cl-1-stream"
```

### 2. Latency Impact

Compare Arizona (with metro latency) vs Texas clusters:

```bash
./perf-tests/run-test.sh baseline --host 192.168.20.200 --label "az-cl-1-3ms"
./perf-tests/run-test.sh baseline --host 192.168.20.206 --label "tx-cl-1-3ms"
```

### 3. Replication Overhead

Compare classic (non-replicated) vs quorum (replicated) on the same cluster:

```bash
./perf-tests/run-test.sh classic-queue --host 192.168.20.200 --label "no-replication"
./perf-tests/run-test.sh baseline --host 192.168.20.200 --label "quorum-replicated"
```

### 4. Federation Throughput

Measure cross-region federation replication performance:

```bash
./perf-tests/run-test.sh federation-test \
  --pub-host 192.168.20.200 \
  --con-host 192.168.20.206 \
  --label "az-to-tx"
```

### 5. Throughput Ceiling

Find the maximum throughput of each cluster:

```bash
./perf-tests/run-test.sh high-throughput --host 192.168.20.200 --label "az-cl-1-max"
./perf-tests/run-test.sh high-throughput --host 192.168.20.206 --label "tx-cl-1-max"
```

## Writing Custom Scenarios

Create a YAML file in `perf-tests/scenarios/`:

```yaml
name: my-test
description: "Description of what this tests"
type: amqp          # amqp or stream

duration: 60        # seconds
publishers: 1
pub_rate: 0         # 0 = unlimited
consumers: 1
consumer_rate: 0
message_size: 1000  # bytes
confirm: true
multi_ack_every: 100
queue_type: quorum  # classic, quorum, or stream
```

For stream tests, use `type: stream` and add stream-specific fields:

```yaml
type: stream
stream: my-stream-name
offset: first       # first, last, or next
```

## Customer Evaluation Tests

Four specialized test scripts for validating RabbitMQ in dispersed regional deployments.

### Criterion 1: Core Broker Features

Validates that basic messaging operations work when nodes are dispersed across datacenters.

```bash
./perf-tests/test-core-features.sh --host 192.168.20.200
./perf-tests/test-core-features.sh --host 192.168.20.200 --verbose
```

**Tests:**
- Cluster connectivity
- Direct exchange messaging (point-to-point)
- Fanout exchange (broadcast)
- Publisher confirms
- Quorum queue replication
- Sustained throughput under latency
- Message ordering preservation

### Criterion 2: Resiliency Features

Validates fault tolerance with hard failures and network chaos.

```bash
# Full test suite (includes chaos testing)
./perf-tests/test-resiliency.sh --host 192.168.20.200

# Skip network chaos (faster, less disruptive)
./perf-tests/test-resiliency.sh --host 192.168.20.200 --skip-chaos
```

**Tests:**
- Quorum queue leader failover (hard kill)
- Message durability through node failure
- Cluster recovery after node restart
- Network partition handling
- Packet loss resilience

**Prerequisites:**
- SSH access to cluster nodes (via ansible user)
- sudo privileges on target nodes

### Criterion 3: Warm Standby Replication

Validates cross-cluster replication for DR scenarios.

```bash
# Full test (all clusters)
./perf-tests/test-warm-standby.sh

# Skip cross-region clusters
./perf-tests/test-warm-standby.sh --skip-cross-region
```

**Tests:**
- Schema replication (vhosts, exchanges)
- Message replication to regional standby (AZ-Cluster-2)
- Message replication to cross-region DR (TX clusters)
- Replication lag measurement
- Sustained replication throughput

**Topology:**
```
AZ-Cluster-1 (upstream)
  ├─→ AZ-Cluster-2 (regional standby, ~3ms)
  ├─→ TX-Cluster-1 (cross-region DR, ~35ms)
  └─→ TX-Cluster-2 (cross-region DR, ~35ms)
```

### Criterion 4: Latency Performance Curves

Produces throughput/latency curves as functions of network latency.

```bash
# Full sweep (0, 1, 2, 3, 5, 10, 15, 20, 35, 50 ms)
./perf-tests/run-latency-sweep.sh --host 192.168.20.200

# Quick sweep (0, 3, 10, 35 ms only)
./perf-tests/run-latency-sweep.sh --host 192.168.20.200 --quick
```

**Output:**
- CSV file for charting: `results/YYYYMMDD-HHMMSS-latency-sweep.csv`
- Summary report: `results/YYYYMMDD-HHMMSS-latency-sweep-report.txt`

**CSV columns:**
```
latency_ms, send_rate_msg_s, recv_rate_msg_s, lat_min_ms, lat_median_ms, lat_p95_ms, lat_p99_ms, lat_max_ms
```

**Workload:** Enterprise-typical (5KB messages, 3k msg/s target, quorum queues)

### Enterprise Workload Scenario

A dedicated scenario matching typical enterprise workload characteristics:

```bash
./perf-tests/run-test.sh enterprise-workload --host 192.168.20.200
```

**Configuration:**
- Message size: 5KB
- Target rate: 3,000 msg/s (2 publishers x 1,500 msg/s)
- Queue type: Quorum (replicated)
- Duration: 120 seconds
- Publisher confirms enabled

### Running All Evaluation Tests

```bash
export RMQ_PASSWORD="your-admin-password"

# 1. Core features (requires working cluster)
./perf-tests/test-core-features.sh --host 192.168.20.200

# 2. Resiliency (WARNING: will stop/restart nodes)
./perf-tests/test-resiliency.sh --host 192.168.20.200

# 3. Warm standby (requires replication configured)
./perf-tests/test-warm-standby.sh

# 4. Latency curves (WARNING: modifies network latency)
./perf-tests/run-latency-sweep.sh --host 192.168.20.200
```

Results are saved to `perf-tests/results/` with timestamps.

## File Structure

```
perf-tests/
  run-test.sh              # Test runner script
  compare-results.sh       # Results comparison tool
  test-core-features.sh    # Criterion 1: Core broker features
  test-resiliency.sh       # Criterion 2: Resiliency features
  test-warm-standby.sh     # Criterion 3: Warm standby replication
  run-latency-sweep.sh     # Criterion 4: Latency performance curves
  scenarios/               # Test scenario definitions
    baseline.yml
    high-throughput.yml
    large-messages.yml
    latency-focus.yml
    classic-queue.yml
    streams.yml
    federation-test.yml
    enterprise-workload.yml # Enterprise-typical workload
  results/                 # Test output (git-ignored)
  tools/                   # Downloaded JARs (git-ignored)
```
