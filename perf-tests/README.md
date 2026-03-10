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

## TLS/SSL Support

All performance test scripts support TLS connections using Java truststore files:

```bash
# Basic TLS connection (assumes empty truststore password)
./perf-tests/run-test.sh baseline --host 192.168.20.200 \
  --truststore /path/to/truststore.p12

# TLS with password-protected truststore
./perf-tests/run-test.sh baseline --host 192.168.20.200 \
  --truststore /path/to/truststore.p12 \
  --truststore-pass mypassword \
  --truststore-type PKCS12

# TLS with different truststore types
./perf-tests/run-test.sh baseline --host 192.168.20.200 \
  --truststore /path/to/truststore.jks \
  --truststore-type JKS
```

### TLS Connection Details

When TLS options are specified:
- **AMQP connections** use `amqps://host:5671` (instead of `amqp://host:5672`)
- **Stream connections** use `rabbitmq-stream+tls://host:5551` (instead of `rabbitmq-stream://host:5552`)
- **Management API** uses `https://host:15671` (instead of `http://host:15672`)

### TLS Options

| Option | Description | Default |
|--------|-------------|---------|
| `--truststore <path>` | Path to Java truststore file | None (no TLS) |
| `--truststore-pass <password>` | Truststore password | Empty string |
| `--truststore-type <type>` | Truststore type (PKCS12, JKS, etc.) | PKCS12 |

### Supported Scripts

All performance test scripts support TLS:
- `run-test.sh` - Main scenario runner
- `test-warm-standby.sh` - Warm standby replication tests
- `test-resiliency.sh` - Resiliency and failover tests
- `test-core-features.sh` - Core broker feature tests
- `run-latency-sweep.sh` - Latency performance curves
- `cleanup-test-queues.sh` - Test queue cleanup

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

**Note:** Federation tests use PerfTest's **instance synchronization** feature to coordinate separate producer and consumer processes. The script automatically:

1. Starts a consumer process on `--con-host` (waits for producer)
2. Starts a producer process on `--pub-host` (synchronizes with consumer)
3. Both processes use the same test ID and `--expected-instances 2`
4. Results are saved to separate files: `results/federation-test-TIMESTAMP.txt` (producer) and `results/federation-test-TIMESTAMP.txt.consumer`

This approach follows RabbitMQ's official recommendation for running producers and consumers on different machines, as documented in the [PerfTest documentation](https://perftest.rabbitmq.com/#running-producers-and-consumers-on-different-machines).

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
  plotter/                 # Performance results parser and plotter
    parse_and_plot.py      # PNG plot generator
    parse_to_csv.py        # CSV data exporter
    install_dependencies.sh # Dependency installer
    requirements.txt       # Python dependencies
  results/                 # Test output (git-ignored)
  tools/                   # Downloaded JARs (git-ignored)
```

---

## Performance Test Results Parser and Plotter

This set of scripts parses RabbitMQ performance test results and generates line graphs or exports CSV data for different metrics across multiple test runs.

### Available Scripts

1. **`plotter/parse_and_plot.py`** - Generates PNG plots directly (requires matplotlib)
2. **`plotter/parse_to_csv.py`** - Exports data to CSV for use with Excel/Google Sheets (no dependencies)
3. **`plotter/install_dependencies.sh`** - Automatic dependency installation script

### Quick Installation

```bash
# Run automatic installation script
./plotter/install_dependencies.sh
```

#### Manual Dependency Installation

Only needed for `plotter/parse_and_plot.py`:

##### Option 1: Using apt (recommended for Debian/Ubuntu systems)
```bash
sudo apt update
sudo apt install python3-matplotlib python3-numpy
```

##### Option 2: Using pip with virtual environment
```bash
python3 -m venv venv
source venv/bin/activate
pip install matplotlib numpy
```

##### Option 3: Using pip with --break-system-packages (not recommended)
```bash
pip3 install matplotlib numpy --break-system-packages
```

### Usage

#### Option 1: Generate PNG Plots (requires matplotlib)

```bash
python3 plotter/parse_and_plot.py <filter> [-n MAX_FILES] [-d RESULTS_DIR]
```

#### Option 2: Export to CSV (no dependencies)

```bash
python3 plotter/parse_to_csv.py <filter> [-n MAX_FILES] [-d RESULTS_DIR]
```

#### Parameters:
- `filter`: Filter name to match files (e.g., "baseline")
- `-n, --max-files`: Maximum number of files to process (newest first)
- `-d, --results-dir`: Directory containing result files (default: perf-tests/results)

#### Examples:

```bash
# Generate PNG plots for all "baseline" files
python3 plotter/parse_and_plot.py baseline

# Export to CSV only the 5 most recent files containing "baseline"
python3 plotter/parse_to_csv.py baseline -n 5

# Use a different directory
python3 plotter/parse_and_plot.py baseline -d /path/to/results

# Process files with specific filter
python3 plotter/parse_to_csv.py max-performance -n 10
```

### Functionality

The scripts:

1. **Search for files** matching the pattern `*-{filter}.txt*` in the results directory
2. **Sort by modification date** (newest first)
3. **Extract scenario name** from the first 5 lines (line starting with "# Scenario:")
4. **Parse data lines** that start with "id: ..., time ..."
5. **Extract the following metrics**:
   - `sent`: Messages sent per second
   - `confirmed`: Messages confirmed per second  
   - `received`: Messages received per second
   - `consumer_latency_median`: Consumer median latency (2nd value from "/" separated string, supports both µs and ms units)
   - `confirm_latency_median`: Confirm median latency (2nd value from "/" separated string, supports both µs and ms units)

6. **Generate 5 line graphs**, one per metric:
   - X-axis: Time in seconds
   - Y-axis: Metric value
   - One series per parsed file

7. **Save output files**:
   - **PNG plots**: `results/plots/` with names like:
     - `{DATE}-{FILTER}-sent.png`
     - `{DATE}-{FILTER}-confirmed.png`
     - `{DATE}-{FILTER}-received.png`
     - `{DATE}-{FILTER}-consumer_latency_median.png`
     - `{DATE}-{FILTER}-confirm_latency_median.png`
   - **CSV files**: `results/csv/` with names like:
     - `{DATE}-{FILTER}-sent.csv`
     - `{DATE}-{FILTER}-confirmed.csv`
     - etc.

### Expected Data Format

The scripts expect files with the following format:

```
# Federation Test - Producer Results
# Scenario: baseline-5-pub-cons
# Date: 2026-03-09 14:21:13 UTC
# Producer Host: 10.85.10.234,10.85.10.235,10.85.10.236
# Consumer Host: 10.85.10.234,10.85.10.235,10.85.10.236
...
id: baseline-1773066073, time 1.001 s, sent: 25996 msg/s, confirmed: 25496 msg/s, nacked: 0 msg/s, min/median/75th/95th/99th/max confirm latency: 6/18/22/29/34/41 ms
id: baseline-1773066073, time 2.000 s, sent: 28155 msg/s, confirmed: 28155 msg/s, nacked: 0 msg/s, min/median/75th/95th/99th/max confirm latency: 6/17/21/27/29/33 ms
...
```

For consumer files:
```
id: baseline-1773066073, time 1.001 s, received: 26120 msg/s, min/median/75th/95th/99th/max consumer latency: 7/18/23/34/22650827150373800/22650827161831440 ms
```

For files with confirm latency:
```
id: baseline, time 1.001 s, sent: 8100 msg/s, confirmed: 8000 msg/s, received: 8000 msg/s, min/median/75th/95th/99th/max consumer latency: 6566/9360/10545/13411/15730/16991 µs, confirm latency: 8109/11607/12814/15905/17658/20405 µs
```

**Note**: The parser automatically detects and handles both microseconds (µs) and milliseconds (ms) units for latency values, converting them to milliseconds in the output.

### Output Files

#### PNG Plots (`plotter/parse_and_plot.py`)
- Saved in `results/plots/`
- PNG format with high resolution (300 DPI)
- One graph per metric with all data series
- Names: `{DATE}-{FILTER}-{METRIC}.png`

#### CSV Files (`plotter/parse_to_csv.py`)
- Saved in `results/csv/`
- CSV format compatible with Excel, Google Sheets, etc.
- One column per data series, one row per time point
- Names: `{DATE}-{FILTER}-{METRIC}.csv`

#### CSV Structure
```csv
time,20260309-073604-baseline,20260309-073327-baseline,20260309-072816-baseline
1.001,50933.0,27975.0,3418.0
2.001,,32718.0,5586.0
3.001,,32670.0,7711.0
...
```

### Use Cases

- **Quick analysis**: Use `plotter/parse_to_csv.py` to export data and create charts in Excel/Google Sheets
- **Presentations**: Use `plotter/parse_and_plot.py` to generate presentation-ready graphs
- **Statistical analysis**: Import CSV into R, Python pandas, or specialized tools