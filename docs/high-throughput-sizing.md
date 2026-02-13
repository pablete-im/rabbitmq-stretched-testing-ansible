# High-Throughput Warm Standby Sizing Guide

Recommendations for running Tanzu RabbitMQ warm standby replication at high message rates, particularly for trading-adjacent and financial services workloads.

## Fan-Out Replication Architecture

Warm standby replication uses a single-upstream, multi-downstream fan-out topology. The upstream cluster writes every replicated message into a stream-backed replication log **once**, regardless of downstream count. Each downstream is an independent consumer of that log, tracking its own position:

```
                    Upstream Cluster
                  [single replication log]
                   /         |          \
                  /          |           \
                 v           v            v
          Downstream 1   Downstream 2   Downstream 3
          (in-region)    (cross-region)  (cross-region)
```

Two subsystems work together:

- **Schema Definition Sync** (AMQP port 5672) — replicates vhosts, users, queues, exchanges, bindings, policies. Downstream clusters poll the upstream at a configurable interval (`schema_definition_sync.downstream.minimum_sync_interval`).
- **Standby Message Replication** (Stream port 5552) — replicates queue contents asynchronously via the RabbitMQ stream protocol in a raw binary transfer mode with minimal encoding overhead.

### Tiered (Cascading) Replication Is Not Supported

A cluster's operating mode is set at the cluster level and is **mutually exclusive**:

```ini
schema_definition_sync.operating_mode = upstream   # OR downstream — never both
standby.replication.operating_mode = upstream       # OR downstream — never both
```

A downstream cluster cannot simultaneously act as an upstream for another cluster. All downstreams must connect directly to the single upstream. There is no way to offload WAN replication to an intermediate cluster.

## Bottleneck Analysis at Scale

At moderate message rates (tens of thousands msg/s), outbound network bandwidth is typically the only constraint that scales with downstream count. At trading-adjacent volumes (hundreds of thousands to millions of msg/s), multiple resources become stressed simultaneously.

### Disk I/O

The replication log is written once regardless of downstream count — adding downstreams does not increase write I/O. However, the upstream is also running its primary workload (quorum queue Raft logs, message store), so total write I/O is the sum of both.

When downstreams are keeping up (reading near the tail of the log), their reads are served from the OS page cache with no disk I/O. Disk reads become significant only when a downstream falls behind and needs to read log segments that have been evicted from cache.

**Example at 500K msg/s, 500 bytes average:**

| Write path | Throughput |
|---|---|
| Replication log | ~250 MB/s |
| Quorum queue Raft logs | ~250 MB/s (varies with queue count and replication factor) |
| **Combined** | **~500+ MB/s sustained** |

#### Lab Environment: SAS SSD Considerations

The lab environment uses SAS SSDs with two tiers:
- **Cache tier**: SAS SSD Write Intensive (high endurance, 3-10+ DWPD)
- **Storage tier**: SAS SSD Read Intensive (lower endurance, 0.3-1 DWPD)

The 12Gbps SAS interface has a theoretical ceiling of ~1.2 GB/s (~1.0-1.1 GB/s practical per device). This is relevant because at 500+ MB/s combined write throughput, a single SAS SSD is approaching 50% of its interface bandwidth — before accounting for the storage virtualization overhead from vSAN or the hypervisor layer.

**How the workload maps to disk tiers:**

| Workload | I/O pattern | Best tier |
|---|---|---|
| Raft WAL (write-ahead log) | Sequential write, fsync-heavy | Write Intensive cache |
| Replication log writes | Sequential write, sustained | Write Intensive cache |
| Message store | Mixed read/write | Write Intensive cache (writes), Read Intensive storage (reads) |
| Replication log reads (downstream catch-up) | Sequential read | Read Intensive storage |

**Key risks with SAS SSDs at high throughput:**
- **Write Intensive SSDs** handle the sustained write patterns well (high DWPD), but the SAS interface bandwidth becomes the ceiling rather than media endurance
- **Read Intensive SSDs** can degrade under sustained write load — if the storage layer places any write-path data on these devices, sustained throughput drops as the drive's write cache fills
- **vSAN/storage virtualization** adds overhead: replication, checksumming, and metadata operations consume additional I/O beyond what RabbitMQ generates, effectively reducing the throughput available to the application

### Network Bandwidth

Outbound replication bandwidth scales linearly with downstream count:

```
Replication outbound = message_rate x avg_message_size x num_downstreams
```

**Example at 500K msg/s, 500 bytes average:**

| Downstreams | Replication outbound | Add client traffic | Total |
|---|---|---|---|
| 1 | ~250 MB/s (~2 Gbps) | Variable | 3-5 Gbps |
| 2 | ~500 MB/s (~4 Gbps) | Variable | 5-7 Gbps |
| 3 | ~750 MB/s (~6 Gbps) | Variable | 8-10+ Gbps |

A 10GbE NIC can be saturated with 3 downstreams at this message rate.

### WAN Throughput

TCP throughput per connection is bounded by the bandwidth-delay product:

```
Max throughput per connection = TCP_window_size / RTT
```

At 35ms RTT, sustaining 2 Gbps requires ~8.75 MB TCP windows. Default kernel settings will not achieve this.

During burst events (market open/close, volatility spikes), the WAN link cannot absorb the peak rate. The downstream falls behind and relies on the replication log retention to catch up after the burst subsides.

### CPU

Replication adds minimal CPU overhead on the upstream. The stream protocol operates in raw binary mode with little encoding/decoding. CPU is dominated by the primary workload: message routing, queue management, Raft consensus.

## Infrastructure Sizing Recommendations

### Storage

**Production recommendation:**

| Recommendation | Rationale |
|---|---|
| NVMe SSDs | SAS SSDs cap at ~1.1 GB/s per device (12Gbps interface); NVMe provides 2-7 GB/s |
| Separate devices for Raft logs, message store, and replication log | Prevents I/O contention between write paths |
| Size replication log disk for retention window | See retention sizing below |

**Lab environment (SAS SSD):**

The lab uses SAS SSD Write Intensive (cache tier) and SAS SSD Read Intensive (storage tier). This is adequate for validating replication behavior and functional testing, but will hit throughput ceilings earlier than NVMe under sustained high-volume load. Expect the SAS interface (~1.1 GB/s) to become the bottleneck before CPU or memory at trading-adjacent message rates. When interpreting lab performance test results, account for this disk throughput ceiling — production hardware with NVMe will have significantly more I/O headroom.

If the storage layer is vSAN, the effective throughput available to RabbitMQ is further reduced by vSAN's own I/O overhead (replication, checksums, metadata). Monitor `iostat` and vSAN performance metrics during load tests to identify whether disk I/O is the constraining factor.

### Network

| Recommendation | Rationale |
|---|---|
| 25GbE+ NICs on upstream nodes | 10GbE saturates with 2-3 downstreams at high throughput |
| Dedicated NIC for replication traffic (if available) | Isolates replication from client traffic |
| Monitor NIC TX utilization as primary capacity metric | Replication outbound is the linearly scaling cost |

### WAN Circuit Sizing

```
Minimum WAN bandwidth = message_rate x avg_message_size x 1.1 (protocol overhead)
```

Add headroom for burst absorption. If sustained rate is 2 Gbps, provision at least 3-5 Gbps to handle bursts without the downstream falling behind immediately.

**TCP tuning for high-latency links** (on upstream and downstream nodes):

```bash
# Increase max TCP receive/send buffer sizes
sysctl -w net.core.rmem_max=16777216
sysctl -w net.core.wmem_max=16777216
sysctl -w net.ipv4.tcp_rmem="4096 87380 16777216"
sysctl -w net.ipv4.tcp_wmem="4096 65536 16777216"

# Ensure window scaling is enabled (usually default)
sysctl -w net.ipv4.tcp_window_scaling=1
```

### Replication Log Retention

The `standby.replication.retention.size_limit.messages` setting must accommodate the slowest downstream catching up after a temporary disconnection or burst event.

```
Retention size = message_rate x avg_message_size x max_catch_up_window
```

**Example:** At 500K msg/s, 500 bytes, with a 30-minute catch-up window:

```
500,000 x 500 x 1,800 = ~450 GB retention
```

The default 5 GB is insufficient for high-throughput workloads. Size retention for your expected burst duration and WAN recovery time.

## Replication Lag and RPO

Warm standby replication is asynchronous. There is always a window of messages that exist on the upstream but have not yet been received by the downstream. During failover, these messages are lost.

**Messages in flight during failover (by replication lag):**

| Replication lag | At 100K msg/s | At 500K msg/s | At 1M msg/s |
|---|---|---|---|
| 10ms | 1,000 | 5,000 | 10,000 |
| 100ms | 10,000 | 50,000 | 100,000 |
| 1 second | 100,000 | 500,000 | 1,000,000 |

The in-region downstream will typically have lag in the low milliseconds. The cross-region downstream's lag depends on WAN latency and bandwidth — expect tens to hundreds of milliseconds under normal conditions, and potentially seconds during burst events.

**There is no zero-data-loss guarantee.** For workloads where losing messages during failover is unacceptable (e.g., order execution flow), warm standby replication alone is insufficient. Consider using RabbitMQ for trading-adjacent workloads (notifications, event distribution, back-office processing) where the RPO window is acceptable, and purpose-built systems with synchronous replication for the order path.

## Downstream Prioritization

There is no built-in RabbitMQ mechanism to prioritize one downstream over another. All downstreams consume from the replication log concurrently at whatever rate the network and downstream can sustain.

Network-level QoS can be applied using `tc` (traffic control) on the upstream nodes to prioritize replication traffic to the in-region downstream over the cross-region downstream. This uses the same `tc`/`prio`/`netem` framework already used in `playbooks/configure_latency.yml` for latency simulation. This is only meaningful when the upstream's outbound NIC is genuinely saturated — otherwise each downstream naturally consumes at its own rate.

## Operational Considerations

### Failover with Multiple Downstreams

Promoting one downstream does not automatically reconfigure the others:

1. Run `rabbitmqctl promote_warm_standby` on the chosen downstream
2. Change the promoted cluster's configuration from `downstream` to `upstream` mode (required for persistence across restarts)
3. Reconfigure remaining downstream clusters to point at the newly promoted upstream (update `set_standby_replication_upstream_endpoints` and `set_schema_replication_upstream_endpoints`)
4. Reconnect remaining downstreams with `rabbitmqctl connect_standby_replication_downstream`

Script this procedure in advance. Under pressure during a real failure is not the time to figure out the reconfiguration steps.

### Monitoring Checklist

| Metric | Where | Why |
|---|---|---|
| `rabbitmqctl standby_replication_status` | Each downstream | Replication lag per downstream |
| `rabbitmqctl schema_replication_status` | Each downstream | Schema sync health |
| Upstream NIC TX bytes/sec | Upstream nodes | NIC saturation risk |
| WAN link utilization | Network monitoring | Bandwidth headroom |
| Upstream disk write latency | Upstream nodes | I/O contention |
| Upstream disk iowait | Upstream nodes | Replication log + Raft log pressure |
| OS page cache hit ratio | Upstream nodes | Indicates if downstream reads hit disk |
| Replication log disk usage | Upstream nodes | Retention capacity |
