# Federation Resiliency Testing

This document describes how to configure and test RabbitMQ Federation for disaster recovery scenarios.

## Overview

Federation provides an alternative to Warm Standby Replication (WSR) for replicating messages between RabbitMQ clusters. Instead of stream-based replication, Federation uses AMQP links to pull messages from upstream exchanges to downstream clusters.

### Architecture

**Upstream Cluster (az-cluster-1):**
- Fanout exchange: `federation-exchange`
- Quorum queue: `federation-upstream-queue`
- Producers publish to the exchange
- Consumers read from the queue

**Downstream Clusters (az-cluster-2, tx-cluster-1):**
- Federated exchange: `federation-exchange` (pulls from upstream)
- Quorum queue: `federation-downstream-queue`
- Federation links automatically forward messages from upstream to downstream
- Consumers can read from downstream queues when upstream fails

## Setup

### 1. Choose Replication Strategy

Edit `site.yml` to choose between WSR and Federation:

```yaml
# Option 1: Warm Standby Replication (WSR)
# - name: Configure Warm Standby Replication
#   ansible.builtin.import_playbook: playbooks/configure_warm_standby.yml
#   tags: [standby, wsr, rmq]

# Option 2: Federated Exchange
- name: Configure Federated Exchange
  ansible.builtin.import_playbook: playbooks/configure_federated_exchange.yml
  tags: [federation, rmq]
```

### 2. Deploy Federation Configuration

```bash
# Full deployment with Federation
ansible-playbook site.yml

# Or, just configure Federation (if RabbitMQ is already installed)
ansible-playbook playbooks/configure_federated_exchange.yml
```

This will:
1. Enable federation plugin on all nodes
2. Create replication user
3. Create upstream exchange and queue on az-cluster-1
4. Configure federation upstream parameters on downstream clusters
5. Create federated exchanges and queues on downstream clusters
6. Apply federation policies to enable automatic message forwarding

### 3. Verify Federation Setup

Check federation status on downstream cluster:

```bash
# Via rabbitmqctl
ssh root@10.85.10.242 "rabbitmqctl eval 'rabbit_federation_status:status().'"

# Via Management API
curl -sf -k -u admin:password https://10.85.10.242:15671/api/federation-links | jq
```

You should see:
- Status: `running`
- Exchange: `federation-exchange`
- Upstream URI pointing to az-cluster-1

## Running the Test

### Basic Usage

```bash
cd perf-tests

# Run with default settings (120s duration)
./test-resiliency-federation.sh \
  --user admin \
  --password admin \
  --ssh-user root \
  --truststore ../pki/rmqtruststore.jks \
  --truststore-pass VMware1!

# Run with longer duration
./test-resiliency-federation.sh \
  --user admin \
  --password admin \
  --duration 180 \
  --truststore ../pki/rmqtruststore.jks \
  --truststore-pass VMware1!
```

### With --standby-no-fail Option

If your cluster-2 has nodes in AZ1, use this option to ensure downstream cluster remains fully operational:

```bash
./test-resiliency-federation.sh \
  --user admin \
  --password admin \
  --standby-no-fail \
  --truststore ../pki/rmqtruststore.jks \
  --truststore-pass VMware1!
```

## Test Phases

### Phase 1: Normal Operation
- Producer publishes to `federation-exchange` on az-cluster-1
- Consumer reads from `federation-upstream-queue` on az-cluster-1
- Federation automatically forwards messages to az-cluster-2
- Messages accumulate in `federation-downstream-queue` on az-cluster-2

### Phase 2: AZ1 Failure Simulation
- Simulates complete AZ1 failure (nodes crash)
- az-cluster-1 loses quorum (2/3 nodes down)
- Producer and consumer-1 detect connection loss
- Federation link temporarily breaks

### Phase 3: Consumer Failover
- Consumer-2 starts on az-cluster-2
- Consumes messages from `federation-downstream-queue`
- Verifies messages that were federated before failure are accessible

### Phase 4: Results Analysis
- Calculates total messages sent vs received
- Measures federation effectiveness
- Typically achieves 95%+ message delivery

### Phase 5: Restoration
- Restarts failed nodes
- Cluster-1 recovers
- Federation links re-establish automatically
- System returns to normal operation

## Expected Results

### Successful Test Indicators

- ✅ Federation links show "running" status before test
- ✅ Messages successfully federated to downstream before failure
- ✅ Consumer failover to downstream cluster succeeds
- ✅ 95%+ message delivery rate
- ✅ Both clusters recover after test
- ✅ Federation links re-establish after recovery

### Key Metrics

```
Phase 1 (cluster1): Sent=50000, Received=10000
Phase 2 (cluster2): Sent=0, Received=38000
TOTAL: Sent=50000, Received=48000
Receive success: 96%
```

## Differences from WSR Test

| Aspect | WSR | Federation |
|--------|-----|------------|
| **Replication Method** | Stream-based (port 5551) | AMQP-based (port 5671) |
| **Promotion Required** | Yes (explicit promotion command) | No (automatic failover) |
| **Message Delivery** | Exactly-once (after promotion) | At-least-once (potential duplicates) |
| **Latency** | Lower (stream protocol) | Slightly higher (AMQP overhead) |
| **Setup Complexity** | Higher (WSR + schema sync) | Lower (federation only) |
| **Recovery** | Requires WSR reconfiguration | Automatic reconnection |
| **Best For** | Mission-critical, zero data loss | High availability, simpler ops |

## Troubleshooting

### Federation Links Not Establishing

```bash
# Check federation upstream parameters
ssh root@10.85.10.242 "rabbitmqctl list_parameters"

# Check federation policy
ssh root@10.85.10.242 "rabbitmqctl list_policies"

# Check upstream connectivity
ssh root@10.85.10.242 "curl -sf -k https://10.85.10.234:15671/api/overview"
```

### No Messages in Downstream Queue

- Verify federation link status is "running"
- Check that producers are publishing to the exchange (not directly to queue)
- Verify exchange bindings exist
- Check federation policy matches exchange name pattern

### Test Fails with "Resources Not Found"

Run the configuration playbook:

```bash
ansible-playbook playbooks/configure_federated_exchange.yml
```

### Low Message Delivery Rate

- Federation may experience message loss during upstream failure
- This is expected behavior vs WSR
- 80-95% delivery is normal for federation under failure conditions
- For higher guarantees, consider WSR instead

## Federation vs WSR Decision Guide

**Choose Federation if:**
- You prioritize operational simplicity
- You can tolerate potential message loss during failures
- You want automatic failover without promotion
- You have network constraints that prevent stream protocols
- You need to federate with external/OSS RabbitMQ clusters

**Choose WSR if:**
- You require zero message loss guarantees
- You have Tanzu RabbitMQ on all clusters
- You can manage promotion operations
- You need the lowest possible latency
- Compliance requires exactly-once delivery

## Cleanup

To remove federation resources:

```bash
# Delete exchanges and queues
curl -sf -k -X DELETE -u admin:password \
  https://10.85.10.234:15671/api/exchanges/%2F/federation-exchange

curl -sf -k -X DELETE -u admin:password \
  https://10.85.10.234:15671/api/queues/%2F/federation-upstream-queue

# Remove federation parameters (on downstream clusters)
ssh root@10.85.10.242 "rabbitmqctl clear_parameter federation-upstream upstream-cluster"
ssh root@10.85.10.242 "rabbitmqctl clear_policy federation-policy"
```

## Additional Resources

- [RabbitMQ Federation Guide](https://www.rabbitmq.com/federation.html)
- [Federation Reference](https://www.rabbitmq.com/federation-reference.html)
- [Federated Exchanges](https://www.rabbitmq.com/federated-exchanges.html)
