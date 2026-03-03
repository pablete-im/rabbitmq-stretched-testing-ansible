#!/bin/bash
# =============================================================================
# Criterion 2: Core Resiliency Features Test
#
# Validates that RabbitMQ resiliency features work correctly when nodes
# are dispersed across datacenters. Tests hard failures and network chaos.
#
# Tests:
#   1. Quorum queue leader failover (hard kill)
#   2. Message durability through node failure
#   3. Cluster recovery after node restart
#   4. Network partition handling
#   5. Packet loss resilience
#
# Prerequisites:
#   - SSH access to RabbitMQ nodes (via ansible user)
#   - sudo privileges on target nodes
#   - Ansible inventory configured
#
# Usage:
#   ./perf-tests/test-resiliency.sh --hosts 192.168.20.200
#   ./perf-tests/test-resiliency.sh --hosts 192.168.20.200 --skip-chaos
#
# TLS Usage:
#   ./perf-tests/test-resiliency.sh --hosts 192.168.20.200 --truststore /path/to/truststore.p12 --truststore-pass mypass
#   ./perf-tests/test-resiliency.sh --hosts 192.168.20.200 --truststore /path/to/truststore.p12 --truststore-pass mypass
# =============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
TOOLS_DIR="$SCRIPT_DIR/tools"
RESULTS_DIR="$SCRIPT_DIR/results"

# Defaults
HOSTS="10.85.10.234"
USER="admin"
PASSWORD=""
SKIP_CHAOS=false
ONLY_CHAOS=false
SSH_USER="ansible"
TRUSTSTORE=""
TRUSTSTORE_PASS=""
TRUSTSTORE_TYPE="JKS"

# Cluster nodes (AZ-Cluster-1)
NODE1_HOST="10.85.10.234"  # node1 in AZ1
NODE2_HOST="10.85.10.235"  # node1 in AZ2
NODE3_HOST="10.85.10.236"  # node1 in AZ3

# Colors for terminal output (disabled when piped/redirected)
if [[ -t 1 ]]; then
    RED='\033[0;31m'
    GREEN='\033[0;32m'
    YELLOW='\033[1;33m'
    BLUE='\033[0;34m'
    NC='\033[0m'
else
    RED=''
    GREEN=''
    YELLOW=''
    BLUE=''
    NC=''
fi

# --- Parse arguments ---
while [[ $# -gt 0 ]]; do
    case "$1" in
        --hosts)           HOSTS="$2"; shift 2 ;;
        --user)            USER="$2"; shift 2 ;;
        --password)        PASSWORD="$2"; shift 2 ;;
        --ssh-user)        SSH_USER="$2"; shift 2 ;;
        --skip-chaos)      SKIP_CHAOS=true; shift ;;
        --only-chaos)      ONLY_CHAOS=true; shift ;;
        --truststore)      TRUSTSTORE="$2"; shift 2 ;;
        --truststore-pass) TRUSTSTORE_PASS="$2"; shift 2 ;;
        --truststore-type) TRUSTSTORE_TYPE="$2"; shift 2 ;;
        *)                 echo "Unknown option: $1"; exit 1 ;;
    esac
done

if $SKIP_CHAOS && $ONLY_CHAOS; then
    echo "Error: Cannot specify both --skip-chaos and --only-chaos"
    exit 1
fi

if [[ -z "$PASSWORD" ]]; then
    PASSWORD="${RMQ_PASSWORD:-}"
fi
if [[ -z "$PASSWORD" ]]; then
    read -rsp "RabbitMQ password for '$USER': " PASSWORD
    echo
fi

# --- Configure TLS settings ---
if [[ -n "$TRUSTSTORE" ]]; then
    echo "🔐 TLS Mode: Using truststore $TRUSTSTORE"
    MGMT_PROTOCOL="https"
    MGMT_PORT="15671"
    AMQP_PROTOCOL="amqps"
    AMQP_PORT="5671"
    
    # JVM options for performance tests
    JVM_OPTS="-Djavax.net.ssl.trustStore=$TRUSTSTORE"
    JVM_OPTS="$JVM_OPTS -Djavax.net.ssl.trustStorePassword=$TRUSTSTORE_PASS"
    JVM_OPTS="$JVM_OPTS -Djavax.net.ssl.trustStoreType=$TRUSTSTORE_TYPE"
else
    echo "🔓 Non-TLS Mode: Using standard connections"
    MGMT_PROTOCOL="http"
    MGMT_PORT="15672"
    AMQP_PROTOCOL="amqp"
    AMQP_PORT="5672"
    JVM_OPTS=""
fi

# Helper function to build URI list from comma-separated hosts
build_uris() {
    local hosts="$1"
    local protocol="$2"
    local port="$3"
    local user="$4"
    local password="$5"
    
    local uris=""
    IFS=',' read -ra HOST_ARRAY <<< "$hosts"
    for host in "${HOST_ARRAY[@]}"; do
        host=$(echo "$host" | xargs)  # trim whitespace
        if [[ -n "$uris" ]]; then
            uris="${uris},"
        fi
        uris="${uris}${protocol}://${user}:${password}@${host}:${port}"
    done
    echo "$uris"
}

AMQP_URIS=$(build_uris "$HOSTS" "$AMQP_PROTOCOL" "$AMQP_PORT" "$USER" "$PASSWORD")
# For management API, we'll use the first host
FIRST_HOST=$(echo "$HOSTS" | cut -d',' -f1 | xargs)
MGMT_URL="${MGMT_PROTOCOL}://${FIRST_HOST}:${MGMT_PORT}"

# --- Helper functions ---
log_info() { echo -e "${BLUE}[INFO]${NC} $1"; }
log_warn() { echo -e "${YELLOW}[WARN]${NC} $1"; }
log_error() { echo -e "${RED}[FAIL]${NC} $1"; }
log_pass() { echo -e "${GREEN}[PASS]${NC} $1"; }

# Execute command on remote node via SSH
ssh_cmd() {
    local host="$1"
    local cmd="$2"
    ssh -o StrictHostKeyChecking=no -o ConnectTimeout=5 "${SSH_USER}@${host}" "$cmd" 2>/dev/null
}

# Execute sudo command on remote node
ssh_sudo() {
    local host="$1"
    local cmd="$2"
    ssh -o StrictHostKeyChecking=no -o ConnectTimeout=5 "${SSH_USER}@${host}" "sudo $cmd" 2>/dev/null
}

# Get cluster status from management API
get_cluster_status() {
    curl -sf -k -u "${USER}:${PASSWORD}" "${MGMT_URL}/api/nodes" 2>/dev/null || echo "[]"
}

# Get running node count
get_running_nodes() {
    get_cluster_status | python3 -c "
import sys, json
try:
    nodes = json.load(sys.stdin)
    running = [n['name'] for n in nodes if n.get('running', False)]
    print(len(running))
except:
    print(0)
"
}

# Get quorum queue leader
get_quorum_leader() {
    local queue="$1"
    curl -sf -k -u "${USER}:${PASSWORD}" "${MGMT_URL}/api/queues/%2F/${queue}" 2>/dev/null | \
        python3 -c "import sys, json; d=json.load(sys.stdin); print(d.get('leader', 'unknown'))" 2>/dev/null || echo "unknown"
}

# Get quorum queue member nodes
get_quorum_members() {
    local queue="$1"
    curl -sf -u "${USER}:${PASSWORD}" "${MGMT_URL}/api/queues/%2F/${queue}" 2>/dev/null | \
        python3 -c "
import sys, json
try:
    d = json.load(sys.stdin)
    members = d.get('members', [])
    print(' '.join(members))
except:
    print('')
" 2>/dev/null || echo ""
}

# Wait for queue to reach expected message count
wait_for_queue_messages() {
    local queue="$1"
    local expected="$2"
    local timeout="${3:-30}"
    local wait=2
    local elapsed=0
    local stable_count=0
    local last_val=-1

    while [[ $elapsed -lt $timeout ]]; do
        local current
        current=$(curl -sf -k -u "${USER}:${PASSWORD}" "${MGMT_URL}/api/queues/%2F/${queue}" 2>/dev/null | \
            python3 -c "import sys, json; d=json.load(sys.stdin); print(d.get('messages', 0))" 2>/dev/null || echo "-1")
        
        # Connection error
        if [[ "$current" == "-1" ]]; then
            sleep "$wait"
            ((elapsed+=wait))
            continue
        fi

        # Success case
        if [[ "$current" -eq "$expected" ]]; then
            echo "$current"
            return 0
        fi

        # Check for stability (stuck count)
        if [[ "$current" -eq "$last_val" ]]; then
            ((stable_count++))
        else
            stable_count=0
        fi
        last_val="$current"

        # If stuck for 10 seconds (5 attempts * 2s) with wrong value, abort
        if [[ "$stable_count" -ge 5 ]]; then
            echo "$current"
            return 1  # Return failure signal, but echo value
        fi

        sleep "$wait"
        ((elapsed+=wait))
    done

    # Timeout reached
    echo "$last_val"
    return 1
}

# Get message count (legacy wrapper)
get_queue_messages() {
    # If called with just queue name, assumes we want > 0 (old behavior)
    # But better to use wait_for_queue_messages explicitly
    local queue="$1"
    curl -sf -k -u "${USER}:${PASSWORD}" "${MGMT_URL}/api/queues/%2F/${queue}" 2>/dev/null | \
        python3 -c "import sys, json; d=json.load(sys.stdin); print(d.get('messages', 0))" 2>/dev/null || echo "0"
}

# Wait for cluster to have N running nodes
wait_for_nodes() {
    local expected="$1"
    local timeout="${2:-300}"
    local elapsed=0

    while [[ $elapsed -lt $timeout ]]; do
        local count
        count=$(get_running_nodes)
        if [[ "$count" -ge "$expected" ]]; then
            return 0
        fi
        
        # Log progress every 10 seconds
        if (( elapsed % 10 == 0 )); then
            echo "       Waiting for nodes... ${elapsed}/${timeout}s ($count/$expected running)"
        fi
        
        sleep 2
        ((elapsed+=2))
    done
    return 1
}

# Wait for quorum queue to elect new leader
wait_for_leader() {
    local queue="$1"
    local old_leader="$2"
    local timeout="${3:-60}"
    local elapsed=0

    while [[ $elapsed -lt $timeout ]]; do
        local new_leader
        new_leader=$(get_quorum_leader "$queue")
        if [[ "$new_leader" != "unknown" && "$new_leader" != "$old_leader" ]]; then
            echo "$new_leader"
            return 0
        fi
        sleep 2
        ((elapsed+=2))
    done
    echo "timeout"
    return 1
}

# Map node name to IP
node_to_ip() {
    local node="$1"
    case "$node" in
        *pve-schwab-rmq01*) echo "$NODE1_HOST" ;;
        *pve-schwab-rmq02*) echo "$NODE2_HOST" ;;
        *pve-schwab-rmq03*) echo "$NODE3_HOST" ;;
        *az-rmq-01*) echo "$NODE1_HOST" ;;  # Fallback
        *az-rmq-02*) echo "$NODE2_HOST" ;;  # Fallback
        *az-rmq-03*) echo "$NODE3_HOST" ;;  # Fallback
        *) echo "" ;;
    esac
}

# Apply packet loss preserving existing latency (if any)
apply_packet_loss() {
    local node="$1"
    local iface="$2"
    local loss_pct="5%"

    log_info "  Applying ${loss_pct} packet loss on $node ($iface)..."

    # Check for existing Ansible config (Metro/Band 2 usually has handle 20: or just 20:)
    local qdisc_show
    qdisc_show=$(ssh_cmd "$node" "tc qdisc show dev $iface")
    
    # Debug output to understand why detection fails
    log_info "  Current TC config: $(echo "$qdisc_show" | tr '\n' ' ')"

    # Check for band 20 (can appear as 'qdisc netem 20:' or 'handle 20:')
    if echo "$qdisc_show" | grep -qE "(qdisc netem 20:|handle 20:)"; then
        # Extract existing delay parameters
        # Grep ONLY the line for band 20 first
        local band_line
        band_line=$(echo "$qdisc_show" | grep -oP '(qdisc|class) netem 20:.*')
        
        local delay_params
        # Extract delay and jitter (e.g., "delay 1.5ms 500us" or "delay 1.5ms")
        delay_params=$(echo "$band_line" | grep -oP 'delay [0-9.]+(ms|us)(\s+[0-9.]+(ms|us))?')
        
        if [[ -z "$delay_params" ]]; then
             # Fallback
             delay_params=$(echo "$band_line" | sed -n 's/.*\(delay [0-9.]\+ms\(\s\+[0-9.]\+ms\)\?\).*/\1/p')
        fi

        log_info "  Detected existing latency config: $delay_params"
        
        # Modify existing qdisc: keep delay, add loss
        ssh_sudo "$node" "tc qdisc change dev $iface parent 1:2 handle 20: netem $delay_params loss $loss_pct" || \
            log_error "Failed to modify existing TC rule"
    else
        # No existing config, add root netem
        log_info "  No existing TC config detected, adding root rule"
        local tc_output
        tc_output=$(ssh_sudo "$node" "tc qdisc add dev $iface root netem loss $loss_pct 2>&1 || tc qdisc change dev $iface root netem loss $loss_pct 2>&1")
        
        if [[ $? -ne 0 ]]; then
            log_error "Failed to add TC rule: $tc_output"
        fi
    fi
    
    # Verify TC configuration
    local verify_tc
    verify_tc=$(ssh_cmd "$node" "tc qdisc show dev $iface")
    log_info "  TC Config after apply: $verify_tc"
}

# Remove packet loss preserving existing latency
remove_packet_loss() {
    local node="$1"
    local iface="$2"

    log_info "  Removing packet loss on $node ($iface)..."

    local qdisc_show
    qdisc_show=$(ssh_cmd "$node" "tc qdisc show dev $iface")

    if echo "$qdisc_show" | grep -qE "(qdisc netem 20:|handle 20:)"; then
        # Extract existing delay parameters
        # Grep ONLY the line for band 20 first
        local band_line
        band_line=$(echo "$qdisc_show" | grep -oP '(qdisc|class) netem 20:.*')
        
        local delay_params
        # Extract delay and jitter
        delay_params=$(echo "$band_line" | grep -oP 'delay [0-9.]+(ms|us)(\s+[0-9.]+(ms|us))?')
        
        log_info "  Restoring latency config: $delay_params"
        
        # Modify qdisc: restore delay, remove loss
        ssh_sudo "$node" "tc qdisc change dev $iface parent 1:2 handle 20: netem $delay_params" || true
    else
        # No Ansible config, clean root
        log_info "  Cleaning root TC rule"
        ssh_sudo "$node" "tc qdisc del dev $iface root 2>/dev/null" || true
    fi
}

# --- Cleanup functions ---

# Clean network rules on all nodes
cleanup_network_all_nodes() {
    log_info "  Cleaning network rules on cluster nodes..."
    for host in "$NODE1_HOST" "$NODE2_HOST" "$NODE3_HOST"; do
        # Clean specific drop rules
        ssh_sudo "$host" "iptables -D INPUT -s $NODE1_HOST -j DROP 2>/dev/null || true"
        ssh_sudo "$host" "iptables -D OUTPUT -d $NODE1_HOST -j DROP 2>/dev/null || true"
        ssh_sudo "$host" "iptables -D INPUT -s $NODE2_HOST -j DROP 2>/dev/null || true"
        ssh_sudo "$host" "iptables -D OUTPUT -d $NODE2_HOST -j DROP 2>/dev/null || true"
        ssh_sudo "$host" "iptables -D INPUT -j DROP 2>/dev/null || true"
        ssh_sudo "$host" "iptables -D OUTPUT -j DROP 2>/dev/null || true"
        
        # Clean TC packet loss if present (checking common interfaces)
        for iface in ens33 ens192 eth0; do
            if ssh_cmd "$host" "ip link show $iface >/dev/null 2>&1"; then
                # Check if it has loss rule
                local tc_show
                tc_show=$(ssh_cmd "$host" "tc qdisc show dev $iface")
                if echo "$tc_show" | grep -q "loss"; then
                    remove_packet_loss "$host" "$iface"
                fi
            fi
        done
    done
}

cleanup() {
    echo ""
    log_info "Running global cleanup..."

    # 1. Clean network rules
    cleanup_network_all_nodes

    # 2. Ensure all nodes are started
    for host in "$NODE1_HOST" "$NODE2_HOST" "$NODE3_HOST"; do
        log_info "  Ensuring RabbitMQ is running on $host..."
        force_restart_node "$host"
    done
}

trap cleanup EXIT INT TERM

# Check packet loss to a node
check_packet_loss() {
    local target_ip="$1"
    local count=50
    
    log_info "  Checking packet loss to $target_ip (from $NODE1_HOST)..."
    local ping_output
    # Ping FROM Node 1 TO target (Node 2) via SSH
    ping_output=$(ssh_cmd "$NODE1_HOST" "ping -c $count -i 0.1 $target_ip" 2>&1)
    
    local loss
    loss=$(echo "$ping_output" | grep -oP '\d+(?=% packet loss)')
    
    if [[ -z "$loss" ]]; then
         # Fallback for systems without grep -P or different ping output
         loss=$(echo "$ping_output" | grep "packet loss" | awk '{print $6}' | tr -d '%')
    fi
    
    log_info "  Measured packet loss: ${loss}%"
    return 0
}

# Force restart a node (nuclear option)
force_restart_node() {
    local ip="$1"
    log_warn "  Force restarting node $ip..."
    
    # Ensure no stale network partition rules exist
    ssh_sudo "$ip" "iptables -D INPUT -j DROP 2>/dev/null || true"
    ssh_sudo "$ip" "iptables -D OUTPUT -j DROP 2>/dev/null || true"
    
    ssh_sudo "$ip" "systemctl stop tanzu-rabbitmq-server || true"
    ssh_sudo "$ip" "pkill -9 beam.smp || true"
    ssh_sudo "$ip" "pkill -9 epmd || true"
    ssh_sudo "$ip" "rm -f /var/lib/rabbitmq/mnesia/rabbit@*/cluster_nodes.config.tmp || true" # Clean temp config if stuck
    ssh_sudo "$ip" "systemctl reset-failed tanzu-rabbitmq-server || true"
    
    sleep 5 # Give OS time to clean up sockets
    
    ssh_sudo "$ip" "systemctl start tanzu-rabbitmq-server --no-block" || true
}

# --- Test functions ---

test_initial_health() {
    log_info "Test 0: Initial cluster health check"

    local nodes
    nodes=$(get_running_nodes)
    if [[ "$nodes" -ge 3 ]]; then
        log_pass "Cluster healthy with $nodes running nodes"
        return 0
    else
        log_error "Cluster not healthy, only $nodes nodes running"
        return 1
    fi
}

test_quorum_leader_failover() {
    log_info "Test 1: Quorum queue leader failover (hard kill)"

    local queue="resiliency-test-failover"
    
    # Ensure queue is clean before starting
    curl -sf -k -X DELETE -u "${USER}:${PASSWORD}" "${MGMT_URL}/api/queues/%2F/${queue}" > /dev/null 2>&1 || true

    # Create quorum queue with messages
    log_info "  Creating quorum queue and publishing messages..."
    java $JVM_OPTS -jar "$TOOLS_DIR/perf-test.jar" \
        --uris "$AMQP_URIS" \
        --quorum-queue \
        --queue "$queue" \
        --producers 1 \
        --consumers 0 \
        --pmessages 1000 \
        --confirm 10 \
        --size 1000 \
        --id "failover-setup" > /dev/null 2>&1

    local initial_messages
    initial_messages=$(wait_for_queue_messages "$queue" 1000)
    log_info "  Initial message count: $initial_messages"

    # Get current leader
    local leader
    leader=$(get_quorum_leader "$queue")
    log_info "  Current leader: $leader"

    local leader_ip
    leader_ip=$(node_to_ip "$leader")
    if [[ -z "$leader_ip" ]]; then
        log_error "Could not determine leader IP"
        return 1
    fi

    # Hard kill the leader using systemctl (simulates hard failure)
    log_info "  Killing leader node (hard failure)..."
    # Use systemctl kill with SIGKILL for a clean hard stop
    # Fall back to pkill if systemctl fails (e.g., service not managed by systemd)
    ssh_sudo "$leader_ip" "systemctl kill -s SIGKILL tanzu-rabbitmq-server 2>/dev/null || pkill -9 beam.smp" || true

    sleep 5

    # Wait for new leader election
    log_info "  Waiting for leader election..."
    local new_leader
    new_leader=$(wait_for_leader "$queue" "$leader" 60)

    if [[ "$new_leader" == "timeout" ]]; then
        log_error "Leader election timed out"
        # Try to recover
        force_restart_node "$leader_ip"
        return 1
    fi

    log_info "  New leader elected: $new_leader"

    # Verify messages are intact
    local final_messages
    if ! final_messages=$(wait_for_queue_messages "$queue" 1000); then
        log_warn "Message count mismatch or stuck ($final_messages vs 1000)"
    fi
    log_info "  Final message count: $final_messages"

    # Restart the failed node
    log_info "  Restarting failed node..."
    force_restart_node "$leader_ip"

    # Wait for cluster recovery
    log_info "  Waiting for cluster recovery..."
    if ! wait_for_nodes 3 300; then
        log_warn "  Cluster did not fully recover (may need manual intervention)"
    fi

    if [[ "$final_messages" -ge "$initial_messages" ]]; then
        log_pass "Leader failover successful, no message loss ($final_messages messages)"
        return 0
    else
        log_error "Message loss detected: $initial_messages -> $final_messages"
        return 1
    fi
}

test_message_durability() {
    log_info "Test 2: Message durability through node failure"

    local queue="resiliency-test-durability"
    
    # Ensure queue is clean before starting
    curl -sf -k -X DELETE -u "${USER}:${PASSWORD}" "${MGMT_URL}/api/queues/%2F/${queue}" > /dev/null 2>&1 || true

    local expected_messages=500

    # Publish messages with confirms (ensures durability)
    log_info "  Publishing durable messages..."
    java $JVM_OPTS -jar "$TOOLS_DIR/perf-test.jar" \
        --uris "$AMQP_URIS" \
        --quorum-queue \
        --queue "$queue" \
        --producers 1 \
        --consumers 0 \
        --pmessages "$expected_messages" \
        --confirm 1 \
        --size 5000 \
        --id "durability-pub" > /dev/null 2>&1

    # Wait for messages to be visible in API
    sleep 2

    local initial_messages
    initial_messages=$(wait_for_queue_messages "$queue" 500)
    log_info "  Published $initial_messages messages (expected $expected_messages)"

    # Stop a non-leader node
    local leader
    leader=$(get_quorum_leader "$queue")
    local target_node=""
    local target_ip=""

    for node_ip in "$NODE2_HOST" "$NODE3_HOST"; do
        if [[ "$(node_to_ip "$leader")" != "$node_ip" ]]; then
            target_ip="$node_ip"
            break
        fi
    done

    if [[ -z "$target_ip" ]]; then
        target_ip="$NODE2_HOST"
    fi

    log_info "  Stopping follower node at $target_ip..."
    ssh_sudo "$target_ip" "systemctl stop tanzu-rabbitmq-server" || true
    sleep 5

    # Verify messages still accessible
    local during_messages
    if ! during_messages=$(wait_for_queue_messages "$queue" 500); then
        log_warn "Message count mismatch during failure ($during_messages)"
    fi

    # Restart node
    log_info "  Restarting node..."
    force_restart_node "$target_ip"
    wait_for_nodes 3 300 || true

    # Consume all messages
    log_info "  Consuming messages..."
    local consume_output
    consume_output=$(java $JVM_OPTS -jar "$TOOLS_DIR/perf-test.jar" \
        --uris "$AMQP_URIS" \
        --queue "$queue" \
        --quorum-queue \
        --producers 0 \
        --consumers 1 \
        --cmessages "$expected_messages" \
        --id "durability-con" 2>&1) || true
    
    # Check if consumption failed
    if [[ $? -ne 0 ]]; then
         log_warn "Consumption command failed (exit code $?)"
         echo "$consume_output" | head -n 5
    fi

    sleep 2
    # Wait for expected messages
    local final_messages
    if ! final_messages=$(wait_for_queue_messages "$queue" 0); then
        log_warn "Message count did not reach 0 (stuck at $final_messages)"
    fi
    
    log_info "  Final message count: $final_messages"
    if [[ "$during_messages" -ge "$expected_messages" && "$final_messages" -eq 0 ]]; then
        log_pass "Message durability verified ($during_messages messages survived failure)"
        return 0
    else
        log_error "Durability issue: expected=$expected_messages, during=$during_messages, final=$final_messages"
        return 1
    fi
}

test_cluster_recovery() {
    log_info "Test 3: Cluster recovery after node restart"

    # Get initial state
    local initial_nodes
    initial_nodes=$(get_running_nodes)

    # Stop a node gracefully
    log_info "  Stopping node at $NODE3_HOST..."
    ssh_sudo "$NODE3_HOST" "systemctl stop tanzu-rabbitmq-server" || true
    sleep 5

    local during_nodes
    during_nodes=$(get_running_nodes)
    log_info "  Running nodes during failure: $during_nodes"

    # Restart node
    log_info "  Restarting node..."
    force_restart_node "$NODE3_HOST"

    # Wait for recovery
    log_info "  Waiting for cluster recovery..."
    if wait_for_nodes "$initial_nodes" 300; then
        local final_nodes
        final_nodes=$(get_running_nodes)
        log_pass "Cluster recovered ($final_nodes nodes running)"
        return 0
    else
        log_error "Cluster did not recover to $initial_nodes nodes"
        return 1
    fi
}

test_network_partition() {
    log_info "Test 4: Network partition handling"

    local queue="resiliency-test-partition"

    # Ensure queue is clean before starting
    curl -sf -k -X DELETE -u "${USER}:${PASSWORD}" "${MGMT_URL}/api/queues/%2F/${queue}" > /dev/null 2>&1 || true

    # Create queue and publish messages
    log_info "  Setting up test queue..."
    java $JVM_OPTS -jar "$TOOLS_DIR/perf-test.jar" \
        --uris "$AMQP_URIS" \
        --quorum-queue \
        --queue "$queue" \
        --producers 1 \
        --consumers 0 \
        --pmessages 500 \
        --confirm 10 \
        --size 1000 \
        --id "partition-setup" > /dev/null 2>&1

    local initial_messages
    initial_messages=$(wait_for_queue_messages "$queue" 500)

    # Simulate network partition using iptables (block traffic from NODE3 to NODE1)
    log_info "  Simulating partial network partition (isolating $NODE3_HOST from $NODE1_HOST)..."
    ssh_sudo "$NODE3_HOST" "iptables -A INPUT -s $NODE1_HOST -j DROP" || true
    ssh_sudo "$NODE3_HOST" "iptables -A OUTPUT -d $NODE1_HOST -j DROP" || true

    # Wait for partition detection
    sleep 30

    # Check cluster status
    local status
    status=$(curl -sf -k -u "${USER}:${PASSWORD}" "${MGMT_URL}/api/nodes" 2>/dev/null | \
        python3 -c "
import sys, json
try:
    nodes = json.load(sys.stdin)
    for n in nodes:
        parts = n.get('partitions', [])
        if parts:
            print('partitioned')
            sys.exit(0)
    print('healthy')
except:
    print('error')
")

    log_info "  Cluster status: $status"

    # Heal partition
    log_info "  Healing network partition..."
    ssh_sudo "$NODE3_HOST" "iptables -D INPUT -s $NODE1_HOST -j DROP" 2>/dev/null || true
    ssh_sudo "$NODE3_HOST" "iptables -D OUTPUT -d $NODE1_HOST -j DROP" 2>/dev/null || true

    # Wait for healing
    sleep 30

    # Verify messages intact
    local final_messages
    if ! final_messages=$(wait_for_queue_messages "$queue" 500); then
        log_warn "Message count mismatch after network partition ($final_messages vs 500)"
    fi

    if [[ "$final_messages" -ge "$initial_messages" ]]; then
        log_pass "Network partition handled, messages intact ($final_messages)"
        return 0
    else
        log_error "Message loss during partition: $initial_messages -> $final_messages"
        return 1
    fi
}

test_split_brain_partition() {
    log_info "Test 5: Split Brain (Total Node Isolation)"

    local queue="resiliency-test-split-brain"
    
    # Ensure queue is clean before starting
    curl -sf -k -X DELETE -u "${USER}:${PASSWORD}" "${MGMT_URL}/api/queues/%2F/${queue}" > /dev/null 2>&1 || true

    # Create queue and publish messages
    log_info "  Setting up test queue..."
    java $JVM_OPTS -jar "$TOOLS_DIR/perf-test.jar" \
        --uris "$AMQP_URIS" \
        --quorum-queue \
        --queue "$queue" \
        --producers 1 \
        --consumers 0 \
        --pmessages 500 \
        --confirm 10 \
        --size 1000 \
        --id "split-brain-setup" > /dev/null 2>&1

    local initial_messages
    initial_messages=$(wait_for_queue_messages "$queue" 500)

    # Simulate TOTAL network partition (Isolate N3 from N1 AND N2)
    log_info "  Simulating Split Brain (Isolating $NODE3_HOST from everyone)..."
    ssh_sudo "$NODE3_HOST" "iptables -A INPUT -s $NODE1_HOST -j DROP" || true
    ssh_sudo "$NODE3_HOST" "iptables -A OUTPUT -d $NODE1_HOST -j DROP" || true
    ssh_sudo "$NODE3_HOST" "iptables -A INPUT -s $NODE2_HOST -j DROP" || true
    ssh_sudo "$NODE3_HOST" "iptables -A OUTPUT -d $NODE2_HOST -j DROP" || true

    # Wait for partition detection
    sleep 30

    # Check cluster status (should show partitions)
    local status
    status=$(curl -sf -k -u "${USER}:${PASSWORD}" "${MGMT_URL}/api/nodes" 2>/dev/null | \
        python3 -c "
import sys, json
try:
    nodes = json.load(sys.stdin)
    for n in nodes:
        parts = n.get('partitions', [])
        if parts:
            print('partitioned')
            sys.exit(0)
    print('healthy')
except:
    print('error')
")

    log_info "  Cluster status: $status"

    # Heal partition
    log_info "  Healing Split Brain..."
    ssh_sudo "$NODE3_HOST" "iptables -D INPUT -s $NODE1_HOST -j DROP" 2>/dev/null || true
    ssh_sudo "$NODE3_HOST" "iptables -D OUTPUT -d $NODE1_HOST -j DROP" 2>/dev/null || true
    ssh_sudo "$NODE3_HOST" "iptables -D INPUT -s $NODE2_HOST -j DROP" 2>/dev/null || true
    ssh_sudo "$NODE3_HOST" "iptables -D OUTPUT -d $NODE2_HOST -j DROP" 2>/dev/null || true

    # Wait for healing
    sleep 30

    # Verify messages intact
    local final_messages
    if ! final_messages=$(wait_for_queue_messages "$queue" 500); then
        log_warn "Message count mismatch after Split Brain ($final_messages vs 500)"
    fi

    if [[ "$final_messages" -ge "$initial_messages" ]]; then
        log_pass "Split Brain handled, messages intact ($final_messages)"
        return 0
    else
        log_error "Message loss during Split Brain: $initial_messages -> $final_messages"
        return 1
    fi
}

test_packet_loss_resilience() {
    log_info "Test 6: Packet loss resilience"

    # Introduce 5% packet loss on one node
    log_info "  Baseline throughput check (no packet loss)..."
    local queue="resiliency-packet-loss"
    
    # Ensure queue is clean before starting
    curl -sf -k -X DELETE -u "${USER}:${PASSWORD}" "${MGMT_URL}/api/queues/%2F/${queue}" > /dev/null 2>&1 || true

    # Baseline ping
    check_packet_loss "$NODE2_HOST"

    local baseline_output
    baseline_output=$(java $JVM_OPTS -jar "$TOOLS_DIR/perf-test.jar" \
        --uris "$AMQP_URIS" \
        --quorum-queue \
        --queue "$queue" \
        --producers 2 \
        --consumers 2 \
        --time 15 \
        --size 5000 \
        --confirm 50 \
        --id "packet-loss-baseline" 2>&1) || true

    local baseline_rate
    baseline_rate=$(echo "$baseline_output" | sed -n 's/.*sending rate avg: \([0-9][0-9]*\).*/\1/p' | tail -1)
    baseline_rate="${baseline_rate:-0}"
    log_info "  Baseline throughput: $baseline_rate msg/s"

    log_info "  Introducing 5% packet loss on $NODE2_HOST..."
    
    # Dynamically detect main interface (prefer ens33)
    local iface
    if ssh_cmd "$NODE2_HOST" "ip link show ens33 >/dev/null 2>&1"; then
        iface="ens33"
    else
        iface=$(ssh_cmd "$NODE2_HOST" "ip route | grep default | awk '{print \$5}' | head -n 1")
    fi
    
    if [[ -z "$iface" ]]; then
        log_warn "Could not detect network interface, defaulting to eth0"
        iface="eth0"
    else
        log_info "  Target interface: $iface"
    fi

    # Introduce 5% packet loss on one node
    apply_packet_loss "$NODE2_HOST" "$iface"

    # Verify packet loss with ping
    check_packet_loss "$NODE2_HOST"

    # Run throughput test
    log_info "  Running throughput test under packet loss..."
    local output
    output=$(java $JVM_OPTS -jar "$TOOLS_DIR/perf-test.jar" \
        --uris "$AMQP_URIS" \
        --quorum-queue \
        --queue "resiliency-packet-loss" \
        --producers 2 \
        --consumers 2 \
        --time 30 \
        --size 5000 \
        --confirm 50 \
        --id "packet-loss" 2>&1) || true

    # Remove packet loss
    remove_packet_loss "$NODE2_HOST" "$iface"

    # Final ping check
    check_packet_loss "$NODE2_HOST"

    # Check if test completed with reasonable throughput
    local send_rate
    send_rate=$(echo "$output" | sed -n 's/.*sending rate avg: \([0-9][0-9]*\).*/\1/p' | tail -1)
    send_rate="${send_rate:-0}"

    # Calculate degradation percentage if baseline exists
    if [[ "$baseline_rate" -gt 0 ]]; then
        local drop_pct=$(( 100 - (send_rate * 100 / baseline_rate) ))
        log_info "  Throughput dropped by ${drop_pct}% ($baseline_rate -> $send_rate)"
    fi

    # Pass if rate > 50 msg/s (severe network degradation expected, but not total stall)
    if [[ "$send_rate" -gt 50 ]]; then
        log_pass "Packet loss resilience verified (throughput: $send_rate msg/s, baseline: $baseline_rate msg/s)"
        return 0
    else
        log_error "Cluster stalled under packet loss (throughput: $send_rate msg/s, baseline: $baseline_rate msg/s)"
        return 1
    fi
}

# --- Main ---
echo "=============================================="
echo "  Criterion 2: Core Resiliency Features Test"
echo "=============================================="
echo "  Target Host: $HOSTS"
echo "  Skip Chaos:  $SKIP_CHAOS"
echo "  Only Chaos:  $ONLY_CHAOS"
echo "=============================================="
echo ""
echo -e "${YELLOW}WARNING: This test will stop/restart RabbitMQ nodes!${NC}"
echo -e "${YELLOW}Ensure this is a test/lab environment.${NC}"
echo ""
read -rp "Continue? [y/N] " confirm
if [[ ! "$confirm" =~ ^[Yy]$ ]]; then
    echo "Aborted."
    exit 1
fi
echo ""

mkdir -p "$RESULTS_DIR"
TIMESTAMP=$(date +%Y%m%d-%H%M%S)
RESULT_FILE="$RESULTS_DIR/${TIMESTAMP}-resiliency.txt"

TESTS_PASSED=0
TESTS_FAILED=0

{
    echo "# Core Resiliency Features Test"
    echo "# Date: $(date -u '+%Y-%m-%d %H:%M:%S UTC')"
    echo "# Host: $HOSTS"
    echo "# Skip Chaos: $SKIP_CHAOS"
    echo "# Only Chaos: $ONLY_CHAOS"
    echo "#"
    echo ""
} > "$RESULT_FILE"

# Helper to strip ANSI color codes for file output
strip_ansi() {
    sed 's/\x1b\[[0-9;]*m//g'
}

# Initial global cleanup to ensure clean state
cleanup_network_all_nodes

# Define test lists
TESTS_BASE=(
    "test_initial_health"
    "test_quorum_leader_failover"
    "test_message_durability"
    "test_cluster_recovery"
)

TESTS_CHAOS=(
    "test_network_partition"
    "test_split_brain_partition"
    "test_packet_loss_resilience"
)

TESTS_TO_RUN=()

if $ONLY_CHAOS; then
    TESTS_TO_RUN=("${TESTS_CHAOS[@]}")
elif $SKIP_CHAOS; then
    TESTS_TO_RUN=("${TESTS_BASE[@]}")
else
    TESTS_TO_RUN=("${TESTS_BASE[@]}" "${TESTS_CHAOS[@]}")
fi

for test_func in "${TESTS_TO_RUN[@]}"; do
    echo ""
    echo "" >> "$RESULT_FILE"
    # Run test, display with colors, strip colors for file
    if $test_func 2>&1 | tee >(strip_ansi >> "$RESULT_FILE"); then
        ((TESTS_PASSED+=1))
    else
        ((TESTS_FAILED+=1))
    fi
done

# Summary
echo ""
echo "" >> "$RESULT_FILE"
echo "=============================================="
echo "==============================================" >> "$RESULT_FILE"
echo "  SUMMARY"
echo "  SUMMARY" >> "$RESULT_FILE"
echo "=============================================="
echo "==============================================" >> "$RESULT_FILE"
echo "  Tests Passed: $TESTS_PASSED"
echo "  Tests Passed: $TESTS_PASSED" >> "$RESULT_FILE"
echo "  Tests Failed: $TESTS_FAILED"
echo "  Tests Failed: $TESTS_FAILED" >> "$RESULT_FILE"
echo ""
echo "" >> "$RESULT_FILE"

if [[ "$TESTS_FAILED" -eq 0 ]]; then
    echo -e "${GREEN}  CRITERION 2: PASSED${NC}"
    echo "  CRITERION 2: PASSED" >> "$RESULT_FILE"
    echo "  Core resiliency features work when nodes are dispersed."
    echo "  Core resiliency features work when nodes are dispersed." >> "$RESULT_FILE"
else
    echo -e "${RED}  CRITERION 2: FAILED${NC}"
    echo "  CRITERION 2: FAILED" >> "$RESULT_FILE"
    echo "  Some resiliency features did not work as expected."
    echo "  Some resiliency features did not work as expected." >> "$RESULT_FILE"
fi

echo "=============================================="
echo "==============================================" >> "$RESULT_FILE"
echo ""
echo "Results saved to: $RESULT_FILE"

exit $TESTS_FAILED
