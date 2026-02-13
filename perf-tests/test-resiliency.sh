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
#   ./perf-tests/test-resiliency.sh --host 192.168.20.200
#   ./perf-tests/test-resiliency.sh --host 192.168.20.200 --skip-chaos
# =============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
TOOLS_DIR="$SCRIPT_DIR/tools"
RESULTS_DIR="$SCRIPT_DIR/results"

# Defaults
HOST="192.168.20.200"
USER="admin"
PASSWORD=""
SKIP_CHAOS=false
SSH_USER="ansible"

# Cluster nodes (AZ-Cluster-1)
NODE1_HOST="192.168.20.200"  # az-rmq-01 (Phoenix DC)
NODE2_HOST="192.168.20.201"  # az-rmq-02 (Chandler DC)
NODE3_HOST="192.168.20.202"  # az-rmq-03 (Chandler DC)

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
        --host)       HOST="$2"; shift 2 ;;
        --user)       USER="$2"; shift 2 ;;
        --password)   PASSWORD="$2"; shift 2 ;;
        --ssh-user)   SSH_USER="$2"; shift 2 ;;
        --skip-chaos) SKIP_CHAOS=true; shift ;;
        *)            echo "Unknown option: $1"; exit 1 ;;
    esac
done

if [[ -z "$PASSWORD" ]]; then
    PASSWORD="${RMQ_PASSWORD:-}"
fi
if [[ -z "$PASSWORD" ]]; then
    read -rsp "RabbitMQ password for '$USER': " PASSWORD
    echo
fi

AMQP_URI="amqp://${USER}:${PASSWORD}@${HOST}:5672"
MGMT_URL="http://${HOST}:15672"

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
    curl -sf -u "${USER}:${PASSWORD}" "${MGMT_URL}/api/nodes" 2>/dev/null || echo "[]"
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
    curl -sf -u "${USER}:${PASSWORD}" "${MGMT_URL}/api/queues/%2F/${queue}" 2>/dev/null | \
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

# Get message count in queue
get_queue_messages() {
    local queue="$1"
    curl -sf -u "${USER}:${PASSWORD}" "${MGMT_URL}/api/queues/%2F/${queue}" 2>/dev/null | \
        python3 -c "import sys, json; d=json.load(sys.stdin); print(d.get('messages', 0))" 2>/dev/null || echo "0"
}

# Wait for cluster to have N running nodes
wait_for_nodes() {
    local expected="$1"
    local timeout="${2:-120}"
    local elapsed=0

    while [[ $elapsed -lt $timeout ]]; do
        local count
        count=$(get_running_nodes)
        if [[ "$count" -ge "$expected" ]]; then
            return 0
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
        *az-rmq-01*) echo "$NODE1_HOST" ;;
        *az-rmq-02*) echo "$NODE2_HOST" ;;
        *az-rmq-03*) echo "$NODE3_HOST" ;;
        *) echo "" ;;
    esac
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

    # Create quorum queue with messages
    log_info "  Creating quorum queue and publishing messages..."
    "$TOOLS_DIR/perf-test" \
        --uri "$AMQP_URI" \
        --quorum-queue \
        --queue "$queue" \
        --producers 1 \
        --consumers 0 \
        --pmessages 1000 \
        --confirm 10 \
        --size 1000 \
        --id "failover-setup" > /dev/null 2>&1

    local initial_messages
    initial_messages=$(get_queue_messages "$queue")
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
        ssh_sudo "$leader_ip" "systemctl start tanzu-rabbitmq-server" || true
        return 1
    fi

    log_info "  New leader elected: $new_leader"

    # Verify messages are intact
    local final_messages
    final_messages=$(get_queue_messages "$queue")
    log_info "  Final message count: $final_messages"

    # Restart the failed node
    log_info "  Restarting failed node..."
    ssh_sudo "$leader_ip" "systemctl start tanzu-rabbitmq-server" || true

    # Wait for cluster recovery
    log_info "  Waiting for cluster recovery..."
    if ! wait_for_nodes 3 120; then
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
    local expected_messages=500

    # Publish messages with confirms (ensures durability)
    log_info "  Publishing durable messages..."
    "$TOOLS_DIR/perf-test" \
        --uri "$AMQP_URI" \
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
    initial_messages=$(get_queue_messages "$queue")
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
    during_messages=$(get_queue_messages "$queue")

    # Restart node
    log_info "  Restarting node..."
    ssh_sudo "$target_ip" "systemctl start tanzu-rabbitmq-server" || true
    wait_for_nodes 3 60 || true

    # Consume all messages
    log_info "  Consuming messages..."
    "$TOOLS_DIR/perf-test" \
        --uri "$AMQP_URI" \
        --queue "$queue" \
        --producers 0 \
        --consumers 1 \
        --cmessages "$expected_messages" \
        --id "durability-con" > /dev/null 2>&1

    sleep 2
    local final_messages
    final_messages=$(get_queue_messages "$queue")

    # Success if messages were preserved during failure and all consumed
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
    ssh_sudo "$NODE3_HOST" "systemctl start tanzu-rabbitmq-server" || true

    # Wait for recovery
    log_info "  Waiting for cluster recovery..."
    if wait_for_nodes "$initial_nodes" 120; then
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

    if $SKIP_CHAOS; then
        log_warn "Skipped (--skip-chaos specified)"
        return 0
    fi

    local queue="resiliency-test-partition"

    # Create queue and publish messages
    log_info "  Setting up test queue..."
    "$TOOLS_DIR/perf-test" \
        --uri "$AMQP_URI" \
        --quorum-queue \
        --queue "$queue" \
        --producers 1 \
        --consumers 0 \
        --pmessages 500 \
        --confirm 10 \
        --size 1000 \
        --id "partition-setup" > /dev/null 2>&1

    local initial_messages
    initial_messages=$(get_queue_messages "$queue")

    # Simulate network partition using iptables (block traffic from NODE3 to NODE1)
    log_info "  Simulating network partition..."
    ssh_sudo "$NODE3_HOST" "iptables -A INPUT -s $NODE1_HOST -j DROP" || true
    ssh_sudo "$NODE3_HOST" "iptables -A OUTPUT -d $NODE1_HOST -j DROP" || true

    # Wait for partition detection
    sleep 30

    # Check cluster status
    local status
    status=$(curl -sf -u "${USER}:${PASSWORD}" "${MGMT_URL}/api/nodes" 2>/dev/null | \
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
    final_messages=$(get_queue_messages "$queue")

    if [[ "$final_messages" -ge "$initial_messages" ]]; then
        log_pass "Network partition handled, messages intact ($final_messages)"
        return 0
    else
        log_error "Message loss during partition: $initial_messages -> $final_messages"
        return 1
    fi
}

test_packet_loss_resilience() {
    log_info "Test 5: Packet loss resilience"

    if $SKIP_CHAOS; then
        log_warn "Skipped (--skip-chaos specified)"
        return 0
    fi

    # Introduce 5% packet loss on one node
    log_info "  Introducing 5% packet loss on $NODE2_HOST..."
    ssh_sudo "$NODE2_HOST" "tc qdisc add dev ens192 root netem loss 5% 2>/dev/null || tc qdisc change dev ens192 root netem loss 5%" || true

    # Run throughput test
    log_info "  Running throughput test under packet loss..."
    local output
    output=$("$TOOLS_DIR/perf-test" \
        --uri "$AMQP_URI" \
        --quorum-queue \
        --queue "resiliency-packet-loss" \
        --producers 2 \
        --consumers 2 \
        --time 30 \
        --size 5000 \
        --confirm 50 \
        --id "packet-loss" 2>&1) || true

    # Remove packet loss
    log_info "  Removing packet loss..."
    ssh_sudo "$NODE2_HOST" "tc qdisc del dev ens192 root 2>/dev/null" || true

    # Check if test completed with reasonable throughput (macOS compatible)
    local send_rate
    send_rate=$(echo "$output" | sed -n 's/.*sending rate avg: \([0-9][0-9]*\).*/\1/p' | tail -1)
    send_rate="${send_rate:-0}"

    if [[ "$send_rate" -gt 100 ]]; then
        log_pass "Packet loss resilience verified (throughput: $send_rate msg/s)"
        return 0
    else
        log_error "Severe throughput degradation under packet loss"
        return 1
    fi
}

# --- Main ---
echo "=============================================="
echo "  Criterion 2: Core Resiliency Features Test"
echo "=============================================="
echo "  Target Host: $HOST"
echo "  Skip Chaos:  $SKIP_CHAOS"
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
    echo "# Host: $HOST"
    echo "# Skip Chaos: $SKIP_CHAOS"
    echo "#"
    echo ""
} > "$RESULT_FILE"

# Helper to strip ANSI color codes for file output
strip_ansi() {
    sed 's/\x1b\[[0-9;]*m//g'
}

for test_func in \
    test_initial_health \
    test_quorum_leader_failover \
    test_message_durability \
    test_cluster_recovery \
    test_network_partition \
    test_packet_loss_resilience
do
    echo ""
    echo "" >> "$RESULT_FILE"
    # Run test, display with colors, strip colors for file
    if $test_func 2>&1 | tee >(strip_ansi >> "$RESULT_FILE"); then
        ((TESTS_PASSED++))
    else
        ((TESTS_FAILED++))
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
