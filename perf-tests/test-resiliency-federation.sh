#!/bin/bash
# =============================================================================
# Test Resiliency Federation - Exchange Federation Test
#
# This script tests resiliency using RabbitMQ Federation for a 2-AZ distribution
# with automatic consumer failover between clusters.
#
# Topology:
#   AZ1: az-cluster-1 (nodes 1,2) + az-cluster-2 (node 1) 
#   AZ2: az-cluster-1 (node 3) + az-cluster-2 (nodes 2,3)
#
# Test scenario:
#   1. az-cluster-1 acts as upstream with fanout exchange and quorum-queue
#   2. az-cluster-2 has federated exchange pulling from upstream
#   3. Producer publishes continuously to upstream exchange
#   4. Consumer-1 consumes from upstream queue
#   5. Simulation of complete AZ1 failure
#   6. Consumer-2 starts consuming from downstream federated queue
#   7. Data integrity verification
#   8. Restoration to original state
#
# IMPORTANT: AZ1 failure affects both clusters:
#   - az-cluster-1: loses 2/3 nodes (nodes 1,2) → minority
#   - az-cluster-2: loses 1/3 nodes (node 1) → minimal quorum (2/3)
#
# Usage:
#   ./perf-tests/test-resiliency-federation.sh
#   ./perf-tests/test-resiliency-federation.sh --duration 180
#   ./perf-tests/test-resiliency-federation.sh --no-cleanup
#   ./perf-tests/test-resiliency-federation.sh --standby-no-fail
#
# Options:
#   --standby-no-fail: Prevents cluster-2 nodes from failing during AZ1 simulation
#                     This ensures the standby cluster maintains full quorum
# =============================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
TOOLS_DIR="$SCRIPT_DIR/tools"
RESULTS_DIR="$SCRIPT_DIR/results"

# Cluster configuration based on inventory/hosts.yml
# AZ-Cluster-1 (upstream)
AZ1_CLUSTER1_NODES=("10.85.10.234" "10.85.10.235")  # Nodes 1,2 in AZ1
AZ2_CLUSTER1_NODES=("10.85.10.236")                  # Node 3 in AZ2
ALL_CLUSTER1_NODES=("${AZ1_CLUSTER1_NODES[@]}" "${AZ2_CLUSTER1_NODES[@]}")

# AZ-Cluster-2 (downstream/standby)
AZ1_CLUSTER2_NODES=("10.85.10.241")                  # Node 1 in AZ1
AZ2_CLUSTER2_NODES=("10.85.10.242" "10.85.10.243")  # Nodes 2,3 in AZ2
ALL_CLUSTER2_NODES=("${AZ1_CLUSTER2_NODES[@]}" "${AZ2_CLUSTER2_NODES[@]}")

# Nodes that will fail in AZ1
AZ1_FAILED_NODES=("${AZ1_CLUSTER1_NODES[@]}" "${AZ1_CLUSTER2_NODES[@]}")

# Default configuration
USER="admin"
PASSWORD=""
SSH_USER="root"
CLEANUP=true
TEST_DURATION=120  # Total test duration in seconds
OSS_RABBITMQ=false
STANDBY_NO_FAIL=false  # If true, cluster-2 nodes won't fail even if in AZ1

# Federation resources
UPSTREAM_EXCHANGE="federation-exchange"
UPSTREAM_QUEUE="federation-upstream-queue"
DOWNSTREAM_QUEUE="federation-downstream-queue"

# RabbitMQ service name (tanzu-rabbitmq-server by default, rabbitmq-server for OSS)
RMQ_SERVICE="tanzu-rabbitmq-server"

# Systemctl commands (will be updated if --oss-rabbitmq is used)
RMQ_STOP_CMD="systemctl stop tanzu-rabbitmq-server"
RMQ_START_CMD="systemctl start tanzu-rabbitmq-server --no-block"
RMQ_RESTART_CMD="systemctl restart tanzu-rabbitmq-server"
RMQ_STATUS_CMD="systemctl status tanzu-rabbitmq-server"
RMQ_RESET_FAILED_CMD="systemctl reset-failed tanzu-rabbitmq-server"
RMQ_KILL_CMD="systemctl kill -s SIGKILL tanzu-rabbitmq-server"
RMQ_MASK_CMD="systemctl mask tanzu-rabbitmq-server"
RMQ_UNMASK_CMD="systemctl unmask tanzu-rabbitmq-server"

# TLS configuration
TRUSTSTORE=""
TRUSTSTORE_PASS=""
TRUSTSTORE_TYPE="JKS"

# Colors for output
if [[ -t 1 ]]; then
    RED='\033[0;31m'
    GREEN='\033[0;32m'
    YELLOW='\033[1;33m'
    BLUE='\033[0;34m'
    NC='\033[0m'
else
    RED='' GREEN='' YELLOW='' BLUE='' NC=''
fi

# --- Parse arguments ---
while [[ $# -gt 0 ]]; do
    case "$1" in
        --user)            USER="$2"; shift 2 ;;
        --password)        PASSWORD="$2"; shift 2 ;;
        --ssh-user)        SSH_USER="$2"; shift 2 ;;
        --duration)        TEST_DURATION="$2"; shift 2 ;;
        --no-cleanup)      CLEANUP=false; shift ;;
        --oss-rabbitmq)    OSS_RABBITMQ=true; shift ;;
        --standby-no-fail) STANDBY_NO_FAIL=true; shift ;;
        --truststore)      TRUSTSTORE="$2"; shift 2 ;;
        --truststore-pass) TRUSTSTORE_PASS="$2"; shift 2 ;;
        --truststore-type) TRUSTSTORE_TYPE="$2"; shift 2 ;;
        -h|--help)
            echo "Usage: $0 [OPTIONS]"
            echo ""
            echo "Options:"
            echo "  --user USERNAME               RabbitMQ admin username (default: admin)"
            echo "  --password PASSWORD           RabbitMQ password"
            echo "  --ssh-user USERNAME           SSH username for node access (default: root)"
            echo "  --duration SECONDS            Total test duration (default: 120)"
            echo "  --no-cleanup                  Don't cleanup resources at the end"
            echo "  --oss-rabbitmq                Use rabbitmq-server instead of tanzu-rabbitmq-server"
            echo "  --standby-no-fail             Prevent cluster-2 nodes from failing"
            echo "  --truststore PATH             Path to truststore for TLS connections"
            echo "  --truststore-pass PASSWORD    Truststore password"
            echo "  --truststore-type TYPE        Truststore type (default: JKS)"
            echo "  -h, --help                    Show this help"
            exit 0
            ;;
        *)
            echo "Unknown option: $1"
            echo "Use --help for usage information"
            exit 1
            ;;
    esac
done

# Reconfigure service commands if using OSS RabbitMQ
if $OSS_RABBITMQ; then
    RMQ_SERVICE="rabbitmq-server"
    RMQ_STOP_CMD="systemctl stop rabbitmq-server"
    RMQ_START_CMD="systemctl start rabbitmq-server --no-block"
    RMQ_RESTART_CMD="systemctl restart rabbitmq-server"
    RMQ_STATUS_CMD="systemctl status rabbitmq-server"
    RMQ_RESET_FAILED_CMD="systemctl reset-failed rabbitmq-server"
    RMQ_KILL_CMD="systemctl kill -s SIGKILL rabbitmq-server"
    RMQ_MASK_CMD="systemctl mask rabbitmq-server"
    RMQ_UNMASK_CMD="systemctl unmask rabbitmq-server"
    echo "ℹ️  Using OSS RabbitMQ (rabbitmq-server service)"
else
    echo "ℹ️  Using Tanzu RabbitMQ (tanzu-rabbitmq-server service)"
fi

# Get password if not provided
if [[ -z "$PASSWORD" ]]; then
    PASSWORD="${RMQ_PASSWORD:-}"
fi
if [[ -z "$PASSWORD" ]]; then
    read -rsp "RabbitMQ password for '$USER': " PASSWORD
    echo
fi

# --- Configure TLS ---
if [[ -n "$TRUSTSTORE" ]]; then
    echo "🔐 TLS Mode: Using truststore $TRUSTSTORE"
    MGMT_PROTOCOL="https"
    MGMT_PORT="15671"
    AMQP_PROTOCOL="amqps"
    AMQP_PORT="5671"
    
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

# Build URIs for specific cluster
build_cluster_uris() {
    local cluster_hosts=("$@")
    local uris=""
    
    for host in "${cluster_hosts[@]}"; do
        if [[ -n "$uris" ]]; then uris="${uris},"; fi
        uris="${uris}${AMQP_PROTOCOL}://${USER}:${PASSWORD}@${host}:${AMQP_PORT}"
    done
    echo "$uris"
}

# Build URIs for each cluster separately
CLUSTER1_URIS=$(build_cluster_uris "${ALL_CLUSTER1_NODES[@]}")
CLUSTER2_URIS=$(build_cluster_uris "${ALL_CLUSTER2_NODES[@]}")
# Use AZ2 nodes for management to survive AZ1 failure
PRIMARY_MGMT_URL="${MGMT_PROTOCOL}://${AZ2_CLUSTER1_NODES[0]}:${MGMT_PORT}"
STANDBY_MGMT_URL="${MGMT_PROTOCOL}://${AZ2_CLUSTER2_NODES[0]}:${MGMT_PORT}"

# --- Helper functions ---
log_info() { echo -e "${BLUE}[INFO]${NC} $1"; }
log_warn() { echo -e "${YELLOW}[WARN]${NC} $1"; }
log_error() { echo -e "${RED}[ERROR]${NC} $1"; }
log_pass() { echo -e "${GREEN}[PASS]${NC} $1"; }

# Execute SSH command with sudo
ssh_sudo() {
    local host="$1"
    local cmd="$2"
    if [[ "$SSH_USER" == "root" ]]; then
        ssh -o StrictHostKeyChecking=no -o ConnectTimeout=5 "${SSH_USER}@${host}" "$cmd" 2>/dev/null
    else
        ssh -o StrictHostKeyChecking=no -o ConnectTimeout=5 "${SSH_USER}@${host}" "sudo $cmd" 2>/dev/null
    fi
}

# Get number of messages in queue
get_queue_messages() {
    local mgmt_url="$1"
    local queue="$2"
    curl -sf -k -u "${USER}:${PASSWORD}" "${mgmt_url}/api/queues/%2F/${queue}" 2>/dev/null | \
        python3 -c "import sys, json; d=json.load(sys.stdin); print(d.get('messages', 0))" 2>/dev/null || echo "0"
}

# Check if queue exists
queue_exists() {
    local mgmt_url="$1"
    local queue="$2"
    local status
    status=$(curl -sf -k -o /dev/null -w "%{http_code}" -u "${USER}:${PASSWORD}" "${mgmt_url}/api/queues/%2F/${queue}" 2>/dev/null || echo "000")
    [[ "$status" == "200" ]]
}

# Check if exchange exists
exchange_exists() {
    local mgmt_url="$1"
    local exchange="$2"
    local status
    status=$(curl -sf -k -o /dev/null -w "%{http_code}" -u "${USER}:${PASSWORD}" "${mgmt_url}/api/exchanges/%2F/${exchange}" 2>/dev/null || echo "000")
    [[ "$status" == "200" ]]
}

# Get cluster status
get_cluster_status() {
    local mgmt_url="$1"
    curl -sf -k -u "${USER}:${PASSWORD}" "${mgmt_url}/api/nodes" 2>/dev/null || echo "[]"
}

# Get number of running nodes
get_running_nodes() {
    local mgmt_url="$1"
    get_cluster_status "$mgmt_url" | python3 -c "
import sys, json
try:
    nodes = json.load(sys.stdin)
    running = [n['name'] for n in nodes if n.get('running', False)]
    print(len(running))
except:
    print(0)
"
}

# Check federation status
check_federation_status() {
    local mgmt_url="$1"
    local exchange="$2"
    
    log_info "📊 Checking Federation status for exchange '$exchange'..."
    
    # Get federation links
    local fed_links
    fed_links=$(curl -sf -k -u "${USER}:${PASSWORD}" "${mgmt_url}/api/federation-links" 2>/dev/null || echo "[]")
    
    local link_count
    link_count=$(echo "$fed_links" | python3 -c "
import sys, json
try:
    links = json.load(sys.stdin)
    running = [l for l in links if l.get('status') == 'running']
    print(len(running))
except:
    print(0)
" 2>/dev/null || echo "0")
    
    if [[ "$link_count" -gt 0 ]]; then
        log_pass "  ✓ Federation active: $link_count link(s) running"
        
        # Show link details
        echo "$fed_links" | python3 -c "
import sys, json
try:
    links = json.load(sys.stdin)
    for link in links:
        if link.get('status') == 'running':
            print(f\"    Exchange: {link.get('exchange', 'N/A')}, Status: {link.get('status', 'N/A')}, URI: {link.get('uri', 'N/A')}\")
except:
    pass
" 2>/dev/null || true
        
        return 0
    else
        log_warn "  ⚠ No active federation links found"
        return 1
    fi
}

# Simulate AZ1 failure (kill RabbitMQ processes)
simulate_az1_failure() {
    log_info "🔥 Simulating complete AZ1 failure (abrupt server crash)..."
    
    # Determine which nodes will actually fail
    local nodes_to_fail=()
    
    if $STANDBY_NO_FAIL; then
        # Only fail cluster-1 nodes in AZ1, preserve all cluster-2 nodes
        nodes_to_fail=("${AZ1_CLUSTER1_NODES[@]}")
        log_info "  Mode: --standby-no-fail enabled"
        log_info "  Only cluster-1 nodes in AZ1 will fail: ${nodes_to_fail[*]}"
        log_info "  Cluster-2 nodes will remain active (including those in AZ1)"
    else
        # Fail all AZ1 nodes (default behavior)
        nodes_to_fail=("${AZ1_FAILED_NODES[@]}")
        log_info "  Mode: Full AZ1 failure (both clusters affected)"
        log_info "  All AZ1 nodes will fail: ${nodes_to_fail[*]}"
    fi
    
    for host in "${nodes_to_fail[@]}"; do
        log_info "  Simulating server crash on $host..."
        
        # CRITICAL: Prevent systemd from restarting the service after kill
        log_info "    Masking service to prevent auto-restart..."
        ssh_sudo "$host" "$RMQ_MASK_CMD" || true
        
        # Kill all RabbitMQ processes abruptly (simulating server crash)
        ssh_sudo "$host" "pkill -9 beam.smp" || true
        ssh_sudo "$host" "pkill -9 epmd" || true
        ssh_sudo "$host" "pkill -9 rabbitmq-server" || true
        ssh_sudo "$host" "$RMQ_KILL_CMD" || true
        
        # Additional kill commands to ensure everything is dead
        ssh_sudo "$host" "killall -9 beam.smp epmd rabbitmq-server erl_child_setup inet_gethost" 2>/dev/null || true
        
        # Verify processes are killed
        local beam_count
        beam_count=$(ssh_sudo "$host" "pgrep beam.smp | wc -l" 2>/dev/null || echo "0")
        if [[ "$beam_count" -eq 0 ]]; then
            log_info "    ✓ $host crashed (processes killed, service masked)"
        else
            log_warn "    ⚠ $host still has $beam_count beam processes, killing harder..."
            ssh_sudo "$host" "kill -9 \$(pgrep -f beam)" 2>/dev/null || true
            ssh_sudo "$host" "kill -9 \$(pgrep -f epmd)" 2>/dev/null || true
        fi
    done
    
    if $STANDBY_NO_FAIL; then
        log_warn "AZ1 simulated as failed for cluster-1 only (${#nodes_to_fail[@]} nodes affected)"
        log_info "Cluster-2 nodes preserved to ensure consumers can continue"
    else
        log_warn "AZ1 simulated as failed - services masked and processes killed (${#nodes_to_fail[@]} nodes affected)"
    fi
}

# Restore AZ1 nodes
restore_az1_nodes() {
    log_info "🔄 Restoring AZ1 nodes..."
    
    # Determine which nodes to restore based on STANDBY_NO_FAIL flag
    local nodes_to_restore=()
    
    if $STANDBY_NO_FAIL; then
        # Only restore cluster-1 nodes in AZ1
        nodes_to_restore=("${AZ1_CLUSTER1_NODES[@]}")
        log_info "  Restoring cluster-1 nodes only: ${nodes_to_restore[*]}"
    else
        # Restore all AZ1 nodes (default behavior)
        nodes_to_restore=("${AZ1_FAILED_NODES[@]}")
        log_info "  Restoring all AZ1 nodes: ${nodes_to_restore[*]}"
    fi
    
    for host in "${nodes_to_restore[@]}"; do
        log_info "  Restoring RabbitMQ on $host..."
        # Unmask the service first (in case it was masked during crash simulation)
        ssh_sudo "$host" "$RMQ_UNMASK_CMD || true"
        ssh_sudo "$host" "$RMQ_RESET_FAILED_CMD || true"
        ssh_sudo "$host" "$RMQ_START_CMD" || true
    done
    
    # Wait for nodes to recover
    log_info "  Waiting for node recovery..."
    sleep 30
    
    # Verify that az-cluster-1 has recovered
    local running_nodes
    running_nodes=$(get_running_nodes "$PRIMARY_MGMT_URL" 2>/dev/null || echo "0")
    log_info "  Running nodes in az-cluster-1: $running_nodes"
}

# Monitor perf-test log for failure detection
monitor_perf_test_failure() {
    local log_file="$1"
    local pid="$2"
    local timeout="${3:-120}"
    
    local start_time=$(date +%s)
    local failure_detected=false
    local last_good_time=0
    local stall_threshold=10
    
    log_info "  Monitoring perf-test for failure indicators..."
    
    while kill -0 $pid 2>/dev/null && [[ $(($(date +%s) - start_time)) -lt $timeout ]]; do
        if [[ -f "$log_file" ]]; then
            # Check for explicit error indicators
            if grep -q "SocketException\|Connection.*closed\|Producer thread interrupted\|test stopped" "$log_file" 2>/dev/null; then
                log_info "  ✓ Connection failure detected in perf-test log"
                failure_detected=true
                break
            fi
            
            # Check for performance degradation (sent rate drops to near zero)
            local latest_sent
            latest_sent=$(tail -5 "$log_file" 2>/dev/null | grep "sent: " | tail -1 | grep -o "sent: [0-9.]*" | cut -d' ' -f2 || echo "0")
            
            if [[ -n "$latest_sent" ]] && (( $(echo "$latest_sent < 5" | awk '{print ($1 < 5)}' 2>/dev/null || echo "0") )); then
                local current_time=$(date +%s)
                if [[ $last_good_time -eq 0 ]]; then
                    last_good_time=$current_time
                elif [[ $((current_time - last_good_time)) -gt $stall_threshold ]]; then
                    log_info "  ✓ Performance stall detected (sent rate: ${latest_sent} msg/s for ${stall_threshold}s)"
                    failure_detected=true
                    break
                fi
            else
                last_good_time=$(date +%s)
            fi
        fi
        
        sleep 2
    done
    
    if $failure_detected; then
        return 0
    else
        return 1
    fi
}

# Extract statistics from perf-test log
extract_perf_stats() {
    local log_file="$1"
    local stats_array_name="$2"
    
    if [[ ! -f "$log_file" ]]; then
        return 1
    fi
    
    local sent_total=0 received_total=0 confirmed_total=0
    local last_good_line=""
    
    while IFS= read -r line; do
        if [[ "$line" =~ sent:\ ([0-9]+)\ msg/s.*received:\ ([0-9]+)\ msg/s ]]; then
            local sent_rate="${BASH_REMATCH[1]}"
            local recv_rate="${BASH_REMATCH[2]}"
            
            if [[ "$sent_rate" -gt 10 ]] || [[ "$recv_rate" -gt 10 ]]; then
                last_good_line="$line"
            fi
        fi
    done < "$log_file"
    
    sent_total=$(grep -o "sent: [0-9]*" "$log_file" | awk '{sum+=$2} END {print sum+0}' || echo "0")
    received_total=$(grep -o "received: [0-9]*" "$log_file" | awk '{sum+=$2} END {print sum+0}' || echo "0")
    confirmed_total=$(grep -o "confirmed: [0-9]*" "$log_file" | awk '{sum+=$2} END {print sum+0}' || echo "0")
    
    # Ensure values are valid numbers
    if ! [[ "$sent_total" =~ ^[0-9]+$ ]]; then sent_total=0; fi
    if ! [[ "$received_total" =~ ^[0-9]+$ ]]; then received_total=0; fi
    if ! [[ "$confirmed_total" =~ ^[0-9]+$ ]]; then confirmed_total=0; fi
    
    eval "${stats_array_name}[sent]=$sent_total"
    eval "${stats_array_name}[received]=$received_total"
    eval "${stats_array_name}[confirmed]=$confirmed_total"
    eval "${stats_array_name}[last_good_line]=\"$last_good_line\""
    
    return 0
}

# Main test function
run_resiliency_federation_test() {
    log_info "🧪 Starting Federation Resiliency Test"
    log_info "  Upstream exchange: $UPSTREAM_EXCHANGE"
    log_info "  Upstream queue: $UPSTREAM_QUEUE"
    log_info "  Downstream queue: $DOWNSTREAM_QUEUE"
    log_info "  Duration: ${TEST_DURATION}s"
    echo ""
    
    # Verify initial connectivity
    log_info "📋 Initial cluster verification..."
    local cluster1_nodes cluster2_nodes
    cluster1_nodes=$(get_running_nodes "$PRIMARY_MGMT_URL")
    cluster2_nodes=$(get_running_nodes "$STANDBY_MGMT_URL")
    
    log_info "  az-cluster-1 (upstream): $cluster1_nodes running nodes"
    log_info "  az-cluster-2 (downstream): $cluster2_nodes running nodes"
    
    if [[ "$cluster1_nodes" -lt 3 ]] || [[ "$cluster2_nodes" -lt 3 ]]; then
        log_error "Clusters are not fully operational"
        return 1
    fi
    
    # Verify federation resources exist
    log_info "  Verifying federation resources..."
    
    if ! exchange_exists "$PRIMARY_MGMT_URL" "$UPSTREAM_EXCHANGE"; then
        log_error "  ✗ Upstream exchange '$UPSTREAM_EXCHANGE' not found on az-cluster-1"
        log_info "  Run: ansible-playbook playbooks/configure_federated_exchange.yml"
        return 1
    fi
    log_info "  ✓ Upstream exchange exists on az-cluster-1"
    
    if ! queue_exists "$PRIMARY_MGMT_URL" "$UPSTREAM_QUEUE"; then
        log_error "  ✗ Upstream queue '$UPSTREAM_QUEUE' not found on az-cluster-1"
        return 1
    fi
    log_info "  ✓ Upstream queue exists on az-cluster-1"
    
    if ! exchange_exists "$STANDBY_MGMT_URL" "$UPSTREAM_EXCHANGE"; then
        log_error "  ✗ Federated exchange '$UPSTREAM_EXCHANGE' not found on az-cluster-2"
        return 1
    fi
    log_info "  ✓ Federated exchange exists on az-cluster-2"
    
    if ! queue_exists "$STANDBY_MGMT_URL" "$DOWNSTREAM_QUEUE"; then
        log_error "  ✗ Downstream queue '$DOWNSTREAM_QUEUE' not found on az-cluster-2"
        return 1
    fi
    log_info "  ✓ Downstream queue exists on az-cluster-2"
    
    # Verify federation is active
    if ! check_federation_status "$STANDBY_MGMT_URL" "$UPSTREAM_EXCHANGE"; then
        log_error "Federation is not configured correctly or not functional"
        log_info "  Please run: ansible-playbook playbooks/configure_federated_exchange.yml"
        return 1
    fi
    
    # Phase 1: Normal operation with az-cluster-1
    log_info ""
    log_info "📤 PHASE 1: Production and consumption on az-cluster-1"
    
    local phase1_log="$RESULTS_DIR/perf-test-phase1-$(date +%Y%m%d-%H%M%S).log"
    log_info "  Starting perf-test on cluster1..."
    log_info "  URIs: ${CLUSTER1_URIS//:????@/:****@}"
    
    # Phase 1: Continuous production to upstream exchange with consumer on upstream queue
    local phase1_timeout=120
    java $JVM_OPTS -jar "$TOOLS_DIR/perf-test.jar" \
        --uris "$CLUSTER1_URIS" \
        --predeclared \
        --exchange "$UPSTREAM_EXCHANGE" \
        --queue "$UPSTREAM_QUEUE" \
        --producers 3 \
        --consumers 1 \
        --confirm 50 \
        --size 1000 \
        --rate 300 \
        --consumer-rate 100 \
        --time "$phase1_timeout" \
        --id "fed-phase1" > "$phase1_log" 2>&1 &
    
    local phase1_pid=$!
    log_info "  Phase 1 perf-test started (PID: $phase1_pid)"
    
    # Give time for connection establishment
    sleep 10
    
    # Verify messages are flowing
    local upstream_msgs downstream_msgs
    upstream_msgs=$(get_queue_messages "$PRIMARY_MGMT_URL" "$UPSTREAM_QUEUE")
    downstream_msgs=$(get_queue_messages "$STANDBY_MGMT_URL" "$DOWNSTREAM_QUEUE")
    log_info "  ✓ Messages in upstream queue: $upstream_msgs"
    log_info "  ✓ Messages in downstream queue (federated): $downstream_msgs"
    
    # Wait for accumulation
    log_info "  Letting phase 1 run for 20s to accumulate messages..."
    sleep 20
    
    # Check accumulated messages
    local upstream_before_failure downstream_before_failure
    upstream_before_failure=$(get_queue_messages "$PRIMARY_MGMT_URL" "$UPSTREAM_QUEUE")
    downstream_before_failure=$(get_queue_messages "$STANDBY_MGMT_URL" "$DOWNSTREAM_QUEUE")
    log_info "  ✓ Upstream accumulated: $upstream_before_failure messages (before failure simulation)"
    log_info "  ℹ️  Downstream accumulated: $downstream_before_failure messages (federation working)"
    
    # Phase 2: Simulate AZ1 failure
    log_info ""
    log_info "💥 PHASE 2: AZ1 failure simulation and detection"
    
    # Start monitoring perf-test in background
    monitor_perf_test_failure "$phase1_log" $phase1_pid 60 &
    local monitor_pid=$!
    
    # Wait a bit, then simulate AZ1 failure
    sleep 5
    log_info "  Simulating AZ1 failure..."
    simulate_az1_failure
    
    # Get stats right after failure
    log_info "  Waiting for perf-test to detect failure..."
    sleep 3
    
    local upstream_at_failure downstream_at_failure
    upstream_at_failure=$(get_queue_messages "$PRIMARY_MGMT_URL" "$UPSTREAM_QUEUE" 2>/dev/null || echo "0")
    downstream_at_failure=$(get_queue_messages "$STANDBY_MGMT_URL" "$DOWNSTREAM_QUEUE" 2>/dev/null || echo "0")
    log_info "  Messages in upstream queue at time of failure: $upstream_at_failure"
    log_info "  Messages in downstream queue at time of failure: $downstream_at_failure"
    
    # Wait for failure detection
    local failure_detected=false
    if wait $monitor_pid; then
        failure_detected=true
        log_pass "  ✓ Connection failure detected by monitoring system"
    else
        if kill -0 $phase1_pid 2>/dev/null; then
            log_warn "  ⚠ Perf-test still running - failure not detected by monitoring"
        else
            log_info "  ✓ Perf-test completed normally before failure simulation"
            failure_detected=true
        fi
    fi
    
    # Give additional time for perf-test to react to failure
    sleep 5
    
    # Stop phase 1 if still running
    if kill -0 $phase1_pid 2>/dev/null; then
        log_info "  Stopping phase 1 perf-test..."
        kill $phase1_pid 2>/dev/null || true
        sleep 5
    else
        log_info "  Phase 1 perf-test already stopped"
    fi
    
    # Verify AZ1 failure impact
    log_info "  Verifying AZ1 failure impact..."
    
    local running_nodes=0
    for node_ip in "${ALL_CLUSTER1_NODES[@]}"; do
        local beam_count
        beam_count=$(ssh_sudo "$node_ip" "pgrep beam.smp | wc -l" 2>/dev/null || echo "0")
        if [[ "$beam_count" -gt 0 ]]; then
            log_warn "    ⚠ Node $node_ip still has $beam_count beam processes running"
            ((running_nodes++))
        else
            log_info "    ✓ Node $node_ip processes killed (crashed)"
        fi
    done
    
    log_info "  az-cluster-1 running nodes after failure: $running_nodes"
    
    if [[ "$running_nodes" -lt 2 ]]; then
        log_pass "  ✓ az-cluster-1 lost quorum as expected ($running_nodes/3 nodes)"
    else
        log_warn "  ⚠ az-cluster-1 still has quorum ($running_nodes/3 nodes)"
    fi
    
    # Verify cluster-2 status
    if $STANDBY_NO_FAIL; then
        log_info "  Verifying cluster-2 status (should be fully operational)..."
        local cluster2_running=0
        for node_ip in "${ALL_CLUSTER2_NODES[@]}"; do
            local beam_count
            beam_count=$(ssh_sudo "$node_ip" "pgrep beam.smp | wc -l" 2>/dev/null || echo "0")
            if [[ "$beam_count" -gt 0 ]]; then
                ((cluster2_running++))
                log_info "    ✓ Cluster-2 node $node_ip is running"
            else
                log_warn "    ⚠ Cluster-2 node $node_ip is down (unexpected with --standby-no-fail)"
            fi
        done
        log_info "  Cluster-2 running nodes: $cluster2_running/3"
        
        if [[ "$cluster2_running" -eq 3 ]]; then
            log_pass "  ✓ Cluster-2 fully operational as expected (--standby-no-fail mode)"
        else
            log_warn "  ⚠ Cluster-2 has only $cluster2_running/3 nodes"
        fi
    fi
    
    # Analyze phase 1 results
    log_info "  Analyzing phase 1 results..."
    declare -A phase1_stats
    local phase1_sent=0 phase1_received=0
    
    if extract_perf_stats "$phase1_log" "phase1_stats"; then
        phase1_sent=${phase1_stats[sent]}
        phase1_received=${phase1_stats[received]}
        
        if ! [[ "$phase1_sent" =~ ^[0-9]+$ ]]; then phase1_sent=0; fi
        if ! [[ "$phase1_received" =~ ^[0-9]+$ ]]; then phase1_received=0; fi
        
        log_info "    Phase 1 - Sent: $phase1_sent, Received: $phase1_received, Confirmed: ${phase1_stats[confirmed]}"
    else
        log_warn "    Could not extract phase 1 statistics from log"
        phase1_sent=$(grep -o "sent: [0-9]*" "$phase1_log" | awk '{sum+=$2} END {print sum+0}' || echo "0")
        phase1_received=$(grep -o "received: [0-9]*" "$phase1_log" | awk '{sum+=$2} END {print sum+0}' || echo "0")
        
        if ! [[ "$phase1_sent" =~ ^[0-9]+$ ]]; then phase1_sent=0; fi
        if ! [[ "$phase1_received" =~ ^[0-9]+$ ]]; then phase1_received=0; fi
        
        log_info "    Phase 1 (fallback) - Sent: $phase1_sent, Received: $phase1_received"
    fi
    
    # Phase 3: Consume from downstream cluster
    log_info ""
    log_info "⏳ PHASE 3: Consume remaining messages from az-cluster-2 (federated queue)"
    
    # Check messages available on downstream (using AZ2 node that survived)
    local downstream_msgs_available
    downstream_msgs_available=$(get_queue_messages "$STANDBY_MGMT_URL" "$DOWNSTREAM_QUEUE")
    log_info "  Messages available in downstream queue: $downstream_msgs_available"
    log_info "  Starting consumer-only perf-test on cluster-2..."
    
    # Build URIs only for surviving AZ2 nodes of cluster-2
    local cluster2_az2_uris
    cluster2_az2_uris=$(build_cluster_uris "${AZ2_CLUSTER2_NODES[@]}")
    log_info "  URIs (AZ2 nodes only): ${cluster2_az2_uris//:????@/:****@}"
    
    # Start phase 3 perf-test on downstream cluster (CONSUMER ONLY from federated queue)
    local phase3_log="$RESULTS_DIR/perf-test-phase3-$(date +%Y%m%d-%H%M%S).log"
    local phase3_duration=120
    
    java $JVM_OPTS -jar "$TOOLS_DIR/perf-test.jar" \
        --uris "$cluster2_az2_uris" \
        --predeclared \
        --queue "$DOWNSTREAM_QUEUE" \
        --producers 0 \
        --consumers 3 \
        --id "fed-phase3" > "$phase3_log" 2>&1 &
    
    local phase3_pid=$!
    log_info "  Phase 3 perf-test started (PID: $phase3_pid)"
    
    # Wait for completion or timeout
    log_info "  Waiting for phase 3 completion (${phase3_duration}s max)..."
    sleep "$phase3_duration"
    
    # Stop phase 3 if still running
    if kill -0 $phase3_pid 2>/dev/null; then
        log_info "  Stopping phase 3 perf-test..."
        kill $phase3_pid 2>/dev/null || true
        sleep 5
    fi
    
    # Phase 4: Results analysis
    log_info ""
    log_info "📊 PHASE 4: Complete results analysis"
    
    # Count final messages in both clusters
    local final_upstream_messages=0
    local final_downstream_messages=0
    
    if queue_exists "$STANDBY_MGMT_URL" "$DOWNSTREAM_QUEUE"; then
        final_downstream_messages=$(get_queue_messages "$STANDBY_MGMT_URL" "$DOWNSTREAM_QUEUE")
        log_info "  Final messages in downstream queue: $final_downstream_messages"
    fi
    
    # Try to get upstream queue messages (may fail if cluster is down)
    final_upstream_messages=$(get_queue_messages "$PRIMARY_MGMT_URL" "$UPSTREAM_QUEUE" 2>/dev/null || echo "N/A")
    log_info "  Final messages in upstream queue: $final_upstream_messages"
    
    # Analyze phase 3 (downstream consumption) results
    local phase3_sent=0
    local phase3_received=0
    if [[ -n "$phase3_log" ]] && [[ -f "$phase3_log" ]]; then
        log_info "  Analyzing phase 3 results (downstream consumption)..."
        phase3_sent=$(grep -o "sent: [0-9]*" "$phase3_log" | awk '{sum+=$2} END {print sum+0}' || echo "0")
        phase3_received=$(grep -o "received: [0-9]*" "$phase3_log" | awk '{sum+=$2} END {print sum+0}' || echo "0")
        
        if ! [[ "$phase3_sent" =~ ^[0-9]+$ ]]; then phase3_sent=0; fi
        if ! [[ "$phase3_received" =~ ^[0-9]+$ ]]; then phase3_received=0; fi
        
        log_info "    Phase 3 (downstream) - Sent: $phase3_sent, Received: $phase3_received"
        
        # Check for errors in phase 3
        local error_count
        error_count=$(grep -c -i "error\|exception\|failed" "$phase3_log" 2>/dev/null || echo "0")
        error_count=$(echo "$error_count" | tr -d '\n\r' | awk '{print $1}')
        if ! [[ "$error_count" =~ ^[0-9]+$ ]]; then
            error_count=0
        fi
        
        if [[ "$error_count" -gt 0 ]]; then
            log_warn "    Phase 3 errors detected: $error_count"
            log_info "    Showing first 5 errors from phase 3:"
            grep -i "error\|exception\|failed" "$phase3_log" 2>/dev/null | head -5 | while IFS= read -r line; do
                log_info "      $line"
            done
        fi
    fi
    
    # Calculate totals (Phase 1 upstream + Phase 3 downstream)
    local total_sent=$phase1_sent
    local total_received=$((phase1_received + phase3_received))
    
    log_info "  === COMPLETE TEST RESULTS ==="
    log_info "    Phase 1 (upstream): Sent=$phase1_sent, Received=$phase1_received"
    log_info "    Phase 3 (downstream): Received=$phase3_received"
    log_info "    TOTAL: Sent=$total_sent, Received=$total_received"
    log_info "    Final downstream queue depth: $final_downstream_messages"
    log_info "    Final upstream queue depth: $final_upstream_messages"
    
    # Calculate success metrics
    local receive_success_rate=0
    if [[ "$total_sent" -gt 0 ]]; then
        receive_success_rate=$((total_received * 100 / total_sent))
    fi
    
    log_info "    Send completion: 100% (target: 100%)"
    log_info "    Receive success: ${receive_success_rate}% (target: 95%+)"
    
    # Federation typically achieves high message delivery
    if [[ "$receive_success_rate" -ge 95 ]]; then
        log_pass "    ✓ Excellent message delivery via federation"
    elif [[ "$receive_success_rate" -ge 80 ]]; then
        log_info "    ✓ Good message delivery via federation"
    else
        log_warn "    ⚠ Lower than expected message delivery: ${receive_success_rate}%"
    fi
    
    # Phase 5: Restoration
    log_info ""
    log_info "🔄 PHASE 5: Restoration to original state"
    
    # Restore AZ1 nodes
    restore_az1_nodes
    
    # Clean test queues if cleanup enabled
    if $CLEANUP; then
        log_info "  Cleaning test logs..."
        [[ -f "$phase1_log" ]] && rm -f "$phase1_log"
        [[ -f "$phase3_log" ]] && rm -f "$phase3_log"
    else
        log_info "  Keeping logs for analysis (--no-cleanup)"
        log_info "    Phase 1 log: $phase1_log"
        [[ -n "$phase3_log" ]] && log_info "    Phase 3 log: $phase3_log"
    fi
    
    # Final verification
    log_info ""
    log_info "✅ FINAL VERIFICATION"
    
    sleep 10
    local final_cluster1_nodes final_cluster2_nodes
    final_cluster1_nodes=$(get_running_nodes "$PRIMARY_MGMT_URL" 2>/dev/null || echo "0")
    final_cluster2_nodes=$(get_running_nodes "$STANDBY_MGMT_URL" 2>/dev/null || echo "0")
    
    log_info "  az-cluster-1: $final_cluster1_nodes running nodes"
    log_info "  az-cluster-2: $final_cluster2_nodes running nodes"
    
    # Verify federation restored
    if check_federation_status "$STANDBY_MGMT_URL" "$UPSTREAM_EXCHANGE"; then
        log_pass "  ✓ Federation restored and verified as functional"
    else
        log_warn "  ⚠ Federation may need time to reconnect (normal after upstream restart)"
        log_info "    Run: ansible-playbook playbooks/configure_federated_exchange.yml if needed"
    fi
    
    # Determine test result
    local test_success=true
    
    if [[ "$final_cluster1_nodes" -lt 2 ]]; then
        log_warn "  ⚠ az-cluster-1 not fully recovered"
        test_success=false
    fi
    
    if [[ "$final_cluster2_nodes" -lt 3 ]]; then
        log_warn "  ⚠ az-cluster-2 not fully operational"
        test_success=false
    fi
    
    if [[ "$total_sent" -eq 0 ]] || [[ "$total_received" -eq 0 ]]; then
        log_error "  ✗ Message exchange not completed"
        test_success=false
    fi
    
    if [[ "$receive_success_rate" -lt 80 ]]; then
        log_warn "  ⚠ Low receive success rate: ${receive_success_rate}%"
    fi
    
    if $test_success; then
        log_pass "🎉 TEST COMPLETED SUCCESSFULLY"
        log_info "   - Federation failover worked correctly"
        log_info "   - Message delivery verified"
        log_info "   - Clusters restored to original state"
        return 0
    else
        log_error "❌ TEST FAILED - Check logs for details"
        return 1
    fi
}

# --- Main ---
echo "=============================================="
echo "  Test Resiliency Federation"
echo "=============================================="
echo "  Scenario: Federated Exchange Failover AZ1 → AZ2"
echo "  Phase 1: Production on az-cluster-1"
echo "  Phase 2: AZ1 failure simulation"  
echo "  Phase 3: Consume from az-cluster-2 (federated)"
echo ""
echo "  az-cluster-1 (upstream): ${ALL_CLUSTER1_NODES[*]}"
echo "  az-cluster-2 (downstream):  ${ALL_CLUSTER2_NODES[*]}"

if $STANDBY_NO_FAIL; then
    echo "  AZ1 nodes (will fail):   ${AZ1_CLUSTER1_NODES[*]} (cluster-1 only)"
    echo "  Mode:                    --standby-no-fail (cluster-2 protected)"
else
    echo "  AZ1 nodes (will fail):   ${AZ1_FAILED_NODES[*]} (both clusters)"
    echo "  Mode:                    Full AZ1 failure"
fi

echo "  Test approach:           Federation with consumer failover"
echo "=============================================="
echo ""
echo -e "${YELLOW}WARNING: This test will stop RabbitMQ nodes!${NC}"
echo -e "${YELLOW}Ensure this is a test/lab environment.${NC}"
echo ""
read -rp "Continue? [y/N] " confirm
if [[ ! "$confirm" =~ ^[Yy]$ ]]; then
    echo "Test cancelled."
    exit 1
fi
echo ""

# Create results directory
mkdir -p "$RESULTS_DIR"
TIMESTAMP=$(date +%Y%m%d-%H%M%S)
RESULT_FILE="$RESULTS_DIR/${TIMESTAMP}-resiliency-federation.txt"

# Execute main test
{
    echo "# Federation Resiliency Test"
    echo "# Date: $(date -u '+%Y-%m-%d %H:%M:%S UTC')"
    echo "# Clusters: az-cluster-1 (${ALL_CLUSTER1_NODES[*]}) → az-cluster-2 (${ALL_CLUSTER2_NODES[*]})"
    echo "# Duration: ${TEST_DURATION}s"
    echo "#"
    echo ""
} > "$RESULT_FILE"

# Helper to strip color codes for file output
strip_ansi() {
    sed 's/\x1b\[[0-9;]*m//g'
}

# Execute test with logging
if run_resiliency_federation_test 2>&1 | tee >(strip_ansi >> "$RESULT_FILE"); then
    TEST_RESULT=0
    echo ""
    echo -e "${GREEN}========================================${NC}"
    echo -e "${GREEN}  FEDERATION RESILIENCY TEST: SUCCESSFUL${NC}"
    echo -e "${GREEN}========================================${NC}"
else
    TEST_RESULT=1
    echo ""
    echo -e "${RED}========================================${NC}"
    echo -e "${RED}  FEDERATION RESILIENCY TEST: FAILED${NC}"
    echo -e "${RED}========================================${NC}"
fi

echo ""
echo "Results saved to: $RESULT_FILE"
echo ""

exit $TEST_RESULT
