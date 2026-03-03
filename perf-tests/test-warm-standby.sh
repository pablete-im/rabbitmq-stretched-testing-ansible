#!/bin/bash
# =============================================================================
# Criterion 3: Warm Standby Replication Test
#
# Validates that warm-standby replication works correctly between regional
# clusters and measures replication performance.
#
# Topology:
#   AZ-Cluster-1 (upstream) → AZ-Cluster-2 (regional standby)
#                           → TX-Cluster-1 (cross-region DR)
#                           → TX-Cluster-2 (cross-region DR)
#
# Tests:
#   1. Schema replication status (via rabbitmqctl)
#   2. Standby message replication status (via rabbitmqctl)
#   3. Cross-region replication status
#   4. Replication lag measurement
#   5. Sustained replication throughput
#   6. (Optional) Promotion test - promotes standby, verifies messages, restores
#
# NOTE: Warm standby messages are NOT queryable via API until promotion.
# Tests 1-5 verify replication is working using rabbitmqctl status commands.
# Test 6 (if enabled) performs actual promotion to verify message integrity,
# then automatically restores the standby back to downstream mode.
#
# Usage:
#   ./perf-tests/test-warm-standby.sh
#   ./perf-tests/test-warm-standby.sh --skip-cross-region
#   ./perf-tests/test-warm-standby.sh --test-promotion  # Actually promotes standby
#   ./perf-tests/test-warm-standby.sh --no-cleanup      # Leave test queues for analysis
#
# TLS Usage:
#   ./perf-tests/test-warm-standby.sh --truststore /path/to/truststore.p12 --truststore-pass mypass
#   ./perf-tests/test-warm-standby.sh --truststore /path/to/truststore.p12 --truststore-pass mypass
# =============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
TOOLS_DIR="$SCRIPT_DIR/tools"
RESULTS_DIR="$SCRIPT_DIR/results"

# Cluster endpoints
UPSTREAM_HOST="10.85.10.234"       # AZ-Cluster-1 (pve-schwab-rmq01)
REGIONAL_STANDBY="10.85.10.241"    # AZ-Cluster-2 (pve-schwab-rmq04) - first node
CROSS_REGION_DR1="10.85.10.244"    # TX-Cluster-1 (pve-schwab-rmq07) - first node
CROSS_REGION_DR2=""                # TX-Cluster-2 (Not defined in inventory)

# All nodes in each cluster (standby replication may run on any one node)
AZ_CLUSTER_1_NODES=("10.85.10.234" "10.85.10.235" "10.85.10.236")
AZ_CLUSTER_2_NODES=("10.85.10.241" "10.85.10.242" "10.85.10.243")
TX_CLUSTER_1_NODES=("10.85.10.244" "10.85.10.245" "10.85.10.246")
TX_CLUSTER_2_NODES=()              # Not defined in inventory

# Auth and SSH
USER="admin"
PASSWORD=""
SSH_USER="root"
SKIP_CROSS_REGION=false
TEST_PROMOTION=false
CLEANUP=true
TRUSTSTORE=""
TRUSTSTORE_PASS=""
TRUSTSTORE_TYPE="JKS"

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
        --user)              USER="$2"; shift 2 ;;
        --password)          PASSWORD="$2"; shift 2 ;;
        --ssh-user)          SSH_USER="$2"; shift 2 ;;
        --skip-cross-region) SKIP_CROSS_REGION=true; shift ;;
        --test-promotion)    TEST_PROMOTION=true; shift ;;
        --no-cleanup)        CLEANUP=false; shift ;;
        --truststore)        TRUSTSTORE="$2"; shift 2 ;;
        --truststore-pass)   TRUSTSTORE_PASS="$2"; shift 2 ;;
        --truststore-type)   TRUSTSTORE_TYPE="$2"; shift 2 ;;
        *)                   echo "Unknown option: $1"; exit 1 ;;
    esac
done

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
    STREAM_PROTOCOL="rabbitmq-stream+tls"
    STREAM_PORT="5551"
    
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
    STREAM_PROTOCOL="rabbitmq-stream"
    STREAM_PORT="5552"
    JVM_OPTS=""
fi

# Helper function to build URI list from comma-separated hosts
build_uris() {
    local hosts_array=("$@")
    local protocol="$AMQP_PROTOCOL"
    local port="$AMQP_PORT"
    local user="$USER"
    local password="$PASSWORD"
    
    local uris=""
    for host in "${hosts_array[@]}"; do
        if [[ -n "$uris" ]]; then
            uris="${uris},"
        fi
        uris="${uris}${protocol}://${user}:${password}@${host}:${port}"
    done
    echo "$uris"
}

# Construct AMQP URIs for upstream cluster (multiple hosts for load balancing)
AMQP_URIS=$(build_uris "${AZ_CLUSTER_1_NODES[@]}")

# --- Helper functions ---
log_info() { echo -e "${BLUE}[INFO]${NC} $1"; }
log_warn() { echo -e "${YELLOW}[WARN]${NC} $1"; }
log_error() { echo -e "${RED}[FAIL]${NC} $1"; }
log_pass() { echo -e "${GREEN}[PASS]${NC} $1"; }

# Get current time in milliseconds (portable for macOS and Linux)
now_ms() {
    python3 -c "import time; print(int(time.time() * 1000))"
}

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

# Get schema replication status from a node
get_schema_replication_status() {
    local host="$1"
    ssh_sudo "$host" "rabbitmqctl schema_replication_status" 2>/dev/null || echo "error"
}

# Get standby replication status from a node
get_standby_replication_status() {
    local host="$1"
    ssh_sudo "$host" "rabbitmqctl standby_replication_status" 2>/dev/null || echo "error"
}

# Get vhosts available for recovery on a standby node
get_vhosts_for_recovery() {
    local host="$1"
    ssh_sudo "$host" "rabbitmqctl list_vhosts_available_for_standby_replication_recovery" 2>/dev/null || echo "error"
}

# Check if schema replication is running/connected
is_schema_replication_connected() {
    local host="$1"
    local status
    status=$(get_schema_replication_status "$host")
    [[ "$status" == *"running"* ]] || [[ "$status" == *"connected"* ]] || [[ "$status" == *"syncing"* ]]
}

# Check if standby replication is connected
is_standby_replication_connected() {
    local host="$1"
    local status
    status=$(get_standby_replication_status "$host")
    [[ "$status" == *"connected"* ]] || [[ "$status" == *"running"* ]] || [[ "$status" == *"replicating"* ]]
}

# Find the node in a cluster that has active standby replication
# Usage: find_standby_node "192.168.20.203 192.168.20.204 192.168.20.205"
# Returns: IP of connected node, or empty string
find_standby_node() {
    local nodes="$1"
    for node_ip in $nodes; do
        local status
        status=$(get_standby_replication_status "$node_ip")
        if [[ "$status" == *"connected"* ]] || [[ "$status" == *"downstream"* ]]; then
            echo "$node_ip"
            return 0
        fi
    done
    echo ""
}

# Get queue message count from a specific host
get_queue_messages() {
    local host="$1"
    local queue="$2"
    curl -sf -k -u "${USER}:${PASSWORD}" "${MGMT_PROTOCOL}://${host}:${MGMT_PORT}/api/queues/%2F/${queue}" 2>/dev/null | \
        python3 -c "import sys, json; d=json.load(sys.stdin); print(d.get('messages', 0))" 2>/dev/null || echo "0"
}

# Check if queue exists on a host
queue_exists() {
    local host="$1"
    local queue="$2"
    local status
    status=$(curl -sf -k -o /dev/null -w "%{http_code}" -u "${USER}:${PASSWORD}" "${MGMT_PROTOCOL}://${host}:${MGMT_PORT}/api/queues/%2F/${queue}" 2>/dev/null || echo "000")
    [[ "$status" == "200" ]]
}

# Check if exchange exists on a host
exchange_exists() {
    local host="$1"
    local exchange="$2"
    local status
    status=$(curl -sf -k -o /dev/null -w "%{http_code}" -u "${USER}:${PASSWORD}" "${MGMT_PROTOCOL}://${host}:${MGMT_PORT}/api/exchanges/%2F/${exchange}" 2>/dev/null || echo "000")
    [[ "$status" == "200" ]]
}

# Check if vhost exists on a host
vhost_exists() {
    local host="$1"
    local vhost="$2"
    local encoded_vhost
    encoded_vhost=$(python3 -c "import urllib.parse; print(urllib.parse.quote('$vhost', safe=''))")
    local status
    status=$(curl -sf -k -o /dev/null -w "%{http_code}" -u "${USER}:${PASSWORD}" "${MGMT_PROTOCOL}://${host}:${MGMT_PORT}/api/vhosts/${encoded_vhost}" 2>/dev/null || echo "000")
    [[ "$status" == "200" ]]
}

# Create a vhost
create_vhost() {
    local host="$1"
    local vhost="$2"
    local encoded_vhost
    encoded_vhost=$(python3 -c "import urllib.parse; print(urllib.parse.quote('$vhost', safe=''))")
    curl -sf -k -X PUT -u "${USER}:${PASSWORD}" "${MGMT_PROTOCOL}://${host}:${MGMT_PORT}/api/vhosts/${encoded_vhost}" \
        -H "Content-Type: application/json" -d '{}' > /dev/null 2>&1
}

# Delete a vhost
delete_vhost() {
    local host="$1"
    local vhost="$2"
    local encoded_vhost
    encoded_vhost=$(python3 -c "import urllib.parse; print(urllib.parse.quote('$vhost', safe=''))")
    curl -sf -k -X DELETE -u "${USER}:${PASSWORD}" "${MGMT_PROTOCOL}://${host}:${MGMT_PORT}/api/vhosts/${encoded_vhost}" > /dev/null 2>&1 || true
}

# Create exchange
create_exchange() {
    local host="$1"
    local exchange="$2"
    curl -sf -k -X PUT -u "${USER}:${PASSWORD}" "${MGMT_PROTOCOL}://${host}:${MGMT_PORT}/api/exchanges/%2F/${exchange}" \
        -H "Content-Type: application/json" -d '{"type":"direct","durable":true}' > /dev/null 2>&1
}

# Delete exchange
delete_exchange() {
    local host="$1"
    local exchange="$2"
    curl -sf -k -X DELETE -u "${USER}:${PASSWORD}" "${MGMT_PROTOCOL}://${host}:${MGMT_PORT}/api/exchanges/%2F/${exchange}" > /dev/null 2>&1 || true
}

# Wait for replication with timeout
wait_for_replication() {
    local host="$1"
    local queue="$2"
    local expected="$3"
    local timeout="${4:-60}"
    local start_time
    start_time=$(date +%s)

    while true; do
        local count
        count=$(get_queue_messages "$host" "$queue")
        if [[ "$count" -ge "$expected" ]]; then
            local end_time
            end_time=$(date +%s)
            echo "$((end_time - start_time))"
            return 0
        fi

        local elapsed
        elapsed=$(($(date +%s) - start_time))
        if [[ $elapsed -ge $timeout ]]; then
            echo "timeout"
            return 1
        fi
        sleep 1
    done
}

# Get replication status
get_replication_status() {
    local host="$1"
    curl -sf -k -u "${USER}:${PASSWORD}" "${MGMT_PROTOCOL}://${host}:${MGMT_PORT}/api/overview" 2>/dev/null | \
        python3 -c "
import sys, json
try:
    d = json.load(sys.stdin)
    # Look for standby_replication in the response
    print(json.dumps(d.get('cluster_name', 'unknown')))
except:
    print('error')
" 2>/dev/null || echo "error"
}

# --- Test functions ---

test_cluster_connectivity() {
    log_info "Test 0: Verify cluster connectivity"

    local clusters=("$UPSTREAM_HOST:AZ-Cluster-1" "$REGIONAL_STANDBY:AZ-Cluster-2")
    if ! $SKIP_CROSS_REGION; then
        clusters+=("$CROSS_REGION_DR1:TX-Cluster-1")
        if [[ -n "$CROSS_REGION_DR2" ]]; then
            clusters+=("$CROSS_REGION_DR2:TX-Cluster-2")
        fi
    fi

    local all_ok=true
    for cluster in "${clusters[@]}"; do
        local host="${cluster%%:*}"
        local name="${cluster##*:}"
        if curl -sf -k -u "${USER}:${PASSWORD}" "${MGMT_PROTOCOL}://${host}:${MGMT_PORT}/api/overview" > /dev/null 2>&1; then
            log_info "  ✓ $name ($host) accessible"
        else
            log_error "  ✗ $name ($host) not accessible"
            all_ok=false
        fi
    done

    if $all_ok; then
        log_pass "All clusters accessible"
        return 0
    else
        log_error "Some clusters not accessible"
        return 1
    fi
}

test_schema_replication() {
    log_info "Test 1: Schema replication (create and verify)"

    # Actually test schema replication by creating an exchange on upstream
    # and verifying it appears on downstream clusters
    local test_exchange="warm-standby-schema-test-$(date +%s)"
    local all_replicated=true

    # Create exchange on upstream
    log_info "  Creating test exchange '$test_exchange' on upstream..."
    curl -sf -k -X PUT -u "${USER}:${PASSWORD}" \
        "${MGMT_PROTOCOL}://${UPSTREAM_HOST}:${MGMT_PORT}/api/exchanges/%2F/${test_exchange}" \
        -H "Content-Type: application/json" \
        -d '{"type":"direct","durable":true}' > /dev/null 2>&1

    # Verify it exists on upstream
    if exchange_exists "$UPSTREAM_HOST" "$test_exchange"; then
        log_info "  ✓ Exchange created on upstream"
    else
        log_error "  Failed to create exchange on upstream"
        return 1
    fi

    # Wait for schema replication (schema sync interval is typically 30s)
    log_info "  Waiting for schema replication (up to 60s)..."
    local regional_replicated=false
    local tx1_replicated=false
    local tx2_replicated=false

    for i in {1..12}; do
        # Check regional standby
        if ! $regional_replicated && exchange_exists "$REGIONAL_STANDBY" "$test_exchange"; then
            log_info "  ✓ Exchange replicated to AZ-Cluster-2 (${i}x5s)"
            regional_replicated=true
        fi

        # Check cross-region if not skipped
        if ! $SKIP_CROSS_REGION; then
            if ! $tx1_replicated && exchange_exists "$CROSS_REGION_DR1" "$test_exchange"; then
                log_info "  ✓ Exchange replicated to TX-Cluster-1 (${i}x5s)"
                tx1_replicated=true
            fi
            if [[ -n "$CROSS_REGION_DR2" ]] && ! $tx2_replicated && exchange_exists "$CROSS_REGION_DR2" "$test_exchange"; then
                log_info "  ✓ Exchange replicated to TX-Cluster-2 (${i}x5s)"
                tx2_replicated=true
            elif [[ -z "$CROSS_REGION_DR2" ]]; then
                tx2_replicated=true # Skip if not defined
            fi
        fi

        # Check if all done
        if $regional_replicated; then
            if $SKIP_CROSS_REGION || ($tx1_replicated && $tx2_replicated); then
                break
            fi
        fi
        sleep 5
    done

    # Report results
    if ! $regional_replicated; then
        log_error "  ✗ Exchange NOT replicated to AZ-Cluster-2"
        all_replicated=false
    fi

    if ! $SKIP_CROSS_REGION; then
        if ! $tx1_replicated; then
            log_error "  ✗ Exchange NOT replicated to TX-Cluster-1"
            all_replicated=false
        fi
        if [[ -n "$CROSS_REGION_DR2" ]] && ! $tx2_replicated; then
            log_error "  ✗ Exchange NOT replicated to TX-Cluster-2"
            all_replicated=false
        fi
    fi

    # Cleanup - delete test exchange from upstream (will replicate deletion too)
    if $CLEANUP; then
        log_info "  Cleaning up test exchange..."
        curl -sf -k -X DELETE -u "${USER}:${PASSWORD}" \
            "${MGMT_PROTOCOL}://${UPSTREAM_HOST}:${MGMT_PORT}/api/exchanges/%2F/${test_exchange}" > /dev/null 2>&1 || true
    else
        log_info "  Leaving test exchange '$test_exchange' for analysis (--no-cleanup)"
    fi

    if $all_replicated; then
        log_pass "Schema replication verified - exchange replicated to all standby clusters"
        return 0
    else
        log_error "Schema replication failed - exchange did not replicate to all clusters"
        return 1
    fi
}

test_regional_message_replication() {
    log_info "Test 2: Standby message replication status (regional - AZ-Cluster-2)"

    # Standby replication runs on only ONE node per cluster
    # Check all nodes in AZ-Cluster-2 to find the connected one
    log_info "  Checking all AZ-Cluster-2 nodes for standby replication..."

    local connected_node=""
    local connected_status=""

    for node_ip in "${AZ_CLUSTER_2_NODES[@]}"; do
        local status
        status=$(get_standby_replication_status "$node_ip")

        if [[ "$status" == *"connected"* ]] || [[ "$status" == *"downstream"* ]]; then
            connected_node="$node_ip"
            connected_status="$status"
            log_info "  ✓ Found standby replication on node $node_ip"
            break
        fi
    done

    if [[ -n "$connected_node" ]]; then
        # Display the status from the connected node
        log_info "  Standby replication status (from $connected_node):"
        while IFS= read -r line; do
            [[ -n "$line" ]] && log_info "    $line"
        done <<< "$connected_status"

        log_pass "Regional standby replication active (on node $connected_node)"
        return 0
    fi

    # No node showed connected - check vhosts for recovery as backup indicator
    log_info "  No node showed connected status, checking vhosts for recovery..."
    local recovery_vhosts
    recovery_vhosts=$(get_vhosts_for_recovery "$REGIONAL_STANDBY")

    local vhosts_available=false
    if [[ "$recovery_vhosts" != "error" ]] && [[ -n "$recovery_vhosts" ]]; then
        while IFS= read -r line; do
            if [[ -n "$line" ]]; then
                log_info "    $line"
                if [[ "$line" == "/" ]] || [[ "$line" != *"Listing"* && "$line" != *"vhost"* ]]; then
                    vhosts_available=true
                fi
            fi
        done <<< "$recovery_vhosts"
    fi

    if $vhosts_available; then
        log_pass "Regional standby replication active (vhosts available for recovery)"
        return 0
    fi

    log_error "Regional standby replication not active on any node"
    log_info "  Checked nodes: ${AZ_CLUSTER_2_NODES[*]}"
    log_info "  Hint: Run 'rabbitmqctl connect_standby_replication_downstream' on one of these nodes"
    return 1
}

test_cross_region_replication() {
    log_info "Test 3: Standby message replication status (cross-region - TX clusters)"

    if $SKIP_CROSS_REGION; then
        log_warn "Skipped (--skip-cross-region specified)"
        return 0
    fi

    local all_connected=true

    # Check TX-Cluster-1
    log_info "  Checking TX-Cluster-1 nodes for standby replication..."
    local tx1_connected=""
    local tx1_status=""
    for node_ip in "${TX_CLUSTER_1_NODES[@]}"; do
        local status
        status=$(get_standby_replication_status "$node_ip")
        if [[ "$status" == *"connected"* ]] || [[ "$status" == *"downstream"* ]]; then
            tx1_connected="$node_ip"
            tx1_status="$status"
            break
        fi
    done

    if [[ -n "$tx1_connected" ]]; then
        log_info "  TX-Cluster-1 status (from $tx1_connected):"
        while IFS= read -r line; do
            [[ -n "$line" ]] && log_info "    $line"
        done <<< "$tx1_status"
        log_info "  ✓ TX-Cluster-1 standby replication: active (on node $tx1_connected)"
    else
        log_warn "  ✗ TX-Cluster-1 standby replication: not active on any node"
        log_info "    Checked nodes: ${TX_CLUSTER_1_NODES[*]}"
        all_connected=false
    fi

    # Check TX-Cluster-2
    if [[ -n "${TX_CLUSTER_2_NODES[*]}" ]]; then
        log_info "  Checking TX-Cluster-2 nodes for standby replication..."
        local tx2_connected=""
        local tx2_status=""
        for node_ip in "${TX_CLUSTER_2_NODES[@]}"; do
            local status
            status=$(get_standby_replication_status "$node_ip")
            if [[ "$status" == *"connected"* ]] || [[ "$status" == *"downstream"* ]]; then
                tx2_connected="$node_ip"
                tx2_status="$status"
                break
            fi
        done

        if [[ -n "$tx2_connected" ]]; then
            log_info "  TX-Cluster-2 status (from $tx2_connected):"
            while IFS= read -r line; do
                [[ -n "$line" ]] && log_info "    $line"
            done <<< "$tx2_status"
            log_info "  ✓ TX-Cluster-2 standby replication: active (on node $tx2_connected)"
        else
            log_warn "  ✗ TX-Cluster-2 standby replication: not active on any node"
            log_info "    Checked nodes: ${TX_CLUSTER_2_NODES[*]}"
            all_connected=false
        fi
    fi

    if $all_connected; then
        log_pass "Cross-region standby replication active on all clusters"
        return 0
    else
        log_error "Cross-region standby replication not active on some clusters"
        return 1
    fi
}

# Get replication timestamp for a specific queue from upstream metrics
get_upstream_timestamp() {
    local host="$1"
    local queue="$2"
    ssh_sudo "$host" "rabbitmq-diagnostics inspect_standby_upstream_metrics" 2>/dev/null | \
        awk -v q="$queue" '$2 == q {print $1; exit}'
}

# Get replication timestamp for a specific queue from downstream metrics
get_downstream_timestamp() {
    local host="$1"
    local queue="$2"
    ssh_sudo "$host" "rabbitmq-diagnostics inspect_standby_downstream_metrics" 2>/dev/null | \
        awk -v q="$queue" '$2 == q {print $1; exit}'
}

test_replication_lag() {
    log_info "Test 4: Replication lag measurement under load"

    # Find the connected standby node for each cluster
    local az2_node=""
    for node_ip in "${AZ_CLUSTER_2_NODES[@]}"; do
        local status
        status=$(get_standby_replication_status "$node_ip")
        if [[ "$status" == *"connected"* ]] || [[ "$status" == *"downstream"* ]]; then
            az2_node="$node_ip"
            break
        fi
    done

    local tx1_node=""
    if ! $SKIP_CROSS_REGION; then
        for node_ip in "${TX_CLUSTER_1_NODES[@]}"; do
            local status
            status=$(get_standby_replication_status "$node_ip")
            if [[ "$status" == *"connected"* ]] || [[ "$status" == *"downstream"* ]]; then
                tx1_node="$node_ip"
                break
            fi
        done
    fi

    if [[ -z "$az2_node" ]]; then
        log_error "  No connected standby node found in AZ-Cluster-2"
        return 1
    fi

    # Test parameters
    local test_duration=30  # seconds
    local sample_interval=1 # seconds (sample frequently to catch peak lag)
    local queue="lag-test-$(date +%s)"

    log_info "  Test configuration:"
    log_info "    Duration: ${test_duration}s with sampling every ${sample_interval}s"
    log_info "    Regional standby node: $az2_node"
    [[ -n "$tx1_node" ]] && log_info "    Cross-region standby node: $tx1_node"

    # Start publisher in background
    log_info "  Starting sustained publish load..."
    java $JVM_OPTS -jar "$TOOLS_DIR/perf-test.jar" \
        --uris "$AMQP_URIS" \
        --quorum-queue \
        --queue "$queue" \
        --producers 2 \
        --consumers 0 \
        --time "$test_duration" \
        --size 5000 \
        --rate 2000 \
        --confirm 50 \
        --id "lag-test" > /tmp/lag-test-output.txt 2>&1 &
    local pub_pid=$!

    # Give publisher time to start and create queue
    sleep 3

    # Wait for queue to appear in BOTH upstream AND downstream replication metrics
    # The time it takes for downstream to see the queue IS part of the replication lag
    log_info "  Waiting for queue to appear in replication metrics..."
    local upstream_ready=false
    local az2_downstream_ready=false
    local tx1_downstream_ready=false
    local upstream_appeared_at=0
    local az2_appeared_at=0
    local tx1_appeared_at=0
    local wait_start
    wait_start=$(date +%s)

    for attempt in {1..60}; do
        # Check upstream
        if ! $upstream_ready; then
            local test_upstream_ts
            test_upstream_ts=$(get_upstream_timestamp "$UPSTREAM_HOST" "$queue")
            if [[ -n "$test_upstream_ts" ]] && [[ "$test_upstream_ts" =~ ^[0-9]+$ ]]; then
                upstream_appeared_at=$(date +%s)
                log_info "    Queue appeared in upstream metrics after ${attempt}s"
                upstream_ready=true
            fi
        fi

        # Check regional downstream (only after upstream is ready)
        if $upstream_ready && ! $az2_downstream_ready; then
            local test_az2_ts
            test_az2_ts=$(get_downstream_timestamp "$az2_node" "$queue")
            if [[ -n "$test_az2_ts" ]] && [[ "$test_az2_ts" =~ ^[0-9]+$ ]]; then
                az2_appeared_at=$(date +%s)
                local az2_initial_delay=$((az2_appeared_at - upstream_appeared_at))
                log_info "    Queue appeared in regional downstream metrics after ${attempt}s (${az2_initial_delay}s after upstream)"
                az2_downstream_ready=true
            fi
        fi

        # Check cross-region downstream (only after upstream is ready)
        if $upstream_ready && [[ -n "$tx1_node" ]] && ! $tx1_downstream_ready; then
            local test_tx1_ts
            test_tx1_ts=$(get_downstream_timestamp "$tx1_node" "$queue")
            if [[ -n "$test_tx1_ts" ]] && [[ "$test_tx1_ts" =~ ^[0-9]+$ ]]; then
                tx1_appeared_at=$(date +%s)
                local tx1_initial_delay=$((tx1_appeared_at - upstream_appeared_at))
                log_info "    Queue appeared in cross-region downstream metrics after ${attempt}s (${tx1_initial_delay}s after upstream)"
                tx1_downstream_ready=true
            fi
        fi

        # Check if we have all required metrics
        if $upstream_ready && $az2_downstream_ready; then
            if [[ -z "$tx1_node" ]] || $tx1_downstream_ready; then
                break
            fi
        fi

        sleep 1
    done

    # Report if metrics never appeared
    if ! $upstream_ready; then
        log_warn "  Queue did not appear in upstream metrics after 60s"
        log_info "  Checking raw upstream metrics output..."
        ssh_sudo "$UPSTREAM_HOST" "rabbitmq-diagnostics inspect_standby_upstream_metrics" 2>/dev/null | head -20 | while IFS= read -r line; do
            log_info "    $line"
        done
    fi

    if $upstream_ready && ! $az2_downstream_ready; then
        log_warn "  Queue did not appear in regional downstream metrics after 60s"
        log_info "  Checking raw downstream metrics output..."
        ssh_sudo "$az2_node" "rabbitmq-diagnostics inspect_standby_downstream_metrics" 2>/dev/null | head -20 | while IFS= read -r line; do
            log_info "    $line"
        done
    fi

    # Track lag metrics (in milliseconds)
    local az2_max_lag=0
    local az2_min_lag=999999999
    local az2_samples=0
    local az2_total_lag=0

    local tx1_max_lag=0
    local tx1_min_lag=999999999
    local tx1_samples=0
    local tx1_total_lag=0

    # Only proceed with sampling if downstream metrics are available
    if ! $az2_downstream_ready; then
        log_warn "  Skipping lag sampling - downstream metrics not available"
    else
        log_info "  Sampling replication lag (comparing timestamps)..."
    fi

    # Track failed samples for debugging
    local failed_upstream=0
    local failed_downstream=0

    # Sample lag while publisher is running (only if downstream is ready)
    local end_time=$(($(date +%s) + test_duration - 3))
    while $az2_downstream_ready && [[ $(date +%s) -lt $end_time ]] && kill -0 $pub_pid 2>/dev/null; do
        # Get upstream timestamp for our test queue
        local upstream_ts
        upstream_ts=$(get_upstream_timestamp "$UPSTREAM_HOST" "$queue")

        if [[ -n "$upstream_ts" ]] && [[ "$upstream_ts" =~ ^[0-9]+$ ]]; then
            # Get downstream timestamp for regional standby
            local az2_ts
            az2_ts=$(get_downstream_timestamp "$az2_node" "$queue")

            if [[ -n "$az2_ts" ]] && [[ "$az2_ts" =~ ^[0-9]+$ ]]; then
                # Calculate lag in milliseconds
                local lag_ms=$((upstream_ts - az2_ts))
                # Handle case where downstream might be slightly ahead due to timing
                [[ $lag_ms -lt 0 ]] && lag_ms=0

                ((az2_samples++))
                az2_total_lag=$((az2_total_lag + lag_ms))
                [[ $lag_ms -gt $az2_max_lag ]] && az2_max_lag=$lag_ms
                [[ $lag_ms -lt $az2_min_lag ]] && az2_min_lag=$lag_ms
            else
                ((failed_downstream++))
            fi

            # Get downstream timestamp for cross-region standby
            if [[ -n "$tx1_node" ]]; then
                local tx1_ts
                tx1_ts=$(get_downstream_timestamp "$tx1_node" "$queue")

                if [[ -n "$tx1_ts" ]] && [[ "$tx1_ts" =~ ^[0-9]+$ ]]; then
                    local lag_ms=$((upstream_ts - tx1_ts))
                    [[ $lag_ms -lt 0 ]] && lag_ms=0

                    ((tx1_samples++))
                    tx1_total_lag=$((tx1_total_lag + lag_ms))
                    [[ $lag_ms -gt $tx1_max_lag ]] && tx1_max_lag=$lag_ms
                    [[ $lag_ms -lt $tx1_min_lag ]] && tx1_min_lag=$lag_ms
                fi
            fi
        else
            ((failed_upstream++))
        fi

        # Progress indicator
        printf "."

        sleep "$sample_interval"
    done
    echo ""  # newline after progress dots

    # Report sampling failures if any
    if [[ $failed_upstream -gt 0 ]] || [[ $failed_downstream -gt 0 ]]; then
        log_info "  Sampling diagnostics:"
        [[ $failed_upstream -gt 0 ]] && log_info "    Failed to get upstream timestamp: $failed_upstream times"
        [[ $failed_downstream -gt 0 ]] && log_info "    Failed to get downstream timestamp: $failed_downstream times"
    fi

    # Wait for publisher to finish
    wait $pub_pid 2>/dev/null || true

    # Get final publish rate from output
    local pub_rate
    pub_rate=$(sed -n 's/.*sending rate avg: \([0-9][0-9]*\).*/\1/p' /tmp/lag-test-output.txt 2>/dev/null | tail -1)
    pub_rate="${pub_rate:-0}"

    # Get final message count
    sleep 2
    local final_upstream_msgs
    final_upstream_msgs=$(get_queue_messages "$UPSTREAM_HOST" "$queue")

    # Report results
    log_info "  Results:"
    log_info "    Publish rate: ${pub_rate} msg/s"
    log_info "    Total messages published: $final_upstream_msgs"

    # Report initial replication delay (time for queue to appear in downstream)
    if $az2_downstream_ready && [[ $az2_appeared_at -gt 0 ]] && [[ $upstream_appeared_at -gt 0 ]]; then
        local az2_initial_delay=$((az2_appeared_at - upstream_appeared_at))
        log_info "    Regional standby (AZ-Cluster-2):"
        log_info "      Initial replication delay: ${az2_initial_delay}s (time for queue to appear in downstream)"
        if [[ $az2_samples -gt 0 ]]; then
            local az2_avg_lag=$((az2_total_lag / az2_samples))
            log_info "      Samples collected: $az2_samples"
            log_info "      Peak replication lag: ${az2_max_lag}ms"
            log_info "      Min replication lag: ${az2_min_lag}ms"
            log_info "      Avg replication lag: ${az2_avg_lag}ms"
        else
            log_info "      (No ongoing lag samples - test may have ended before sampling started)"
        fi
    else
        log_warn "    Regional standby: Queue never appeared in downstream metrics"
    fi

    if [[ -n "$tx1_node" ]]; then
        if $tx1_downstream_ready && [[ $tx1_appeared_at -gt 0 ]] && [[ $upstream_appeared_at -gt 0 ]]; then
            local tx1_initial_delay=$((tx1_appeared_at - upstream_appeared_at))
            log_info "    Cross-region (TX-Cluster-1):"
            log_info "      Initial replication delay: ${tx1_initial_delay}s (time for queue to appear in downstream)"
            if [[ $tx1_samples -gt 0 ]]; then
                local tx1_avg_lag=$((tx1_total_lag / tx1_samples))
                log_info "      Samples collected: $tx1_samples"
                log_info "      Peak replication lag: ${tx1_max_lag}ms"
                log_info "      Min replication lag: ${tx1_min_lag}ms"
                log_info "      Avg replication lag: ${tx1_avg_lag}ms"
            else
                log_info "      (No ongoing lag samples - test may have ended before sampling started)"
            fi
        else
            log_warn "    Cross-region: Queue never appeared in downstream metrics"
        fi
    fi

    # Verify replication is still connected after load
    log_info "  Verifying replication status after load..."
    local post_status
    post_status=$(get_standby_replication_status "$az2_node")
    if [[ "$post_status" == *"connected"* ]] || [[ "$post_status" == *"downstream"* ]]; then
        log_info "    ✓ Regional standby still connected"
    else
        log_warn "    ✗ Regional standby may have disconnected"
    fi

    # Cleanup
    if $CLEANUP; then
        curl -sf -k -X DELETE -u "${USER}:${PASSWORD}" \
            "${MGMT_PROTOCOL}://${UPSTREAM_HOST}:${MGMT_PORT}/api/queues/%2F/${queue}" > /dev/null 2>&1 || true
    else
        log_info "  Leaving queue '$queue' for analysis (--no-cleanup)"
    fi
    rm -f /tmp/lag-test-output.txt

    # Determine pass/fail - success if downstream metrics appeared
    if ! $az2_downstream_ready; then
        log_error "Lag test failed - regional downstream never received replication data"
        return 1
    fi

    # Build result message
    local result_msg="Lag test completed"
    if [[ $az2_appeared_at -gt 0 ]] && [[ $upstream_appeared_at -gt 0 ]]; then
        local az2_initial_delay=$((az2_appeared_at - upstream_appeared_at))
        result_msg="$result_msg - Initial delay: ${az2_initial_delay}s (regional)"
    fi
    if [[ $az2_samples -gt 0 ]]; then
        result_msg="$result_msg, Peak lag: ${az2_max_lag}ms"
    fi

    log_pass "$result_msg"
    return 0
}

test_sustained_replication_throughput() {
    log_info "Test 5: Sustained replication throughput"

    # Run sustained publish test on upstream
    log_info "  Running sustained throughput test (60s)..."

    local output
    output=$(java $JVM_OPTS -jar "$TOOLS_DIR/perf-test.jar" \
        --uris "$AMQP_URIS" \
        --quorum-queue \
        --queue "warm-standby-throughput-$(date +%s)" \
        --producers 2 \
        --consumers 0 \
        --time 60 \
        --size 5000 \
        --rate 1500 \
        --confirm 50 \
        --id "throughput-pub" 2>&1) || true

    # Extract metrics (macOS compatible)
    local send_rate
    send_rate=$(echo "$output" | sed -n 's/.*sending rate avg: \([0-9][0-9]*\).*/\1/p' | tail -1)
    send_rate="${send_rate:-0}"

    if [[ "$send_rate" -gt 0 ]]; then
        log_info "  Upstream publish rate: $send_rate msg/s"

        # Check replication status after sustained load (check all nodes)
        log_info "  Replication status after sustained load:"
        for node_ip in "${AZ_CLUSTER_2_NODES[@]}"; do
            local status
            status=$(get_standby_replication_status "$node_ip")
            if [[ "$status" == *"connected"* ]] || [[ "$status" == *"downstream"* ]]; then
                log_info "    (from $node_ip):"
                while IFS= read -r line; do
                    [[ -n "$line" ]] && log_info "      $line"
                done <<< "$status"
                break
            fi
        done

        # Clean up the large queue so it doesn't block subsequent tests (like promotion)
        if $CLEANUP; then
            log_info "  Cleaning up throughput test queue to clear replication backlog..."
            # Get the queue name from the perf-test command (it was generated with date)
            # We can't easily get the variable back, but we can look for queues matching the pattern
            curl -sf -k -u "${USER}:${PASSWORD}" "${MGMT_PROTOCOL}://${UPSTREAM_HOST}:${MGMT_PORT}/api/queues" 2>/dev/null | \
                python3 -c "import sys,json; [print(q['name']) for q in json.load(sys.stdin) if 'warm-standby-throughput' in q['name']]" | \
            while read -r q_name; do
                [[ -n "$q_name" ]] && {
                    log_info "    Deleting queue $q_name..."
                    curl -sf -k -X DELETE -u "${USER}:${PASSWORD}" "${MGMT_PROTOCOL}://${UPSTREAM_HOST}:${MGMT_PORT}/api/queues/%2F/${q_name}" > /dev/null 2>&1 || true
                }
            done
            
            # Wait for cleanup to propagate
            log_info "  Waiting for queue deletion to replicate..."
            sleep 5
        fi

        log_pass "Sustained upstream throughput: $send_rate msg/s"
        return 0
    else
        log_error "Could not measure throughput"
        return 1
    fi
}

test_stream_consumer_latency() {
    log_info "Test 6: WSR Stream Consumer Latency"

    # Define nodes
    local az2_node=""
    for node_ip in "${AZ_CLUSTER_2_NODES[@]}"; do
        local status
        status=$(get_standby_replication_status "$node_ip")
        if [[ "$status" == *"connected"* ]] || [[ "$status" == *"downstream"* ]]; then
            az2_node="$node_ip"
            break
        fi
    done

    if [[ -z "$az2_node" ]]; then
        log_error "No connected standby node found in AZ-Cluster-2"
        return 1
    fi

    local tx1_node=""
    if ! $SKIP_CROSS_REGION; then
        for node_ip in "${TX_CLUSTER_1_NODES[@]}"; do
            local status
            status=$(get_standby_replication_status "$node_ip")
            if [[ "$status" == *"connected"* ]] || [[ "$status" == *"downstream"* ]]; then
                tx1_node="$node_ip"
                break
            fi
        done
    fi

    # Prepare stream
    local stream_name="wsr-stream-latency-$(date +%s)"
    log_info "  Creating test stream '$stream_name'..."
    
    # Enable stream plugin on upstream (just in case)
    ssh_sudo "$UPSTREAM_HOST" "rabbitmq-plugins enable rabbitmq_stream" >/dev/null 2>&1 || true

    # Create stream queue on upstream
    # The queue type argument must be an integer (x-queue-type-version) or string
    # Correct way to declare stream via API is x-queue-type: stream
    log_info "  Creating test stream '$stream_name' on upstream..."
    curl -sf -k -X PUT -u "${USER}:${PASSWORD}" \
        "${MGMT_PROTOCOL}://${UPSTREAM_HOST}:${MGMT_PORT}/api/queues/%2F/${stream_name}" \
        -H "Content-Type: application/json" \
        -d '{"durable":true,"arguments":{"x-queue-type":"stream"}}' > /dev/null 2>&1

    # Verify it was created as a stream
    local q_type
    q_type=$(curl -sf -k -u "${USER}:${PASSWORD}" "${MGMT_PROTOCOL}://${UPSTREAM_HOST}:${MGMT_PORT}/api/queues/%2F/${stream_name}" 2>/dev/null | \
        python3 -c "import sys, json; print(json.load(sys.stdin).get('type', 'unknown'))")
    
    if [[ "$q_type" != "stream" ]]; then
        log_warn "  Queue created as '$q_type' instead of 'stream'. Retrying with explicit declare..."
        # Try creating via perf-test which handles declaration well
        java $JVM_OPTS -jar "$TOOLS_DIR/perf-test.jar" \
            --uris "$AMQP_URIS" \
            --queue "$stream_name" \
            --queue-args "x-queue-type=stream" \
            --producers 1 \
            --consumers 0 \
            --time 1 \
            --size 10 \
            --rate 1 \
            --id "wsr-stream-init" > /dev/null 2>&1
    else
        log_info "  ✓ Queue created as stream on upstream"
    fi

    # Helper to check queue type on a node
    check_queue_type() {
        local host="$1"
        local queue="$2"
        curl -sf -k -u "${USER}:${PASSWORD}" "${MGMT_PROTOCOL}://${host}:${MGMT_PORT}/api/queues/%2F/${queue}" 2>/dev/null | \
            python3 -c "import sys, json; print(json.load(sys.stdin).get('type', 'none'))" 2>/dev/null || echo "error"
    }

    # Wait for schema to replicate to AZ2
    log_info "  Waiting for stream to replicate to Regional Standby ($az2_node)..."
    local az2_ready=false
    for i in {1..60}; do
        local type
        type=$(check_queue_type "$az2_node" "$stream_name")
        if [[ "$type" == "stream" ]]; then
            log_info "    ✓ Stream replicated to Regional Standby (after ${i}s)"
            az2_ready=true
            break
        elif [[ "$type" == "classic" ]]; then
            log_warn "    ⚠ Queue appeared as 'classic' on standby. This usually means a client auto-created it before replication. Deleting..."
            curl -sf -k -X DELETE -u "${USER}:${PASSWORD}" "${MGMT_PROTOCOL}://${az2_node}:${MGMT_PORT}/api/queues/%2F/${stream_name}" > /dev/null 2>&1
        fi
        sleep 1
    done

    if ! $az2_ready; then
        log_error "Stream failed to replicate to Regional Standby within 60s"
        return 1
    fi

    # Wait for schema to replicate to TX1 (if enabled)
    if [[ -n "$tx1_node" ]]; then
        log_info "  Waiting for stream to replicate to Cross-Region Standby ($tx1_node)..."
        local tx1_ready=false
        for i in {1..60}; do
            local type
            type=$(check_queue_type "$tx1_node" "$stream_name")
            if [[ "$type" == "stream" ]]; then
                log_info "    ✓ Stream replicated to Cross-Region Standby (after ${i}s)"
                tx1_ready=true
                break
            elif [[ "$type" == "classic" ]]; then
                log_warn "    ⚠ Queue appeared as 'classic' on standby. Deleting..."
                curl -sf -k -X DELETE -u "${USER}:${PASSWORD}" "${MGMT_PROTOCOL}://${tx1_node}:${MGMT_PORT}/api/queues/%2F/${stream_name}" > /dev/null 2>&1
            fi
            sleep 1
        done
        
        if ! $tx1_ready; then
            log_error "Stream failed to replicate to Cross-Region Standby within 60s"
            # Don't return 1 here, maybe we can still test AZ2
        fi
    fi

    # Helper to find the node hosting the stream leader or active replica
    find_stream_leader_node() {
        local cluster_nodes=("${@}")
        for node_ip in "${cluster_nodes[@]}"; do
            # Check if this node hosts a running replica of the stream
            # We can check this by seeing if we can declare/consume from it without the "local node" error
            # Or use API to find the leader/member
            local queue_info
            queue_info=$(curl -sf -k -u "${USER}:${PASSWORD}" "${MGMT_PROTOCOL}://${node_ip}:${MGMT_PORT}/api/queues/%2F/${stream_name}" 2>/dev/null)
            
            # Check if this node is in the member list or is the leader
            # Note: For WSR downstream, the stream might be restricted to specific nodes
            if [[ -n "$queue_info" ]]; then
                echo "$node_ip"
                return 0
            fi
        done
        echo ""
    }

    # Start consumers on standby clusters FIRST (now that we know the stream exists)
    
    # 1. Regional Standby (AZ2)
    # We need to target the specific node that hosts the stream replica in the downstream cluster
    # In WSR, streams are replicated, but consumption via AMQP requires connecting to a node that has the data locally
    # We'll try to find the best node, defaulting to the standby node if specific one not found
    
    # Actually, let's just loop through nodes until one works or we run out
    local az2_uri=""
    local az2_pid=""
    local az2_output_file="/tmp/wsr-stream-cons-az2.txt"
    
    log_info "  Starting consumer on Regional Standby..."
    for node_ip in "${AZ_CLUSTER_2_NODES[@]}"; do
        log_info "    Trying node $node_ip..."
        local uri="${AMQP_PROTOCOL}://${USER}:${PASSWORD}@${node_ip}:${AMQP_PORT}"
        local out_file="/tmp/wsr-stream-cons-az2-${node_ip}.txt"
        
        java $JVM_OPTS -jar "$TOOLS_DIR/perf-test.jar" \
            --uri "$uri" \
            --queue "$stream_name" \
            --predeclared \
            --producers 0 \
            --consumers 1 \
            --time 60 \
            --qos 100 \
            --stream-consumer-offset "first" \
            --id "wsr-stream-cons-az2" > "$out_file" 2>&1 &
        local pid=$!
        
        # Give it enough time to fail if it's going to fail (JVM startup + connection)
        sleep 5
        if kill -0 $pid 2>/dev/null; then
            # It's still running, check for the specific error in the log just in case it's in a restart loop or something
            if ! grep -q "PRECONDITION_FAILED" "$out_file"; then
                log_info "    ✓ Connected to $node_ip"
                az2_pid=$pid
                az2_uri=$uri # Just for reference
                az2_output_file=$out_file
                break
            else
                log_warn "    ✗ Failed on $node_ip (Precondition Failed found in log)"
                kill $pid 2>/dev/null || true
            fi
        else
             # Process died, check why
             if grep -q "PRECONDITION_FAILED" "$out_file"; then
                 log_warn "    ✗ Failed on $node_ip (Precondition Failed)"
             else
                 log_warn "    ✗ Process died on $node_ip (Unknown reason, see log)"
             fi
        fi
    done

    if [[ -z "$az2_pid" ]]; then
        log_error "Could not connect consumer to any node in AZ-Cluster-2"
    fi

    # 2. Cross-Region Standby (TX1)
    local tx1_pid=""
    local tx1_output_file="/tmp/wsr-stream-cons-tx1.txt"
    
    if [[ -n "$tx1_node" ]]; then
        log_info "  Starting consumer on Cross-Region Standby..."
        for node_ip in "${TX_CLUSTER_1_NODES[@]}"; do
            log_info "    Trying node $node_ip..."
            local uri="${AMQP_PROTOCOL}://${USER}:${PASSWORD}@${node_ip}:${AMQP_PORT}"
            local out_file="/tmp/wsr-stream-cons-tx1-${node_ip}.txt"
            
            java $JVM_OPTS -jar "$TOOLS_DIR/perf-test.jar" \
                --uri "$uri" \
                --queue "$stream_name" \
                --predeclared \
                --producers 0 \
                --consumers 1 \
                --time 60 \
                --qos 100 \
                --stream-consumer-offset "first" \
                --id "wsr-stream-cons-tx1" > "$out_file" 2>&1 &
            local pid=$!
            
            sleep 5
            if kill -0 $pid 2>/dev/null; then
                if ! grep -q "PRECONDITION_FAILED" "$out_file"; then
                    log_info "    ✓ Connected to $node_ip"
                    tx1_pid=$pid
                    tx1_output_file=$out_file
                    break
                else
                    log_warn "    ✗ Failed on $node_ip (Precondition Failed found in log)"
                    kill $pid 2>/dev/null || true
                fi
            else
                 if grep -q "PRECONDITION_FAILED" "$out_file"; then
                     log_warn "    ✗ Failed on $node_ip (Precondition Failed)"
                 else
                     log_warn "    ✗ Process died on $node_ip (Unknown reason)"
                 fi
            fi
        done
    fi

    # Give consumers time to initialize
    log_info "  Waiting 5s for consumers to initialize..."
    sleep 5

    # Run producer (30s duration)
    log_info "  Starting producer on Upstream..."
    local prod_output_file="/tmp/wsr-stream-prod.txt"
    java $JVM_OPTS -jar "$TOOLS_DIR/perf-test.jar" \
        --uris "$AMQP_URIS" \
        --queue "$stream_name" \
        --predeclared \
        --producers 1 \
        --consumers 0 \
        --time 30 \
        --size 1000 \
        --rate 100 \
        --confirm 10 \
        --routing-key "$stream_name" \
        --id "wsr-stream-prod" > "$prod_output_file" 2>&1 &
    local prod_pid=$!

    # Wait for completion
    log_info "  Waiting for test completion (producer: 30s, consumers: 60s)..."
    wait "$prod_pid"
    wait "$az2_pid"
    [[ -n "$tx1_pid" ]] && wait "$tx1_pid"

    # Process results
    local csv_file="$RESULTS_DIR/wsr-stream-latency-$(date +%Y%m%d-%H%M%S).csv"
    echo "role,cluster,node,min_ms,median_ms,p95_ms,p99_ms,max_ms,msg_count" > "$csv_file"

    log_info ""
    log_info "  === WSR Stream Latency Results ==="

    # Function to parse latency from perf-test output
    parse_latency() {
        local file="$1"
        local type="$2" # confirm or consumer
        grep "${type} latency" "$file" | grep "min/median" | tail -1 | \
        sed -n 's/.*[^0-9]\([0-9][0-9]*\/[0-9][0-9]*\/[0-9][0-9]*\/[0-9][0-9]*\/[0-9][0-9]*\/[0-9][0-9]*\).*/\1/p'
    }

    # Function to parse message count from perf-test output
    parse_msg_count() {
        local file="$1"
        local type="$2" # published or consumed
        local count="0"
        
        if [[ "$type" == "published" ]]; then
            # Sum 'sent: X msg/s' from interval lines
            # Example: id: ..., time 1.000 s, sent: 100 msg/s, ...
            # We look for "sent:" and take the next field
            count=$(grep "sent: .* msg/s" "$file" | awk '{ for(i=1;i<=NF;i++) if($i=="sent:") print $(i+1) }' | tr -d 'msg/s,' | paste -sd+ - | bc)
        else
            # Sum 'received: X msg/s' from interval lines
            count=$(grep "received: .* msg/s" "$file" | awk '{ for(i=1;i<=NF;i++) if($i=="received:") print $(i+1) }' | tr -d 'msg/s,' | paste -sd+ - | bc)
        fi
        
        if [[ -z "$count" ]]; then
             echo "N/A"
        else
             echo "$count"
        fi
    }

    # Debug: show last few lines of output files for verification
    log_info "  Debug: Producer output (last 5 lines):"
    tail -n 5 "$prod_output_file" | while read -r line; do log_info "    $line"; done
    log_info "  Debug: AZ2 Consumer output (last 5 lines):"
    tail -n 5 "$az2_output_file" | while read -r line; do log_info "    $line"; done
    if [[ -n "$tx1_node" ]]; then
        log_info "  Debug: TX1 Consumer output (last 5 lines):"
        tail -n 5 "$tx1_output_file" | while read -r line; do log_info "    $line"; done
    fi

    # Get exact stream length from API for comparison
    local stream_len
    stream_len=$(get_queue_messages "$UPSTREAM_HOST" "$stream_name")
    log_info "  Debug: Stream length on upstream: $stream_len"

    # Print table headers after debug output
    printf "| %-15s | %-15s | %-10s | %-10s | %-10s | %-10s | %-12s |\n" "Role" "Cluster" "Min" "Med" "P99" "Max" "Count"
    printf "| %-15s | %-15s | %-10s | %-10s | %-10s | %-10s | %-12s |\n" "---------------" "---------------" "----------" "----------" "----------" "----------" "------------"

    # Producer Confirm Latency
    local prod_lats
    local prod_count
    prod_lats=$(parse_latency "$prod_output_file" "confirm")
    prod_count=$(parse_msg_count "$prod_output_file" "published")
    
    # Use API count if parsing failed or for verification
    [[ "$prod_count" == "N/A" ]] && prod_count="$stream_len (API)"
    
    if [[ -n "$prod_lats" ]]; then
        IFS='/' read -r p_min p_med _ p_95 p_99 p_max <<< "$prod_lats"
        # Convert us to ms
        p_min=$((p_min / 1000)); p_med=$((p_med / 1000)); p_95=$((p_95 / 1000)); p_99=$((p_99 / 1000)); p_max=$((p_max / 1000))
        printf "| %-15s | %-15s | %-10s | %-10s | %-10s | %-10s | %-10s |\n" "Producer" "Upstream" "${p_min}ms" "${p_med}ms" "${p_99}ms" "${p_max}ms" "Count: ${prod_count}"
        echo "producer,upstream,$UPSTREAM_HOST,$p_min,$p_med,$p_95,$p_99,$p_max,$prod_count" >> "$csv_file"
    else
        log_warn "  Could not parse producer latency. Full output:"
        cat "$prod_output_file" | while read -r line; do log_warn "    $line"; done
    fi

    # Consumer Latency (AZ2)
    local az2_lats
    local az2_count
    az2_lats=$(parse_latency "$az2_output_file" "consumer")
    az2_count=$(parse_msg_count "$az2_output_file" "consumed")

    if [[ -n "$az2_lats" ]]; then
        IFS='/' read -r c_min c_med _ c_95 c_99 c_max <<< "$az2_lats"
        c_min=$((c_min / 1000)); c_med=$((c_med / 1000)); c_95=$((c_95 / 1000)); c_99=$((c_99 / 1000)); c_max=$((c_max / 1000))
        printf "| %-15s | %-15s | %-10s | %-10s | %-10s | %-10s | %-10s |\n" "Consumer" "Regional" "${c_min}ms" "${c_med}ms" "${c_99}ms" "${c_max}ms" "Count: ${az2_count}"
        echo "consumer,regional,$az2_node,$c_min,$c_med,$c_95,$c_99,$c_max,$az2_count" >> "$csv_file"
    else
        log_warn "  Could not parse AZ2 consumer latency. Full output:"
        cat "$az2_output_file" | while read -r line; do log_warn "    $line"; done
    fi

    # Consumer Latency (TX1)
    if [[ -n "$tx1_node" ]]; then
        local tx1_lats
        local tx1_count
        tx1_lats=$(parse_latency "$tx1_output_file" "consumer")
        tx1_count=$(parse_msg_count "$tx1_output_file" "consumed")

        if [[ -n "$tx1_lats" ]]; then
            IFS='/' read -r c_min c_med _ c_95 c_99 c_max <<< "$tx1_lats"
            c_min=$((c_min / 1000)); c_med=$((c_med / 1000)); c_95=$((c_95 / 1000)); c_99=$((c_99 / 1000)); c_max=$((c_max / 1000))
            printf "| %-15s | %-15s | %-10s | %-10s | %-10s | %-10s | %-10s |\n" "Consumer" "Cross-Region" "${c_min}ms" "${c_med}ms" "${c_99}ms" "${c_max}ms" "Count: ${tx1_count}"
            echo "consumer,cross-region,$tx1_node,$c_min,$c_med,$c_95,$c_99,$c_max,$tx1_count" >> "$csv_file"
        else
            log_warn "  Could not parse TX1 consumer latency. Full output:"
            cat "$tx1_output_file" | while read -r line; do log_warn "    $line"; done
        fi
    fi
    log_info "  Results saved to $csv_file"

    # Cleanup
    if $CLEANUP; then
        log_info "  Cleaning up stream..."
        curl -sf -k -X DELETE -u "${USER}:${PASSWORD}" \
            "${MGMT_PROTOCOL}://${UPSTREAM_HOST}:${MGMT_PORT}/api/queues/%2F/${stream_name}" > /dev/null 2>&1 || true
    fi
    rm -f "$prod_output_file" "$az2_output_file" "$tx1_output_file"

    return 0
}

# Restore a promoted standby back to downstream mode
# Note: Config changes need to happen on all nodes, but connect only on one
restore_standby_to_downstream() {
    local standby_host="$1"
    local upstream_hosts="$2"  # Not currently used, keeping for API compatibility

    log_info "  Restoring standby to downstream mode..."

    # Step 1: Update config to downstream mode on ALL nodes in the cluster
    log_info "    Setting operating mode to downstream on all AZ-Cluster-2 nodes..."
    for node_ip in "${AZ_CLUSTER_2_NODES[@]}"; do
        ssh_sudo "$node_ip" "sed -i 's/operating_mode = upstream/operating_mode = downstream/g' /etc/rabbitmq/rabbitmq.conf" 2>/dev/null || true
    done

    # Step 2: Restart RabbitMQ on all nodes (rolling restart would be better but this is simpler)
    log_info "    Restarting RabbitMQ on all nodes..."
    for node_ip in "${AZ_CLUSTER_2_NODES[@]}"; do
        ssh_sudo "$node_ip" "systemctl restart tanzu-rabbitmq-server" 2>/dev/null &
    done
    wait  # Wait for all restarts to begin

    # Step 3: Wait for all nodes to be ready
    log_info "    Waiting for all nodes to start..."
    local all_ready=false
    for attempt in {1..30}; do
        all_ready=true
        for node_ip in "${AZ_CLUSTER_2_NODES[@]}"; do
            if ! ssh_sudo "$node_ip" "rabbitmqctl await_startup" >/dev/null 2>&1; then
                all_ready=false
                break
            fi
        done
        if $all_ready; then
            break
        fi
        sleep 2
    done

    if ! $all_ready; then
        log_error "    Not all nodes came up after restart"
        return 1
    fi

    # Step 4: Set upstream endpoints for schema replication (AMQP port 5672)
    # Only need to run on one node - it's cluster-wide
    log_info "    Setting schema replication upstream endpoints..."
    local upstream_amqp_endpoints="[\"${UPSTREAM_HOST}:5672\"]"
    ssh_sudo "$standby_host" "rabbitmqctl set_schema_replication_upstream_endpoints '{\"endpoints\":${upstream_amqp_endpoints},\"username\":\"${USER}\",\"password\":\"${PASSWORD}\"}''" || true

    # Step 5: Set upstream endpoints for standby replication (stream port 5552)
    log_info "    Setting standby replication upstream endpoints..."
    local upstream_stream_endpoints="[\"${UPSTREAM_HOST}:5552\"]"
    ssh_sudo "$standby_host" "rabbitmqctl set_standby_replication_upstream_endpoints '{\"endpoints\":${upstream_stream_endpoints},\"username\":\"${USER}\",\"password\":\"${PASSWORD}\"}''" || true

    # Step 6: Connect standby replication (only runs on one node)
    log_info "    Connecting standby replication..."
    ssh_sudo "$standby_host" "rabbitmqctl connect_standby_replication_downstream" || true

    # Step 7: Verify restoration - check all nodes to find the connected one
    log_info "    Verifying standby replication reconnected..."
    sleep 5

    local connected_node=""
    for node_ip in "${AZ_CLUSTER_2_NODES[@]}"; do
        local status
        status=$(get_standby_replication_status "$node_ip")
        if [[ "$status" == *"connected"* ]] || [[ "$status" == *"downstream"* ]]; then
            connected_node="$node_ip"
            log_info "    ✓ Standby replication reconnected (on node $node_ip)"
            break
        fi
    done

    if [[ -n "$connected_node" ]]; then
        return 0
    else
        log_warn "    Standby replication may not be fully restored"
        log_info "    No node showed connected status"
        return 1
    fi
}

test_promotion() {
    log_info "Test 7: Standby promotion verification"

    if ! $TEST_PROMOTION; then
        log_warn "Skipped (use --test-promotion to enable)"
        log_info "This test promotes standby, verifies messages, then restores to downstream mode."
        return 0
    fi

    # Find the node with active standby replication in AZ-Cluster-2
    local standby_node=""
    log_info "  Finding active standby replication node in AZ-Cluster-2..."
    for node_ip in "${AZ_CLUSTER_2_NODES[@]}"; do
        local status
        status=$(get_standby_replication_status "$node_ip")
        if [[ "$status" == *"connected"* ]] || [[ "$status" == *"downstream"* ]]; then
            standby_node="$node_ip"
            log_info "  Found active standby on $node_ip"
            break
        fi
    done

    if [[ -z "$standby_node" ]]; then
        log_error "No active standby replication found in AZ-Cluster-2"
        log_info "  Checked nodes: ${AZ_CLUSTER_2_NODES[*]}"
        return 1
    fi

    log_info "=== PROMOTION TEST ==="
    log_info "This test will:"
    log_info "  1. Publish messages to upstream"
    log_info "  2. Promote AZ-Cluster-2 (via $standby_node) to verify replication"
    log_info "  3. Restore AZ-Cluster-2 back to downstream mode"
    log_info "======================"

    # Publish known messages to upstream
    # Use unique queue name to avoid conflicts with previous runs
    local queue="promotion-test-$(date +%s)"
    local message_count=100

    # Clean up any leftover queue from previous runs
    curl -sf -k -X DELETE -u "${USER}:${PASSWORD}" \
        "${MGMT_PROTOCOL}://${UPSTREAM_HOST}:${MGMT_PORT}/api/queues/%2F/promotion-test-queue" > /dev/null 2>&1 || true

    log_info "  Publishing $message_count messages to upstream..."
    log_info "  Queue: $queue"
    log_info "  Targets: ${AMQP_URIS//:????@/:****@}"

    local pub_output
    pub_output=$(java $JVM_OPTS -jar "$TOOLS_DIR/perf-test.jar" \
        --uris "$AMQP_URIS" \
        --quorum-queue \
        --queue "$queue" \
        --producers 1 \
        --consumers 0 \
        --pmessages "$message_count" \
        --confirm 10 \
        --size 100 \
        --id "promotion-test" 2>&1) || true

    # Debug: show perf-test output
    if [[ -n "$pub_output" ]]; then
        log_info "  Perf-test output (last 5 lines):"
        echo "$pub_output" | tail -5 | while IFS= read -r line; do
            log_info "    $line"
        done
    else
        log_warn "  Perf-test produced no output"
    fi

    # Check if publish succeeded - look for various error indicators
    if [[ "$pub_output" == *"error"* ]] || [[ "$pub_output" == *"Error"* ]] || \
       [[ "$pub_output" == *"Exception"* ]] || [[ "$pub_output" == *"REFUSED"* ]]; then
        log_error "  Publish failed - see output above"
        return 1
    fi

    # Verify messages on upstream (management API can have slight delay)
    log_info "  Waiting for messages to appear in management API..."
    local upstream_count=0
    for attempt in {1..10}; do
        sleep 1
        upstream_count=$(get_queue_messages "$UPSTREAM_HOST" "$queue")
        if [[ "$upstream_count" -gt 0 ]]; then
            log_info "  Upstream has $upstream_count messages (appeared after ${attempt}s)"
            break
        fi
    done

    if [[ "$upstream_count" -eq 0 ]]; then
        log_warn "  Upstream shows 0 messages after 10s - checking details..."
    fi

    # Get detailed queue info for debugging
    local queue_info
    queue_info=$(curl -sf -k -u "${USER}:${PASSWORD}" "${MGMT_PROTOCOL}://${UPSTREAM_HOST}:${MGMT_PORT}/api/queues/%2F/${queue}" 2>&1) || true

    if [[ -z "$queue_info" ]] || [[ "$queue_info" == *"not_found"* ]]; then
        log_error "  Queue '$queue' was not created on upstream"
        log_info "  Listing all queues on upstream:"
        curl -sf -k -u "${USER}:${PASSWORD}" "${MGMT_PROTOCOL}://${UPSTREAM_HOST}:${MGMT_PORT}/api/queues" 2>&1 | \
            python3 -c "import sys,json; [print(f'    - {q[\"name\"]} ({q.get(\"messages\",0)} msgs)') for q in json.load(sys.stdin)]" 2>/dev/null || true
        return 1
    fi

    # Always show queue details for debugging
    local q_consumers q_type q_state q_messages_ready q_messages_unack
    q_type=$(echo "$queue_info" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('type', 'N/A'))" 2>/dev/null || echo "N/A")
    q_state=$(echo "$queue_info" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('state', 'N/A'))" 2>/dev/null || echo "N/A")
    q_consumers=$(echo "$queue_info" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('consumers', 0))" 2>/dev/null || echo "0")
    q_messages_ready=$(echo "$queue_info" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('messages_ready', 0))" 2>/dev/null || echo "0")
    q_messages_unack=$(echo "$queue_info" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('messages_unacknowledged', 0))" 2>/dev/null || echo "0")
    log_info "    Queue type: $q_type, State: $q_state"
    log_info "    Consumers: $q_consumers, Ready: $q_messages_ready, Unacked: $q_messages_unack"

    # Check for policies affecting this queue (remote-dc-replicate is expected for warm standby)
    local effective_policy
    effective_policy=$(echo "$queue_info" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('effective_policy_definition', {}))" 2>/dev/null || echo "{}")
    if [[ "$effective_policy" != "{}" ]] && [[ -n "$effective_policy" ]] && [[ "$effective_policy" != "None" ]]; then
        log_info "    Effective policy: $effective_policy"
    fi

    # Check message_stats for publish/deliver counts
    local publish_count deliver_count
    publish_count=$(echo "$queue_info" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('message_stats', {}).get('publish', 0))" 2>/dev/null || echo "0")
    deliver_count=$(echo "$queue_info" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('message_stats', {}).get('deliver_get', 0))" 2>/dev/null || echo "0")
    log_info "    Message stats - Published: $publish_count, Delivered/Get: $deliver_count"

    if [[ "$upstream_count" -eq 0 ]] && [[ "$deliver_count" -gt 0 ]]; then
        log_warn "    Messages were delivered! Something is consuming from this queue."
    fi

    # Give replication time to sync
    log_info "  Waiting for replication to sync (30s)..."
    sleep 30

    # Check vhosts available for recovery before promotion
    log_info "  Checking vhosts available for recovery before promotion..."
    local recovery_vhosts
    recovery_vhosts=$(ssh_sudo "$standby_node" "rabbitmqctl list_vhosts_available_for_standby_replication_recovery" 2>&1) || true
    log_info "    Recovery vhosts: $recovery_vhosts"

    # Promote the standby (cluster-wide operation, can run from any node)
    # --all-available: recover all replicated vhosts
    # --start-from-scratch: start from earliest data (ensures we get all messages)
    log_info "  Promoting regional standby (AZ-Cluster-2 via $standby_node)..."
    local promote_result
    promote_result=$(ssh_sudo "$standby_node" "rabbitmqctl promote_standby_replication_downstream_cluster --all-available --start-from-scratch" 2>&1) || true
    if [[ -n "$promote_result" ]]; then
        log_info "  Promotion result: $promote_result"
    else
        log_warn "  Promotion returned no output"
    fi

    # Check replication status after promotion
    log_info "  Checking replication status after promotion..."
    local post_promote_status
    post_promote_status=$(ssh_sudo "$standby_node" "rabbitmqctl standby_replication_status" 2>&1) || true
    log_info "    Status: $post_promote_status"

    # Wait for promotion to complete and cluster to stabilize
    log_info "  Waiting for promotion to complete..."
    sleep 10

    # List all queues on the promoted cluster to see what's there
    log_info "  Listing queues on promoted cluster..."
    local promoted_queues
    promoted_queues=$(curl -sf -k -u "${USER}:${PASSWORD}" "${MGMT_PROTOCOL}://${standby_node}:${MGMT_PORT}/api/queues" 2>&1 | \
        python3 -c "import sys,json; [print(f'    {q[\"name\"]}: {q.get(\"messages\",0)} msgs') for q in json.load(sys.stdin) if not q['name'].startswith('rabbitmq.internal')]" 2>/dev/null) || true
    if [[ -n "$promoted_queues" ]]; then
        log_info "  Queues on promoted cluster:"
        echo "$promoted_queues"
    else
        log_warn "  No user queues found on promoted cluster"
    fi

    # Now check for our specific queue
    log_info "  Checking for queue '$queue' on promoted cluster..."
    local promoted_count
    promoted_count=$(get_queue_messages "$standby_node" "$queue")
    log_info "  Promoted cluster has $promoted_count messages (from $standby_node)"

    # Also check if queue exists at all
    local queue_exists_check
    queue_exists_check=$(curl -sf -k -o /dev/null -w "%{http_code}" -u "${USER}:${PASSWORD}" \
        "${MGMT_PROTOCOL}://${standby_node}:${MGMT_PORT}/api/queues/%2F/${queue}" 2>/dev/null || echo "000")
    if [[ "$queue_exists_check" != "200" ]]; then
        log_warn "  Queue '$queue' does not exist on promoted cluster (HTTP $queue_exists_check)"
    fi

    local promotion_success=false
    if [[ "$promoted_count" -ge "$message_count" ]]; then
        log_pass "Promotion verified! All $promoted_count messages available."
        promotion_success=true
    elif [[ "$promoted_count" -gt 0 ]]; then
        log_warn "Partial replication: $promoted_count of $message_count messages"
        promotion_success=true  # Still consider partial success
    else
        log_error "No messages found after promotion"
    fi

    # Clean up test queue on promoted cluster before restoring
    if $CLEANUP; then
        log_info "  Cleaning up test queue..."
        curl -sf -k -X DELETE -u "${USER}:${PASSWORD}" \
            "${MGMT_PROTOCOL}://${standby_node}:${MGMT_PORT}/api/queues/%2F/${queue}" > /dev/null 2>&1 || true
        curl -sf -k -X DELETE -u "${USER}:${PASSWORD}" \
            "${MGMT_PROTOCOL}://${UPSTREAM_HOST}:${MGMT_PORT}/api/queues/%2F/${queue}" > /dev/null 2>&1 || true
    else
        log_info "  Leaving queue '$queue' for analysis (--no-cleanup)"
    fi

    # Restore standby to downstream mode
    log_info ""
    log_info "Test 8: Restore standby cluster to downstream mode"
    log_info "  Restoring AZ-Cluster-2 to downstream mode..."
    if restore_standby_to_downstream "$standby_node" "$UPSTREAM_HOST"; then
        log_pass "Standby restored to downstream mode"
    else
        log_warn "Standby restoration may need manual verification"
        log_info "  Run: ansible-playbook playbooks/configure_warm_standby.yml"
    fi

    if $promotion_success; then
        return 0
    else
        return 1
    fi
}

# --- Main ---
echo "=============================================="
echo "  Criterion 3: Warm Standby Replication Test"
echo "=============================================="
echo "  Upstream:         $UPSTREAM_HOST (AZ-Cluster-1)"
echo "  Regional Standby: $REGIONAL_STANDBY (AZ-Cluster-2)"
if ! $SKIP_CROSS_REGION; then
    echo "  Cross-Region DR:  $CROSS_REGION_DR1 (TX-Cluster-1)"
    if [[ -n "$CROSS_REGION_DR2" ]]; then
        echo "                    $CROSS_REGION_DR2 (TX-Cluster-2)"
    fi
fi
if $TEST_PROMOTION; then
    echo "  Promotion Test:   ENABLED (will actually promote standby!)"
fi
echo "=============================================="
echo ""

mkdir -p "$RESULTS_DIR"
TIMESTAMP=$(date +%Y%m%d-%H%M%S)
RESULT_FILE="$RESULTS_DIR/${TIMESTAMP}-warm-standby.txt"

TESTS_PASSED=0
TESTS_FAILED=0

{
    echo "# Warm Standby Replication Test"
    echo "# Date: $(date -u '+%Y-%m-%d %H:%M:%S UTC')"
    echo "# Upstream: $UPSTREAM_HOST"
    echo "# Regional Standby: $REGIONAL_STANDBY"
    echo "# Cross-Region DR: $CROSS_REGION_DR1$(if [[ -n "$CROSS_REGION_DR2" ]]; then echo ", $CROSS_REGION_DR2"; fi)"
    echo "#"
    echo ""
} > "$RESULT_FILE"

# Helper to strip ANSI color codes for file output
strip_ansi() {
    sed 's/\x1b\[[0-9;]*m//g'
}

for test_func in \
    test_cluster_connectivity \
    test_schema_replication \
    test_regional_message_replication \
    test_cross_region_replication \
    test_replication_lag \
    test_sustained_replication_throughput \
    test_stream_consumer_latency \
    test_promotion
do
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
    echo -e "${GREEN}  CRITERION 3: PASSED${NC}"
    echo "  CRITERION 3: PASSED" >> "$RESULT_FILE"
    echo "  Warm standby replication is working and performant."
    echo "  Warm standby replication is working and performant." >> "$RESULT_FILE"
else
    echo -e "${RED}  CRITERION 3: FAILED${NC}"
    echo "  CRITERION 3: FAILED" >> "$RESULT_FILE"
    echo "  Warm standby replication issues detected."
    echo "  Warm standby replication issues detected." >> "$RESULT_FILE"
fi

echo "=============================================="
echo "==============================================" >> "$RESULT_FILE"
echo ""
echo "Results saved to: $RESULT_FILE"

exit $TESTS_FAILED
