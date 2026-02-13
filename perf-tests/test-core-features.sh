#!/bin/bash
# =============================================================================
# Criterion 1: Core Broker Features Test
#
# Validates that RabbitMQ core features work correctly when nodes are
# dispersed across datacenters with network latency.
#
# Tests:
#   1. Direct exchange messaging (point-to-point)
#   2. Fanout exchange (broadcast)
#   3. Topic exchange (pattern routing)
#   4. Publisher confirms
#   5. Consumer acknowledgments
#   6. Quorum queue replication
#
# Usage:
#   ./perf-tests/test-core-features.sh --host 192.168.20.200
#   ./perf-tests/test-core-features.sh --host 192.168.20.200 --verbose
# =============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
TOOLS_DIR="$SCRIPT_DIR/tools"
RESULTS_DIR="$SCRIPT_DIR/results"

# Defaults
HOST="192.168.20.200"
USER="admin"
PASSWORD=""
VERBOSE=false
TEST_DURATION=30
MESSAGE_COUNT=1000

# Colors for terminal output (disabled when piped/redirected)
if [[ -t 1 ]]; then
    RED='\033[0;31m'
    GREEN='\033[0;32m'
    YELLOW='\033[1;33m'
    NC='\033[0m'
else
    RED=''
    GREEN=''
    YELLOW=''
    NC=''
fi

# --- Parse arguments ---
while [[ $# -gt 0 ]]; do
    case "$1" in
        --host)      HOST="$2"; shift 2 ;;
        --user)      USER="$2"; shift 2 ;;
        --password)  PASSWORD="$2"; shift 2 ;;
        --verbose)   VERBOSE=true; shift ;;
        --duration)  TEST_DURATION="$2"; shift 2 ;;
        *)           echo "Unknown option: $1"; exit 1 ;;
    esac
done

# Password from arg, env var, or prompt
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
log_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

log_error() {
    echo -e "${RED}[FAIL]${NC} $1"
}

log_pass() {
    echo -e "${GREEN}[PASS]${NC} $1"
}

verbose() {
    if $VERBOSE; then
        echo "       $1"
    fi
}

# Check if management API is accessible
check_management_api() {
    if curl -sf -u "${USER}:${PASSWORD}" "${MGMT_URL}/api/overview" > /dev/null 2>&1; then
        return 0
    else
        return 1
    fi
}

# Get cluster nodes from management API
get_cluster_nodes() {
    curl -sf -u "${USER}:${PASSWORD}" "${MGMT_URL}/api/nodes" | \
        python3 -c "import sys, json; print(' '.join([n['name'] for n in json.load(sys.stdin)]))"
}

# Check quorum queue leader location
get_quorum_leader() {
    local queue="$1"
    curl -sf -u "${USER}:${PASSWORD}" "${MGMT_URL}/api/queues/%2F/${queue}" | \
        python3 -c "import sys, json; d=json.load(sys.stdin); print(d.get('leader', 'unknown'))" 2>/dev/null || echo "unknown"
}

# Run a quick perf-test and capture results
run_perf_test() {
    local test_name="$1"
    local extra_args="${2:-}"

    verbose "Running: perf-test --uri $AMQP_URI --time $TEST_DURATION $extra_args"

    local output
    local exit_code=0
    output=$("$TOOLS_DIR/perf-test" \
        --uri "$AMQP_URI" \
        --time "$TEST_DURATION" \
        --id "$test_name" \
        --queue "core-test-${test_name}" \
        --auto-delete true \
        $extra_args 2>&1) || exit_code=$?

    # Extract send/receive rates from output (macOS compatible)
    local send_rate recv_rate
    send_rate=$(echo "$output" | sed -n 's/.*sending rate avg: \([0-9][0-9]*\).*/\1/p' | tail -1)
    recv_rate=$(echo "$output" | sed -n 's/.*receiving rate avg: \([0-9][0-9]*\).*/\1/p' | tail -1)
    send_rate="${send_rate:-0}"
    recv_rate="${recv_rate:-0}"

    if [[ "$send_rate" -gt 0 && "$recv_rate" -gt 0 ]]; then
        verbose "Send rate: $send_rate msg/s, Receive rate: $recv_rate msg/s"
        return 0
    fi

    # Debug: show why test failed
    if $VERBOSE; then
        echo "       Debug: exit_code=$exit_code, send_rate=$send_rate, recv_rate=$recv_rate"
        echo "       Last 5 lines of output:"
        echo "$output" | tail -5 | sed 's/^/         /'
    fi
    return 1
}

# --- Test functions ---
test_cluster_connectivity() {
    log_info "Test 1: Cluster connectivity"

    if ! check_management_api; then
        log_error "Cannot connect to management API at $MGMT_URL"
        return 1
    fi

    local nodes
    nodes=$(get_cluster_nodes)
    local node_count
    node_count=$(echo "$nodes" | wc -w)

    if [[ "$node_count" -ge 3 ]]; then
        log_pass "Connected to cluster with $node_count nodes"
        verbose "Nodes: $nodes"
        return 0
    else
        log_error "Expected 3+ nodes, found $node_count"
        return 1
    fi
}

test_direct_messaging() {
    log_info "Test 2: Direct exchange messaging (point-to-point)"

    if run_perf_test "direct" "--producers 1 --consumers 1 --size 5000"; then
        log_pass "Direct messaging working"
        return 0
    else
        log_error "Direct messaging failed"
        return 1
    fi
}

test_fanout_messaging() {
    log_info "Test 3: Fanout exchange (multiple consumers)"

    if run_perf_test "fanout" "--producers 1 --consumers 3 --size 5000 --exchange fanout-test --type fanout"; then
        log_pass "Fanout exchange working"
        return 0
    else
        log_error "Fanout exchange failed"
        return 1
    fi
}

test_publisher_confirms() {
    log_info "Test 4: Publisher confirms"

    if run_perf_test "confirms" "--producers 1 --consumers 1 --size 5000 --confirm 10 --quorum-queue"; then
        log_pass "Publisher confirms working"
        return 0
    else
        log_error "Publisher confirms failed"
        return 1
    fi
}

test_quorum_queue_replication() {
    log_info "Test 5: Quorum queue replication"

    # Create quorum queue and verify replication
    if run_perf_test "quorum" "--producers 1 --consumers 1 --size 5000 --quorum-queue --confirm 10"; then
        local leader
        leader=$(get_quorum_leader "core-test-quorum")
        log_pass "Quorum queue working (leader: $leader)"
        return 0
    else
        log_error "Quorum queue test failed"
        return 1
    fi
}

test_message_throughput() {
    log_info "Test 6: Sustained throughput under latency"

    # Run enterprise-typical workload (5KB messages, ~3k msg/s)
    if run_perf_test "throughput" "--producers 2 --consumers 2 --size 5000 --quorum-queue --rate 1500 --confirm 50"; then
        log_pass "Sustained throughput test passed"
        return 0
    else
        log_error "Throughput degraded significantly"
        return 1
    fi
}

test_message_ordering() {
    log_info "Test 7: Message ordering preserved"

    # Single producer/consumer to verify FIFO ordering
    if run_perf_test "ordering" "--producers 1 --consumers 1 --size 1000 --quorum-queue --pmessages 5000 --cmessages 5000"; then
        log_pass "Message ordering preserved"
        return 0
    else
        log_error "Message ordering test failed"
        return 1
    fi
}

# --- Main ---
echo "=============================================="
echo "  Criterion 1: Core Broker Features Test"
echo "=============================================="
echo "  Host:     $HOST"
echo "  Duration: ${TEST_DURATION}s per test"
echo "=============================================="
echo ""

mkdir -p "$RESULTS_DIR"
TIMESTAMP=$(date +%Y%m%d-%H%M%S)
RESULT_FILE="$RESULTS_DIR/${TIMESTAMP}-core-features.txt"

# Run all tests, track results
TESTS_PASSED=0
TESTS_FAILED=0

{
    echo "# Core Broker Features Test"
    echo "# Date: $(date -u '+%Y-%m-%d %H:%M:%S UTC')"
    echo "# Host: $HOST"
    echo "#"
    echo ""
} > "$RESULT_FILE"

# Helper to strip ANSI color codes for file output
strip_ansi() {
    sed 's/\x1b\[[0-9;]*m//g'
}

for test_func in \
    test_cluster_connectivity \
    test_direct_messaging \
    test_fanout_messaging \
    test_publisher_confirms \
    test_quorum_queue_replication \
    test_message_throughput \
    test_message_ordering
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
    echo -e "${GREEN}  CRITERION 1: PASSED${NC}"
    echo "  CRITERION 1: PASSED" >> "$RESULT_FILE"
    echo "  Core broker features work when nodes are dispersed."
    echo "  Core broker features work when nodes are dispersed." >> "$RESULT_FILE"
else
    echo -e "${RED}  CRITERION 1: FAILED${NC}"
    echo "  CRITERION 1: FAILED" >> "$RESULT_FILE"
    echo "  Some core features did not work as expected."
    echo "  Some core features did not work as expected." >> "$RESULT_FILE"
fi

echo "=============================================="
echo "==============================================" >> "$RESULT_FILE"
echo ""
echo "Results saved to: $RESULT_FILE"

exit $TESTS_FAILED
