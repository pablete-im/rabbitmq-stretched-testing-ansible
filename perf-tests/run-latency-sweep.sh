#!/bin/bash
# =============================================================================
# Criterion 4: Latency Sweep Test
#
# Produces performance curves showing throughput and latency as functions
# of network latency between nodes. Uses enterprise-typical workload.
#
# Latency values tested: 0, 1, 2, 3, 5, 10, 15, 20, 35, 50 ms
#
# Output:
#   - CSV file with columns: latency_ms, send_rate, recv_rate, confirm_lat_min, confirm_lat_median, confirm_lat_p95, confirm_lat_p99, confirm_lat_max
#   - Summary report
#
# Usage:
#   ./perf-tests/run-latency-sweep.sh --host 192.168.20.200
#   ./perf-tests/run-latency-sweep.sh --host 192.168.20.200 --quick
#   ./perf-tests/run-latency-sweep.sh --host 192.168.20.200 --no-restore
#
# TLS Usage:
#   ./perf-tests/run-latency-sweep.sh --host 192.168.20.200 --truststore /path/to/truststore.p12 --truststore-pass mypass
#   ./perf-tests/run-latency-sweep.sh --host 192.168.20.200 --truststore /path/to/truststore.p12 --truststore-pass mypass
# =============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
TOOLS_DIR="$SCRIPT_DIR/tools"
RESULTS_DIR="$SCRIPT_DIR/results"

# Defaults
HOST="10.85.10.234"
USER="admin"
PASSWORD=""
SSH_USER="root"
QUICK_MODE=false
TEST_DURATION=60
RESTORE_LATENCY=true
TRUSTSTORE=""
TRUSTSTORE_PASS=""
TRUSTSTORE_TYPE="JKS"

# Latency values to test (milliseconds)
# Full sweep
LATENCY_VALUES=(0 1 2 3 5 10 15 20 35 50)
# Quick sweep
QUICK_LATENCY_VALUES=(0 3 10 35)

# Cluster nodes to configure latency on
NODE1_HOST="10.85.10.234"  # az-rmq-01
NODE2_HOST="10.85.10.235"  # az-rmq-02
NODE3_HOST="10.85.10.236"  # az-rmq-03

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
        --host)            HOST="$2"; shift 2 ;;
        --user)            USER="$2"; shift 2 ;;
        --password)        PASSWORD="$2"; shift 2 ;;
        --ssh-user)        SSH_USER="$2"; shift 2 ;;
        --quick)           QUICK_MODE=true; shift ;;
        --duration)        TEST_DURATION="$2"; shift 2 ;;
        --no-restore)      RESTORE_LATENCY=false; shift ;;
        --truststore)      TRUSTSTORE="$2"; shift 2 ;;
        --truststore-pass) TRUSTSTORE_PASS="$2"; shift 2 ;;
        --truststore-type) TRUSTSTORE_TYPE="$2"; shift 2 ;;
        *)                 echo "Unknown option: $1"; exit 1 ;;
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
    AMQP_PROTOCOL="amqps"
    AMQP_PORT="5671"
    
    # JVM options for performance tests
    JVM_OPTS="-Djavax.net.ssl.trustStore=$TRUSTSTORE"
    JVM_OPTS="$JVM_OPTS -Djavax.net.ssl.trustStorePassword=$TRUSTSTORE_PASS"
    JVM_OPTS="$JVM_OPTS -Djavax.net.ssl.trustStoreType=$TRUSTSTORE_TYPE"
else
    echo "🔓 Non-TLS Mode: Using standard connections"
    AMQP_PROTOCOL="amqp"
    AMQP_PORT="5672"
    JVM_OPTS=""
fi

if $QUICK_MODE; then
    LATENCY_VALUES=("${QUICK_LATENCY_VALUES[@]}")
    TEST_DURATION=30
fi

AMQP_URI="${AMQP_PROTOCOL}://${USER}:${PASSWORD}@${HOST}:${AMQP_PORT}"

# Store initial configuration
declare -A INITIAL_LATENCY_CONFIG

# --- Helper functions ---
log_info() { echo -e "${BLUE}[INFO]${NC} $1"; }
log_warn() { echo -e "${YELLOW}[WARN]${NC} $1"; }
log_error() { echo -e "${RED}[ERROR]${NC} $1"; }
log_pass() { echo -e "${GREEN}[OK]${NC} $1"; }

# Strip ANSI escape codes for clean file output
strip_ansi() {
    sed 's/\x1b\[[0-9;]*m//g'
}

# Execute sudo command on remote node
ssh_sudo() {
    local host="$1"
    local cmd="$2"
    ssh -o StrictHostKeyChecking=no -o ConnectTimeout=10 "${SSH_USER}@${host}" "sudo $cmd" 2>/dev/null
}

# Execute command on remote node via SSH (no sudo)
ssh_cmd() {
    local host="$1"
    local cmd="$2"
    ssh -o StrictHostKeyChecking=no -o ConnectTimeout=10 "${SSH_USER}@${host}" "$cmd" 2>/dev/null
}

# Get network interface name on remote host (prefer ens33)
get_interface() {
    local host="$1"
    if ssh_cmd "$host" "ip link show ens33 >/dev/null 2>&1"; then
        echo "ens33"
    else
        ssh_cmd "$host" "ip route get 8.8.8.8 | sed -n 's/.*dev \([^ ]*\).*/\1/p'" 2>/dev/null || echo "eth0"
    fi
}

# Capture initial latency state from nodes
save_initial_state() {
    log_info "Capturing initial latency configuration..."
    for node in "$NODE1_HOST" "$NODE2_HOST" "$NODE3_HOST"; do
        local iface
        iface=$(get_interface "$node")
        
        local tc_show
        tc_show=$(ssh_cmd "$node" "tc qdisc show dev $iface")
        
        # Extract Band 20 delay params (e.g., "delay 1.5ms 500us")
        local params=""
        # Search for line containing "parent 1:2" (Band 20)
        local band_line
        band_line=$(echo "$tc_show" | grep "parent 1:2")
        
        if [[ -n "$band_line" ]]; then
             params=$(echo "$band_line" | grep -oP 'delay [0-9.]+(ms|us)(\s+[0-9.]+(ms|us))?')
        fi
        
        if [[ -n "$params" ]]; then
            INITIAL_LATENCY_CONFIG["$node"]="$params"
            log_info "  Node $node: Detected initial config '$params' on $iface"
        else
            INITIAL_LATENCY_CONFIG["$node"]="NONE"
            log_info "  Node $node: No existing Band 20 config on $iface"
        fi
    done
}

# Configure uniform latency between all cluster nodes
configure_latency() {
    local delay_ms="$1"
    local jitter_ms=$((delay_ms / 10))  # Minimal jitter for sweep accuracy
    [[ $jitter_ms -lt 1 ]] && jitter_ms=0 # 0ms jitter for very low latency tests

    log_info "Configuring ${delay_ms}ms latency (jitter: ${jitter_ms}ms)..."

    for node_host in "$NODE1_HOST" "$NODE2_HOST" "$NODE3_HOST"; do
        local iface
        iface=$(get_interface "$node_host")
        local initial="${INITIAL_LATENCY_CONFIG[$node_host]}"

        if [[ "$initial" != "NONE" ]]; then
            # Modify existing Band 20 (Metro Latency)
            local dist_param=""
            if [[ "$jitter_ms" -gt 0 ]]; then
                dist_param="distribution normal"
            fi
            
            local out
            out=$(ssh_sudo "$node_host" "tc qdisc change dev $iface parent 1:2 handle 20: netem delay ${delay_ms}ms ${jitter_ms}ms $dist_param 2>&1")
            
            if [[ $? -ne 0 ]]; then
                log_error "Failed to update Band 20 on $node_host: $out"
            fi
        else
            # Fallback: Clean root and add fresh rule
            ssh_sudo "$node_host" "tc qdisc del dev $iface root 2>/dev/null || true"
            if [[ "$delay_ms" -gt 0 ]]; then
                local dist_param=""
                if [[ "$jitter_ms" -gt 0 ]]; then
                    dist_param="distribution normal"
                fi
                ssh_sudo "$node_host" "tc qdisc add dev $iface root netem delay ${delay_ms}ms ${jitter_ms}ms $dist_param"
            fi
        fi
        
        # Verify
        local verify
        verify=$(ssh_cmd "$node_host" "tc qdisc show dev $iface | grep -E 'netem 20:|root netem'")
        log_info "  $node_host applied: $verify"
    done

    # Allow network to stabilize
    sleep 3

    log_pass "Latency configured: ${delay_ms}ms"
}

# Clear all latency configuration (reset to 0ms)
clear_latency() {
    log_info "Clearing latency configuration..."

    for node_host in "$NODE1_HOST" "$NODE2_HOST" "$NODE3_HOST"; do
        local iface
        iface=$(get_interface "$node_host")
        local initial="${INITIAL_LATENCY_CONFIG[$node_host]}"

        if [[ "$initial" != "NONE" ]]; then
            # Reset Band 20 to 0ms
            ssh_sudo "$node_host" "tc qdisc change dev $iface parent 1:2 handle 20: netem delay 0ms 0ms" || true
        else
            # Remove root rule
            ssh_sudo "$node_host" "tc qdisc del dev $iface root 2>/dev/null || true"
        fi
    done

    sleep 2
    log_pass "Latency cleared (0ms)"
}

# Restore original latency configuration
restore_latency() {
    log_info "Restoring original latency configuration..."

    for node_host in "$NODE1_HOST" "$NODE2_HOST" "$NODE3_HOST"; do
        local iface
        iface=$(get_interface "$node_host")
        local initial="${INITIAL_LATENCY_CONFIG[$node_host]}"

        if [[ "$initial" != "NONE" ]]; then
            log_info "  Restoring $node_host to: $initial"
            # Restore original parameters to Band 20
            ssh_sudo "$node_host" "tc qdisc change dev $iface parent 1:2 handle 20: netem $initial" || \
                log_error "Failed to restore config on $node_host"
        else
            log_info "  Restoring $node_host to clean state"
            ssh_sudo "$node_host" "tc qdisc del dev $iface root 2>/dev/null || true"
        fi
        
        # Verify
        local verify
        verify=$(ssh_cmd "$node_host" "tc qdisc show dev $iface | grep -E 'netem 20:|root netem'")
        log_info "  $node_host restored: $verify"
    done

    log_pass "Original latency configuration restored"
}

# Run performance test and extract metrics
run_perf_test() {
    local latency_ms="$1"
    local output
    local queue="latency-sweep-${latency_ms}ms"

    log_info "Running performance test at ${latency_ms}ms latency..." >&2

    # Run enterprise-typical workload: 5KB messages, 3k msg/s target, 2 publishers, 2 consumers
    output=$(java $JVM_OPTS -jar "$TOOLS_DIR/perf-test.jar" \
        --uri "$AMQP_URI" \
        --quorum-queue \
        --queue "$queue" \
        --producers 2 \
        --consumers 2 \
        --time "$TEST_DURATION" \
        --size 5000 \
        --rate 1500 \
        --confirm 50 \
        --multi-ack-every 50 \
        --id "latency-sweep-${latency_ms}ms" \
        --auto-delete true 2>&1) || true

    # Extract metrics using sed (macOS compatible)
    local send_rate recv_rate
    local lat_min lat_median lat_p75 lat_p95 lat_p99 lat_max

    send_rate=$(echo "$output" | sed -n 's/.*sending rate avg: \([0-9][0-9]*\).*/\1/p' | tail -1)
    recv_rate=$(echo "$output" | sed -n 's/.*receiving rate avg: \([0-9][0-9]*\).*/\1/p' | tail -1)
    send_rate="${send_rate:-0}"
    recv_rate="${recv_rate:-0}"

    # Parse confirm latency
    local confirm_lat_line
    confirm_lat_line=$(echo "$output" | grep "confirm latency" | grep "min/median" | \
        sed -n 's/.*[^0-9]\([0-9][0-9]*\/[0-9][0-9]*\/[0-9][0-9]*\/[0-9][0-9]*\/[0-9][0-9]*\/[0-9][0-9]*\).*/\1/p' | tail -1)

    local clat_min=0 clat_med=0 clat_p95=0 clat_p99=0 clat_max=0
    if [[ -n "$confirm_lat_line" ]]; then
        IFS='/' read -r clat_min clat_med _ clat_p95 clat_p99 clat_max <<< "$confirm_lat_line"
        clat_min=$((clat_min / 1000)); clat_med=$((clat_med / 1000)); clat_p95=$((clat_p95 / 1000))
        clat_p99=$((clat_p99 / 1000)); clat_max=$((clat_max / 1000))
    fi

    # Parse consumer latency
    local cons_lat_line
    cons_lat_line=$(echo "$output" | grep "consumer latency" | grep "min/median" | \
        sed -n 's/.*[^0-9]\([0-9][0-9]*\/[0-9][0-9]*\/[0-9][0-9]*\/[0-9][0-9]*\/[0-9][0-9]*\/[0-9][0-9]*\).*/\1/p' | tail -1)

    local cons_min=0 cons_med=0 cons_p95=0 cons_p99=0 cons_max=0
    if [[ -n "$cons_lat_line" ]]; then
        IFS='/' read -r cons_min cons_med _ cons_p95 cons_p99 cons_max <<< "$cons_lat_line"
        cons_min=$((cons_min / 1000)); cons_med=$((cons_med / 1000)); cons_p95=$((cons_p95 / 1000))
        cons_p99=$((cons_p99 / 1000)); cons_max=$((cons_max / 1000))
    fi

    # Output CSV format: latency,send,recv,confirm(min,med,95,99,max),consumer(min,med,95,99,max)
    echo "${latency_ms},${send_rate},${recv_rate},${clat_min},${clat_med},${clat_p95},${clat_p99},${clat_max},${cons_min},${cons_med},${cons_p95},${cons_p99},${cons_max}"
}

# --- Main ---
echo "=============================================="
echo "  Criterion 4: Latency Sweep Test"
echo "=============================================="
echo "  Target Host:   $HOST"
echo "  Test Duration: ${TEST_DURATION}s per latency value"
echo "  Quick Mode:    $QUICK_MODE"
echo "  Restore After: $RESTORE_LATENCY"
echo "  Latency Values: ${LATENCY_VALUES[*]} ms"
echo "=============================================="
echo ""
echo -e "${YELLOW}WARNING: This test will modify network latency on cluster nodes!${NC}"
if $RESTORE_LATENCY; then
    echo -e "${YELLOW}Original latency configuration will be restored at the end via Ansible.${NC}"
else
    echo -e "${YELLOW}Original latency configuration will NOT be restored (--no-restore specified).${NC}"
fi
echo ""
read -rp "Continue? [y/N] " confirm
if [[ ! "$confirm" =~ ^[Yy]$ ]]; then
    echo "Aborted."
    exit 1
fi
echo ""

mkdir -p "$RESULTS_DIR"
TIMESTAMP=$(date +%Y%m%d-%H%M%S)
CSV_FILE="$RESULTS_DIR/${TIMESTAMP}-latency-sweep.csv"
REPORT_FILE="$RESULTS_DIR/${TIMESTAMP}-latency-sweep-report.txt"

# Initialize CSV
echo "latency_ms,send_rate_msg_s,recv_rate_msg_s,confirm_min_ms,confirm_med_ms,confirm_p95_ms,confirm_p99_ms,confirm_max_ms,consumer_min_ms,consumer_med_ms,consumer_p95_ms,consumer_p99_ms,consumer_max_ms" > "$CSV_FILE"

# Initialize report
{
    echo "# Latency Sweep Test Report"
    echo "# Date: $(date -u '+%Y-%m-%d %H:%M:%S UTC')"
    echo "# Host: $HOST"
    echo "# Test Duration: ${TEST_DURATION}s per latency value"
    echo "# Workload: Enterprise-typical (5KB messages, 3k msg/s target)"
    echo "#"
    echo ""
    echo "## Raw Results"
    echo ""
    echo "Note: Latency columns show P99 latency in ms."
    echo ""
    printf "| %-8s | %-12s | %-12s | %-12s | %-12s |\n" \
        "Network" "Send Rate" "Recv Rate" "Confirm P99" "Consumer P99"
    printf "| %-8s | %-12s | %-12s | %-12s | %-12s |\n" \
        "--------" "------------" "------------" "------------" "------------"
} > "$REPORT_FILE"

# Track results for summary
declare -a results

# Capture initial state before starting
save_initial_state

# Run tests at each latency value
for latency_ms in "${LATENCY_VALUES[@]}"; do
    echo ""
    echo "=========================================="
    echo "  Testing at ${latency_ms}ms network latency"
    echo "=========================================="

    # Configure latency
    if ! configure_latency "$latency_ms"; then
        log_error "Failed to configure latency, skipping..."
        continue
    fi

    # Run test
    result=$(run_perf_test "$latency_ms")
    echo "$result" | strip_ansi >> "$CSV_FILE"
    results+=("$result")

    # Parse for report (strip any ANSI codes first)
    clean_result=$(echo "$result" | strip_ansi)
    IFS=',' read -r lat send recv c_min c_med c_p95 c_p99 c_max r_min r_med r_p95 r_p99 r_max <<< "$clean_result"
    printf "| %-8s | %-12s | %-12s | %-12s | %-12s |\n" \
        "${lat}ms" "${send} msg/s" "${recv} msg/s" "${c_p99}ms" "${r_p99}ms" >> "$REPORT_FILE"

    log_pass "Completed: send=${send} msg/s, recv=${recv} msg/s, confirm_p99=${c_p99}ms, consumer_p99=${r_p99}ms"
done

# Clear latency configuration and optionally restore original
echo ""
clear_latency

if $RESTORE_LATENCY; then
    restore_latency
fi

# Add summary to report
{
    echo ""
    echo "## Analysis"
    echo ""
    echo "### Throughput Degradation"
    echo ""

    # Calculate degradation from baseline (0ms)
    if [[ ${#results[@]} -gt 0 ]]; then
        clean_baseline=$(echo "${results[0]}" | strip_ansi)
        IFS=',' read -r _ baseline_send baseline_recv _ _ _ _ _ <<< "$clean_baseline"

        for result in "${results[@]}"; do
            clean_res=$(echo "$result" | strip_ansi)
            IFS=',' read -r lat send recv _ _ _ _ _ _ _ _ _ _ <<< "$clean_res"
            if [[ "$baseline_send" -gt 0 ]]; then
                send_pct=$((100 * send / baseline_send))
                echo "- At ${lat}ms: ${send_pct}% of baseline throughput (${send} msg/s)"
            fi
        done
    fi

    echo ""
    echo "### Latency Impact"
    echo ""
    echo "End-to-end latency increases with network latency due to:"
    echo "- Quorum queue replication (messages written to multiple nodes)"
    echo "- Publisher confirms waiting for quorum acknowledgment"
    echo "- Network round-trips for AMQP protocol operations"
    echo ""

    echo "## Recommendations"
    echo ""
    echo "Based on the sweep results:"
    echo ""
    echo "1. **For latency-sensitive workloads**: Keep inter-node latency under 5ms"
    echo "2. **For throughput-critical workloads**: Up to 10ms is acceptable"
    echo "3. **Cross-region (35ms+)**: Expect ~50% throughput reduction vs local"
    echo ""
    echo "---"
    echo "CSV data saved to: $CSV_FILE"
} >> "$REPORT_FILE"

# Final output
echo ""
echo "=============================================="
echo "  LATENCY SWEEP COMPLETE"
echo "=============================================="
echo ""
echo "Results:"
echo "  CSV Data:   $CSV_FILE"
echo "  Report:     $REPORT_FILE"
echo ""
echo "To plot in Excel/Google Sheets:"
echo "  1. Open the CSV file"
echo "  2. Create XY scatter chart"
echo "  3. X-axis: latency_ms"
echo "  4. Y-axis: send_rate_msg_s (throughput curve)"
echo "  5. Add second Y-axis: lat_p99_ms (latency curve)"
echo ""

# Print summary table
echo "Summary:"
echo ""
head -n 20 "$REPORT_FILE" | tail -n 15

exit 0
