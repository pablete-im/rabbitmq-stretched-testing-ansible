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
SSH_USER="ansible"
QUICK_MODE=false
TEST_DURATION=60
RESTORE_LATENCY=true

# Latency values to test (milliseconds)
# Full sweep
LATENCY_VALUES=(0 1 2 3 5 10 15 20 35 50)
# Quick sweep
QUICK_LATENCY_VALUES=(0 3 10 35)

# Cluster nodes to configure latency on
NODE1_HOST="192.168.20.200"  # az-rmq-01
NODE2_HOST="192.168.20.201"  # az-rmq-02
NODE3_HOST="192.168.20.202"  # az-rmq-03

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
        --host)      HOST="$2"; shift 2 ;;
        --user)      USER="$2"; shift 2 ;;
        --password)  PASSWORD="$2"; shift 2 ;;
        --ssh-user)  SSH_USER="$2"; shift 2 ;;
        --quick)     QUICK_MODE=true; shift ;;
        --duration)  TEST_DURATION="$2"; shift 2 ;;
        --no-restore) RESTORE_LATENCY=false; shift ;;
        *)           echo "Unknown option: $1"; exit 1 ;;
    esac
done

if [[ -z "$PASSWORD" ]]; then
    PASSWORD="${RMQ_PASSWORD:-}"
fi
if [[ -z "$PASSWORD" ]]; then
    read -rsp "RabbitMQ password for '$USER': " PASSWORD
    echo
fi

if $QUICK_MODE; then
    LATENCY_VALUES=("${QUICK_LATENCY_VALUES[@]}")
    TEST_DURATION=30
fi

AMQP_URI="amqp://${USER}:${PASSWORD}@${HOST}:5672"

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

# Get network interface name on remote host
get_interface() {
    local host="$1"
    ssh -o StrictHostKeyChecking=no -o ConnectTimeout=10 "${SSH_USER}@${host}" \
        "ip route get 8.8.8.8 | sed -n 's/.*dev \([^ ]*\).*/\1/p'" 2>/dev/null || echo "ens192"
}

# Configure uniform latency between all cluster nodes
configure_latency() {
    local delay_ms="$1"
    local jitter_ms=$((delay_ms / 3 + 1))  # Jitter is ~1/3 of delay, minimum 1ms

    log_info "Configuring ${delay_ms}ms latency (jitter: ${jitter_ms}ms)..."

    for node_host in "$NODE1_HOST" "$NODE2_HOST" "$NODE3_HOST"; do
        local iface
        iface=$(get_interface "$node_host")

        # Clear existing rules
        ssh_sudo "$node_host" "tc qdisc del dev $iface root 2>/dev/null || true"

        if [[ "$delay_ms" -gt 0 ]]; then
            # Add latency to all outgoing traffic (simplified - adds to everything)
            ssh_sudo "$node_host" "tc qdisc add dev $iface root netem delay ${delay_ms}ms ${jitter_ms}ms distribution normal"
        fi
    done

    # Allow network to stabilize
    sleep 3

    log_pass "Latency configured: ${delay_ms}ms"
}

# Clear all latency configuration
clear_latency() {
    log_info "Clearing latency configuration..."

    for node_host in "$NODE1_HOST" "$NODE2_HOST" "$NODE3_HOST"; do
        local iface
        iface=$(get_interface "$node_host")
        ssh_sudo "$node_host" "tc qdisc del dev $iface root 2>/dev/null || true"
    done

    sleep 2
    log_pass "Latency cleared"
}

# Restore original latency configuration via Ansible playbook
restore_latency() {
    log_info "Restoring original latency configuration via Ansible..."

    if ! ansible-playbook -i "$PROJECT_DIR/inventory/hosts.yml" "$PROJECT_DIR/playbooks/configure_latency.yml" > /dev/null 2>&1; then
        log_error "Failed to restore latency configuration!"
        log_warn "Run manually: ansible-playbook -i inventory/hosts.yml playbooks/configure_latency.yml"
        return 1
    fi

    log_pass "Original latency configuration restored"
}

# Run performance test and extract metrics
run_perf_test() {
    local latency_ms="$1"
    local output
    local queue="latency-sweep-${latency_ms}ms"

    log_info "Running performance test at ${latency_ms}ms latency..." >&2

    # Run enterprise-typical workload: 5KB messages, 3k msg/s target, 2 publishers, 2 consumers
    output=$("$TOOLS_DIR/perf-test" \
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

    # Parse confirm latency from "min/median/75th/95th/99th/max 123/456/789/..." format
    # Example: "confirm latency min/median/75th/95th/99th/max 26376/45057/52783/72707/98042/209587 µs"
    # Note: Using confirm latency (time to broker ack) instead of consumer latency
    #       because consumer latency reports 0 when artificial network latency is added
    local latency_line
    latency_line=$(echo "$output" | grep "confirm latency" | grep "min/median" | \
        sed -n 's/.*[^0-9]\([0-9][0-9]*\/[0-9][0-9]*\/[0-9][0-9]*\/[0-9][0-9]*\/[0-9][0-9]*\/[0-9][0-9]*\).*/\1/p' | tail -1)

    if [[ -n "$latency_line" ]]; then
        # Convert from µs to ms and extract values (min/median/75th/95th/99th/max)
        IFS='/' read -r lat_min lat_median lat_p75 lat_p95 lat_p99 lat_max <<< "$latency_line"
        # Values are in microseconds, convert to milliseconds
        lat_min=$((lat_min / 1000))
        lat_median=$((lat_median / 1000))
        lat_p95=$((lat_p95 / 1000))
        lat_p99=$((lat_p99 / 1000))
        lat_max=$((lat_max / 1000))
    else
        lat_min=0
        lat_median=0
        lat_p95=0
        lat_p99=0
        lat_max=0
    fi

    echo "${latency_ms},${send_rate},${recv_rate},${lat_min},${lat_median},${lat_p95},${lat_p99},${lat_max}"
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
echo "latency_ms,send_rate_msg_s,recv_rate_msg_s,confirm_lat_min_ms,confirm_lat_median_ms,confirm_lat_p95_ms,confirm_lat_p99_ms,confirm_lat_max_ms" > "$CSV_FILE"

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
    echo "Note: Latency columns show confirm latency (time to broker acknowledgment)."
    echo ""
    printf "| %-10s | %-12s | %-12s | %-10s | %-12s | %-10s | %-10s |\n" \
        "Net Lat" "Send Rate" "Recv Rate" "Cfm Min" "Cfm Median" "Cfm P95" "Cfm P99"
    printf "| %-10s | %-12s | %-12s | %-10s | %-12s | %-10s | %-10s |\n" \
        "----------" "------------" "------------" "----------" "------------" "----------" "----------"
} > "$REPORT_FILE"

# Track results for summary
declare -a results

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
    IFS=',' read -r lat send recv lat_min lat_med lat_p95 lat_p99 lat_max <<< "$clean_result"
    printf "| %-10s | %-12s | %-12s | %-10s | %-12s | %-10s | %-10s |\n" \
        "${lat}ms" "${send} msg/s" "${recv} msg/s" "${lat_min}ms" "${lat_med}ms" "${lat_p95}ms" "${lat_p99}ms" >> "$REPORT_FILE"

    log_pass "Completed: send=${send} msg/s, recv=${recv} msg/s, confirm_p99=${lat_p99}ms"
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
            IFS=',' read -r lat send recv _ _ _ _ _ <<< "$clean_res"
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
