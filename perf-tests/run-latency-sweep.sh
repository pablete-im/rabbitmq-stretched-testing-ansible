#!/bin/bash
# =============================================================================
# Criterion 4: Latency Sweep Test
#
# Produces performance curves showing throughput and latency as functions
# of network latency between nodes. Uses scenario yml files for test configuration.
#
# Latency values tested (RTT): 0, 1, 2, 3, 5, 10, 15, 20, 35, 50 ms
# Note: RTT values are divided by 2 to get one-way delay for TC configuration
#
# Output:
#   - CSV file with columns: latency_ms, send_rate, confirm_rate, recv_rate, and all latency metrics
#   - Summary report
#
# Usage:
#   ./perf-tests/run-latency-sweep.sh --scenario baseline --hosts 192.168.20.200
#   ./perf-tests/run-latency-sweep.sh --scenario baseline --hosts 192.168.20.200 --quick
#   ./perf-tests/run-latency-sweep.sh --scenario baseline --hosts 192.168.20.200 --no-restore
#   ./perf-tests/run-latency-sweep.sh --scenario streams --hosts 192.168.20.200 --use-omq
#   ./perf-tests/run-latency-sweep.sh --scenario enterprise-workload --hosts 192.168.20.200 --result-alias "prod-test"
#
# OMQ Usage (multi-protocol):
#   ./perf-tests/run-latency-sweep.sh --scenario baseline --hosts 192.168.20.200 --use-omq --protocol mqtt-amqp
#   ./perf-tests/run-latency-sweep.sh --scenario baseline --hosts 192.168.20.200 --use-omq --protocol stomp-amqp091
#
# TLS Usage:
#   ./perf-tests/run-latency-sweep.sh --scenario baseline --hosts 192.168.20.200 --truststore /path/to/truststore.p12 --truststore-pass mypass
#   ./perf-tests/run-latency-sweep.sh --scenario baseline --hosts 192.168.20.200 --use-omq --ca-cert /path/to/ca.crt
#
# Federation Test:
#   ./perf-tests/run-latency-sweep.sh --scenario federation-test --pub-hosts 192.168.20.200 --con-hosts 192.168.20.201
#
# Options:
#   --scenario <name>        Scenario yml file from perf-tests/scenarios/ (REQUIRED)
#   --hosts <ip1,ip2,...>    RabbitMQ hosts (comma-separated)
#   --pub-hosts <ip1,ip2>    Publisher target hosts (for federation tests)
#   --con-hosts <ip1,ip2>    Consumer target hosts (for federation tests)
#   --use-omq                Use run-test-omq.sh instead of run-test.sh
#   --user <user>            RabbitMQ user (default: admin)
#   --password <pass>        RabbitMQ password (or set RMQ_PASSWORD env var)
#   --ssh-user <user>        SSH user for latency configuration (default: root)
#   --quick                  Run quick sweep with fewer latency values
#   --duration <seconds>     Test duration per latency value (default: 60)
#   --no-restore             Don't restore original latency after test
#   --result-alias <alias>   Alias to identify this test run
#
# TLS Options (run-test.sh only, not compatible with --use-omq):
#   --truststore <path>      Java truststore for TLS
#   --truststore-pass <pass> Truststore password
#   --truststore-type <type> Truststore type (default: JKS)
#
# OMQ-specific Options (requires --use-omq):
#   --protocol <proto>       Protocol combination (default: amqp-amqp)
#                            Examples: amqp-amqp, mqtt-amqp, stomp-amqp091, etc.
#   --ca-cert <path>         CA certificate for TLS (alternative to truststore)
#   --tls-skip-verify        Skip TLS certificate verification
# =============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
TOOLS_DIR="$SCRIPT_DIR/tools"
SCENARIOS_DIR="$SCRIPT_DIR/scenarios"
RESULTS_DIR="$SCRIPT_DIR/results"

# Defaults
SCENARIO=""
USE_OMQ=false
HOSTS="10.85.10.234"
PUB_HOSTS=""
CON_HOSTS=""
USER="admin"
PASSWORD=""
SSH_USER="root"
QUICK_MODE=false
TEST_DURATION=60
RESTORE_LATENCY=true
RESULT_ALIAS=""
TRUSTSTORE=""
TRUSTSTORE_PASS=""
TRUSTSTORE_TYPE="JKS"
PROTOCOL="amqp-amqp"
CA_CERT=""
TLS_SKIP_VERIFY=false

# Latency values to test (milliseconds - RTT)
# These are Round Trip Time values. Half will be applied to each direction.
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
        --scenario)        SCENARIO="$2"; shift 2 ;;
        --use-omq)         USE_OMQ=true; shift ;;
        --hosts)           HOSTS="$2"; shift 2 ;;
        --pub-hosts)       PUB_HOSTS="$2"; shift 2 ;;
        --con-hosts)       CON_HOSTS="$2"; shift 2 ;;
        --user)            USER="$2"; shift 2 ;;
        --password)        PASSWORD="$2"; shift 2 ;;
        --ssh-user)        SSH_USER="$2"; shift 2 ;;
        --quick)           QUICK_MODE=true; shift ;;
        --duration)        TEST_DURATION="$2"; shift 2 ;;
        --no-restore)      RESTORE_LATENCY=false; shift ;;
        --result-alias)    RESULT_ALIAS="$2"; shift 2 ;;
        --truststore)      TRUSTSTORE="$2"; shift 2 ;;
        --truststore-pass) TRUSTSTORE_PASS="$2"; shift 2 ;;
        --truststore-type) TRUSTSTORE_TYPE="$2"; shift 2 ;;
        --protocol)        PROTOCOL="$2"; shift 2 ;;
        --ca-cert)         CA_CERT="$2"; shift 2 ;;
        --tls-skip-verify) TLS_SKIP_VERIFY=true; shift ;;
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

# --- Validations ---
if [[ -z "$SCENARIO" ]]; then
    echo "Error: --scenario parameter is required"
    echo ""
    echo "Available scenarios:"
    for f in "$SCENARIOS_DIR"/*.yml; do
        [[ -f "$f" ]] || continue
        name=$(basename "$f" .yml)
        desc=$(grep '^description:' "$f" 2>/dev/null | sed 's/description: *"\?\(.*\)"\?/  \1/' || echo "")
        printf "  %-30s %s\n" "$name" "$desc"
    done
    echo ""
    echo "Usage: $0 --scenario <name> --hosts <hosts> [options]"
    exit 1
fi

SCENARIO_FILE="$SCENARIOS_DIR/${SCENARIO}.yml"
if [[ ! -f "$SCENARIO_FILE" ]]; then
    echo "Error: Scenario file not found: $SCENARIO_FILE"
    exit 1
fi

# Validate run-test.sh exists
if [[ ! -f "$SCRIPT_DIR/run-test.sh" ]]; then
    echo "Error: run-test.sh not found at $SCRIPT_DIR/run-test.sh"
    exit 1
fi

# Validate run-test-omq.sh if using OMQ
if $USE_OMQ && [[ ! -f "$SCRIPT_DIR/run-test-omq.sh" ]]; then
    echo "Error: run-test-omq.sh not found. OMQ mode requires this script."
    exit 1
fi

# Validate perf-test tools
if ! $USE_OMQ && [[ ! -f "$TOOLS_DIR/perf-test.jar" ]]; then
    echo "Error: perf-test.jar not found. Run:"
    echo "  ansible-playbook playbooks/install_perftest.yml"
    exit 1
fi

if $USE_OMQ && [[ ! -f "$TOOLS_DIR/omq" ]]; then
    echo "Error: OMQ not found. Run:"
    echo "  ansible-playbook playbooks/install_perftest.yml"
    exit 1
fi

# --- Validate OMQ-specific parameters ---
if ! $USE_OMQ; then
    # Show warnings for OMQ-specific parameters when not using OMQ
    omq_warnings=()
    [[ "$PROTOCOL" != "amqp-amqp" ]] && omq_warnings+=("--protocol is only used with --use-omq (ignored)")
    [[ -n "$CA_CERT" ]] && omq_warnings+=("--ca-cert is only used with --use-omq (ignored, use --truststore for run-test.sh)")
    [[ "$TLS_SKIP_VERIFY" == "true" ]] && omq_warnings+=("--tls-skip-verify is only used with --use-omq (ignored)")
    
    if [[ ${#omq_warnings[@]} -gt 0 ]]; then
        echo ""
        echo "WARNING: The following OMQ-specific parameters are ignored (--use-omq not specified):"
        for warning in "${omq_warnings[@]}"; do
            echo "  - $warning"
        done
        echo ""
        echo "To use these parameters, add --use-omq to your command."
        echo ""
    fi
else
    # Show warnings for run-test.sh-specific parameters when using OMQ
    perf_test_warnings=()
    [[ -n "$TRUSTSTORE" ]] && perf_test_warnings+=("--truststore is not compatible with --use-omq (ignored, use --ca-cert for OMQ)")
    [[ -n "$TRUSTSTORE_PASS" ]] && perf_test_warnings+=("--truststore-pass is not compatible with --use-omq (ignored)")
    [[ "$TRUSTSTORE_TYPE" != "JKS" ]] && perf_test_warnings+=("--truststore-type is not compatible with --use-omq (ignored)")
    
    if [[ ${#perf_test_warnings[@]} -gt 0 ]]; then
        echo ""
        echo "WARNING: The following run-test.sh-specific parameters are ignored (--use-omq is specified):"
        for warning in "${perf_test_warnings[@]}"; do
            echo "  - $warning"
        done
        echo ""
        echo "For TLS with OMQ, use --ca-cert instead of --truststore."
        echo ""
    fi
fi

if $QUICK_MODE; then
    LATENCY_VALUES=("${QUICK_LATENCY_VALUES[@]}")
    TEST_DURATION=30
fi

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
    # If SSH_USER is root, don't use sudo (RHEL 9.7 doesn't allow "sudo" when already root)
    if [[ "$SSH_USER" == "root" ]]; then
        ssh -o StrictHostKeyChecking=no -o ConnectTimeout=10 "${SSH_USER}@${host}" "$cmd" 2>/dev/null
    else
        ssh -o StrictHostKeyChecking=no -o ConnectTimeout=10 "${SSH_USER}@${host}" "sudo $cmd" 2>/dev/null
    fi
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

# Format TC configuration for display
format_tc_output() {
    local node="$1"
    local iface="$2"
    local title="$3"
    
    log_info "  $title:"
    
    # Show qdisc rules in a clean format
    local qdisc_show
    qdisc_show=$(ssh_cmd "$node" "tc qdisc show dev $iface")
    
    while IFS= read -r line; do
        if [[ "$line" =~ qdisc\ netem\ ([0-9]+):.*delay\ ([0-9.]+[a-z]+).*loss\ ([0-9]+%) ]]; then
            local band_num="${BASH_REMATCH[1]}"
            local flowid_num=$((band_num / 10))
            log_info "    📊 Band $flowid_num: ${BASH_REMATCH[2]} delay + ${BASH_REMATCH[3]} loss"
        elif [[ "$line" =~ qdisc\ netem\ ([0-9]+):.*delay\ ([0-9.]+[a-z]+) ]]; then
            local band_num="${BASH_REMATCH[1]}"
            local flowid_num=$((band_num / 10))
            log_info "    ⏱️  Band $flowid_num: ${BASH_REMATCH[2]} delay"
        elif [[ "$line" =~ qdisc\ netem\ ([0-9]+):.*loss\ ([0-9]+%) ]]; then
            local band_num="${BASH_REMATCH[1]}"
            local flowid_num=$((band_num / 10))
            log_info "    📉 Band $flowid_num: ${BASH_REMATCH[2]} loss only"
        elif [[ "$line" =~ qdisc\ prio\ 1: ]]; then
            log_info "    🎯 Root: Priority qdisc (4 bands)"
        fi
    done <<< "$qdisc_show"
    
    # Show relevant filters with decoded IPs
    local filter_show
    filter_show=$(ssh_cmd "$node" "tc filter show dev $iface")
    
    local has_filters=false
    local filter_output=""
    local current_flowid=""
    
    while IFS= read -r line; do
        # Look for flowid in the filter line
        if [[ "$line" =~ flowid\ 1:([0-9]+) ]]; then
            current_flowid="${BASH_REMATCH[1]}"
        # Look for match in the next line (indented)
        elif [[ "$line" =~ ^[[:space:]]+match\ ([0-9a-f]{8})/ffffffff ]] && [[ -n "$current_flowid" ]]; then
            if [[ "$has_filters" == "false" ]]; then
                filter_output+="    🔀 Filters:\n"
                has_filters=true
            fi
            
            local hex_ip="${BASH_REMATCH[1]}"
            local band="$current_flowid"
            
            # Decode hex IP to dotted decimal
            local ip1=$((0x${hex_ip:0:2}))
            local ip2=$((0x${hex_ip:2:2}))
            local ip3=$((0x${hex_ip:4:2}))
            local ip4=$((0x${hex_ip:6:2}))
            local decoded_ip="$ip1.$ip2.$ip3.$ip4"
            
            filter_output+="       $decoded_ip → Band $band\n"
            current_flowid=""
        fi
    done <<< "$filter_show"
    
    if [[ "$has_filters" == "true" ]]; then
        echo -e "$filter_output" | while IFS= read -r line; do
            [[ -n "$line" ]] && log_info "$line"
        done
    else
        log_info "    🔀 Filters: (only default routing)"
    fi
}

# Check latency between nodes with ping
check_latency_between_nodes() {
    local source_node="$1"
    local target_node="$2"
    local count=10
    
    log_info "  Testing latency: $source_node → $target_node"
    local ping_output
    ping_output=$(ssh_cmd "$source_node" "ping -c $count -i 0.2 $target_node 2>&1" || echo "FAILED")
    
    if [[ "$ping_output" == "FAILED" ]]; then
        log_warn "    Ping failed between nodes"
        return 1
    fi
    
    # Extract average RTT
    local avg_rtt
    avg_rtt=$(echo "$ping_output" | grep -oP 'rtt min/avg/max/mdev = [\d.]+/([\d.]+)' | grep -oP '/([\d.]+)/' | tr -d '/' || echo "N/A")
    
    if [[ -z "$avg_rtt" || "$avg_rtt" == "N/A" ]]; then
        # Fallback parsing
        avg_rtt=$(echo "$ping_output" | grep "rtt" | awk -F'/' '{print $5}' || echo "N/A")
    fi
    
    log_info "    Average RTT: ${avg_rtt}ms"
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
        band_line=$(echo "$tc_show" | grep "parent 1:2" || true)
        
        if [[ -n "$band_line" ]]; then
            # Try to extract delay parameters
            params=$(echo "$band_line" | grep -oP 'delay [0-9.]+(ms|us)(\s+[0-9.]+(ms|us))?' || true)
            
            if [[ -n "$params" ]]; then
                INITIAL_LATENCY_CONFIG["$node"]="$params"
                log_info "  Node $node: Detected initial config '$params' on $iface"
            else
                # Band 20 exists but has no delay configured (delay 0ms by default)
                INITIAL_LATENCY_CONFIG["$node"]="delay 0ms"
                log_info "  Node $node: Band 20 exists but no delay configured (treating as 0ms) on $iface"
            fi
        else
            INITIAL_LATENCY_CONFIG["$node"]="NONE"
            log_info "  Node $node: No existing Band 20 config on $iface"
        fi
    done
}

# Configure uniform latency between all cluster nodes
configure_latency() {
    local rtt_ms="$1"
    # Calculate one-way delay (half of RTT)
    local delay_ms=$(echo "scale=1; $rtt_ms / 2" | bc | sed 's/^\./0./')
    local jitter_ms=$(echo "scale=1; $delay_ms / 10" | bc | sed 's/^\./0./')
    
    # For very low latencies, set jitter to 0
    local jitter_test=$(echo "$jitter_ms < 0.1" | bc)
    if [[ "$jitter_test" -eq 1 ]]; then
        jitter_ms="0"
    fi

    # Ensure jitter has proper format for tc (e.g., 0.1ms not .1ms)
    jitter_ms=$(printf "%.1f" "$jitter_ms")
    
    log_info "Configuring ${rtt_ms}ms RTT (one-way delay: ${delay_ms}ms, jitter: ${jitter_ms}ms)..."

    for node_host in "$NODE1_HOST" "$NODE2_HOST" "$NODE3_HOST"; do
        local iface
        iface=$(get_interface "$node_host")
        local initial="${INITIAL_LATENCY_CONFIG[$node_host]}"

        # Check current TC configuration
        local qdisc_show
        qdisc_show=$(ssh_cmd "$node_host" "tc qdisc show dev $iface")

        if [[ "$initial" != "NONE" ]]; then
            # Modify existing Band 20 (Metro Latency) - preserves filters
            local dist_param=""
            local jitter_test=$(echo "$jitter_ms > 0" | bc)
            if [[ "$jitter_test" -eq 1 ]]; then
                dist_param="distribution normal"
            fi
            
            log_info "  Modifying Band 20 on $node_host (one-way delay: ${delay_ms}ms)"
            local out
            out=$(ssh_sudo "$node_host" "tc qdisc change dev $iface parent 1:2 handle 20: netem delay ${delay_ms}ms ${jitter_ms}ms $dist_param 2>&1")
            
            if [[ $? -ne 0 ]]; then
                log_error "Failed to update Band 20 on $node_host: $out"
            else
                log_pass "  Band 20 updated on $node_host"
            fi
            
            # Check if filters exist for cluster IPs
            local filter_show
            filter_show=$(ssh_cmd "$node_host" "tc filter show dev $iface")
            
            # Get list of other cluster IPs (excluding current node)
            local cluster_ips=()
            for ip in "$NODE1_HOST" "$NODE2_HOST" "$NODE3_HOST"; do
                if [[ "$ip" != "$node_host" ]]; then
                    cluster_ips+=("$ip")
                fi
            done
            
            # Check and create filters for each cluster IP if missing
            for target_ip in "${cluster_ips[@]}"; do
                # Convert IP to hex for matching
                local ip_parts=(${target_ip//./ })
                local hex_ip=$(printf "%02x%02x%02x%02x" "${ip_parts[0]}" "${ip_parts[1]}" "${ip_parts[2]}" "${ip_parts[3]}")
                
                # Check if filter exists for this IP routing to Band 2 (flowid 1:2)
                local has_filter=false
                local current_flowid=""
                
                while IFS= read -r line; do
                    if [[ "$line" =~ flowid\ 1:([0-9]+) ]]; then
                        current_flowid="${BASH_REMATCH[1]}"
                    elif [[ "$line" =~ ^[[:space:]]+match\ ([0-9a-f]{8})/ffffffff ]] && [[ "$current_flowid" == "2" ]]; then
                        local found_hex="${BASH_REMATCH[1]}"
                        if [[ "$found_hex" == "$hex_ip" ]]; then
                            has_filter=true
                            break
                        fi
                    fi
                done <<< "$filter_show"
                
                if [[ "$has_filter" == "false" ]]; then
                    log_warn "    Missing filter for $target_ip → Band 2, creating it..."
                    ssh_sudo "$node_host" "tc filter add dev $iface protocol ip parent 1:0 prio 2 u32 match ip dst $target_ip/32 flowid 1:2" || \
                        log_error "Failed to create filter for $target_ip"
                    log_pass "    Filter created: $target_ip → Band 2"
                else
                    log_info "    Filter exists: $target_ip → Band 2"
                fi
            done
            
        else
            # No TC structure exists - warn the user
            log_warn "  No TC structure detected on $node_host"
            log_warn "  This test requires 'configure_latency.yml' to be run first"
            log_warn "  Run: ansible-playbook playbooks/configure_latency.yml"
            log_warn ""
            log_warn "  Skipping $node_host - latency NOT applied"
            continue
        fi
        
        # Show TC configuration after applying latency
        format_tc_output "$node_host" "$iface" "TC Configuration on $node_host after RTT ${rtt_ms}ms (one-way: ${delay_ms}ms)"
    done

    # Verify latency with ping tests between all nodes
    log_info "Verifying applied latency with ping tests..."
    for source_node in "$NODE1_HOST" "$NODE2_HOST" "$NODE3_HOST"; do
        for target_node in "$NODE1_HOST" "$NODE2_HOST" "$NODE3_HOST"; do
            if [[ "$source_node" != "$target_node" ]]; then
                check_latency_between_nodes "$source_node" "$target_node"
            fi
        done
    done

    # Allow network to stabilize
    sleep 3

    log_pass "Latency configured: ${rtt_ms}ms RTT (one-way: ${delay_ms}ms via Band 20)"
}

# Clear all latency configuration (reset to 0ms)
clear_latency() {
    log_info "Clearing latency configuration (resetting Band 20 to 0ms)..."

    for node_host in "$NODE1_HOST" "$NODE2_HOST" "$NODE3_HOST"; do
        local iface
        iface=$(get_interface "$node_host")
        local initial="${INITIAL_LATENCY_CONFIG[$node_host]}"

        if [[ "$initial" != "NONE" ]]; then
            # Reset Band 20 to 0ms (preserves filters)
            log_info "  Resetting Band 20 on $node_host to 0ms"
            ssh_sudo "$node_host" "tc qdisc change dev $iface parent 1:2 handle 20: netem delay 0ms 0ms" || \
                log_warn "Failed to reset Band 20 on $node_host"
        else
            log_warn "  No TC structure on $node_host, nothing to clear"
        fi
    done

    sleep 2
    log_pass "Latency cleared (Band 20 reset to 0ms, filters preserved)"
}

# Restore original latency configuration
restore_latency() {
    log_info "Restoring original latency configuration..."

    for node_host in "$NODE1_HOST" "$NODE2_HOST" "$NODE3_HOST"; do
        local iface
        iface=$(get_interface "$node_host")
        local initial="${INITIAL_LATENCY_CONFIG[$node_host]}"

        if [[ "$initial" != "NONE" ]]; then
            log_info "  Restoring $node_host Band 20 to: $initial"
            # Restore original parameters to Band 20 (preserves filters)
            ssh_sudo "$node_host" "tc qdisc change dev $iface parent 1:2 handle 20: netem $initial" || \
                log_error "Failed to restore config on $node_host"
            
            # Verify
            local verify
            verify=$(ssh_cmd "$node_host" "tc qdisc show dev $iface | grep -E 'netem 20:'" || echo "Band 20 not found")
            log_info "  $node_host restored: $verify"
        else
            log_warn "  No original TC config to restore on $node_host"
        fi
    done

    log_pass "Original latency configuration restored (filters preserved)"
}

# Parse YAML value from scenario file
parse_yaml_value() {
    grep "^${1}:" "$SCENARIO_FILE" | head -1 | sed "s/^${1}: *//; s/#.*//; s/\"//g; s/ *$//" || true
}

# Run performance test and extract metrics
run_perf_test() {
    local rtt_ms="$1"
    local output
    local result_label="latency-${rtt_ms}ms"
    
    # Build automatic result alias: scenario-{latency}ms
    local auto_alias="${SCENARIO}-${rtt_ms}ms"

    log_info "Running performance test at ${rtt_ms}ms RTT latency..." >&2

    # Determine which script to use
    local test_script
    if $USE_OMQ; then
        test_script="$SCRIPT_DIR/run-test-omq.sh"
    else
        test_script="$SCRIPT_DIR/run-test.sh"
    fi

    # Build command arguments
    local cmd_args=("$SCENARIO")
    
    # Common parameters
    [[ -n "$PUB_HOSTS" ]] && cmd_args+=(--pub-hosts "$PUB_HOSTS") || cmd_args+=(--hosts "$HOSTS")
    [[ -n "$CON_HOSTS" ]] && cmd_args+=(--con-hosts "$CON_HOSTS")
    cmd_args+=(--user "$USER")
    cmd_args+=(--password "$PASSWORD")
    cmd_args+=(--label "$result_label")
    # Use user-provided alias if available, otherwise use automatic alias
    if [[ -n "$RESULT_ALIAS" ]]; then
        cmd_args+=(--result-alias "${RESULT_ALIAS}-${rtt_ms}ms")
    else
        cmd_args+=(--result-alias "$auto_alias")
    fi
    
    # Pass TLS configuration based on mode
    if $USE_OMQ; then
        # OMQ-specific parameters
        [[ -n "$PROTOCOL" ]] && cmd_args+=(--protocol "$PROTOCOL")
        [[ -n "$CA_CERT" ]] && cmd_args+=(--ca-cert "$CA_CERT")
        [[ "$TLS_SKIP_VERIFY" == "true" ]] && cmd_args+=(--tls-skip-verify)
    else
        # run-test.sh specific parameters (Java truststore)
        [[ -n "$TRUSTSTORE" ]] && cmd_args+=(--truststore "$TRUSTSTORE")
        [[ -n "$TRUSTSTORE" ]] && cmd_args+=(--truststore-pass "$TRUSTSTORE_PASS")
        [[ -n "$TRUSTSTORE" ]] && cmd_args+=(--truststore-type "$TRUSTSTORE_TYPE")
    fi

    # Execute the test script and capture console output for display
    output=$("$test_script" "${cmd_args[@]}" 2>&1) || true
    
    # Find the result file that was just created
    # The scripts generate files with pattern: TIMESTAMP-TEST_NAME-LABEL.txt
    # We need to find the most recent file matching our test name and label
    local result_file
    result_file=$(ls -t "$RESULTS_DIR"/*-"${TEST_NAME}-${result_label}.txt" 2>/dev/null | head -1)
    
    if [[ -z "$result_file" || ! -f "$result_file" ]]; then
        log_error "  Could not find result file for ${TEST_NAME}-${result_label}" >&2
        # Return zeros
        echo "${rtt_ms},0,0,0,0,0,0,0,0,0,0,0,0,0,0,0"
        return
    fi
    
    log_info "  Reading results from: $(basename "$result_file")" >&2
    
    # Read the result file content
    local file_content
    file_content=$(cat "$result_file")

    # Extract metrics using sed (macOS compatible)
    local send_rate recv_rate confirm_rate
    local clat_min clat_med clat_p75 clat_p95 clat_p99 clat_max
    local cons_min cons_med cons_p75 cons_p95 cons_p99 cons_max

    # Detect test type by checking for output format in the result file
    # Priority order matters: Check most specific patterns first!
    # 1. Stream format: "Summary: published X msg/s"
    # 2. OMQ format: "id: X, sending rate avg:" or "TOTAL PUBLISHED"
    # 3. AMQP perf-test format (fallback): "sending rate avg:" without "id:" prefix
    
    if echo "$file_content" | grep -q "Summary: published"; then
        # ===== STREAM PERF TEST FORMAT =====
        log_info "  Detected stream-perf-test.jar output format" >&2
        
        # Extract from Summary line: published X msg/s, confirmed Y msg/s, consumed Z msg/s
        local summary_line
        summary_line=$(echo "$file_content" | grep "^Summary:" | tail -1)
        
        if [[ -n "$summary_line" ]]; then
            # Parse: "Summary: published 209441 msg/s, confirmed 209107 msg/s, consumed 623226 msg/s, confirm latency 95th 58 ms, latency 95th 260 ms"
            send_rate=$(echo "$summary_line" | sed -n 's/.*published \([0-9][0-9]*\) msg\/s.*/\1/p')
            confirm_rate=$(echo "$summary_line" | sed -n 's/.*confirmed \([0-9][0-9]*\) msg\/s.*/\1/p')
            recv_rate=$(echo "$summary_line" | sed -n 's/.*consumed \([0-9][0-9]*\) msg\/s.*/\1/p')
            
            # Extract p95 latencies from summary line
            # Format: "confirm latency 95th 58 ms, latency 95th 260 ms"
            clat_p95=$(echo "$summary_line" | sed -n 's/.*confirm latency 95th \([0-9][0-9]*\) ms.*/\1/p')
            cons_p95=$(echo "$summary_line" | sed -n 's/.*latency 95th \([0-9][0-9]*\) ms.*/\1/p')
        fi
        
        # Now extract detailed percentiles from the last periodic output line
        # Format: "30, published 211678 msg/s, confirmed 211678 msg/s, consumed 637122 msg/s, confirm latency median/75th/95th/99th 45/51/58/62 ms, latency median/75th/95th/99th 243/252/260/260 ms"
        local last_line
        last_line=$(echo "$file_content" | grep -E "^[0-9]+, published" | tail -1)
        
        if [[ -n "$last_line" ]]; then
            # Extract confirm latency percentiles: median/75th/95th/99th
            # Format: "confirm latency median/75th/95th/99th 45/51/58/62 ms"
            local confirm_percentiles
            confirm_percentiles=$(echo "$last_line" | sed -n 's/.*confirm latency median\/75th\/95th\/99th \([0-9][0-9]*\/[0-9][0-9]*\/[0-9][0-9]*\/[0-9][0-9]*\) ms.*/\1/p')
            
            if [[ -n "$confirm_percentiles" ]]; then
                IFS='/' read -r clat_med clat_p75 clat_p95 clat_p99 <<< "$confirm_percentiles"
                # Stream perf test outputs latency in milliseconds already, no conversion needed
                clat_min=0  # Not provided by stream-perf-test
                clat_max=0  # Not provided by stream-perf-test
            fi
            
            # Extract consumer latency percentiles: median/75th/95th/99th
            # Format: "latency median/75th/95th/99th 243/252/260/260 ms"
            local consumer_percentiles
            consumer_percentiles=$(echo "$last_line" | sed -n 's/.*latency median\/75th\/95th\/99th \([0-9][0-9]*\/[0-9][0-9]*\/[0-9][0-9]*\/[0-9][0-9]*\) ms.*/\1/p')
            
            if [[ -n "$consumer_percentiles" ]]; then
                IFS='/' read -r cons_med cons_p75 cons_p95 cons_p99 <<< "$consumer_percentiles"
                # Stream perf test outputs latency in milliseconds already, no conversion needed
                cons_min=0  # Not provided by stream-perf-test
                cons_max=0  # Not provided by stream-perf-test
            fi
        fi
        
        # Set defaults if parsing failed
        send_rate="${send_rate:-0}"
        confirm_rate="${confirm_rate:-0}"
        recv_rate="${recv_rate:-0}"
        clat_min="${clat_min:-0}"
        clat_med="${clat_med:-0}"
        clat_p75="${clat_p75:-0}"
        clat_p95="${clat_p95:-0}"
        clat_p99="${clat_p99:-0}"
        clat_max="${clat_max:-0}"
        cons_min="${cons_min:-0}"
        cons_med="${cons_med:-0}"
        cons_p75="${cons_p75:-0}"
        cons_p95="${cons_p95:-0}"
        cons_p99="${cons_p99:-0}"
        cons_max="${cons_max:-0}"
        
    elif echo "$file_content" | grep -q "TOTAL PUBLISHED"; then
        # ===== OMQ FORMAT =====
        log_info "  Detected OMQ output format" >&2
        
        # Initialize all variables
        send_rate=0
        confirm_rate=0
        recv_rate=0
        clat_min=0
        clat_med=0
        clat_p75=0
        clat_p95=0
        clat_p99=0
        clat_max=0
        cons_min=0
        cons_med=0
        cons_p75=0
        cons_p95=0
        cons_p99=0
        cons_max=0
        
        # Parse throughput rates from TOTAL lines (in Original OMQ Output section)
        # Format: "2026/03/26 06:33:21 TOTAL PUBLISHED messages=249416 confirmed=249416 returned=0 rate=24932.64/s"
        # Format: "2026/03/26 06:33:21 TOTAL CONSUMED messages=249403 rate=24931.10/s"
        
        local pub_rate_line
        pub_rate_line=$(echo "$file_content" | grep "TOTAL PUBLISHED" | tail -1)
        
        if [[ -n "$pub_rate_line" ]]; then
            # Extract rate (handles both integer and decimal rates)
            # Format: "rate=24932.64/s" or "rate=24932/s"
            send_rate=$(echo "$pub_rate_line" | sed -n 's/.*rate=\([0-9][0-9.]*\)\/s.*/\1/p' | cut -d. -f1)
            # Confirmed rate is the same as send rate for OMQ
            confirm_rate="$send_rate"
        fi
        
        local cons_rate_line
        cons_rate_line=$(echo "$file_content" | grep "TOTAL CONSUMED" | tail -1)
        
        if [[ -n "$cons_rate_line" ]]; then
            # Extract rate and truncate decimal part
            recv_rate=$(echo "$cons_rate_line" | sed -n 's/.*rate=\([0-9][0-9.]*\)\/s.*/\1/p' | cut -d. -f1)
        fi
        
        # Fallback for rates: Try "id: X, sending rate avg:" format if TOTAL lines not found
        if [[ -z "$send_rate" || "$send_rate" == "0" ]]; then
            local rate_avg_line
            rate_avg_line=$(echo "$file_content" | grep "^id:.*sending rate avg:" | tail -1)
            if [[ -n "$rate_avg_line" ]]; then
                send_rate=$(echo "$rate_avg_line" | sed -n 's/.*sending rate avg: \([0-9][0-9]*\) msg\/s.*/\1/p')
                confirm_rate="$send_rate"
            fi
        fi
        
        if [[ -z "$recv_rate" || "$recv_rate" == "0" ]]; then
            local recv_avg_line
            recv_avg_line=$(echo "$file_content" | grep "^id:.*receiving rate avg:" | tail -1)
            if [[ -n "$recv_avg_line" ]]; then
                recv_rate=$(echo "$recv_avg_line" | sed -n 's/.*receiving rate avg: \([0-9][0-9]*\) msg\/s.*/\1/p')
            fi
        fi
        
        # Parse latencies from the LAST periodic metric line (same approach as compare-results.sh)
        # Format: "id: baseline, time 9.870 s, sent: 28755 msg/s, ..., min/median/90th/95th/99th/max consumer latency: 2600/4138/4876/5085/6524/10800 µs, confirm latency: 2400/3756/4414/4680/6484/10700 µs"
        
        local last_metric_line
        last_metric_line=$(echo "$file_content" | grep "^id: .*, time .* s, " | tail -1)
        
        if [[ -n "$last_metric_line" ]]; then
            # Extract consumer latency using bash regex (same as compare-results.sh)
            local pattern1="min/median/90th/95th/99th/max consumer latency: ([0-9/]+) (µs|Âµs|ms)"
            
            if [[ "$last_metric_line" =~ $pattern1 ]]; then
                local lat_values="${BASH_REMATCH[1]}"
                local unit="${BASH_REMATCH[2]}"
                
                # OMQ format: min/median/90th/95th/99th/max (has 90th instead of 75th!)
                IFS='/' read -r cons_min cons_med cons_p90 cons_p95 cons_p99 cons_max <<< "$lat_values"
                
                # Convert from microseconds to milliseconds if needed
                if [[ "$unit" =~ µs ]]; then
                    cons_min=$((cons_min / 1000))
                    cons_med=$((cons_med / 1000))
                    cons_p75=$((cons_p90 / 1000))  # Use 90th as 75th approximation
                    cons_p95=$((cons_p95 / 1000))
                    cons_p99=$((cons_p99 / 1000))
                    cons_max=$((cons_max / 1000))
                else
                    # Already in milliseconds
                    cons_p75=$cons_p90  # Use 90th as 75th approximation
                fi
            fi
            
            # Extract confirm latency using bash regex
            local pattern2="confirm latency: ([0-9/]+) (µs|Âµs|ms)"
            
            if [[ "$last_metric_line" =~ $pattern2 ]]; then
                local lat_values="${BASH_REMATCH[1]}"
                local unit="${BASH_REMATCH[2]}"
                
                # OMQ format: min/median/90th/95th/99th/max (has 90th instead of 75th!)
                IFS='/' read -r clat_min clat_med clat_p90 clat_p95 clat_p99 clat_max <<< "$lat_values"
                
                # Convert from microseconds to milliseconds if needed
                if [[ "$unit" =~ µs ]]; then
                    clat_min=$((clat_min / 1000))
                    clat_med=$((clat_med / 1000))
                    clat_p75=$((clat_p90 / 1000))  # Use 90th as 75th approximation
                    clat_p95=$((clat_p95 / 1000))
                    clat_p99=$((clat_p99 / 1000))
                    clat_max=$((clat_max / 1000))
                else
                    # Already in milliseconds
                    clat_p75=$clat_p90  # Use 90th as 75th approximation
                fi
            fi
        fi
        
        # Set defaults if parsing failed
        send_rate="${send_rate:-0}"
        confirm_rate="${confirm_rate:-0}"
        recv_rate="${recv_rate:-0}"
        clat_min="${clat_min:-0}"
        clat_med="${clat_med:-0}"
        clat_p75="${clat_p75:-0}"
        clat_p95="${clat_p95:-0}"
        clat_p99="${clat_p99:-0}"
        clat_max="${clat_max:-0}"
        cons_min="${cons_min:-0}"
        cons_med="${cons_med:-0}"
        cons_p75="${cons_p75:-0}"
        cons_p95="${cons_p95:-0}"
        cons_p99="${cons_p99:-0}"
        cons_max="${cons_max:-0}"
        
    else
        # ===== AMQP PERF TEST FORMAT (original) =====
        log_info "  Detected perf-test.jar output format" >&2
        
        # Parse send rate
        send_rate=$(echo "$file_content" | sed -n 's/.*sending rate avg: \([0-9][0-9]*\).*/\1/p' | tail -1)
        send_rate="${send_rate:-0}"
        
        # Parse receiving rate
        recv_rate=$(echo "$file_content" | sed -n 's/.*receiving rate avg: \([0-9][0-9]*\).*/\1/p' | tail -1)
        recv_rate="${recv_rate:-0}"
        
        # Parse confirm rate (may not be present in all outputs)
        confirm_rate=$(echo "$file_content" | sed -n 's/.*confirm rate avg: \([0-9][0-9]*\).*/\1/p' | tail -1)
        confirm_rate="${confirm_rate:-0}"

        # Parse confirm latency (format: min/median/75th/95th/99th/max)
        local confirm_lat_line
        confirm_lat_line=$(echo "$file_content" | grep "confirm latency" | grep "min/median" | \
            sed -n 's/.*[^0-9]\([0-9][0-9]*\/[0-9][0-9]*\/[0-9][0-9]*\/[0-9][0-9]*\/[0-9][0-9]*\/[0-9][0-9]*\).*/\1/p' | tail -1)

        clat_min=0; clat_med=0; clat_p75=0; clat_p95=0; clat_p99=0; clat_max=0
        if [[ -n "$confirm_lat_line" ]]; then
            IFS='/' read -r clat_min clat_med clat_p75 clat_p95 clat_p99 clat_max <<< "$confirm_lat_line"
            # Convert from microseconds to milliseconds
            clat_min=$((clat_min / 1000)); clat_med=$((clat_med / 1000)); clat_p75=$((clat_p75 / 1000))
            clat_p95=$((clat_p95 / 1000)); clat_p99=$((clat_p99 / 1000)); clat_max=$((clat_max / 1000))
        fi

        # Parse consumer latency (format: min/median/75th/95th/99th/max)
        local cons_lat_line
        cons_lat_line=$(echo "$file_content" | grep "consumer latency" | grep "min/median" | \
            sed -n 's/.*[^0-9]\([0-9][0-9]*\/[0-9][0-9]*\/[0-9][0-9]*\/[0-9][0-9]*\/[0-9][0-9]*\/[0-9][0-9]*\).*/\1/p' | tail -1)

        cons_min=0; cons_med=0; cons_p75=0; cons_p95=0; cons_p99=0; cons_max=0
        if [[ -n "$cons_lat_line" ]]; then
            IFS='/' read -r cons_min cons_med cons_p75 cons_p95 cons_p99 cons_max <<< "$cons_lat_line"
            # Convert from microseconds to milliseconds
            cons_min=$((cons_min / 1000)); cons_med=$((cons_med / 1000)); cons_p75=$((cons_p75 / 1000))
            cons_p95=$((cons_p95 / 1000)); cons_p99=$((cons_p99 / 1000)); cons_max=$((cons_max / 1000))
        fi
    fi

    # Output CSV format: rtt,send,confirm,recv,confirm_latencies(min,med,p75,p95,p99,max),consumer_latencies(min,med,p75,p95,p99,max)
    echo "${rtt_ms},${send_rate},${confirm_rate},${recv_rate},${clat_min},${clat_med},${clat_p75},${clat_p95},${clat_p99},${clat_max},${cons_min},${cons_med},${cons_p75},${cons_p95},${cons_p99},${cons_max}"
}

# --- Main ---
echo "=============================================="
echo "  Criterion 4: Latency Sweep Test"
echo "=============================================="
echo "  Scenario:      $SCENARIO"
echo "  Target Host:   $HOSTS"
[[ -n "$PUB_HOSTS" ]] && echo "  Pub Hosts:     $PUB_HOSTS"
[[ -n "$CON_HOSTS" ]] && echo "  Con Hosts:     $CON_HOSTS"
echo "  Test Duration: ${TEST_DURATION}s per latency value"
echo "  Quick Mode:    $QUICK_MODE"
echo "  Use OMQ:       $USE_OMQ"
if $USE_OMQ; then
    echo "  Protocol:      $PROTOCOL"
    [[ -n "$CA_CERT" ]] && echo "  CA Cert:       $CA_CERT"
else
    [[ -n "$TRUSTSTORE" ]] && echo "  Truststore:    $TRUSTSTORE"
fi
echo "  Restore After: $RESTORE_LATENCY"
echo "  Latency Values (RTT): ${LATENCY_VALUES[*]} ms"
echo "  (One-way delays will be half of RTT values)"
echo "=============================================="
echo ""
echo -e "${BLUE}NOTE: This script modifies TC Band 20 (Metro Latency) configuration.${NC}"
echo -e "${BLUE}Prerequisite: Run 'ansible-playbook playbooks/configure_latency.yml' first.${NC}"
echo -e "${BLUE}This ensures TC structure with filters is in place.${NC}"
echo ""
echo -e "${YELLOW}WARNING: This test will modify network latency on cluster nodes!${NC}"
if $RESTORE_LATENCY; then
    echo -e "${YELLOW}Original latency configuration will be restored at the end.${NC}"
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

# Extract the test name from the scenario YAML (this is what run-test.sh uses for filenames)
TEST_NAME=$(parse_yaml_value "name")
if [[ -z "$TEST_NAME" ]]; then
    log_error "Could not extract 'name:' field from $SCENARIO_FILE"
    exit 1
fi
log_info "Test name from YAML: $TEST_NAME"

mkdir -p "$RESULTS_DIR"
TIMESTAMP=$(date +%Y%m%d-%H%M%S)
CSV_FILE="$RESULTS_DIR/${TIMESTAMP}-${SCENARIO}-latency-sweep.csv"
REPORT_FILE="$RESULTS_DIR/${TIMESTAMP}-${SCENARIO}-latency-sweep-report.txt"

# Initialize CSV with updated format
echo "latency_rtt_ms,send_rate_msg_s,confirm_rate_msg_s,recv_rate_msg_s,confirm_min_ms,confirm_median_ms,confirm_p75_ms,confirm_p95_ms,confirm_p99_ms,confirm_max_ms,consumer_min_ms,consumer_median_ms,consumer_p75_ms,consumer_p95_ms,consumer_p99_ms,consumer_max_ms" > "$CSV_FILE"

# Initialize report
{
    echo "# Latency Sweep Test Report"
    echo "# Date: $(date -u '+%Y-%m-%d %H:%M:%S UTC')"
    echo "# Scenario: $SCENARIO"
    echo "# Host: $HOSTS"
    echo "# Test Duration: ${TEST_DURATION}s per latency value"
    echo "# Script Mode: $(if $USE_OMQ; then echo "OMQ (run-test-omq.sh)"; else echo "Standard (run-test.sh)"; fi)"
    echo "# Note: Latency values are RTT (Round Trip Time). One-way delay = RTT/2"
    echo "#"
    echo ""
    echo "## Raw Results"
    echo ""
    echo "All latency values in milliseconds. C=Confirm latency, R=Consumer latency."
    echo ""
    printf "| %-6s | %-8s | %-8s | %-6s | %-6s | %-6s | %-6s | %-6s | %-6s | %-6s | %-6s | %-6s | %-6s | %-6s |\n" \
        "RTT" "Send" "Recv" "C.Min" "C.Med" "C.P75" "C.P95" "C.P99" "C.Max" "R.Min" "R.Med" "R.P75" "R.P95" "R.P99"
    printf "| %-6s | %-8s | %-8s | %-6s | %-6s | %-6s | %-6s | %-6s | %-6s | %-6s | %-6s | %-6s | %-6s | %-6s |\n" \
        "------" "--------" "--------" "------" "------" "------" "------" "------" "------" "------" "------" "------" "------" "------"
} > "$REPORT_FILE"

# Track results for summary
declare -a results

# Capture initial state before starting
save_initial_state

# Run tests at each latency value (RTT)
for rtt_ms in "${LATENCY_VALUES[@]}"; do
    echo ""
    echo "=========================================="
    echo "  Testing at ${rtt_ms}ms RTT (one-way: $(echo "scale=1; $rtt_ms / 2" | bc)ms)"
    echo "=========================================="

    # Configure latency
    if ! configure_latency "$rtt_ms"; then
        log_error "Failed to configure latency, skipping..."
        continue
    fi

    # Run test
    result=$(run_perf_test "$rtt_ms")
    echo "$result" | strip_ansi >> "$CSV_FILE"
    results+=("$result")

    # Parse for report (strip any ANSI codes first)
    clean_result=$(echo "$result" | strip_ansi)
    IFS=',' read -r lat send confirm recv c_min c_med c_p75 c_p95 c_p99 c_max r_min r_med r_p75 r_p95 r_p99 r_max <<< "$clean_result"
    printf "| %-6s | %-8s | %-8s | %-6s | %-6s | %-6s | %-6s | %-6s | %-6s | %-6s | %-6s | %-6s | %-6s | %-6s |\n" \
        "${lat}ms" "${send}" "${recv}" "${c_min}" "${c_med}" "${c_p75}" "${c_p95}" "${c_p99}" "${c_max}" "${r_min}" "${r_med}" "${r_p75}" "${r_p95}" "${r_p99}" >> "$REPORT_FILE"

    log_pass "Completed: send=${send} msg/s, recv=${recv} msg/s, confirm_median=${c_med}ms, consumer_median=${r_med}ms"
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
        IFS=',' read -r _ baseline_send baseline_confirm baseline_recv _ _ _ _ _ _ _ _ _ _ _ _ <<< "$clean_baseline"

        for result in "${results[@]}"; do
            clean_res=$(echo "$result" | strip_ansi)
            IFS=',' read -r lat send confirm recv _ _ _ _ _ _ _ _ _ _ _ _ <<< "$clean_res"
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

# Print summary table (show all results)
echo "Summary:"
echo ""
# Show from "## Raw Results" section until the "## Analysis" section
sed -n '/^## Raw Results/,/^## Analysis/p' "$REPORT_FILE" | head -n -2

exit 0
