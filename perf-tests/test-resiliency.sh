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
#   ./perf-tests/test-resiliency.sh --hosts 192.168.20.200 --only-packet-loss
#
# TLS Usage:
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
ONLY_PACKET_LOSS=false
PRIORITIZE_NODE="node2"
SSH_USER="ansible"
TRUSTSTORE=""
TRUSTSTORE_PASS=""
TRUSTSTORE_TYPE="JKS"
OSS_RABBITMQ=false

# RabbitMQ service name (tanzu-rabbitmq-server by default, rabbitmq-server for OSS)
RMQ_SERVICE="tanzu-rabbitmq-server"

# Systemctl commands (will be updated if --oss-rabbitmq is used)
RMQ_STOP_CMD="systemctl stop tanzu-rabbitmq-server"
RMQ_START_CMD="systemctl start tanzu-rabbitmq-server --no-block"
RMQ_RESTART_CMD="systemctl restart tanzu-rabbitmq-server"
RMQ_STATUS_CMD="systemctl status tanzu-rabbitmq-server"
RMQ_RESET_FAILED_CMD="systemctl reset-failed tanzu-rabbitmq-server"
RMQ_KILL_CMD="systemctl kill -s SIGKILL tanzu-rabbitmq-server"


# Cluster nodes (AZ-Cluster-1)
NODE1_HOST="10.85.10.234"  # node1
NODE2_HOST="10.85.10.235"  # node2
NODE3_HOST="10.85.10.236"  # node3

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
        --only-packet-loss) ONLY_PACKET_LOSS=true; shift ;;
        --prioritize-tests-node) PRIORITIZE_NODE="$2"; shift 2 ;;
        --truststore)      TRUSTSTORE="$2"; shift 2 ;;
        --truststore-pass) TRUSTSTORE_PASS="$2"; shift 2 ;;
        --truststore-type) TRUSTSTORE_TYPE="$2"; shift 2 ;;
        --oss-rabbitmq)    OSS_RABBITMQ=true; shift ;;
        -h|--help)
            echo "Usage: $0 [OPTIONS]"
            echo ""
            echo "Options:"
            echo "  --hosts HOST1[,HOST2,...]     Comma-separated list of RabbitMQ hosts"
            echo "  --user USERNAME               RabbitMQ admin username (default: admin)"
            echo "  --password PASSWORD           RabbitMQ admin password"
            echo "  --ssh-user USERNAME           SSH username for node access (default: ansible)"
            echo "  --skip-chaos                  Skip chaos/network tests (Tests 5-7)"
            echo "  --only-chaos                  Run only chaos/network tests (Tests 5-7)"
            echo "  --only-packet-loss            Run only packet loss test (Test 7)"
            echo "  --prioritize-tests-node NODE  Prioritize node for failure tests (node1|node2|node3, default: node2)"
            echo "  --oss-rabbitmq                Use rabbitmq-server instead of tanzu-rabbitmq-server"
            echo "  --truststore PATH             Path to truststore for TLS connections"
            echo "  --truststore-pass PASSWORD    Truststore password"
            echo "  --truststore-type TYPE        Truststore type (default: JKS)"
            echo "  -h, --help                    Show this help message"
            echo ""
            echo "Examples:"
            echo "  $0 --hosts 192.168.1.10,192.168.1.11,192.168.1.12"
            echo "  $0 --hosts 192.168.1.10 --skip-chaos"
            echo "  $0 --hosts 192.168.1.10 --only-packet-loss"
            echo "  $0 --hosts 192.168.1.10 --prioritize-tests-node node1"
            echo "  $0 --hosts 192.168.1.10 --oss-rabbitmq"
            echo "  $0 --hosts 192.168.1.10 --truststore /path/to/truststore.p12 --truststore-pass mypass"
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
    echo "ℹ️  Using OSS RabbitMQ (rabbitmq-server service)"
else
    echo "ℹ️  Using Tanzu RabbitMQ (tanzu-rabbitmq-server service)"
fi

if $SKIP_CHAOS && $ONLY_CHAOS; then
    echo "Error: Cannot specify both --skip-chaos and --only-chaos"
    exit 1
fi

# Validate prioritize node option
if [[ "$PRIORITIZE_NODE" != "node1" && "$PRIORITIZE_NODE" != "node2" && "$PRIORITIZE_NODE" != "node3" ]]; then
    echo "Error: --prioritize-tests-node must be one of: node1, node2, node3"
    exit 1
fi

# Configure prioritized node variables
case "$PRIORITIZE_NODE" in
    "node1")
        PRIORITY_HOST="$NODE1_HOST"
        OTHER_HOST1="$NODE2_HOST"
        OTHER_HOST2="$NODE3_HOST"
        ;;
    "node2")
        PRIORITY_HOST="$NODE2_HOST"
        OTHER_HOST1="$NODE1_HOST"
        OTHER_HOST2="$NODE3_HOST"
        ;;
    "node3")
        PRIORITY_HOST="$NODE3_HOST"
        OTHER_HOST1="$NODE1_HOST"
        OTHER_HOST2="$NODE2_HOST"
        ;;
esac

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
        *svm4103*) echo "$NODE1_HOST" ;; #CS ENV
        *svm4104*) echo "$NODE2_HOST" ;; #CS ENV
        *svm4105*) echo "$NODE3_HOST" ;; #CS ENV
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

    # Check current TC configuration
    local qdisc_show
    qdisc_show=$(ssh_cmd "$node" "tc qdisc show dev $iface")
    
    # Show current state in a nice format
    format_tc_output "$node" "$iface" "Current TC configuration"

    # Get all cluster node IPs for filter management
    local cluster_ips=("$NODE1_HOST" "$NODE2_HOST" "$NODE3_HOST")
    local target_node_ip="$node"

    # Check for existing netem 20 (Metro/Cross-region latency band)
    if echo "$qdisc_show" | grep -qE "(qdisc netem 20:|handle 20:)"; then
        log_info "  Found existing netem 20 band"
        
        # ALWAYS modify band 20 to add packet loss (consolidating strategy)
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
        log_info "  Adding packet loss to netem 20 band"
        
        # Modify existing qdisc: keep delay, add loss
        ssh_sudo "$node" "tc qdisc change dev $iface parent 1:2 handle 20: netem $delay_params loss $loss_pct" || \
            log_error "Failed to modify existing TC rule"

        # Now check which cluster IPs need filters to route traffic to band 20
        local filter_show
        filter_show=$(ssh_cmd "$node" "tc filter show dev $iface")
        
        # Check specific IPs: NODE1 and NODE3 (excluding the current node)
        local target_ips=("$NODE1_HOST" "$NODE3_HOST")
        
        for ip in "${target_ips[@]}"; do
            if [[ "$ip" != "$target_node_ip" ]]; then
                # Convert IP to hex for matching in tc filter output
                local ip_parts=(${ip//./ })
                local hex_ip=$(printf "%02x%02x%02x%02x" "${ip_parts[0]}" "${ip_parts[1]}" "${ip_parts[2]}" "${ip_parts[3]}")
                
                # Check if this IP has a filter routing to band 20 (flowid 1:2)
                local has_band20_filter=false
                local current_flowid=""
                
                while IFS= read -r line; do
                    # Look for flowid in the filter line
                    if [[ "$line" =~ flowid\ 1:([0-9]+) ]]; then
                        current_flowid="${BASH_REMATCH[1]}"
                    # Look for match in the next line (indented) and check if it's for band 2 (flowid 1:2)
                    elif [[ "$line" =~ ^[[:space:]]+match\ ([0-9a-f]{8})/ffffffff ]] && [[ "$current_flowid" == "2" ]]; then
                        local found_hex_ip="${BASH_REMATCH[1]}"
                        if [[ "$found_hex_ip" == "$hex_ip" ]]; then
                            has_band20_filter=true
                            break
                        fi
                    fi
                done <<< "$filter_show"
                
                if [[ "$has_band20_filter" == "true" ]]; then
                    log_info "  IP $ip already has filter routing to band 20"
                else
                    log_info "  IP $ip missing filter for band 20, creating band 40 and filter"
                    
                    # Create band 40 if it doesn't exist yet (packet loss only, no latency)
                    if ! echo "$qdisc_show" | grep -qE "(qdisc netem 40:|handle 40:)"; then
                        ssh_sudo "$node" "tc qdisc add dev $iface parent 1:4 handle 40: netem loss $loss_pct" || \
                            log_error "Failed to create packet-loss-only band 40"
                        log_info "  Created packet-loss-only band 40"
                        # Update qdisc_show for next iteration
                        qdisc_show=$(ssh_cmd "$node" "tc qdisc show dev $iface")
                    fi
                    
                    # Add temporary filter for this IP to route to band 40 (use prio 10 to avoid conflicts)
                    log_info "  Adding temporary filter for traffic to $ip -> band 40"
                    ssh_sudo "$node" "tc filter add dev $iface protocol ip parent 1: prio 10 u32 match ip dst $ip/32 flowid 1:4" 2>/dev/null || \
                        log_warn "Failed to add filter for traffic to $ip"
                fi
            fi
        done
    
    elif echo "$qdisc_show" | grep -qE "qdisc prio 1:"; then
        log_info "  Found prio qdisc structure, band 40 will be created if needed for specific IPs"
        # Band 40 creation is handled per-IP in the loop above
    
    else
        log_info "  No prio structure found, creating root netem for packet loss"
        
        # No existing structure, try to add root netem
        local tc_output
        tc_output=$(ssh_sudo "$node" "tc qdisc add dev $iface root netem loss $loss_pct 2>&1 || tc qdisc change dev $iface root netem loss $loss_pct 2>&1")
        
        if [[ $? -ne 0 ]]; then
            log_error "Failed to add root TC rule: $tc_output"
            
            # Last resort: try to replace whatever is there
            log_info "  Attempting to replace existing root qdisc"
            ssh_sudo "$node" "tc qdisc del dev $iface root 2>/dev/null || true"
            ssh_sudo "$node" "tc qdisc add dev $iface root netem loss $loss_pct" || \
                log_error "Failed to create any packet loss rule"
        fi
    fi
    
    # Show final configuration in a nice format
    format_tc_output "$node" "$iface" "TC configuration after applying packet loss"
}

# Remove packet loss preserving existing latency
remove_packet_loss() {
    local node="$1"
    local iface="$2"

    log_info "  Removing packet loss on $node ($iface)..."

    local qdisc_show
    qdisc_show=$(ssh_cmd "$node" "tc qdisc show dev $iface")

    # Step 0: Remove ANY orphaned netem rules with packet loss (residuals from previous tests)
    # These can have arbitrary handles like 8001:, 8002:, etc.
    local orphaned_handles=()
    while IFS= read -r line; do
        # Match any netem qdisc with loss that is NOT handle 20: or 40: (our expected ones)
        if [[ "$line" =~ qdisc\ netem\ ([0-9]+):.*loss ]] && \
           [[ "${BASH_REMATCH[1]}" != "20" ]] && \
           [[ "${BASH_REMATCH[1]}" != "40" ]]; then
            orphaned_handles+=("${BASH_REMATCH[1]}")
        fi
    done <<< "$qdisc_show"
    
    if [[ ${#orphaned_handles[@]} -gt 0 ]]; then
        log_info "  Found ${#orphaned_handles[@]} orphaned netem rule(s) with packet loss from previous tests"
        for handle in "${orphaned_handles[@]}"; do
            log_info "  Removing orphaned netem handle ${handle}:"
            
            # Try to determine parent and remove appropriately
            local handle_line
            handle_line=$(echo "$qdisc_show" | grep "qdisc netem ${handle}:")
            
            if echo "$handle_line" | grep -q "root"; then
                # It's a root qdisc, remove it completely
                ssh_sudo "$node" "tc qdisc del dev $iface root 2>/dev/null" || \
                    log_warn "Failed to remove orphaned root netem ${handle}:"
                log_info "  Removed orphaned root netem ${handle}:"
            elif [[ "$handle_line" =~ parent\ 1:([0-9]+) ]]; then
                # It's attached to a prio band
                local parent_band="${BASH_REMATCH[1]}"
                ssh_sudo "$node" "tc qdisc del dev $iface parent 1:${parent_band} handle ${handle}: 2>/dev/null" || \
                    log_warn "Failed to remove orphaned netem ${handle}: from parent 1:${parent_band}"
                log_info "  Removed orphaned netem ${handle}: from band ${parent_band}"
            else
                # Unknown structure, try generic removal
                ssh_sudo "$node" "tc qdisc del dev $iface handle ${handle}: 2>/dev/null" || \
                    log_warn "Failed to remove orphaned netem ${handle}:"
                log_info "  Removed orphaned netem ${handle}:"
            fi
        done
        
        # Refresh qdisc_show after removing orphaned rules
        qdisc_show=$(ssh_cmd "$node" "tc qdisc show dev $iface")
    fi

    # Step 1: Remove temporary band 40 and its filters (if exists)
    if echo "$qdisc_show" | grep -qE "(qdisc netem 40:|handle 40:)"; then
        log_info "  Found temporary packet-loss-only band 40, removing it"
        
        # Remove ONLY the temporary filters that route to flowid 1:4 (band 40)
        local filter_show
        filter_show=$(ssh_cmd "$node" "tc filter show dev $iface")
        
        # Get specific target IPs to remove only their temporary filters
        local target_ips=("$NODE1_HOST" "$NODE3_HOST")
        local target_node_ip="$node"
        
        for ip in "${target_ips[@]}"; do
            if [[ "$ip" != "$target_node_ip" ]]; then
                # Convert IP to hex for matching in tc filter output
                local ip_parts=(${ip//./ })
                local hex_ip=$(printf "%02x%02x%02x%02x" "${ip_parts[0]}" "${ip_parts[1]}" "${ip_parts[2]}" "${ip_parts[3]}")
                
                # Use the same robust parsing approach to check for band 40 filters
                local has_band40_filter=false
                local current_flowid=""
                
                while IFS= read -r line; do
                    # Look for flowid in the filter line
                    if [[ "$line" =~ flowid\ 1:([0-9]+) ]]; then
                        current_flowid="${BASH_REMATCH[1]}"
                    # Look for match in the next line (indented) and check if it's for band 4 (flowid 1:4)
                    elif [[ "$line" =~ ^[[:space:]]+match\ ([0-9a-f]{8})/ffffffff ]] && [[ "$current_flowid" == "4" ]]; then
                        local found_hex_ip="${BASH_REMATCH[1]}"
                        if [[ "$found_hex_ip" == "$hex_ip" ]]; then
                            has_band40_filter=true
                            break
                        fi
                    fi
                done <<< "$filter_show"
                
                if [[ "$has_band40_filter" == "true" ]]; then
                    log_info "  Removing temporary filter for $ip -> band 40"
                    # Remove this specific filter by matching the destination IP (using prio 10)
                    ssh_sudo "$node" "tc filter del dev $iface protocol ip parent 1: prio 10 u32 match ip dst $ip/32 flowid 1:4 2>/dev/null" || \
                        log_warn "Failed to remove temporary filter for $ip"
                fi
            fi
        done
        
        # Remove the temporary packet-loss-only band
        ssh_sudo "$node" "tc qdisc del dev $iface parent 1:4 handle 40: 2>/dev/null" || \
            log_warn "Failed to remove netem band 40"
        
        log_info "  Removed temporary band 40 and its specific filters"
    fi
    
    # Step 2: Restore band 20 to original state (remove packet loss, keep latency)
    if echo "$qdisc_show" | grep -qE "(qdisc netem 20:|handle 20:)"; then
        local band_line
        band_line=$(echo "$qdisc_show" | grep -oP '(qdisc|class) netem 20:.*')
        
        if echo "$band_line" | grep -q "loss"; then
            log_info "  Found netem 20 with packet loss, restoring original config"
            
            # Extract existing delay parameters, remove loss
            local delay_params
            delay_params=$(echo "$band_line" | grep -oP 'delay [0-9.]+(ms|us)(\s+[0-9.]+(ms|us))?')
            
            if [[ -n "$delay_params" ]]; then
                log_info "  Restoring latency config: $delay_params"
                
                # Modify qdisc: restore delay, remove loss
                ssh_sudo "$node" "tc qdisc change dev $iface parent 1:2 handle 20: netem $delay_params" || \
                    log_warn "Failed to restore original netem 20 config"
            else
                log_info "  Removing packet loss from band 20 (no delay to restore)"
                ssh_sudo "$node" "tc qdisc change dev $iface parent 1:2 handle 20: netem" || \
                    log_warn "Failed to clean netem 20 config"
            fi
            
            log_info "  Restored band 20 to original state (existing filters preserved)"
        else
            log_info "  Band 20 found but no packet loss configured"
        fi
    fi
    
    # Step 3: Handle root netem configurations (fallback cases)
    if echo "$qdisc_show" | grep -q "root.*netem.*loss"; then
        log_info "  Found root netem with packet loss, removing it"
        
        # Check if there was any original configuration we should restore
        # For now, just remove the root netem entirely
        ssh_sudo "$node" "tc qdisc del dev $iface root 2>/dev/null" || \
            log_warn "Failed to remove root netem rule"
        
        log_info "  Removed root packet loss rule"
    fi
    
    # Show final state after cleanup in a nice format
    format_tc_output "$node" "$iface" "TC configuration after cleanup"
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

# Helper function to format TC output nicely
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
            local flowid_num=$((band_num / 10))  # Convert handle to flowid (20->2, 30->3, 40->4)
            log_info "    📊 Band $flowid_num: ${BASH_REMATCH[2]} delay + ${BASH_REMATCH[3]} loss"
        elif [[ "$line" =~ qdisc\ netem\ ([0-9]+):.*delay\ ([0-9.]+[a-z]+) ]]; then
            local band_num="${BASH_REMATCH[1]}"
            local flowid_num=$((band_num / 10))  # Convert handle to flowid (20->2, 30->3, 40->4)
            log_info "    ⏱️  Band $flowid_num: ${BASH_REMATCH[2]} delay"
        elif [[ "$line" =~ qdisc\ netem\ ([0-9]+):.*loss\ ([0-9]+%) ]]; then
            local band_num="${BASH_REMATCH[1]}"
            local flowid_num=$((band_num / 10))  # Convert handle to flowid (20->2, 30->3, 40->4)
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
            current_flowid=""  # Reset for next filter
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

# Check packet loss to a node
check_packet_loss() {
    local target_ip="$1"
    local count=100
    
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

# Check packet loss from NODE2 to a target
check_packet_loss_from_node2() {
    local target_ip="$1"
    local count=100
    
    log_info "  Checking packet loss to $target_ip (from $NODE2_HOST)..."
    local ping_output
    # Ping FROM Node 2 TO target via SSH
    ping_output=$(ssh_cmd "$NODE2_HOST" "ping -c $count -i 0.1 $target_ip" 2>&1)
    
    local loss
    loss=$(echo "$ping_output" | grep -oP '\d+(?=% packet loss)')
    
    if [[ -z "$loss" ]]; then
         # Fallback for systems without grep -P or different ping output
         loss=$(echo "$ping_output" | grep "packet loss" | awk '{print $6}' | tr -d '%')
    fi
    
    log_info "  Measured packet loss to $target_ip: ${loss}%"
    return 0
}

# Check packet loss from prioritized node to a target
check_packet_loss_from_prioritized_node() {
    local target_ip="$1"
    local count=100
    
    log_info "  Checking packet loss to $target_ip (from $PRIORITY_HOST)..."
    local ping_output
    # Ping FROM prioritized node TO target via SSH
    ping_output=$(ssh_cmd "$PRIORITY_HOST" "ping -c $count -i 0.1 $target_ip" 2>&1)
    
    local loss
    loss=$(echo "$ping_output" | grep -oP '\d+(?=% packet loss)')
    
    if [[ -z "$loss" ]]; then
         # Fallback for systems without grep -P or different ping output
         loss=$(echo "$ping_output" | grep "packet loss" | awk '{print $6}' | tr -d '%')
    fi
    
    log_info "  Measured packet loss to $target_ip: ${loss}%"
    return 0
}

# Force restart a node (nuclear option)
force_restart_node() {
    local ip="$1"
    log_warn "  Force restarting node $ip..."
    
    # Ensure no stale network partition rules exist
    ssh_sudo "$ip" "iptables -D INPUT -j DROP 2>/dev/null || true"
    ssh_sudo "$ip" "iptables -D OUTPUT -j DROP 2>/dev/null || true"
    
    ssh_sudo "$ip" "$RMQ_STOP_CMD || true"
    ssh_sudo "$ip" "pkill -9 beam.smp || true"
    ssh_sudo "$ip" "pkill -9 epmd || true"
    ssh_sudo "$ip" "rm -f /var/lib/rabbitmq/mnesia/rabbit@*/cluster_nodes.config.tmp || true" # Clean temp config if stuck
    ssh_sudo "$ip" "$RMQ_RESET_FAILED_CMD || true"
    
    sleep 5 # Give OS time to clean up sockets
    
    ssh_sudo "$ip" "$RMQ_START_CMD" || true
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
    ssh_sudo "$leader_ip" "$RMQ_KILL_CMD 2>/dev/null || pkill -9 beam.smp" || true

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
    log_info "Test 2: Message durability through node failure (prioritizing $PRIORITIZE_NODE)"

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

    # Check if prioritized node is the leader
    local leader
    leader=$(get_quorum_leader "$queue")
    local leader_ip
    leader_ip=$(node_to_ip "$leader")
    
    if [[ "$leader_ip" == "$PRIORITY_HOST" ]]; then
        log_info "  Prioritized node ($PRIORITIZE_NODE) is the leader, forcing leader election first..."
        # Stop the leader to force election
        ssh_sudo "$PRIORITY_HOST" "$RMQ_STOP_CMD" || true
        sleep 5
        
        # Wait for new leader election
        local new_leader
        new_leader=$(wait_for_leader "$queue" "$leader" 60)
        if [[ "$new_leader" == "timeout" ]]; then
            log_error "Leader election timed out"
            force_restart_node "$PRIORITY_HOST"
            return 1
        fi
        log_info "  New leader elected: $new_leader, restarting prioritized node..."
        
        # Restart the prioritized node
        force_restart_node "$PRIORITY_HOST"
        wait_for_nodes 3 300 || true
        sleep 5
    fi

    # Now stop the prioritized node (which should be a follower)
    log_info "  Stopping prioritized node ($PRIORITIZE_NODE) at $PRIORITY_HOST..."
    ssh_sudo "$PRIORITY_HOST" "$RMQ_STOP_CMD" || true
    sleep 5

    # Verify messages still accessible
    local during_messages
    if ! during_messages=$(wait_for_queue_messages "$queue" 500); then
        log_warn "Message count mismatch during failure ($during_messages)"
    fi

    # Restart node
    log_info "  Restarting node..."
    force_restart_node "$PRIORITY_HOST"
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

test_message_durability_streams() {
    log_info "Test 3: Message durability through node failure (Streams)"

    local queue="resiliency-test-durability-stream"
    
    # Ensure queue is clean before starting
    curl -sf -k -X DELETE -u "${USER}:${PASSWORD}" "${MGMT_URL}/api/queues/%2F/${queue}" > /dev/null 2>&1 || true

    local expected_messages=500

    # Publish messages to stream with confirms (ensures durability)
    log_info "  Publishing durable messages to stream..."
    java $JVM_OPTS -jar "$TOOLS_DIR/perf-test.jar" \
        --uris "$AMQP_URIS" \
        --queue "$queue" \
        --stream-queue \
        --producers 1 \
        --consumers 0 \
        --pmessages "$expected_messages" \
        --confirm 1 \
        --size 5000 \
        --id "durability-stream-pub" > /dev/null 2>&1

    # Wait for messages to be visible in API
    sleep 2

    local initial_messages
    initial_messages=$(wait_for_queue_messages "$queue" 500)
    log_info "  Published $initial_messages messages (expected $expected_messages)"

    # Stop a node (streams don't have leader concept like quorum queues, so pick NODE2)
    local target_ip="$NODE2_HOST"
    
    log_info "  Stopping node at $target_ip..."
    ssh_sudo "$target_ip" "$RMQ_STOP_CMD" || true
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

    # Consume all messages from stream
    log_info "  Consuming messages from stream..."
    local consume_output
    consume_output=$(java $JVM_OPTS -jar "$TOOLS_DIR/perf-test.jar" \
        --uris "$AMQP_URIS" \
        --queue "$queue" \
        --predeclared \
        --producers 0 \
        --consumers 1 \
        --cmessages "$expected_messages" \
        --qos 100 \
        --stream-consumer-offset "first" \
        --id "durability-stream-con" 2>&1) || true
    
    # Check if consumption failed
    if [[ $? -ne 0 ]]; then
         log_warn "Consumption command failed (exit code $?)"
         echo "$consume_output" | head -n 5
    fi

    sleep 2
    # For streams, messages remain in the stream after consumption
    # So we check that the stream still contains the expected messages
    local final_messages
    if ! final_messages=$(wait_for_queue_messages "$queue" "$expected_messages"); then
        log_warn "Stream message count unexpected ($final_messages vs expected $expected_messages)"
    fi
    
    log_info "  Final stream message count: $final_messages"
    if [[ "$during_messages" -ge "$expected_messages" && "$final_messages" -ge "$expected_messages" ]]; then
        log_pass "Stream message durability verified ($during_messages messages survived failure, $final_messages remain in stream)"
        return 0
    else
        log_error "Stream durability issue: expected=$expected_messages, during=$during_messages, final=$final_messages"
        return 1
    fi
}

test_cluster_recovery() {
    log_info "Test 4: Cluster recovery after node restart (prioritizing $PRIORITIZE_NODE)"

    # Get initial state
    local initial_nodes
    initial_nodes=$(get_running_nodes)

    # Stop the prioritized node gracefully
    log_info "  Stopping prioritized node ($PRIORITIZE_NODE) at $PRIORITY_HOST..."
    ssh_sudo "$PRIORITY_HOST" "$RMQ_STOP_CMD" || true
    sleep 5

    local during_nodes
    during_nodes=$(get_running_nodes)
    log_info "  Running nodes during failure: $during_nodes"

    # Restart node
    log_info "  Restarting node..."
    force_restart_node "$PRIORITY_HOST"

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
    log_info "Test 5: Network partition handling (prioritizing $PRIORITIZE_NODE)"

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

    # Determine isolation target based on prioritized node
    local isolation_target
    if [[ "$PRIORITIZE_NODE" == "node1" ]]; then
        isolation_target="$NODE2_HOST"
    else
        isolation_target="$NODE1_HOST"
    fi

    # Simulate network partition using iptables (isolate prioritized node from target)
    log_info "  Simulating partial network partition (isolating $PRIORITY_HOST from $isolation_target)..."
    ssh_sudo "$PRIORITY_HOST" "iptables -A INPUT -s $isolation_target -j DROP" || true
    ssh_sudo "$PRIORITY_HOST" "iptables -A OUTPUT -d $isolation_target -j DROP" || true

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
    ssh_sudo "$PRIORITY_HOST" "iptables -D INPUT -s $isolation_target -j DROP" 2>/dev/null || true
    ssh_sudo "$PRIORITY_HOST" "iptables -D OUTPUT -d $isolation_target -j DROP" 2>/dev/null || true

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
    log_info "Test 6: Split Brain (Total Node Isolation) (prioritizing $PRIORITIZE_NODE)"

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

    # Simulate TOTAL network partition (Isolate prioritized node from everyone)
    log_info "  Simulating Split Brain (Isolating $PRIORITY_HOST from everyone)..."
    ssh_sudo "$PRIORITY_HOST" "iptables -A INPUT -s $OTHER_HOST1 -j DROP" || true
    ssh_sudo "$PRIORITY_HOST" "iptables -A OUTPUT -d $OTHER_HOST1 -j DROP" || true
    ssh_sudo "$PRIORITY_HOST" "iptables -A INPUT -s $OTHER_HOST2 -j DROP" || true
    ssh_sudo "$PRIORITY_HOST" "iptables -A OUTPUT -d $OTHER_HOST2 -j DROP" || true

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
    ssh_sudo "$PRIORITY_HOST" "iptables -D INPUT -s $OTHER_HOST1 -j DROP" 2>/dev/null || true
    ssh_sudo "$PRIORITY_HOST" "iptables -D OUTPUT -d $OTHER_HOST1 -j DROP" 2>/dev/null || true
    ssh_sudo "$PRIORITY_HOST" "iptables -D INPUT -s $OTHER_HOST2 -j DROP" 2>/dev/null || true
    ssh_sudo "$PRIORITY_HOST" "iptables -D OUTPUT -d $OTHER_HOST2 -j DROP" 2>/dev/null || true

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
    log_info "Test 7: Packet loss resilience (prioritizing $PRIORITIZE_NODE)"

    # Introduce 5% packet loss on prioritized node
    log_info "  Baseline throughput check (no packet loss)..."
    local queue="resiliency-packet-loss"
    
    # Ensure queue is clean before starting
    curl -sf -k -X DELETE -u "${USER}:${PASSWORD}" "${MGMT_URL}/api/queues/%2F/${queue}" > /dev/null 2>&1 || true

    # Baseline ping from prioritized node to the other two
    log_info "  Measuring baseline packet loss from $PRIORITIZE_NODE..."
    check_packet_loss_from_prioritized_node "$OTHER_HOST1"
    check_packet_loss_from_prioritized_node "$OTHER_HOST2"

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

    log_info "  Introducing 5% packet loss on $PRIORITY_HOST..."
    
    # Dynamically detect main interface (prefer ens33)
    local iface
    if ssh_cmd "$PRIORITY_HOST" "ip link show ens33 >/dev/null 2>&1"; then
        iface="ens33"
    else
        iface=$(ssh_cmd "$PRIORITY_HOST" "ip route | grep default | awk '{print \$5}' | head -n 1")
    fi
    
    if [[ -z "$iface" ]]; then
        log_warn "Could not detect network interface, defaulting to eth0"
        iface="eth0"
    else
        log_info "  Target interface: $iface"
    fi

    # Introduce 5% packet loss on prioritized node (for outgoing traffic to other nodes)
    apply_packet_loss "$PRIORITY_HOST" "$iface"

    # Verify packet loss with ping from prioritized node to the other two
    log_info "  Measuring packet loss from $PRIORITIZE_NODE..."
    check_packet_loss_from_prioritized_node "$OTHER_HOST1"
    check_packet_loss_from_prioritized_node "$OTHER_HOST2"

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
    remove_packet_loss "$PRIORITY_HOST" "$iface"

    # Final ping check from prioritized node
    log_info "  Verifying packet loss removal from $PRIORITIZE_NODE..."
    check_packet_loss_from_prioritized_node "$OTHER_HOST1"
    check_packet_loss_from_prioritized_node "$OTHER_HOST2"

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
echo "  Target Hosts: $HOSTS"
echo "  Skip Chaos:  $SKIP_CHAOS"
echo "  Only Chaos:  $ONLY_CHAOS"
echo "  Only Packet Loss: $ONLY_PACKET_LOSS"
echo "=============================================="

# Display test execution plan
if $ONLY_PACKET_LOSS; then
    echo "🎯 Running only Test 7: Packet loss resilience"
elif $ONLY_CHAOS; then
    echo "🌪️  Running only chaos tests (Tests 5-7)"
elif $SKIP_CHAOS; then
    echo "🛡️  Running base tests only (Tests 1-4, skipping chaos)"
else
    echo "🔄 Running all tests (Tests 1-7)"
fi
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
    echo "# Only Packet Loss: $ONLY_PACKET_LOSS"
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
    "test_message_durability_streams"
    "test_cluster_recovery"
)

TESTS_CHAOS=(
    "test_network_partition"
    "test_split_brain_partition"
    "test_packet_loss_resilience"
)

TESTS_TO_RUN=()

if $ONLY_PACKET_LOSS; then
    TESTS_TO_RUN=("test_packet_loss_resilience")
elif $ONLY_CHAOS; then
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
