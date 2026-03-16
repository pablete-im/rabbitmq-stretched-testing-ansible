#!/bin/bash
# =============================================================================
# Cleanup Test Queues
#
# Removes queues created by the performance test scripts.
# Can target specific test patterns or clean all test queues.
#
# Usage:
#   ./perf-tests/cleanup-test-queues.sh                    # Clean all test queues
#   ./perf-tests/cleanup-test-queues.sh --pattern "warm-*" # Clean specific pattern
#   ./perf-tests/cleanup-test-queues.sh --dry-run          # Show what would be deleted
#   ./perf-tests/cleanup-test-queues.sh --all-clusters     # Clean all 4 clusters
#
# TLS Usage:
#   ./perf-tests/cleanup-test-queues.sh --truststore /path/to/truststore.p12 --truststore-pass mypass
#   ./perf-tests/cleanup-test-queues.sh --truststore /path/to/truststore.p12 --truststore-pass mypass
# =============================================================================
set -euo pipefail

# Cluster endpoints
UPSTREAM_HOST="10.85.10.234"       # AZ-Cluster-1
REGIONAL_STANDBY="10.85.10.241"    # AZ-Cluster-2
CROSS_REGION_DR1="10.85.10.244"    # TX-Cluster-1
#CROSS_REGION_DR2="192.168.20.209"    # TX-Cluster-2

USER="admin"
PASSWORD=""
DRY_RUN=false
ALL_CLUSTERS=false
INCLUDE_EXCHANGES=false
PATTERN=""
TRUSTSTORE=""
TRUSTSTORE_PASS=""
TRUSTSTORE_TYPE="JKS"

# Test queue/exchange patterns to clean
TEST_PATTERNS=(
    "^core-test-"
    "^resiliency-"
    "^warm-standby-"
    "^lag-test-"
    "^promotion-test-"
    "^perf-test-"
    "^manual-test-"
    "^fanout-test"
    "^baseline$"
    "^latency-sweep-"
    "^wsr-stream-latency.*"
    "^test-.*"
    "^stream-.*"
    "^streams.*"
)

# Parse arguments
while [[ $# -gt 0 ]]; do
    case "$1" in
        --user)              USER="$2"; shift 2 ;;
        --password)          PASSWORD="$2"; shift 2 ;;
        --dry-run)           DRY_RUN=true; shift ;;
        --all-clusters)      ALL_CLUSTERS=true; shift ;;
        --include-exchanges) INCLUDE_EXCHANGES=true; shift ;;
        --pattern)           PATTERN="$2"; shift 2 ;;
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
else
    echo "🔓 Non-TLS Mode: Using standard connections"
    MGMT_PROTOCOL="http"
    MGMT_PORT="15672"
fi

# Build list of hosts to clean
HOSTS=("$UPSTREAM_HOST")
if $ALL_CLUSTERS; then
    #HOSTS+=("$REGIONAL_STANDBY" "$CROSS_REGION_DR1" "$CROSS_REGION_DR2")
    HOSTS+=("$REGIONAL_STANDBY" "$CROSS_REGION_DR1" )
fi

# Function to check if resource name matches test patterns
is_test_resource() {
    local name="$1"

    # Skip internal RabbitMQ queues
    if [[ "$name" == rabbitmq.internal.* ]]; then
        return 1
    fi

    # Skip internal exchanges
    if [[ "$name" == amqp.* ]]; then
        return 1
    fi

    # If specific pattern provided, use only that
    if [[ -n "$PATTERN" ]]; then
        if [[ "$name" =~ $PATTERN ]]; then
            return 0
        fi
        return 1
    fi

    # Check against all test patterns
    for pattern in "${TEST_PATTERNS[@]}"; do
        if [[ "$name" =~ $pattern ]]; then
            return 0
        fi
    done
    return 1
}

# Function to delete a queue
delete_queue() {
    local host="$1"
    local queue="$2"
    local encoded_queue
    encoded_queue=$(python3 -c "import urllib.parse; print(urllib.parse.quote('$queue', safe=''))")

    if $DRY_RUN; then
        echo "  [DRY-RUN] Would delete queue: $queue"
        return 0
    fi

    local status
    status=$(curl -sf -k -o /dev/null -w "%{http_code}" -X DELETE \
        -u "${USER}:${PASSWORD}" \
        "${MGMT_PROTOCOL}://${host}:${MGMT_PORT}/api/queues/%2F/${encoded_queue}" 2>/dev/null || echo "000")

    if [[ "$status" == "204" ]] || [[ "$status" == "200" ]]; then
        echo "  Deleted queue: $queue"
        return 0
    else
        echo "  Failed to delete queue: $queue (HTTP $status)"
        return 1
    fi
}

# Function to delete an exchange
delete_exchange() {
    local host="$1"
    local exchange="$2"
    local encoded_exchange
    encoded_exchange=$(python3 -c "import urllib.parse; print(urllib.parse.quote('$exchange', safe=''))")

    if $DRY_RUN; then
        echo "  [DRY-RUN] Would delete exchange: $exchange"
        return 0
    fi

    local status
    status=$(curl -sf -k -o /dev/null -w "%{http_code}" -X DELETE \
        -u "${USER}:${PASSWORD}" \
        "${MGMT_PROTOCOL}://${host}:${MGMT_PORT}/api/exchanges/%2F/${encoded_exchange}" 2>/dev/null || echo "000")

    if [[ "$status" == "204" ]] || [[ "$status" == "200" ]]; then
        echo "  Deleted exchange: $exchange"
        return 0
    else
        echo "  Failed to delete exchange: $exchange (HTTP $status)"
        return 1
    fi
}

# Main cleanup
echo "=============================================="
echo "  Test Queue Cleanup"
echo "=============================================="
if $DRY_RUN; then
    echo "  Mode: DRY RUN (no changes will be made)"
fi
if [[ -n "$PATTERN" ]]; then
    echo "  Pattern: $PATTERN"
else
    echo "  Patterns: ${TEST_PATTERNS[*]}"
fi
echo "=============================================="
echo ""

total_deleted=0
total_failed=0

for host in "${HOSTS[@]}"; do
    echo "Checking $host..."

    # Get all queues
    queues=$(curl -sf -k -u "${USER}:${PASSWORD}" "${MGMT_PROTOCOL}://${host}:${MGMT_PORT}/api/queues" 2>/dev/null | \
        python3 -c "import sys,json; [print(q['name']) for q in json.load(sys.stdin)]" 2>/dev/null) || {
        echo "  Failed to connect to $host"
        continue
    }

    host_deleted=0
    while IFS= read -r queue; do
        [[ -z "$queue" ]] && continue

        if is_test_resource "$queue"; then
            if delete_queue "$host" "$queue"; then
                ((host_deleted+=1))
                ((total_deleted+=1))
            else
                ((total_failed+=1))
            fi
        fi
    done <<< "$queues"

    if $INCLUDE_EXCHANGES; then
        echo "  Scanning exchanges..."
        exchanges=$(curl -sf -k -u "${USER}:${PASSWORD}" "${MGMT_PROTOCOL}://${host}:${MGMT_PORT}/api/exchanges" 2>/dev/null | \
            python3 -c "import sys,json; [print(e['name']) for e in json.load(sys.stdin) if e['name']]" 2>/dev/null) || {
            echo "  Failed to list exchanges on $host"
        }

        while IFS= read -r exchange; do
            [[ -z "$exchange" ]] && continue
            
            # Skip internal exchanges
            [[ "$exchange" == amqp.* ]] && continue

            if is_test_resource "$exchange"; then
                if delete_exchange "$host" "$exchange"; then
                    ((host_deleted+=1))
                    ((total_deleted+=1))
                else
                    ((total_failed+=1))
                fi
            fi
        done <<< "$exchanges"
    fi

    if [[ $host_deleted -eq 0 ]]; then
        echo "  No test queues or exchanges found"
    else
        echo "  Cleaned $host_deleted items"
    fi
    echo ""
done

echo "=============================================="
echo "  Summary"
echo "=============================================="
if $DRY_RUN; then
    echo "  Would delete: $total_deleted resources"
else
    echo "  Deleted: $total_deleted resources"
    echo "  Failed: $total_failed resources"
fi
echo "=============================================="
