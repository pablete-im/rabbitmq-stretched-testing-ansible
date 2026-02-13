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
# =============================================================================
set -euo pipefail

# Cluster endpoints
UPSTREAM_HOST="192.168.20.200"       # AZ-Cluster-1
REGIONAL_STANDBY="192.168.20.203"    # AZ-Cluster-2
CROSS_REGION_DR1="192.168.20.206"    # TX-Cluster-1
CROSS_REGION_DR2="192.168.20.209"    # TX-Cluster-2

USER="admin"
PASSWORD=""
DRY_RUN=false
ALL_CLUSTERS=false
PATTERN=""

# Test queue patterns to clean
TEST_PATTERNS=(
    "^core-test-"
    "^resiliency-"
    "^warm-standby-"
    "^lag-test-"
    "^promotion-test-"
    "^perf-test-"
    "^manual-test-"
    "^baseline$"
)

# Parse arguments
while [[ $# -gt 0 ]]; do
    case "$1" in
        --user)        USER="$2"; shift 2 ;;
        --password)    PASSWORD="$2"; shift 2 ;;
        --dry-run)     DRY_RUN=true; shift ;;
        --all-clusters) ALL_CLUSTERS=true; shift ;;
        --pattern)     PATTERN="$2"; shift 2 ;;
        *)             echo "Unknown option: $1"; exit 1 ;;
    esac
done

if [[ -z "$PASSWORD" ]]; then
    PASSWORD="${RMQ_PASSWORD:-}"
fi
if [[ -z "$PASSWORD" ]]; then
    read -rsp "RabbitMQ password for '$USER': " PASSWORD
    echo
fi

# Build list of hosts to clean
HOSTS=("$UPSTREAM_HOST")
if $ALL_CLUSTERS; then
    HOSTS+=("$REGIONAL_STANDBY" "$CROSS_REGION_DR1" "$CROSS_REGION_DR2")
fi

# Function to check if queue name matches test patterns
is_test_queue() {
    local name="$1"

    # Skip internal RabbitMQ queues
    if [[ "$name" == rabbitmq.internal.* ]]; then
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
        echo "  [DRY-RUN] Would delete: $queue"
        return 0
    fi

    local status
    status=$(curl -sf -o /dev/null -w "%{http_code}" -X DELETE \
        -u "${USER}:${PASSWORD}" \
        "http://${host}:15672/api/queues/%2F/${encoded_queue}" 2>/dev/null || echo "000")

    if [[ "$status" == "204" ]] || [[ "$status" == "200" ]]; then
        echo "  Deleted: $queue"
        return 0
    else
        echo "  Failed to delete: $queue (HTTP $status)"
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
    queues=$(curl -sf -u "${USER}:${PASSWORD}" "http://${host}:15672/api/queues" 2>/dev/null | \
        python3 -c "import sys,json; [print(q['name']) for q in json.load(sys.stdin)]" 2>/dev/null) || {
        echo "  Failed to connect to $host"
        continue
    }

    host_deleted=0
    while IFS= read -r queue; do
        [[ -z "$queue" ]] && continue

        if is_test_queue "$queue"; then
            if delete_queue "$host" "$queue"; then
                ((host_deleted++))
                ((total_deleted++))
            else
                ((total_failed++))
            fi
        fi
    done <<< "$queues"

    if [[ $host_deleted -eq 0 ]]; then
        echo "  No test queues found"
    else
        echo "  Cleaned $host_deleted queues"
    fi
    echo ""
done

echo "=============================================="
echo "  Summary"
echo "=============================================="
if $DRY_RUN; then
    echo "  Would delete: $total_deleted queues"
else
    echo "  Deleted: $total_deleted queues"
    echo "  Failed: $total_failed queues"
fi
echo "=============================================="
