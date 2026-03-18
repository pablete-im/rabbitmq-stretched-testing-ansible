#!/bin/bash
# =============================================================================
# RabbitMQ Performance Test Runner
#
# Reads YAML scenario files and runs the appropriate perf-test tool.
# Results are saved to perf-tests/results/ with timestamped filenames.
#
# Usage:
#   ./perf-tests/run-test.sh <scenario> [options]
#
# Examples:
#   ./perf-tests/run-test.sh baseline
#   ./perf-tests/run-test.sh baseline --hosts 10.85.10.234
#   ./perf-tests/run-test.sh streams --hosts 10.85.10.234
#   ./perf-tests/run-test.sh federation-test --pub-hosts 10.85.10.234 --con-hosts 10.85.10.234
#   ./perf-tests/run-test.sh baseline --hosts 10.85.10.234 --label "with-latency"
#   ./perf-tests/run-test.sh baseline --hosts 10.85.10.234,10.85.10.235,10.85.10.236
#
# TLS Examples:
#   ./perf-tests/run-test.sh baseline --hosts 110.85.10.234 --truststore /path/to/truststore.p12 --truststore-pass mypass
#   ./perf-tests/run-test.sh streams --hosts 10.85.10.234 --truststore /path/to/truststore.p12 --truststore-pass mypass
#
# Federation Test (uses instance synchronization for separate producer/consumer hosts):
#   ./perf-tests/run-test.sh federation-test --pub-hosts 10.85.10.234 --con-hosts 10.85.10.234
# =============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
TOOLS_DIR="$SCRIPT_DIR/tools"
SCENARIOS_DIR="$SCRIPT_DIR/scenarios"
RESULTS_DIR="$SCRIPT_DIR/results"

# Defaults
HOSTS="10.85.10.234"
PUB_HOSTS=""
CON_HOSTS=""
USER="admin"
PASSWORD=""
LABEL=""
RESULT_ALIAS=""
EXTRA_ARGS=""
TRUSTSTORE=""
TRUSTSTORE_PASS=""
TRUSTSTORE_TYPE="JKS"

# --- Parse arguments ---
SCENARIO="${1:-}"
shift || true

while [[ $# -gt 0 ]]; do
    case "$1" in
        --hosts)           HOSTS="$2"; shift 2 ;;
        --pub-hosts)       PUB_HOSTS="$2"; shift 2 ;;
        --con-hosts)       CON_HOSTS="$2"; shift 2 ;;
        --user)            USER="$2"; shift 2 ;;
        --password)        PASSWORD="$2"; shift 2 ;;
        --label)           LABEL="$2"; shift 2 ;;
        --result-alias)    RESULT_ALIAS="$2"; shift 2 ;;
        --truststore)      TRUSTSTORE="$2"; shift 2 ;;
        --truststore-pass) TRUSTSTORE_PASS="$2"; shift 2 ;;
        --truststore-type) TRUSTSTORE_TYPE="$2"; shift 2 ;;
        --)                shift; EXTRA_ARGS="$*"; break ;;
        *)                 EXTRA_ARGS="$EXTRA_ARGS $1"; shift ;;
    esac
done

# --- Validation ---
if [[ -z "$SCENARIO" ]]; then
    echo "Usage: $0 <scenario> [options]"
    echo ""
    echo "Available scenarios:"
    for f in "$SCENARIOS_DIR"/*.yml; do
        name=$(basename "$f" .yml)
        desc=$(grep '^description:' "$f" | sed 's/description: *"\?\(.*\)"\?/  \1/')
        printf "  %-20s %s\n" "$name" "$desc"
    done
    echo ""
    echo "Options:"
    echo "  --hosts <ip1,ip2,...>    RabbitMQ hosts (comma-separated, default: 10.85.10.234)"
    echo "  --pub-hosts <ip1,ip2>    Publisher target hosts (for federation tests)"
    echo "  --con-hosts <ip1,ip2>    Consumer target hosts (for federation tests)"
    echo "  --user <user>            RabbitMQ user (default: admin)"
    echo "  --password <pass>        RabbitMQ password (or set RMQ_PASSWORD env var)"
    echo "  --label <label>          Label to add to result filename"
    echo "  --result-alias <alias>   Alias to identify this test run in results"
    echo ""
    echo "TLS Options:"
    echo "  --truststore <path>      Path to Java truststore for TLS connections"
    echo "  --truststore-pass <pass> Truststore password"
    echo "  --truststore-type <type> Truststore type (default: PKCS12)"
    echo ""
    echo "  When TLS options are specified:"
    echo "    - AMQP connections use amqps://host:5671"
    echo "    - Stream connections use rabbitmq-stream+tls://host:5551"
    echo "  Without TLS options:"
    echo "    - AMQP connections use amqp://host:5672"
    echo "    - Stream connections use rabbitmq-stream://host:5552"
    echo ""
    echo "Other:"
    echo "  -- <args>                Pass additional args directly to perf-test"
    exit 1
fi

SCENARIO_FILE="$SCENARIOS_DIR/${SCENARIO}.yml"
if [[ ! -f "$SCENARIO_FILE" ]]; then
    echo "Error: Scenario file not found: $SCENARIO_FILE"
    exit 1
fi

if [[ ! -f "$TOOLS_DIR/perf-test" ]]; then
    echo "Error: perf-test not installed. Run:"
    echo "  ansible-playbook playbooks/install_perftest.yml"
    exit 1
fi

# Password from arg, env var, or prompt
if [[ -z "$PASSWORD" ]]; then
    PASSWORD="${RMQ_PASSWORD:-}"
fi
if [[ -z "$PASSWORD" ]]; then
    read -rsp "RabbitMQ password for '$USER': " PASSWORD
    echo
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

# Helper function to delete queue/stream before test
delete_queue_if_exists() {
    local queue_name="$1"
    local hosts="$2"
    local user="$3"
    local password="$4"
    local use_tls="${5:-false}"
    
    # Always use HTTP port 15672 for management API
    # (HTTPS management port 15671 may not be configured)
    local mgmt_port="15672"
    local mgmt_protocol="http"
    
    echo "🧹 Deleting queue/stream '$queue_name' if exists..."
    
    # Try to delete on all hosts (one will succeed if it exists)
    local deleted=false
    IFS=',' read -ra HOST_ARRAY <<< "$hosts"
    for host in "${HOST_ARRAY[@]}"; do
        host=$(echo "$host" | xargs)  # trim whitespace
        
        # Try to delete the queue (will fail silently if it doesn't exist)
        local url="${mgmt_protocol}://${host}:${mgmt_port}/api/queues/%2F/${queue_name}"
        
        local response_code=$(curl -u "${user}:${password}" -X DELETE "$url" -s -o /dev/null -w "%{http_code}" 2>/dev/null || echo "000")
        
        if [[ "$response_code" == "204" || "$response_code" == "404" ]]; then
            if [[ "$response_code" == "204" ]]; then
                echo "   ✓ Queue/stream deleted on $host"
                deleted=true
            fi
        else
            echo "   ⚠ Failed to delete on $host (HTTP $response_code)"
        fi
    done
    
    if [[ "$deleted" == "false" ]]; then
        echo "   Queue/stream did not exist or could not be deleted"
    fi
    
    echo "   Queue/stream cleanup completed"
}
# --- Parse scenario YAML (lightweight, no external deps) ---
parse_yaml_value() {
    grep "^${1}:" "$SCENARIO_FILE" | head -1 | sed "s/^${1}: *//; s/#.*//; s/\"//g; s/ *$//" || true
}

TEST_TYPE=$(parse_yaml_value "type")
DURATION=$(parse_yaml_value "duration")
PUBLISHERS=$(parse_yaml_value "publishers")
PUB_RATE=$(parse_yaml_value "pub_rate")
CONSUMERS=$(parse_yaml_value "consumers")
CONSUMER_RATE=$(parse_yaml_value "consumer_rate")
MESSAGE_SIZE=$(parse_yaml_value "message_size")
CONFIRM=$(parse_yaml_value "confirm")
MULTI_ACK=$(parse_yaml_value "multi_ack_every")
QOS=$(parse_yaml_value "qos")
AUTOACK=$(parse_yaml_value "autoack")
QUEUE_TYPE=$(parse_yaml_value "queue_type")
QUEUE_NAME=$(parse_yaml_value "queue")
STREAM_NAME=$(parse_yaml_value "stream")
OFFSET=$(parse_yaml_value "offset")
TEST_NAME=$(parse_yaml_value "name")

# --- Validation ---
# Check for mutually exclusive options
if [[ "${AUTOACK:-}" == "true" && -n "${MULTI_ACK:-}" ]]; then
    echo "Error: 'autoack' and 'multi_ack_every' are mutually exclusive options."
    echo "  autoack: ${AUTOACK:-unset}"
    echo "  multi_ack_every: ${MULTI_ACK:-unset}"
    echo "Please specify only one of these options in your scenario file."
    exit 1
fi

# --- Prepare result output ---
mkdir -p "$RESULTS_DIR"
TIMESTAMP=$(date +%Y%m%d-%H%M%S)
RESULT_LABEL="${TEST_NAME}"
[[ -n "$LABEL" ]] && RESULT_LABEL="${TEST_NAME}-${LABEL}"
RESULT_FILE="$RESULTS_DIR/${TIMESTAMP}-${RESULT_LABEL}.txt"

# --- Prepare TLS configuration ---
JVM_OPTS=""
if [[ -n "$TRUSTSTORE" ]]; then
    echo "🔐 TLS Mode: Using truststore $TRUSTSTORE"
    JVM_OPTS="-Djavax.net.ssl.trustStore=$TRUSTSTORE"
    JVM_OPTS="$JVM_OPTS -Djavax.net.ssl.trustStorePassword=$TRUSTSTORE_PASS"
    JVM_OPTS="$JVM_OPTS -Djavax.net.ssl.trustStoreType=$TRUSTSTORE_TYPE"
    
    # TLS ports and protocols
    AMQP_PORT="5671"
    STREAM_PORT="5551"
    AMQP_PROTOCOL="amqps"
    STREAM_PROTOCOL="rabbitmq-stream+tls"
else
    echo "🔓 Non-TLS Mode: Using standard connections"
    # Non-TLS ports and protocols
    AMQP_PORT="5672"
    STREAM_PORT="5552"
    AMQP_PROTOCOL="amqp"
    STREAM_PROTOCOL="rabbitmq-stream"
fi

# --- Build command ---
if [[ "$TEST_TYPE" == "stream" ]]; then
    # Stream perf test
    STREAM_URIS=$(build_uris "$HOSTS" "$STREAM_PROTOCOL" "$STREAM_PORT" "$USER" "$PASSWORD")
    
    CMD=("java")
    [[ -n "$JVM_OPTS" ]] && CMD+=($JVM_OPTS)
    CMD+=(-jar "$TOOLS_DIR/stream-perf-test.jar")
    CMD+=(--uris "$STREAM_URIS")
    CMD+=(--delete-streams)
    CMD+=(--confirm-latency)
    [[ -n "$PUBLISHERS" ]] && CMD+=(--producers "$PUBLISHERS")
    [[ -n "$CONSUMERS" ]] && CMD+=(--consumers "$CONSUMERS")
    [[ -n "$MESSAGE_SIZE" ]] && CMD+=(--size "$MESSAGE_SIZE")
    [[ -n "$DURATION" ]] && CMD+=(--time "$DURATION")
    [[ -n "$PUB_RATE" && "$PUB_RATE" != "0" ]] && CMD+=(--rate "$PUB_RATE")
    [[ -n "$STREAM_NAME" ]] && CMD+=(--streams "$STREAM_NAME")
    [[ -n "$OFFSET" ]] && CMD+=(--offset "$OFFSET")
else
    # AMQP perf test
    TARGET_HOSTS="${PUB_HOSTS:-$HOSTS}"
    AMQP_URIS=$(build_uris "$TARGET_HOSTS" "$AMQP_PROTOCOL" "$AMQP_PORT" "$USER" "$PASSWORD")

    CMD=("java")
    [[ -n "$JVM_OPTS" ]] && CMD+=($JVM_OPTS)
    CMD+=(-jar "$TOOLS_DIR/perf-test.jar")
    CMD+=(--uris "$AMQP_URIS")
    CMD+=(--id "$TEST_NAME")
    CMD+=(--queue "${QUEUE_NAME:-$TEST_NAME}")
    [[ -n "$DURATION" ]] && CMD+=(--time "$DURATION")
    [[ -n "$PUBLISHERS" ]] && CMD+=(--producers "$PUBLISHERS")
    [[ -n "$CONSUMERS" ]] && CMD+=(--consumers "$CONSUMERS")
    [[ -n "$MESSAGE_SIZE" ]] && CMD+=(--size "$MESSAGE_SIZE")
    [[ -n "$MULTI_ACK" ]] && CMD+=(--multi-ack-every "$MULTI_ACK")
    [[ -n "$CONFIRM" ]] && CMD+=(--confirm "$CONFIRM")
    [[ -n "$QOS" ]] && CMD+=(--qos "$QOS")
    [[ "${AUTOACK:-}" == "true" ]] && CMD+=(--autoack)
    [[ -n "$PUB_RATE" && "$PUB_RATE" != "0" ]] && CMD+=(--rate "$PUB_RATE")
    [[ -n "$CONSUMER_RATE" && "$CONSUMER_RATE" != "0" ]] && CMD+=(--consumer-rate "$CONSUMER_RATE")

    # Queue type flags
    case "${QUEUE_TYPE:-quorum}" in
        quorum)
            CMD+=(--quorum-queue)
            ;;
        stream)
            CMD+=(--stream-queue)
            # Add stream-consumer-offset for stream queues if specified
            [[ -n "$OFFSET" ]] && CMD+=(--stream-consumer-offset "$OFFSET")
            ;;
        classic)
            # classic is the default, no extra flag needed
            ;;
    esac

    # Federation test: separate producer and consumer hosts
    # Note: Federation tests require running separate processes for producers and consumers
    # This is handled by the federation-specific logic below
fi

# Append any extra args
if [[ -n "$EXTRA_ARGS" ]]; then
    read -ra EXTRA_ARRAY <<< "$EXTRA_ARGS"
    CMD+=("${EXTRA_ARRAY[@]}")
fi

# --- Run ---
echo "=============================================="
echo "  RabbitMQ Performance Test"
echo "=============================================="
echo "  Scenario:  $SCENARIO"
echo "  Type:      $TEST_TYPE"
echo "  Hosts:     ${PUB_HOSTS:-$HOSTS}"
[[ -n "$CON_HOSTS" ]] && echo "  Con Hosts: $CON_HOSTS"
echo "  Duration:  ${DURATION}s"
echo "  Results:   $RESULT_FILE"
echo "=============================================="
echo ""

# --- Handle Federation Tests (separate producer/consumer hosts) ---
if [[ -n "$CON_HOSTS" ]]; then
    echo "🔄 Federation Test Mode: Using instance synchronization"
    echo ""
    
    # Generate unique test ID for synchronization
    SYNC_ID="${TEST_NAME}-$(date +%s)"
    
    # Delete queue before test (federation tests use --predeclared, so we skip deletion)
    echo "⚠️  Federation test uses --predeclared, skipping queue deletion"
    echo ""
    
    # Build producer command (no consumers)
    PRODUCER_CMD=("java")
    [[ -n "$JVM_OPTS" ]] && PRODUCER_CMD+=($JVM_OPTS)
    
    if [[ "$TEST_TYPE" == "stream" ]]; then
        # Stream producer
        PRODUCER_URIS=$(build_uris "$HOSTS" "$STREAM_PROTOCOL" "$STREAM_PORT" "$USER" "$PASSWORD")
        PRODUCER_CMD+=(-jar "$TOOLS_DIR/stream-perf-test.jar")
        PRODUCER_CMD+=(--uris "$PRODUCER_URIS")
        PRODUCER_CMD+=(--producers "${PUBLISHERS:-1}")
        PRODUCER_CMD+=(--consumers 0)
        PRODUCER_CMD+=(--delete-streams)
        PRODUCER_CMD+=(--confirm-latency)
        [[ -n "$DURATION" ]] && PRODUCER_CMD+=(--time "$DURATION")
        [[ -n "$MESSAGE_SIZE" ]] && PRODUCER_CMD+=(--size "$MESSAGE_SIZE")
        [[ -n "$PUB_RATE" && "$PUB_RATE" != "0" ]] && PRODUCER_CMD+=(--rate "$PUB_RATE")
        [[ -n "$STREAM_NAME" ]] && PRODUCER_CMD+=(--streams "$STREAM_NAME")
    else
        # AMQP producer
        PRODUCER_URIS=$(build_uris "${PUB_HOSTS:-$HOSTS}" "$AMQP_PROTOCOL" "$AMQP_PORT" "$USER" "$PASSWORD")
        PRODUCER_CMD+=(-jar "$TOOLS_DIR/perf-test.jar")
        PRODUCER_CMD+=(--uris "$PRODUCER_URIS")
        PRODUCER_CMD+=(--producers "${PUBLISHERS:-1}")
        PRODUCER_CMD+=(--consumers 0)
        PRODUCER_CMD+=(--id "$SYNC_ID")
        PRODUCER_CMD+=(--expected-instances 2)
        PRODUCER_CMD+=(--use-millis)
        PRODUCER_CMD+=(--routing-key "${QUEUE_NAME:-$TEST_NAME}")
        PRODUCER_CMD+=(--predeclared)
        PRODUCER_CMD+=(--queue "${QUEUE_NAME:-$TEST_NAME}")
        [[ -n "$DURATION" ]] && PRODUCER_CMD+=(--time "$DURATION")
        [[ -n "$MESSAGE_SIZE" ]] && PRODUCER_CMD+=(--size "$MESSAGE_SIZE")
        [[ -n "$PUB_RATE" && "$PUB_RATE" != "0" ]] && PRODUCER_CMD+=(--rate "$PUB_RATE")
        [[ -n "$CONFIRM" ]] && PRODUCER_CMD+=(--confirm "$CONFIRM")
        
        # Add queue type for AMQP producer
        case "${QUEUE_TYPE:-quorum}" in
            quorum)
                PRODUCER_CMD+=(--quorum-queue)
                ;;
            stream)
                PRODUCER_CMD+=(--stream-queue)
                ;;
        esac
    fi
    
    # Build consumer command (no producers, different host)
    CONSUMER_CMD=("java")
    [[ -n "$JVM_OPTS" ]] && CONSUMER_CMD+=($JVM_OPTS)
    
    if [[ "$TEST_TYPE" == "stream" ]]; then
        # Stream consumer
        CONSUMER_URIS=$(build_uris "$CON_HOSTS" "$STREAM_PROTOCOL" "$STREAM_PORT" "$USER" "$PASSWORD")
        CONSUMER_CMD+=(-jar "$TOOLS_DIR/stream-perf-test.jar")
        CONSUMER_CMD+=(--uris "$CONSUMER_URIS")
        CONSUMER_CMD+=(--producers 0)
        CONSUMER_CMD+=(--consumers "${CONSUMERS:-1}")
        [[ -n "$DURATION" ]] && CONSUMER_CMD+=(--time "$DURATION")
        [[ -n "$STREAM_NAME" ]] && CONSUMER_CMD+=(--streams "$STREAM_NAME")
        [[ -n "$OFFSET" ]] && CONSUMER_CMD+=(--offset "$OFFSET")
    else
        # AMQP consumer
        CONSUMER_URIS=$(build_uris "$CON_HOSTS" "$AMQP_PROTOCOL" "$AMQP_PORT" "$USER" "$PASSWORD")
        CONSUMER_CMD+=(-jar "$TOOLS_DIR/perf-test.jar")
        CONSUMER_CMD+=(--uris "$CONSUMER_URIS")
        CONSUMER_CMD+=(--producers 0)
        CONSUMER_CMD+=(--consumers "${CONSUMERS:-1}")
        CONSUMER_CMD+=(--id "$SYNC_ID")
        CONSUMER_CMD+=(--expected-instances 2)
        CONSUMER_CMD+=(--use-millis)
        CONSUMER_CMD+=(--routing-key "${QUEUE_NAME:-$TEST_NAME}")
        CONSUMER_CMD+=(--predeclared)
        CONSUMER_CMD+=(--queue "${QUEUE_NAME:-$TEST_NAME}")
        [[ -n "$DURATION" ]] && CONSUMER_CMD+=(--time "$DURATION")
        [[ -n "$CONSUMER_RATE" && "$CONSUMER_RATE" != "0" ]] && CONSUMER_CMD+=(--consumer-rate "$CONSUMER_RATE")
        [[ -n "$MULTI_ACK" ]] && CONSUMER_CMD+=(--multi-ack-every "$MULTI_ACK")
        [[ -n "$QOS" ]] && CONSUMER_CMD+=(--qos "$QOS")
        [[ "${AUTOACK:-}" == "true" ]] && CONSUMER_CMD+=(--autoack)
        
        # Add queue type for AMQP consumer
        case "${QUEUE_TYPE:-quorum}" in
            quorum)
                CONSUMER_CMD+=(--quorum-queue)
                ;;
            stream)
                CONSUMER_CMD+=(--stream-queue)
                ;;
        esac
    fi
    
    # Display commands for federation test
    echo "📤 Producer Command (${PUB_HOSTS:-$HOSTS}):"
    echo "   ${PRODUCER_CMD[*]}"
    echo ""
    echo "📥 Consumer Command ($CON_HOSTS):"
    echo "   ${CONSUMER_CMD[*]}"
    echo ""
    
    # Write headers to result files
    {
        echo "# Federation Test - Producer Results"
        echo "# Scenario: $SCENARIO"
        [[ -n "$RESULT_ALIAS" ]] && echo "# Alias: $RESULT_ALIAS"
        echo "# Date: $(date -u '+%Y-%m-%d %H:%M:%S UTC')"
        echo "# Producer Host: ${PUB_HOSTS:-$HOSTS}"
        echo "# Consumer Host: $CON_HOSTS"
        [[ -n "$LABEL" ]] && echo "# Label: $LABEL"
        echo "# Duration: ${DURATION}s"
        echo "# Sync ID: $SYNC_ID"
        echo "# Producer Command: ${PRODUCER_CMD[*]}"
        echo "#"
    } > "$RESULT_FILE"
    
    {
        echo "# Federation Test - Consumer Results"
        echo "# Scenario: $SCENARIO"
        [[ -n "$RESULT_ALIAS" ]] && echo "# Alias: $RESULT_ALIAS"
        echo "# Date: $(date -u '+%Y-%m-%d %H:%M:%S UTC')"
        echo "# Producer Host: ${PUB_HOSTS:-$HOSTS}"
        echo "# Consumer Host: $CON_HOSTS"
        [[ -n "$LABEL" ]] && echo "# Label: $LABEL"
        echo "# Duration: ${DURATION}s"
        echo "# Sync ID: $SYNC_ID"
        echo "# Consumer Command: ${CONSUMER_CMD[*]}"
        echo "#"
    } > "${RESULT_FILE}.consumer"
    
    echo "🚀 Starting consumer process on $CON_HOSTS (will wait for producer)..."
    "${CONSUMER_CMD[@]}" 2>&1 | tee -a "${RESULT_FILE}.consumer" &
    CONSUMER_PID=$!
    
    echo ""
    echo "🚀 Starting producer process on ${PUB_HOSTS:-$HOSTS} (will synchronize with consumer)..."
    echo ""
    
    "${PRODUCER_CMD[@]}" 2>&1 | tee -a "$RESULT_FILE"
    
    # Wait for consumer to finish
    echo ""
    echo "⏳ Waiting for consumer process to complete..."
    wait $CONSUMER_PID
    
    echo ""
    echo "📊 Federation Test Results:"
    echo "  Producer: $RESULT_FILE"
    echo "  Consumer: ${RESULT_FILE}.consumer"
    
else
    # --- Standard single-host test ---
    
    # Delete queue/stream before test
    USE_TLS="false"
    [[ -n "$TRUSTSTORE" ]] && USE_TLS="true"
    
    if [[ "$TEST_TYPE" == "stream" ]]; then
        QUEUE_TO_DELETE="${STREAM_NAME:-$TEST_NAME}"
    else
        QUEUE_TO_DELETE="${QUEUE_NAME:-$TEST_NAME}"
    fi
    
    delete_queue_if_exists "$QUEUE_TO_DELETE" "$HOSTS" "$USER" "$PASSWORD" "$USE_TLS"
    echo ""
    
    echo "Command: ${CMD[*]}"
    echo ""
    
    # Write header to results
    {
        echo "# Scenario: $SCENARIO"
        [[ -n "$RESULT_ALIAS" ]] && echo "# Alias: $RESULT_ALIAS"
        echo "# Date: $(date -u '+%Y-%m-%d %H:%M:%S UTC')"
        echo "# Host: ${PUB_HOSTS:-$HOSTS}"
        [[ -n "$LABEL" ]] && echo "# Label: $LABEL"
        echo "# Duration: ${DURATION}s"
        echo "# Command: ${CMD[*]}"
        echo "#"
    } > "$RESULT_FILE"
    
    # Run test, tee output to both console and result file
    "${CMD[@]}" 2>&1 | tee -a "$RESULT_FILE"
    
    echo ""
    echo "Results saved to: $RESULT_FILE"
fi
