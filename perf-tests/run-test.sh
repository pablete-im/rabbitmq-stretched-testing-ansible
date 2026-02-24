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
#   ./perf-tests/run-test.sh baseline --host 192.168.20.200
#   ./perf-tests/run-test.sh streams --host 192.168.20.200
#   ./perf-tests/run-test.sh federation-test --pub-host 192.168.20.200 --con-host 192.168.20.206
#   ./perf-tests/run-test.sh baseline --host 192.168.20.200 --label "with-latency"
#
# TLS Examples:
#   ./perf-tests/run-test.sh baseline --host 192.168.20.200 --truststore /path/to/truststore.p12
#   ./perf-tests/run-test.sh streams --host 192.168.20.200 --truststore /path/to/truststore.p12 --truststore-pass mypass
#
# Federation Test (uses instance synchronization for separate producer/consumer hosts):
#   ./perf-tests/run-test.sh federation-test --pub-host 192.168.20.200 --con-host 192.168.20.206
# =============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
TOOLS_DIR="$SCRIPT_DIR/tools"
SCENARIOS_DIR="$SCRIPT_DIR/scenarios"
RESULTS_DIR="$SCRIPT_DIR/results"

# Defaults
HOST="10.85.10.234"
PUB_HOST=""
CON_HOST=""
USER="admin"
PASSWORD=""
LABEL=""
EXTRA_ARGS=""
TRUSTSTORE=""
TRUSTSTORE_PASS=""
TRUSTSTORE_TYPE="JKS"

# --- Parse arguments ---
SCENARIO="${1:-}"
shift || true

while [[ $# -gt 0 ]]; do
    case "$1" in
        --host)            HOST="$2"; shift 2 ;;
        --pub-host)        PUB_HOST="$2"; shift 2 ;;
        --con-host)        CON_HOST="$2"; shift 2 ;;
        --user)            USER="$2"; shift 2 ;;
        --password)        PASSWORD="$2"; shift 2 ;;
        --label)           LABEL="$2"; shift 2 ;;
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
    echo "  --host <ip>              RabbitMQ host (default: 192.168.20.200)"
    echo "  --pub-host <ip>          Publisher target host (for federation tests)"
    echo "  --con-host <ip>          Consumer target host (for federation tests)"
    echo "  --user <user>            RabbitMQ user (default: admin)"
    echo "  --password <pass>        RabbitMQ password (or set RMQ_PASSWORD env var)"
    echo "  --label <label>          Label to add to result filename"
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
QUEUE_TYPE=$(parse_yaml_value "queue_type")
QUEUE_NAME=$(parse_yaml_value "queue")
STREAM_NAME=$(parse_yaml_value "stream")
OFFSET=$(parse_yaml_value "offset")
TEST_NAME=$(parse_yaml_value "name")

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
    CMD=("java")
    [[ -n "$JVM_OPTS" ]] && CMD+=($JVM_OPTS)
    CMD+=(-jar "$TOOLS_DIR/stream-perf-test.jar")
    CMD+=(--uris "${STREAM_PROTOCOL}://${USER}:${PASSWORD}@${HOST}:${STREAM_PORT}")
    CMD+=(--delete-streams)
    [[ -n "$PUBLISHERS" ]] && CMD+=(--producers "$PUBLISHERS")
    [[ -n "$CONSUMERS" ]] && CMD+=(--consumers "$CONSUMERS")
    [[ -n "$MESSAGE_SIZE" ]] && CMD+=(--size "$MESSAGE_SIZE")
    [[ -n "$DURATION" ]] && CMD+=(--time "$DURATION")
    [[ -n "$PUB_RATE" && "$PUB_RATE" != "0" ]] && CMD+=(--rate "$PUB_RATE")
    [[ -n "$STREAM_NAME" ]] && CMD+=(--streams "$STREAM_NAME")
    [[ -n "$OFFSET" ]] && CMD+=(--offset "$OFFSET")
else
    # AMQP perf test
    TARGET_HOST="${PUB_HOST:-$HOST}"
    AMQP_URI="${AMQP_PROTOCOL}://${USER}:${PASSWORD}@${TARGET_HOST}:${AMQP_PORT}"

    CMD=("java")
    [[ -n "$JVM_OPTS" ]] && CMD+=($JVM_OPTS)
    CMD+=(-jar "$TOOLS_DIR/perf-test.jar")
    CMD+=(--uri "$AMQP_URI")
    CMD+=(--id "$TEST_NAME")
    CMD+=(--queue "${QUEUE_NAME:-$TEST_NAME}")
    [[ -n "$DURATION" ]] && CMD+=(--time "$DURATION")
    [[ -n "$PUBLISHERS" ]] && CMD+=(--producers "$PUBLISHERS")
    [[ -n "$CONSUMERS" ]] && CMD+=(--consumers "$CONSUMERS")
    [[ -n "$MESSAGE_SIZE" ]] && CMD+=(--size "$MESSAGE_SIZE")
    [[ -n "$MULTI_ACK" ]] && CMD+=(--multi-ack-every "$MULTI_ACK")
    [[ "$CONFIRM" == "true" ]] && CMD+=(--confirm "$MULTI_ACK")
    [[ -n "$PUB_RATE" && "$PUB_RATE" != "0" ]] && CMD+=(--rate "$PUB_RATE")
    [[ -n "$CONSUMER_RATE" && "$CONSUMER_RATE" != "0" ]] && CMD+=(--consumer-rate "$CONSUMER_RATE")

    # Queue type flags
    case "${QUEUE_TYPE:-quorum}" in
        quorum)
            CMD+=(--quorum-queue)
            ;;
        stream)
            CMD+=(--stream-queue)
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
echo "  Host:      ${PUB_HOST:-$HOST}"
[[ -n "$CON_HOST" ]] && echo "  Con Host:  $CON_HOST"
echo "  Duration:  ${DURATION}s"
echo "  Results:   $RESULT_FILE"
echo "=============================================="
echo ""

# --- Handle Federation Tests (separate producer/consumer hosts) ---
if [[ -n "$CON_HOST" ]]; then
    echo "🔄 Federation Test Mode: Using instance synchronization"
    echo ""
    
    # Generate unique test ID for synchronization
    SYNC_ID="${TEST_NAME}-$(date +%s)"
    
    # Build producer command (no consumers)
    PRODUCER_CMD=("java")
    [[ -n "$JVM_OPTS" ]] && PRODUCER_CMD+=($JVM_OPTS)
    
    if [[ "$TEST_TYPE" == "stream" ]]; then
        # Stream producer
        PRODUCER_CMD+=(-jar "$TOOLS_DIR/stream-perf-test.jar")
        PRODUCER_CMD+=(--uris "${STREAM_PROTOCOL}://${USER}:${PASSWORD}@${HOST}:${STREAM_PORT}")
        PRODUCER_CMD+=(--producers "${PUBLISHERS:-1}")
        PRODUCER_CMD+=(--consumers 0)
        #PRODUCER_CMD+=(--id "$SYNC_ID")
        #PRODUCER_CMD+=(--expected-instances 2)
        PRODUCER_CMD+=(--delete-streams)
        [[ -n "$DURATION" ]] && PRODUCER_CMD+=(--time "$DURATION")
        [[ -n "$MESSAGE_SIZE" ]] && PRODUCER_CMD+=(--size "$MESSAGE_SIZE")
        [[ -n "$PUB_RATE" && "$PUB_RATE" != "0" ]] && PRODUCER_CMD+=(--rate "$PUB_RATE")
        [[ -n "$STREAM_NAME" ]] && PRODUCER_CMD+=(--streams "$STREAM_NAME")
    else
        # AMQP producer
        PRODUCER_CMD+=(-jar "$TOOLS_DIR/perf-test.jar")
        PRODUCER_CMD+=(--uri "$AMQP_URI")
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
        [[ "$CONFIRM" == "true" ]] && PRODUCER_CMD+=(--confirm "${MULTI_ACK:-1}")
        
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
        CON_URI="${STREAM_PROTOCOL}://${USER}:${PASSWORD}@${CON_HOST}:${STREAM_PORT}"
        CONSUMER_CMD+=(-jar "$TOOLS_DIR/stream-perf-test.jar")
        CONSUMER_CMD+=(--uris "$CON_URI")
        CONSUMER_CMD+=(--producers 0)
        CONSUMER_CMD+=(--consumers "${CONSUMERS:-1}")
        #CONSUMER_CMD+=(--id "$SYNC_ID")
        #CONSUMER_CMD+=(--expected-instances 2)
        [[ -n "$DURATION" ]] && CONSUMER_CMD+=(--time "$DURATION")
        [[ -n "$STREAM_NAME" ]] && CONSUMER_CMD+=(--streams "$STREAM_NAME")
        [[ -n "$OFFSET" ]] && CONSUMER_CMD+=(--offset "$OFFSET")
    else
        # AMQP consumer
        CON_URI="${AMQP_PROTOCOL}://${USER}:${PASSWORD}@${CON_HOST}:${AMQP_PORT}"
        CONSUMER_CMD+=(-jar "$TOOLS_DIR/perf-test.jar")
        CONSUMER_CMD+=(--uri "$CON_URI")
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
    echo "📤 Producer Command (${PUB_HOST:-$HOST}):"
    echo "   ${PRODUCER_CMD[*]}"
    echo ""
    echo "📥 Consumer Command ($CON_HOST):"
    echo "   ${CONSUMER_CMD[*]}"
    echo ""
    
    # Write headers to result files
    {
        echo "# Federation Test - Producer Results"
        echo "# Scenario: $SCENARIO"
        echo "# Date: $(date -u '+%Y-%m-%d %H:%M:%S UTC')"
        echo "# Producer Host: ${PUB_HOST:-$HOST}"
        echo "# Consumer Host: $CON_HOST"
        [[ -n "$LABEL" ]] && echo "# Label: $LABEL"
        echo "# Duration: ${DURATION}s"
        echo "# Sync ID: $SYNC_ID"
        echo "# Producer Command: ${PRODUCER_CMD[*]}"
        echo "#"
    } > "$RESULT_FILE"
    
    {
        echo "# Federation Test - Consumer Results"
        echo "# Scenario: $SCENARIO"
        echo "# Date: $(date -u '+%Y-%m-%d %H:%M:%S UTC')"
        echo "# Producer Host: ${PUB_HOST:-$HOST}"
        echo "# Consumer Host: $CON_HOST"
        [[ -n "$LABEL" ]] && echo "# Label: $LABEL"
        echo "# Duration: ${DURATION}s"
        echo "# Sync ID: $SYNC_ID"
        echo "# Consumer Command: ${CONSUMER_CMD[*]}"
        echo "#"
    } > "${RESULT_FILE}.consumer"
    
    echo "🚀 Starting consumer process on $CON_HOST (will wait for producer)..."
    "${CONSUMER_CMD[@]}" 2>&1 | tee -a "${RESULT_FILE}.consumer" &
    CONSUMER_PID=$!
    
    echo ""
    echo "🚀 Starting producer process on ${PUB_HOST:-$HOST} (will synchronize with consumer)..."
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
    echo "Command: ${CMD[*]}"
    echo ""
    
    # Write header to results
    {
        echo "# Scenario: $SCENARIO"
        echo "# Date: $(date -u '+%Y-%m-%d %H:%M:%S UTC')"
        echo "# Host: ${PUB_HOST:-$HOST}"
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
