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
# =============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
TOOLS_DIR="$SCRIPT_DIR/tools"
SCENARIOS_DIR="$SCRIPT_DIR/scenarios"
RESULTS_DIR="$SCRIPT_DIR/results"

# Defaults
HOST="192.168.20.200"
PUB_HOST=""
CON_HOST=""
USER="admin"
PASSWORD=""
LABEL=""
EXTRA_ARGS=""

# --- Parse arguments ---
SCENARIO="${1:-}"
shift || true

while [[ $# -gt 0 ]]; do
    case "$1" in
        --host)      HOST="$2"; shift 2 ;;
        --pub-host)  PUB_HOST="$2"; shift 2 ;;
        --con-host)  CON_HOST="$2"; shift 2 ;;
        --user)      USER="$2"; shift 2 ;;
        --password)  PASSWORD="$2"; shift 2 ;;
        --label)     LABEL="$2"; shift 2 ;;
        --)          shift; EXTRA_ARGS="$*"; break ;;
        *)           EXTRA_ARGS="$EXTRA_ARGS $1"; shift ;;
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
    echo "  --host <ip>        RabbitMQ host (default: 192.168.20.200)"
    echo "  --pub-host <ip>    Publisher target host (for federation tests)"
    echo "  --con-host <ip>    Consumer target host (for federation tests)"
    echo "  --user <user>      RabbitMQ user (default: admin)"
    echo "  --password <pass>  RabbitMQ password (or set RMQ_PASSWORD env var)"
    echo "  --label <label>    Label to add to result filename"
    echo "  -- <args>          Pass additional args directly to perf-test"
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

# --- Build command ---
if [[ "$TEST_TYPE" == "stream" ]]; then
    # Stream perf test
    CMD=("$TOOLS_DIR/stream-perf-test")
    CMD+=(--uris "rabbitmq-stream://${USER}:${PASSWORD}@${HOST}:5552")
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
    AMQP_URI="amqp://${USER}:${PASSWORD}@${TARGET_HOST}:5672"

    CMD=("$TOOLS_DIR/perf-test")
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

    # Federation test: separate consumer URI
    if [[ -n "$CON_HOST" ]]; then
        CON_URI="amqp://${USER}:${PASSWORD}@${CON_HOST}:5672"
        CMD+=(--consumer-uri "$CON_URI")
    fi
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
echo "Command: ${CMD[*]}"
echo ""

# Write header to results
{
    echo "# Scenario: $SCENARIO"
    echo "# Date: $(date -u '+%Y-%m-%d %H:%M:%S UTC')"
    echo "# Host: ${PUB_HOST:-$HOST}"
    [[ -n "$CON_HOST" ]] && echo "# Consumer Host: $CON_HOST"
    [[ -n "$LABEL" ]] && echo "# Label: $LABEL"
    echo "# Duration: ${DURATION}s"
    echo "# Command: ${CMD[*]}"
    echo "#"
} > "$RESULT_FILE"

# Run test, tee output to both console and result file
"${CMD[@]}" 2>&1 | tee -a "$RESULT_FILE"

echo ""
echo "Results saved to: $RESULT_FILE"
