#!/bin/bash
# =============================================================================
# RabbitMQ Multi-Protocol Performance Test Runner (OMQ)
#
# Reads YAML scenario files and runs OMQ for multi-protocol testing.
# Results are saved to perf-tests/results/ with timestamped filenames.
#
# Usage:
#   ./perf-tests/run-test-omq.sh <scenario> [options]
#
# Examples:
#   ./perf-tests/run-test-omq.sh baseline
#   ./perf-tests/run-test-omq.sh baseline --hosts 10.85.10.234
#   ./perf-tests/run-test-omq.sh federation-test --pub-hosts 10.85.10.234 --con-hosts 10.85.10.234
#   ./perf-tests/run-test-omq.sh baseline --hosts 10.85.10.234 --label "amqp1.0-test"
#   ./perf-tests/run-test-omq.sh baseline --hosts 10.85.10.234,10.85.10.235,10.85.10.236
#
# Multi-Protocol Examples:
#   ./perf-tests/run-test-omq.sh baseline --hosts 10.85.10.234 --protocol mqtt-amqp
#   ./perf-tests/run-test-omq.sh baseline --hosts 10.85.10.234 --protocol stomp-amqp091
#
# TLS Examples (Note: OMQ has known TLS certificate verification issues):
#   ./perf-tests/run-test-omq.sh baseline --hosts pve-schwab-rmq01 --ca-cert /path/to/ca.crt --protocol amqp-amqp
#
# Federation Test (uses instance synchronization for separate producer/consumer hosts):
#   ./perf-tests/run-test-omq.sh federation-test --pub-hosts 10.85.10.234 --con-hosts 10.85.10.234
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
EXTRA_ARGS=""
CA_CERT=""
TLS_SKIP_VERIFY=false
PROTOCOL="amqp-amqp"

# Function to show help
show_help() {
    echo "Usage: $0 <scenario> [options]"
    echo ""
    echo "Available scenarios:"
    for f in "$SCENARIOS_DIR"/*.yml; do
        [[ -f "$f" ]] || continue
        name=$(basename "$f" .yml)
        desc=$(grep '^description:' "$f" 2>/dev/null | sed 's/description: *"\?\(.*\)"\?/  \1/' || echo "")
        printf "  %-20s %s\n" "$name" "$desc"
    done
    echo ""
    echo "Options:"
    echo "  --hosts <ip1,ip2,...>    RabbitMQ hosts (comma-separated, default: 10.85.10.234)"
    echo "  --pub-hosts <ip1,ip2>    Publisher target hosts (for federation tests)"
    echo "  --con-hosts <ip1,ip2>    Consumer target hosts (for federation tests)"
    echo "  --user <user>            RabbitMQ user (default: admin)"
    echo "  --password <pass>        RabbitMQ password (or set RMQ_PASSWORD env var)"
    echo "  --label <label>          Test label for results identification"
    echo "  --protocol <proto>       Protocol combination (default: amqp-amqp)"
    echo ""
    echo "Protocol Options (16 combinations available):"
    echo "  amqp-amqp        AMQP 1.0 publisher → AMQP 1.0 consumer (default)"
    echo "  amqp-amqp091     AMQP 1.0 publisher → AMQP 0.9.1 consumer"
    echo "  amqp-mqtt        AMQP 1.0 publisher → MQTT consumer"
    echo "  amqp-stomp       AMQP 1.0 publisher → STOMP consumer"
    echo "  amqp091-amqp     AMQP 0.9.1 publisher → AMQP 1.0 consumer"
    echo "  amqp091-amqp091  AMQP 0.9.1 publisher → AMQP 0.9.1 consumer"
    echo "  amqp091-mqtt     AMQP 0.9.1 publisher → MQTT consumer"
    echo "  amqp091-stomp    AMQP 0.9.1 publisher → STOMP consumer"
    echo "  mqtt-amqp        MQTT publisher → AMQP 1.0 consumer"
    echo "  mqtt-amqp091     MQTT publisher → AMQP 0.9.1 consumer"
    echo "  mqtt-mqtt        MQTT publisher → MQTT consumer"
    echo "  mqtt-stomp       MQTT publisher → STOMP consumer"
    echo "  stomp-amqp       STOMP publisher → AMQP 1.0 consumer"
    echo "  stomp-amqp091    STOMP publisher → AMQP 0.9.1 consumer"
    echo "  stomp-mqtt       STOMP publisher → MQTT consumer"
    echo "  stomp-stomp      STOMP publisher → STOMP consumer"
    echo ""
    echo "TLS Options:"
    echo "  --ca-cert <path>         Path to CA certificate file (.crt or .pem)"
    echo "  --tls-skip-verify        Skip TLS certificate verification (insecure)"
    echo ""
    echo "  When TLS options are specified:"
    echo "    - AMQP connections use amqps://host:5671"
    echo "    - OMQ uses the provided CA certificate for verification"
    echo "  Without TLS options:"
    echo "    - AMQP connections use amqp://host:5672"
    echo ""
    echo "  For TLS certificate extraction and setup:"
    echo "    ./perf-tests/setup-omq-tls.sh --help"
    echo "    ./perf-tests/fix-tls-hostnames.sh --help  # Fix certificate hostname issues"
    echo ""
    echo "Other:"
    echo "  -- <args>                Pass additional args directly to OMQ"
    echo ""
    echo "Examples:"
    echo "  # Basic AMQP 1.0 test"
    echo "  $0 baseline --hosts 10.85.10.234"
    echo ""
    echo "  # Multi-protocol: MQTT publisher → AMQP 1.0 consumer"
    echo "  $0 baseline --hosts 10.85.10.234 --protocol mqtt-amqp"
    echo ""
    echo "  # Cross-protocol: STOMP publisher → AMQP 0.9.1 consumer"
    echo "  $0 baseline --hosts 10.85.10.234 --protocol stomp-amqp091"
    echo ""
    echo "  # Multi-protocol federation test"
    echo "  $0 federation-test --pub-hosts 10.85.10.234 --con-hosts 10.85.10.235 --protocol stomp-amqp091"
    echo ""
    echo "  # TLS test with CA certificate"
    echo "  $0 baseline --hosts pve-schwab-rmq01 --ca-cert /path/to/ca.crt --protocol amqp-amqp"
    echo ""
    echo "  # Extract CA from Java truststore first:"
    echo "  ./perf-tests/setup-omq-tls.sh --truststore /path/to/truststore.jks --truststore-pass mypass"
    echo "  $0 baseline --hosts pve-schwab-rmq01 --ca-cert ./perf-tests/omq-ca-bundle.crt --protocol mqtt-amqp091"
    echo ""
    echo "Parameter Mapping (scenario YAML → OMQ):"
    echo "  confirm: <value>         → --max-in-flight <value> (publisher flow control)"
    echo "  qos: <value>             → --consumer-credits <value> (consumer flow control)"
    echo "  autoack: true            → --amqp-consume-settled (settled consume mode)"
    echo "                              NOTE: Only for AMQP 1.0 consumer protocol (*-amqp)"
    echo "                              Ignored for: amqp091, mqtt, stomp consumers"
    echo "  multi_ack_every: <value> → Not supported (ignored with warning)"
    echo ""
    echo "Stream Support (AMQP 1.0 Stream Filtering - RabbitMQ 4.1+):"
    echo "  queue_type: stream                              → --queues stream"
    echo "  offset: <offset_spec>                           → --stream-offset <offset_spec>"
    echo "  stream_filter_key: <key>                        → Application property key for filtering"
    echo "  stream_filter_values: <val1,val2,val3>          → --amqp-app-property <key>=<values> (publisher)"
    echo "  stream_filter_match_unfiltered: <filter_expr>   → --amqp-app-property-filter <key>=<expr> (consumer)"
    echo ""
    echo "  Filter expression examples:"
    echo "    'foo'           - Exact match for 'foo'"
    echo "    '&p:ba'         - Prefix match: starts with 'ba'"
    echo "    See OMQ docs for more filter expressions"
    echo ""
    echo "⚠️  NOTE: OMQ has known issues with TLS certificate verification."
    echo "   For production TLS testing, consider using run-test.sh (AMQP 0.9.1)"
}

# --- Parse arguments ---
SCENARIO="${1:-}"

# Check for help before processing
if [[ -z "$SCENARIO" || "$SCENARIO" == "--help" || "$SCENARIO" == "-h" ]]; then
    if [[ -z "$SCENARIO" ]]; then
        echo "Error: No scenario specified"
        echo ""
    fi
    show_help
    exit 1
fi

shift

while [[ $# -gt 0 ]]; do
    case "$1" in
        --hosts)           
            [[ -z "${2:-}" ]] && { echo "Error: --hosts requires a value"; exit 1; }
            HOSTS="$2"; shift 2 ;;
        --pub-hosts)       
            [[ -z "${2:-}" ]] && { echo "Error: --pub-hosts requires a value"; exit 1; }
            PUB_HOSTS="$2"; shift 2 ;;
        --con-hosts)       
            [[ -z "${2:-}" ]] && { echo "Error: --con-hosts requires a value"; exit 1; }
            CON_HOSTS="$2"; shift 2 ;;
        --user)            
            [[ -z "${2:-}" ]] && { echo "Error: --user requires a value"; exit 1; }
            USER="$2"; shift 2 ;;
        --password)        
            [[ -z "${2:-}" ]] && { echo "Error: --password requires a value"; exit 1; }
            PASSWORD="$2"; shift 2 ;;
        --label)           
            [[ -z "${2:-}" ]] && { echo "Error: --label requires a value"; exit 1; }
            LABEL="$2"; shift 2 ;;
        --ca-cert)         
            [[ -z "${2:-}" ]] && { echo "Error: --ca-cert requires a value"; exit 1; }
            CA_CERT="$2"; shift 2 ;;
        --protocol)        
            [[ -z "${2:-}" ]] && { echo "Error: --protocol requires a value"; exit 1; }
            PROTOCOL="$2"; shift 2 ;;
        --tls-skip-verify) TLS_SKIP_VERIFY=true; shift ;;
        --)                shift; EXTRA_ARGS="$*"; break ;;
        *)                 EXTRA_ARGS="$EXTRA_ARGS $1"; shift ;;
    esac
done

# --- File validation ---
SCENARIO_FILE="$SCENARIOS_DIR/${SCENARIO}.yml"
if [[ ! -f "$SCENARIO_FILE" ]]; then
    echo "Error: Scenario file not found: $SCENARIO_FILE"
    exit 1
fi

if [[ ! -f "$TOOLS_DIR/omq" ]]; then
    echo "Error: OMQ not installed. Run:"
    echo "  ansible-playbook playbooks/install_perftest.yml"
    exit 1
fi

# Validate protocol
VALID_PROTOCOLS=(
    "amqp-amqp" "amqp-amqp091" "amqp-mqtt" "amqp-stomp"
    "amqp091-amqp" "amqp091-amqp091" "amqp091-mqtt" "amqp091-stomp"
    "mqtt-amqp" "mqtt-amqp091" "mqtt-mqtt" "mqtt-stomp"
    "stomp-amqp" "stomp-amqp091" "stomp-mqtt" "stomp-stomp"
)

if [[ ! " ${VALID_PROTOCOLS[*]} " =~ " ${PROTOCOL} " ]]; then
    echo "Error: Invalid protocol '$PROTOCOL'"
    echo "Valid protocols: ${VALID_PROTOCOLS[*]}"
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

# Function to build multiple --uri parameters for OMQ from comma-separated hosts
build_omq_uri_params() {
    local hosts="$1"
    local protocol="$2"
    local port="$3"
    local user="$4"
    local password="$5"
    local uri_flag="$6"  # --uri, --publisher-uri, or --consumer-uri
    
    local uri_params=()
    IFS=',' read -ra HOST_ARRAY <<< "$hosts"
    for host in "${HOST_ARRAY[@]}"; do
        host=$(echo "$host" | xargs)  # trim whitespace
        # Add explicit vhost for OMQ to avoid URI parsing issues
        uri_params+=("$uri_flag" "${protocol}://${user}:${password}@${host}:${port}/")
    done
    echo "${uri_params[@]}"
}

# Function to show warnings and info for OMQ parameters
show_amqp10_warnings() {
    local warnings=()
    local info_messages=()
    
    # Extract publisher and consumer protocols from PROTOCOL (format: publisher-consumer)
    local publisher_protocol=$(echo "$PROTOCOL" | cut -d'-' -f1)
    local consumer_protocol=$(echo "$PROTOCOL" | cut -d'-' -f2)
    
    # Only show warnings for truly unsupported parameters
    [[ -n "${MULTI_ACK:-}" ]] && warnings+=("--multi-ack-every is not supported in AMQP 1.0 mode (OMQ)")
    
    # Validate AMQP-specific stream filtering options
    if [[ -n "${STREAM_FILTER_KEY:-}" && -n "${STREAM_FILTER_VALUES:-}" ]]; then
        if [[ "$publisher_protocol" != "amqp" ]]; then
            warnings+=("--amqp-app-property (stream filtering) only works with AMQP 1.0 publisher")
            warnings+=("  Current publisher protocol: $publisher_protocol (stream filtering will be ignored)")
        fi
    fi
    
    if [[ -n "${STREAM_FILTER_KEY:-}" && -n "${STREAM_FILTER_MATCH_UNFILTERED:-}" ]]; then
        if [[ "$consumer_protocol" != "amqp" ]]; then
            warnings+=("--amqp-app-property-filter (stream filtering) only works with AMQP 1.0 consumer")
            warnings+=("  Current consumer protocol: $consumer_protocol (stream filtering will be ignored)")
        fi
    fi
    
    # Show info about supported parameters with different behavior
    if [[ -n "${CONFIRM:-}" ]]; then
        info_messages+=("--confirm: Mapped to OMQ --max-in-flight=$CONFIRM (publisher flow control)")
    else
        info_messages+=("--confirm: Not configured, using OMQ default --max-in-flight=1")
    fi
    
    if [[ -n "${QOS:-}" ]]; then
        info_messages+=("--qos: Mapped to OMQ --consumer-credits=$QOS (consumer flow control)")
    else
        info_messages+=("--qos: Not configured, using OMQ default --consumer-credits=1")
    fi
    
    # autoack handling depends on consumer protocol
    if [[ "${AUTOACK:-false}" == "true" ]]; then
        if [[ "$consumer_protocol" == "amqp" ]]; then
            info_messages+=("--autoack: Using OMQ --amqp-consume-settled (AMQP 1.0 settled mode)")
        else
            warnings+=("--autoack is configured but --amqp-consume-settled only works with AMQP 1.0 consumer")
            warnings+=("  Current consumer protocol: $consumer_protocol (autoack will be ignored)")
        fi
    else
        if [[ "$consumer_protocol" == "amqp" ]]; then
            info_messages+=("--autoack: Using OMQ unsettled consume mode (manual ack)")
        fi
    fi
    
    # Stream-specific info
    if [[ "${QUEUE_TYPE:-quorum}" == "stream" ]]; then
        info_messages+=("--queue_type: stream (AMQP 1.0 stream support)")
        
        if [[ -n "${OFFSET:-}" ]]; then
            info_messages+=("--offset: $OFFSET (stream consumer offset)")
        else
            info_messages+=("--offset: next (default - stream consumer offset)")
        fi
        
        if [[ -n "${STREAM_FILTER_KEY:-}" && -n "${STREAM_FILTER_VALUES:-}" ]]; then
            info_messages+=("--stream_filter: Publishing with property ${STREAM_FILTER_KEY}=${STREAM_FILTER_VALUES}")
        else
            info_messages+=("--stream_filter: No filtering configured (all messages)")
        fi
        
        if [[ -n "${STREAM_FILTER_KEY:-}" && -n "${STREAM_FILTER_MATCH_UNFILTERED:-}" ]]; then
            info_messages+=("--stream_filter: Consuming with filter ${STREAM_FILTER_KEY}=${STREAM_FILTER_MATCH_UNFILTERED}")
        fi
    fi
    
    if [[ ${#warnings[@]} -gt 0 ]]; then
        echo "WARNING: The following parameters are ignored when using OMQ (AMQP 1.0):"
        for warning in "${warnings[@]}"; do
            echo "  - $warning"
        done
        echo ""
    fi
    
    if [[ ${#info_messages[@]} -gt 0 ]]; then
        echo "INFO: OMQ parameter mapping:"
        for info in "${info_messages[@]}"; do
            echo "  - $info"
        done
        echo ""
    fi
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

# Stream filter parameters (AMQP 1.0 stream filtering)
STREAM_FILTER_KEY=$(parse_yaml_value "stream_filter_key")
STREAM_FILTER_VALUES=$(parse_yaml_value "stream_filter_values")
STREAM_FILTER_MATCH_UNFILTERED=$(parse_yaml_value "stream_filter_match_unfiltered")

# Show warnings for unsupported OMQ parameters
show_amqp10_warnings

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
if [[ -n "$CA_CERT" ]]; then
    echo "🔐 TLS Mode: Using CA certificate $CA_CERT"
    
    # Validate CA certificate file exists
    if [[ ! -f "$CA_CERT" ]]; then
        echo "Error: CA certificate file not found: $CA_CERT"
        exit 1
    fi
    
    # Configure OMQ TLS certificate handling
    echo "🔐 Configuring OMQ TLS certificate handling..."
    
    # Create combined CA file for Go applications
    COMBINED_CA_FILE="/tmp/omq-combined-ca-${TIMESTAMP}.crt"
    
    # Start with system CAs
    if [[ -f "/etc/ssl/certs/ca-certificates.crt" ]]; then
        cp /etc/ssl/certs/ca-certificates.crt "$COMBINED_CA_FILE"
    elif [[ -f "/etc/ssl/cert.pem" ]]; then
        cp /etc/ssl/cert.pem "$COMBINED_CA_FILE"
    elif [[ -f "/etc/pki/tls/certs/ca-bundle.crt" ]]; then
        cp /etc/pki/tls/certs/ca-bundle.crt "$COMBINED_CA_FILE"
    else
        # Create empty file if no system CAs found
        touch "$COMBINED_CA_FILE"
        echo "   ⚠️  No system CA bundle found. Using only provided CA certificate."
    fi
    
    # Append provided CA
    cat "$CA_CERT" >> "$COMBINED_CA_FILE"
    
    # Set environment variable for OMQ
    export SSL_CERT_FILE="$COMBINED_CA_FILE"
    echo "   ✅ SSL_CERT_FILE set to $COMBINED_CA_FILE for OMQ"
    
    # TLS ports and protocols
    AMQP_PORT="5671"
    AMQP_PROTOCOL="amqps"
    
    # Warning about OMQ TLS limitations
    echo "⚠️  WARNING: OMQ has known issues with TLS certificate verification"
    echo "   If TLS fails, consider using perf-test (AMQP 0.9.1) instead"
    echo "   Or test OMQ without TLS first: --hosts <ip> (without --ca-cert)"
else
    echo "🔓 Non-TLS Mode: Using standard connections"
    # Non-TLS ports and protocols
    AMQP_PORT="5672"
    AMQP_PROTOCOL="amqp"
fi

# --- Build command ---
# Multi-protocol test with OMQ
TARGET_HOSTS="${PUB_HOSTS:-$HOSTS}"

CMD=("$TOOLS_DIR/omq" "$PROTOCOL")
    
    # Add multiple --uri parameters for OMQ
    OMQ_URI_PARAMS=($(build_omq_uri_params "$TARGET_HOSTS" "$AMQP_PROTOCOL" "$AMQP_PORT" "$USER" "$PASSWORD" "--uri"))
    CMD+=("${OMQ_URI_PARAMS[@]}")
    
    [[ -n "$PUBLISHERS" ]] && CMD+=(--publishers "$PUBLISHERS")
    [[ -n "$CONSUMERS" ]] && CMD+=(--consumers "$CONSUMERS")
    [[ -n "$DURATION" ]] && CMD+=(--time "${DURATION}s")
    [[ -n "$MESSAGE_SIZE" ]] && CMD+=(--size "$MESSAGE_SIZE")
    [[ -n "$PUB_RATE" && "$PUB_RATE" != "0" ]] && CMD+=(--rate "$PUB_RATE")
    
    # Queue configuration for OMQ
    if [[ -n "${QUEUE_NAME:-}" ]]; then
        CMD+=(--publish-to "/queues/${QUEUE_NAME}")
        CMD+=(--consume-from "/queues/${QUEUE_NAME}")
    else
        CMD+=(--publish-to "/queues/${TEST_NAME}")
        CMD+=(--consume-from "/queues/${TEST_NAME}")
    fi
    
    # Queue type for OMQ
    case "${QUEUE_TYPE:-quorum}" in
        quorum)
            CMD+=(--queues quorum)
            ;;
        classic)
            CMD+=(--queues classic)
            ;;
        stream)
            CMD+=(--queues stream)
            ;;
    esac
    
    # TLS options
    [[ "$TLS_SKIP_VERIFY" == "true" ]] && CMD+=(--tls-skip-verify)
    
    # AMQP-specific options based on scenario configuration
    # Handle confirm setting (--max-in-flight)
    if [[ -n "${CONFIRM:-}" ]]; then
        CMD+=(--max-in-flight "$CONFIRM")
    else
        CMD+=(--max-in-flight 1)
    fi
    
    # Handle qos setting (--consumer-credits)
    if [[ -n "${QOS:-}" ]]; then
        CMD+=(--consumer-credits "$QOS")
    else
        CMD+=(--consumer-credits 1)
    fi
    
    # Handle autoack setting (--amqp-consume-settled)
    # Only use --amqp-consume-settled for AMQP 1.0 consumer protocol
    if [[ "${AUTOACK:-false}" == "true" ]]; then
        # Extract consumer protocol from PROTOCOL (format: publisher-consumer)
        CONSUMER_PROTOCOL=$(echo "$PROTOCOL" | cut -d'-' -f2)
        if [[ "$CONSUMER_PROTOCOL" == "amqp" ]]; then
            CMD+=(--amqp-consume-settled)
        fi
        # Warning is already shown in show_amqp10_warnings if protocol is not AMQP 1.0
    fi
    
    # Stream-specific options (AMQP 1.0 stream filtering)
    if [[ "${QUEUE_TYPE:-quorum}" == "stream" ]]; then
        # Stream offset for consumer (default: next)
        if [[ -n "${OFFSET:-}" ]]; then
            CMD+=(--stream-offset "$OFFSET")
        else
            # Default offset: 'next' - start consuming from the next message
            CMD+=(--stream-offset next)
        fi
        
        # AMQP 1.0 Stream Filtering Support (RabbitMQ 4.1+)
        # Extract publisher and consumer protocols
        PUBLISHER_PROTOCOL=$(echo "$PROTOCOL" | cut -d'-' -f1)
        CONSUMER_PROTOCOL=$(echo "$PROTOCOL" | cut -d'-' -f2)
        
        # Publisher: Set application properties with multiple values
        # Only for AMQP 1.0 publisher (amqp-*)
        if [[ -n "${STREAM_FILTER_KEY:-}" && -n "${STREAM_FILTER_VALUES:-}" ]]; then
            if [[ "$PUBLISHER_PROTOCOL" == "amqp" ]]; then
                CMD+=(--amqp-app-property "${STREAM_FILTER_KEY}=${STREAM_FILTER_VALUES}")
            fi
            # Warning is already shown in show_amqp10_warnings if protocol is not AMQP 1.0
        fi
        
        # Consumer: Apply filter to consume only matching messages
        # Only for AMQP 1.0 consumer (*-amqp)
        if [[ -n "${STREAM_FILTER_KEY:-}" && -n "${STREAM_FILTER_MATCH_UNFILTERED:-}" ]]; then
            if [[ "$CONSUMER_PROTOCOL" == "amqp" ]]; then
                CMD+=(--amqp-app-property-filter "${STREAM_FILTER_KEY}=${STREAM_FILTER_MATCH_UNFILTERED}")
            fi
            # Warning is already shown in show_amqp10_warnings if protocol is not AMQP 1.0
        fi
    fi

# Add extra args if provided
if [[ -n "$EXTRA_ARGS" ]]; then
    CMD+=($EXTRA_ARGS)
fi

# --- Run ---
echo "=============================================="
echo "  RabbitMQ Multi-Protocol Performance Test (OMQ)"
echo "=============================================="
echo "  Scenario:  $SCENARIO"
echo "  Type:      $TEST_TYPE"
echo "  Protocol:  $PROTOCOL"
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
    
    # Build producer command (no consumers)
    PRODUCER_CMD=("$TOOLS_DIR/omq" "$PROTOCOL")
    
    # Add multiple --publisher-uri parameters for OMQ
    OMQ_PUB_URI_PARAMS=($(build_omq_uri_params "${PUB_HOSTS:-$HOSTS}" "$AMQP_PROTOCOL" "$AMQP_PORT" "$USER" "$PASSWORD" "--publisher-uri"))
    PRODUCER_CMD+=("${OMQ_PUB_URI_PARAMS[@]}")
    
    # Add management URI for queue declaration (use first publisher host)
    FIRST_PUB_HOST=$(echo "${PUB_HOSTS:-$HOSTS}" | cut -d',' -f1)
    MGMT_URI="${AMQP_PROTOCOL}://${USER}:${PASSWORD}@${FIRST_PUB_HOST}:${AMQP_PORT}/"
    PRODUCER_CMD+=(--management-uri "$MGMT_URI")
    
    PRODUCER_CMD+=(--publishers "${PUBLISHERS:-1}")
    PRODUCER_CMD+=(--consumers 0)
    PRODUCER_CMD+=(--use-millis)
    [[ -n "$DURATION" ]] && PRODUCER_CMD+=(--time "${DURATION}s")
    [[ -n "$MESSAGE_SIZE" ]] && PRODUCER_CMD+=(--size "$MESSAGE_SIZE")
    [[ -n "$PUB_RATE" && "$PUB_RATE" != "0" ]] && PRODUCER_CMD+=(--rate "$PUB_RATE")
    
    # Queue configuration for OMQ producer
    if [[ -n "${QUEUE_NAME:-}" ]]; then
        PRODUCER_CMD+=(--publish-to "/queues/${QUEUE_NAME}")
    else
        PRODUCER_CMD+=(--publish-to "/queues/${TEST_NAME}")
    fi
    
    # Queue type for OMQ producer
    case "${QUEUE_TYPE:-quorum}" in
        quorum)
            PRODUCER_CMD+=(--queues quorum)
            ;;
        classic)
            PRODUCER_CMD+=(--queues classic)
            ;;
        stream)
            PRODUCER_CMD+=(--queues stream)
            ;;
    esac
    
    # TLS options for producer
    [[ "$TLS_SKIP_VERIFY" == "true" ]] && PRODUCER_CMD+=(--tls-skip-verify)
    
    # AMQP-specific options for producer
    if [[ -n "${CONFIRM:-}" ]]; then
        PRODUCER_CMD+=(--max-in-flight "$CONFIRM")
    else
        PRODUCER_CMD+=(--max-in-flight 1)
    fi
    
    # Stream-specific options for producer (AMQP 1.0 stream filtering)
    if [[ "${QUEUE_TYPE:-quorum}" == "stream" ]]; then
        # AMQP 1.0 Stream Filtering: Set application properties with multiple values
        # Only for AMQP 1.0 publisher (amqp-*)
        if [[ -n "${STREAM_FILTER_KEY:-}" && -n "${STREAM_FILTER_VALUES:-}" ]]; then
            PUBLISHER_PROTOCOL=$(echo "$PROTOCOL" | cut -d'-' -f1)
            if [[ "$PUBLISHER_PROTOCOL" == "amqp" ]]; then
                PRODUCER_CMD+=(--amqp-app-property "${STREAM_FILTER_KEY}=${STREAM_FILTER_VALUES}")
            fi
            # Warning is already shown in show_amqp10_warnings if protocol is not AMQP 1.0
        fi
    fi
    
    # Build consumer command (no producers, different host)
    CONSUMER_CMD=("$TOOLS_DIR/omq" "$PROTOCOL")
    
    # Add multiple --consumer-uri parameters for OMQ
    OMQ_CON_URI_PARAMS=($(build_omq_uri_params "$CON_HOSTS" "$AMQP_PROTOCOL" "$AMQP_PORT" "$USER" "$PASSWORD" "--consumer-uri"))
    CONSUMER_CMD+=("${OMQ_CON_URI_PARAMS[@]}")
    
    # Add management URI for queue verification (use first consumer host or publisher host)
    FIRST_CON_HOST=$(echo "$CON_HOSTS" | cut -d',' -f1)
    CON_MGMT_URI="${AMQP_PROTOCOL}://${USER}:${PASSWORD}@${FIRST_CON_HOST}:${AMQP_PORT}/"
    CONSUMER_CMD+=(--management-uri "$CON_MGMT_URI")
    
    CONSUMER_CMD+=(--publishers 0)
    CONSUMER_CMD+=(--consumers "${CONSUMERS:-1}")
    CONSUMER_CMD+=(--use-millis)
    [[ -n "$DURATION" ]] && CONSUMER_CMD+=(--time "${DURATION}s")
    
    # Queue configuration for OMQ consumer
    if [[ -n "${QUEUE_NAME:-}" ]]; then
        CONSUMER_CMD+=(--consume-from "/queues/${QUEUE_NAME}")
    else
        CONSUMER_CMD+=(--consume-from "/queues/${TEST_NAME}")
    fi
    
    # Queue type for OMQ consumer (predeclared since producer creates it)
    CONSUMER_CMD+=(--queues predeclared)
    
    # TLS options for consumer
    [[ "$TLS_SKIP_VERIFY" == "true" ]] && CONSUMER_CMD+=(--tls-skip-verify)
    
    # AMQP-specific options for consumer
    if [[ -n "${QOS:-}" ]]; then
        CONSUMER_CMD+=(--consumer-credits "$QOS")
    else
        CONSUMER_CMD+=(--consumer-credits 1)
    fi
    
    if [[ "${AUTOACK:-false}" == "true" ]]; then
        # Extract consumer protocol from PROTOCOL (format: publisher-consumer)
        CONSUMER_PROTOCOL=$(echo "$PROTOCOL" | cut -d'-' -f2)
        if [[ "$CONSUMER_PROTOCOL" == "amqp" ]]; then
            CONSUMER_CMD+=(--amqp-consume-settled)
        fi
        # Warning is already shown in show_amqp10_warnings if protocol is not AMQP 1.0
    fi
    
    # Stream-specific options for consumer (AMQP 1.0 stream filtering)
    if [[ "${QUEUE_TYPE:-quorum}" == "stream" ]]; then
        # Stream offset for consumer (default: next)
        if [[ -n "${OFFSET:-}" ]]; then
            CONSUMER_CMD+=(--stream-offset "$OFFSET")
        else
            # Default offset: 'next' - start consuming from the next message
            CONSUMER_CMD+=(--stream-offset next)
        fi
        
        # AMQP 1.0 Stream Filtering: Apply filter to consume only matching messages
        # Only for AMQP 1.0 consumer (*-amqp)
        if [[ -n "${STREAM_FILTER_KEY:-}" && -n "${STREAM_FILTER_MATCH_UNFILTERED:-}" ]]; then
            CONSUMER_PROTOCOL=$(echo "$PROTOCOL" | cut -d'-' -f2)
            if [[ "$CONSUMER_PROTOCOL" == "amqp" ]]; then
                CONSUMER_CMD+=(--amqp-app-property-filter "${STREAM_FILTER_KEY}=${STREAM_FILTER_MATCH_UNFILTERED}")
            fi
            # Warning is already shown in show_amqp10_warnings if protocol is not AMQP 1.0
        fi
    fi
    
    # OMQ automatically binds to available ports: 8080 for producer, 8081 for consumer
    
    # Display commands for federation test
    echo "📤 Producer Command (${PUB_HOSTS:-$HOSTS}):"
    echo "   ${PRODUCER_CMD[*]}"
    echo ""
    echo "📥 Consumer Command ($CON_HOSTS):"
    echo "   ${CONSUMER_CMD[*]}"
    echo ""
    echo "ℹ️  Federation Mode: Producer runs first (creates queue), then consumer (predeclared queue)"
    echo "ℹ️  Metrics Ports: OMQ auto-assigns (Producer=8080, Consumer=8081)"
    echo ""
    
    # Write headers to result files
    {
        echo "# Scenario: $SCENARIO (Producer)"
        echo "# Date: $(date -u '+%Y-%m-%d %H:%M:%S UTC')"
        echo "# Producer Host: ${PUB_HOSTS:-$HOSTS}"
        echo "# Consumer Host: $CON_HOSTS"
        [[ -n "$LABEL" ]] && echo "# Label: $LABEL"
        echo "# Duration: ${DURATION}s"
        echo "# Producer Command: ${PRODUCER_CMD[*]}"
        echo "#"
    } > "$RESULT_FILE"
    
    {
        echo "# Scenario: $SCENARIO (Consumer)"
        echo "# Date: $(date -u '+%Y-%m-%d %H:%M:%S UTC')"
        echo "# Producer Host: ${PUB_HOSTS:-$HOSTS}"
        echo "# Consumer Host: $CON_HOSTS"
        [[ -n "$LABEL" ]] && echo "# Label: $LABEL"
        echo "# Duration: ${DURATION}s"
        echo "# Consumer Command: ${CONSUMER_CMD[*]}"
        echo "#"
    } > "${RESULT_FILE}.consumer"
    
    # Write headers to result files
    {
        echo "# Scenario: $SCENARIO (Producer)"
        echo "# Date: $(date -u '+%Y-%m-%d %H:%M:%S UTC')"
        echo "# Producer Host: ${PUB_HOSTS:-$HOSTS}"
        echo "# Consumer Host: $CON_HOSTS"
        [[ -n "$LABEL" ]] && echo "# Label: $LABEL"
        echo "# Duration: ${DURATION}s"
        echo "# Protocol: $PROTOCOL"
        echo "# Producer Command: ${PRODUCER_CMD[*]}"
        echo "#"
    } > "$RESULT_FILE"
    
    {
        echo "# Scenario: $SCENARIO (Consumer)"
        echo "# Date: $(date -u '+%Y-%m-%d %H:%M:%S UTC')"
        echo "# Producer Host: ${PUB_HOSTS:-$HOSTS}"
        echo "# Consumer Host: $CON_HOSTS"
        [[ -n "$LABEL" ]] && echo "# Label: $LABEL"
        echo "# Duration: ${DURATION}s"
        echo "# Protocol: $PROTOCOL"
        echo "# Consumer Command: ${CONSUMER_CMD[*]}"
        echo "#"
    } > "${RESULT_FILE}.consumer"
    
    echo "🚀 Starting producer process on ${PUB_HOSTS:-$HOSTS} (creating queues and publishing messages)..."
    
    # Start producer with metrics collection if available
    if [[ -f "$SCRIPT_DIR/omq-metrics-collector.sh" ]]; then
        echo "📊 Starting producer with metrics collection (port 8080)..."
        "${PRODUCER_CMD[@]}" > "$RESULT_FILE.producer.omq" 2>&1 &
        PRODUCER_PID=$!
        
        "$SCRIPT_DIR/omq-metrics-collector.sh" "${TEST_NAME}-producer" "$PRODUCER_PID" "$RESULT_FILE.producer.metrics" "$RESULT_FILE.producer.omq" 8080 &
        PRODUCER_METRICS_PID=$!
        
        # Give producer a moment to start and create queues
        echo "⏳ Giving producer 3 seconds to create queues..."
        sleep 3
        
    else
        echo "⚠️  Running producer without metrics collection..."
        "${PRODUCER_CMD[@]}" 2>&1 | tee -a "$RESULT_FILE" &
        PRODUCER_PID=$!
        
        # Give producer a moment to start and create queues
        echo "⏳ Giving producer 3 seconds to create queues..."
        sleep 3
    fi
    
    echo "🚀 Starting consumer process on $CON_HOSTS (consuming from predeclared queue)..."
    
    # Start consumer with metrics collection if available
    if [[ -f "$SCRIPT_DIR/omq-metrics-collector.sh" ]]; then
        echo "📊 Starting consumer with metrics collection (port 8081)..."
        "${CONSUMER_CMD[@]}" > "${RESULT_FILE}.consumer.omq" 2>&1 &
        CONSUMER_PID=$!
        
        # Start consumer metrics collector in background but capture its output
        "$SCRIPT_DIR/omq-metrics-collector.sh" "${TEST_NAME}-consumer" "$CONSUMER_PID" "$RESULT_FILE.consumer.metrics" "${RESULT_FILE}.consumer.omq" 8081 > "$RESULT_FILE.consumer.metrics.display" 2>&1 &
        CONSUMER_METRICS_PID=$!
        
    else
        echo "⚠️  Running consumer without metrics collection..."
        "${CONSUMER_CMD[@]}" 2>&1 | tee -a "${RESULT_FILE}.consumer" &
        CONSUMER_PID=$!
    fi
    
    echo ""
    echo "🔄 Both processes running in parallel..."
    echo "   Producer PID: $PRODUCER_PID (metrics port 8080)"
    echo "   Consumer PID: $CONSUMER_PID (metrics port 8081)"
    echo ""
    
    # Monitor and display metrics from both processes in real-time
    echo "📊 Monitoring both processes (press Ctrl+C to stop early)..."
    echo ""
    
    # Function to display metrics from both files
    display_metrics() {
        local producer_metrics="$RESULT_FILE.producer.metrics"
        local consumer_metrics="$RESULT_FILE.consumer.metrics.display"
        local last_producer_lines=0
        local last_consumer_lines=0
        
        while [[ -e /proc/$PRODUCER_PID || -e /proc/$CONSUMER_PID ]]; do
            # Show new producer metrics
            if [[ -f "$producer_metrics" ]]; then
                local current_producer_lines=$(wc -l < "$producer_metrics" 2>/dev/null || echo 0)
                if [[ $current_producer_lines -gt $last_producer_lines ]]; then
                    tail -n +$((last_producer_lines + 1)) "$producer_metrics" 2>/dev/null || true
                    last_producer_lines=$current_producer_lines
                fi
            fi
            
            # Show new consumer metrics
            if [[ -f "$consumer_metrics" ]]; then
                local current_consumer_lines=$(wc -l < "$consumer_metrics" 2>/dev/null || echo 0)
                if [[ $current_consumer_lines -gt $last_consumer_lines ]]; then
                    tail -n +$((last_consumer_lines + 1)) "$consumer_metrics" 2>/dev/null || true
                    last_consumer_lines=$current_consumer_lines
                fi
            fi
            
            sleep 1
        done
    }
    
    # Start monitoring in background
    display_metrics &
    MONITOR_PID=$!
    
    # Wait for both processes to complete
    wait $PRODUCER_PID
    PRODUCER_EXIT_CODE=$?
    
    wait $CONSUMER_PID
    CONSUMER_EXIT_CODE=$?
    
    # Stop monitoring
    kill $MONITOR_PID 2>/dev/null || true
    wait $MONITOR_PID 2>/dev/null || true
    
    # Stop metrics collectors
    if [[ -n "${PRODUCER_METRICS_PID:-}" ]]; then
        kill "$PRODUCER_METRICS_PID" 2>/dev/null || true
        wait "$PRODUCER_METRICS_PID" 2>/dev/null || true
    fi
    
    if [[ -n "${CONSUMER_METRICS_PID:-}" ]]; then
        kill "$CONSUMER_METRICS_PID" 2>/dev/null || true
        wait "$CONSUMER_METRICS_PID" 2>/dev/null || true
    fi
    
    echo ""
    echo "✅ Producer completed with exit code: $PRODUCER_EXIT_CODE"
    echo "✅ Consumer completed with exit code: $CONSUMER_EXIT_CODE"
    echo ""
    
    # Merge outputs like in non-federation mode
    echo "📊 Merging federation test outputs..."
    
    # Create final merged output for producer
    {
        echo "# Scenario: $SCENARIO (Producer)"
        echo "# Date: $(date -u '+%Y-%m-%d %H:%M:%S UTC')"
        echo "# Producer Host: ${PUB_HOSTS:-$HOSTS}"
        echo "# Consumer Host: $CON_HOSTS"
        [[ -n "$LABEL" ]] && echo "# Label: $LABEL"
        echo "# Duration: ${DURATION}s"
        echo "# Protocol: $PROTOCOL"
        echo "# Producer Command: ${PRODUCER_CMD[*]}"
        echo "#"
        echo ""
        
        # Add producer metrics if available
        if [[ -f "$RESULT_FILE.producer.metrics" ]]; then
            echo "# Formatted Metrics (perf-test style):"
            cat "$RESULT_FILE.producer.metrics"
            echo ""
        fi
        
        echo "# Original OMQ Output:"
        cat "$RESULT_FILE.producer.omq"
    } > "$RESULT_FILE"
    
    # Create final merged output for consumer
    {
        echo "# Scenario: $SCENARIO (Consumer)"
        echo "# Date: $(date -u '+%Y-%m-%d %H:%M:%S UTC')"
        echo "# Producer Host: ${PUB_HOSTS:-$HOSTS}"
        echo "# Consumer Host: $CON_HOSTS"
        [[ -n "$LABEL" ]] && echo "# Label: $LABEL"
        echo "# Duration: ${DURATION}s"
        echo "# Protocol: $PROTOCOL"
        echo "# Consumer Command: ${CONSUMER_CMD[*]}"
        echo "#"
        echo ""
        
        # Add consumer metrics if available
        if [[ -f "$RESULT_FILE.consumer.metrics" ]]; then
            echo "# Formatted Metrics (perf-test style):"
            cat "$RESULT_FILE.consumer.metrics"
            echo ""
        fi
        
        echo "# Original OMQ Output:"
        cat "${RESULT_FILE}.consumer.omq"
    } > "${RESULT_FILE}.consumer"
    
    # Cleanup temporary files
    rm -f "$RESULT_FILE.producer.omq" "${RESULT_FILE}.consumer.omq" "$RESULT_FILE.producer.metrics" "$RESULT_FILE.consumer.metrics" "$RESULT_FILE.consumer.metrics.display"
    
    echo "📁 Federation test results:"
    echo "  Producer: $RESULT_FILE"
    echo "  Consumer: ${RESULT_FILE}.consumer"
    
    # Exit with error if either process failed
    if [[ $PRODUCER_EXIT_CODE -ne 0 || $CONSUMER_EXIT_CODE -ne 0 ]]; then
        echo "❌ Federation test failed - Producer: $PRODUCER_EXIT_CODE, Consumer: $CONSUMER_EXIT_CODE"
        exit 1
    fi
    
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
        echo "# Host: ${PUB_HOSTS:-$HOSTS}"
        [[ -n "$LABEL" ]] && echo "# Label: $LABEL"
        echo "# Duration: ${DURATION}s"
        echo "# Command: ${CMD[*]}"
        echo "#"
    } > "$RESULT_FILE"
    
    # Start OMQ in background to capture PID
    echo "🚀 Starting OMQ process..."
    "${CMD[@]}" > "$RESULT_FILE.omq" 2>&1 &
    OMQ_PID=$!
    
    echo "   OMQ PID: $OMQ_PID"
    echo "   OMQ output: $RESULT_FILE.omq"
    echo ""
    
    # Start metrics collector in background
    if [[ -f "$SCRIPT_DIR/omq-metrics-collector.sh" ]]; then
        echo "📊 Starting metrics collection..."
        "$SCRIPT_DIR/omq-metrics-collector.sh" "$TEST_NAME" "$OMQ_PID" "$RESULT_FILE.metrics" "$RESULT_FILE.omq" &
        METRICS_PID=$!
        
        # Wait for OMQ to complete
        wait "$OMQ_PID"
        OMQ_EXIT_CODE=$?
        
        # Stop metrics collector
        kill "$METRICS_PID" 2>/dev/null || true
        wait "$METRICS_PID" 2>/dev/null || true
        
        echo ""
        echo "📊 Merging OMQ output with metrics..."
        
        # Create final merged output
        {
            echo "# Scenario: $SCENARIO"
            echo "# Date: $(date -u '+%Y-%m-%d %H:%M:%S UTC')"
            echo "# Host: ${PUB_HOSTS:-$HOSTS}"
            [[ -n "$LABEL" ]] && echo "# Label: $LABEL"
            echo "# Duration: ${DURATION}s"
            echo "# Protocol: $PROTOCOL"
            echo "# Command: ${CMD[*]}"
            echo "#"
            echo ""
            
            # Add formatted metrics if available
            if [[ -f "$RESULT_FILE.metrics" ]]; then
                echo "# Formatted Metrics (perf-test style):"
                cat "$RESULT_FILE.metrics"
                echo ""
            fi
            
            echo "# Original OMQ Output:"
            cat "$RESULT_FILE.omq"
        } > "$RESULT_FILE"
        
        # Cleanup temporary files
        rm -f "$RESULT_FILE.omq" "$RESULT_FILE.metrics"
        
        exit $OMQ_EXIT_CODE
    else
        echo "⚠️  omq-metrics-collector.sh not found. Using basic output..."
        
        # Fallback: just run OMQ normally and show its output
        wait "$OMQ_PID"
        OMQ_EXIT_CODE=$?
        
        # Move OMQ output to final result file
        {
            echo "# Scenario: $SCENARIO"
            echo "# Date: $(date -u '+%Y-%m-%d %H:%M:%S UTC')"
            echo "# Host: ${PUB_HOSTS:-$HOSTS}"
            [[ -n "$LABEL" ]] && echo "# Label: $LABEL"
            echo "# Duration: ${DURATION}s"
            echo "# Protocol: $PROTOCOL"
            echo "# Command: ${CMD[*]}"
            echo "#"
            echo ""
            cat "$RESULT_FILE.omq"
        } > "$RESULT_FILE"
        
        #rm -f "$RESULT_FILE.omq"
        exit $OMQ_EXIT_CODE
    fi
    
    echo ""
    echo "Results saved to: $RESULT_FILE"
fi

# --- Cleanup temporary files ---
if [[ -n "${COMBINED_CA_FILE:-}" && -f "$COMBINED_CA_FILE" ]]; then
    rm -f "$COMBINED_CA_FILE"
fi