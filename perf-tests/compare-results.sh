#!/bin/bash
# =============================================================================
# Quick Results Summary
#
# Displays a summary of perf-test results for comparison.
# Extracts the final summary line from each result file.
#
# Usage:
#   ./perf-tests/compare-results.sh                    # All results
#   ./perf-tests/compare-results.sh baseline            # Filter by scenario
#   ./perf-tests/compare-results.sh --last 5            # Last 5 results
# =============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
RESULTS_DIR="$SCRIPT_DIR/results"

FILTER=""
LAST=""

while [[ $# -gt 0 ]]; do
    case "$1" in
        --last) LAST="$2"; shift 2 ;;
        *)      FILTER="$1"; shift ;;
    esac
done

if [[ ! -d "$RESULTS_DIR" ]] || [[ -z "$(ls -A "$RESULTS_DIR"/*.txt 2>/dev/null)" ]]; then
    echo "No results found in $RESULTS_DIR"
    echo "Run a test first: ./perf-tests/run-test.sh baseline --host <ip>"
    exit 0
fi

echo "=============================================="
echo "  Performance Test Results"
echo "=============================================="
echo ""

# Helper to convert µs to ms
convert_us_to_ms() {
    local val="$1"
    # Remove any trailing units like ' µs' or ' us' or ' ms'
    val=$(echo "$val" | sed 's/ [µu]s//g' | sed 's/ ms//g')
    
    if [[ -z "$val" || "$val" == "N/A" ]]; then
        echo "N/A"
        return
    fi
    
    # Check if it looks like a slash-separated list of numbers (possibly floating point)
    if [[ "$val" =~ ^[0-9./]+$ ]]; then
        echo "$val" | awk -F'/' '{OFS="/"; for(i=1;i<=NF;i++) $i=sprintf("%.3f", $i/1000)} 1'
    else
        echo "$val"
    fi
}

# Helper to detect test type (streams vs amqp)
detect_test_type() {
    local file="$1"
    if grep -q "stream-perf-test.jar\|--streams" "$file" 2>/dev/null; then
        echo "streams"
    else
        echo "amqp"
    fi
}

# Helper to parse AMQP results
parse_amqp_results() {
    local file="$1"
    local consumer_file="$2"
    
    local send_rate="N/A"
    local recv_rate="N/A"
    local confirm_lat="N/A"
    local consumer_lat="N/A"
    
    if [[ -f "$consumer_file" ]]; then
        # Split files: get non-zero values from each file
        local prod_send=$(grep -E "^id: .*, sending rate avg:" "$file" | sed 's/.*sending rate avg: \([0-9]*\) msg\/s.*/\1/' 2>/dev/null || echo "0")
        local prod_confirm=$(grep -E "^id: .*, confirm latency" "$file" | sed 's/.*confirm latency min\/median\/75th\/95th\/99th\/max \(.*\)/\1/' 2>/dev/null || echo "")
        
        local cons_recv=$(grep -E "^id: .*, receiving rate avg:" "$consumer_file" | sed 's/.*receiving rate avg: \([0-9]*\) msg\/s.*/\1/' 2>/dev/null || echo "0")
        local cons_lat=$(grep -E "^id: .*, consumer latency" "$consumer_file" | sed 's/.*consumer latency min\/median\/75th\/95th\/99th\/max \(.*\)/\1/' 2>/dev/null || echo "")
        
        # Use non-zero values
        [[ "$prod_send" != "0" ]] && send_rate="$prod_send"
        [[ "$cons_recv" != "0" ]] && recv_rate="$cons_recv"
        [[ -n "$prod_confirm" ]] && confirm_lat="$prod_confirm"
        [[ -n "$cons_lat" ]] && consumer_lat="$cons_lat"
        
    else
        # Single file: extract all metrics
        send_rate=$(grep -E "^id: .*, sending rate avg:" "$file" | tail -1 | sed 's/.*sending rate avg: \([0-9]*\) msg\/s.*/\1/' 2>/dev/null || echo "N/A")
        recv_rate=$(grep -E "^id: .*, receiving rate avg:" "$file" | tail -1 | sed 's/.*receiving rate avg: \([0-9]*\) msg\/s.*/\1/' 2>/dev/null || echo "N/A")
        confirm_lat=$(grep -E "^id: .*, confirm latency min/median/75th/95th/99th/max" "$file" | tail -1 | sed 's/.*confirm latency min\/median\/75th\/95th\/99th\/max \(.*\)/\1/' 2>/dev/null || echo "N/A")
        consumer_lat=$(grep -E "^id: .*, consumer latency min/median/75th/95th/99th/max" "$file" | tail -1 | sed 's/.*consumer latency min\/median\/75th\/95th\/99th\/max \(.*\)/\1/' 2>/dev/null || echo "N/A")
        
        # Fallback for inline format (consumer latency: X, confirm latency: Y)
        if [[ "$confirm_lat" == "N/A" ]]; then
            local line=$(grep "confirm latency: [0-9]" "$file" | tail -1 2>/dev/null || true)
            if [[ -n "$line" ]]; then
                confirm_lat=$(echo "$line" | sed -n 's/.*confirm latency: \([0-9\/]*\).*/\1/p')
            fi
        fi
        
        if [[ "$consumer_lat" == "N/A" ]]; then
            local line=$(grep "consumer latency: [0-9]" "$file" | tail -1 2>/dev/null || true)
            if [[ -n "$line" ]]; then
                consumer_lat=$(echo "$line" | sed -n 's/.*consumer latency: \([0-9\/]*\).*/\1/p')
            fi
        fi
    fi
    
    # Convert µs to ms if needed
    if [[ "$confirm_lat" == *"µs"* || "$confirm_lat" == *"us"* ]]; then
        confirm_lat=$(convert_us_to_ms "$confirm_lat")
        confirm_lat="${confirm_lat} ms"
    fi
    
    if [[ "$consumer_lat" == *"µs"* || "$consumer_lat" == *"us"* ]]; then
        consumer_lat=$(convert_us_to_ms "$consumer_lat")
        consumer_lat="${consumer_lat} ms"
    fi
    
    echo "$send_rate|$recv_rate|$confirm_lat|$consumer_lat"
}

# Helper to parse Stream results
parse_stream_results() {
    local file="$1"
    local consumer_file="$2"
    
    local send_rate="N/A"
    local recv_rate="N/A"
    local confirm_lat="N/A"
    local consumer_lat="N/A"
    
    # Parse producer file
    local summary=$(grep "^Summary:" "$file" | tail -1 2>/dev/null || true)
    if [[ -n "$summary" ]]; then
        send_rate=$(echo "$summary" | sed -n 's/.*published \([0-9]*\) msg\/s.*/\1/p')
        
        # If single file, get consumer data too
        if [[ ! -f "$consumer_file" ]]; then
            recv_rate=$(echo "$summary" | sed -n 's/.*consumed \([0-9]*\) msg\/s.*/\1/p')
            local lat_val=$(echo "$summary" | sed -n 's/.*latency 95th \([0-9]*\) ms.*/\1/p')
            if [[ -n "$lat_val" ]]; then
                consumer_lat="${lat_val} ms (95th)"
            fi
        fi
    fi
    
    # Parse consumer file if exists
    if [[ -f "$consumer_file" ]]; then
        local cons_summary=$(grep "^Summary:" "$consumer_file" | tail -1 2>/dev/null || true)
        if [[ -n "$cons_summary" ]]; then
            local cons_recv=$(echo "$cons_summary" | sed -n 's/.*consumed \([0-9]*\) msg\/s.*/\1/p')
            [[ "$cons_recv" != "0" ]] && recv_rate="$cons_recv"
            
            local lat_val=$(echo "$cons_summary" | sed -n 's/.*latency 95th \([0-9]*\) ms.*/\1/p')
            if [[ -n "$lat_val" ]]; then
                consumer_lat="${lat_val} ms (95th)"
            fi
        fi
    fi
    
    echo "$send_rate|$recv_rate|$confirm_lat|$consumer_lat"
}

# Collect matching files
FILES=()
for f in "$RESULTS_DIR"/*.txt; do
    if [[ -n "$FILTER" ]]; then
        if grep -q "# Scenario: .*${FILTER}" "$f" 2>/dev/null; then
            FILES+=("$f")
        fi
    else
        FILES+=("$f")
    fi
done

# Apply --last filter
if [[ -n "$LAST" && ${#FILES[@]} -gt $LAST ]]; then
    FILES=("${FILES[@]: -$LAST}")
fi

for f in "${FILES[@]}"; do
    FILENAME=$(basename "$f")
    SCENARIO=$(grep "^# Scenario:" "$f" 2>/dev/null | sed 's/# Scenario: //' || true)
    DATE=$(grep "^# Date:" "$f" 2>/dev/null | sed 's/# Date: //' || true)
    LABEL=$(grep "^# Label:" "$f" 2>/dev/null | sed 's/# Label: //' || true)
    
    HOST=$(grep "^# Host:" "$f" 2>/dev/null | sed 's/# Host: //' || true)
    if [[ -z "$HOST" ]]; then
        HOST=$(grep "^# Producer Host:" "$f" 2>/dev/null | sed 's/# Producer Host: //' || true)
    fi

    echo "--- $FILENAME ---"
    echo "  Scenario: $SCENARIO"
    echo "  Date:     $DATE"
    echo "  Host:     $HOST"
    [[ -n "$LABEL" ]] && echo "  Label:    $LABEL"

    # Detect test type and parse accordingly
    TEST_TYPE=$(detect_test_type "$f")
    CONSUMER_FILE="${f}.consumer"
    
    if [[ "$TEST_TYPE" == "streams" ]]; then
        RESULTS=$(parse_stream_results "$f" "$CONSUMER_FILE")
    else
        RESULTS=$(parse_amqp_results "$f" "$CONSUMER_FILE")
    fi
    
    IFS='|' read -r SEND_RATE RECV_RATE CONFIRM_LAT CONSUMER_LAT <<< "$RESULTS"

    # Display Tables
    printf "\n  %-20s | %-15s | %-15s\n" "Metric" "Send Rate" "Recv Rate"
    printf "  %-20s | %-15s | %-15s\n" "--------------------" "---------------" "---------------"
    printf "  %-20s | %-15s | %-15s\n" "Throughput (msg/s)" "$SEND_RATE" "$RECV_RATE"

    printf "\n  %-20s | %-60s\n" "Metric" "Latency (min/med/75/95/99/max)"
    printf "  %-20s | %-60s\n" "--------------------" "------------------------------------------------------------"
    printf "  %-20s | %-60s\n" "Confirm Latency" "$CONFIRM_LAT"
    printf "  %-20s | %-60s\n" "Consumer Latency" "$CONSUMER_LAT"

    echo ""
done