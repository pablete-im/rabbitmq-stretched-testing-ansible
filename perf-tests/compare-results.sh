#!/bin/bash
# =============================================================================
# Quick Results Summary
#
# Displays a summary of perf-test results for comparison.
# Organized by metric (one table per metric showing all tests).
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

# Associative arrays to store metrics for all tests
declare -A SENT_RATES
declare -A CONFIRMED_RATES
declare -A RECV_RATES
declare -A CONFIRM_LATS
declare -A CONSUMER_LATS
declare -A TEST_DATES
declare -A TEST_HOSTS
declare -A TEST_SCENARIOS

# Helper to parse OMQ formatted metrics and calculate averages
parse_formatted_metrics() {
    local file="$1"
    local metric="$2"  # "sent", "confirmed", "received", "published", "consumed"
    
    # Extract all metric values from "id: X, time Y s, sent: Z msg/s" lines
    local values=$(grep "^id: .*, time .* s, " "$file" 2>/dev/null | \
                   grep -o "${metric}: [0-9]* msg/s" | \
                   awk '{print $2}')
    
    if [[ -z "$values" ]]; then
        echo "N/A"
        return
    fi
    
    # Calculate average
    local sum=0
    local count=0
    while IFS= read -r val; do
        if [[ "$val" =~ ^[0-9]+$ ]]; then
            sum=$((sum + val))
            count=$((count + 1))
        fi
    done <<< "$values"
    
    if [[ $count -eq 0 ]]; then
        echo "N/A"
    else
        echo $((sum / count))
    fi
}

# Helper to parse stream-perf-test formatted metrics
parse_stream_perf_metrics() {
    local file="$1"
    local metric="$2"  # "published", "confirmed", "consumed"
    
    # Extract all metric values from stream-perf-test lines like:
    # "N, published X msg/s, confirmed Y msg/s, consumed Z msg/s, ..."
    local values=$(grep "^[0-9]*, ${metric} [0-9]* msg/s" "$file" 2>/dev/null | \
                   sed -n "s/.*${metric} \([0-9]*\) msg\/s.*/\1/p")
    
    if [[ -z "$values" ]]; then
        # Try summary line for stream-perf-test
        values=$(grep "^Summary: " "$file" 2>/dev/null | \
                 sed -n "s/.*${metric} \([0-9]*\) msg\/s.*/\1/p")
    fi
    
    if [[ -z "$values" ]]; then
        echo "N/A"
        return
    fi
    
    # Calculate average
    local sum=0
    local count=0
    while IFS= read -r val; do
        if [[ "$val" =~ ^[0-9]+$ ]]; then
            sum=$((sum + val))
            count=$((count + 1))
        fi
    done <<< "$values"
    
    if [[ $count -eq 0 ]]; then
        echo "N/A"
    else
        echo $((sum / count))
    fi
}

# Helper to extract final latencies from file and parse into individual quantiles
extract_final_latencies() {
    local file="$1"
    local latency_type="$2"  # "confirm" or "consumer"
    
    # First, check if this is a stream-perf-test file (has lines starting with numbers)
    # Format: "N, published X msg/s, confirmed Y msg/s, consumed Z msg/s, confirm latency median/75th/95th/99th W/X/Y/Z ms, latency median/75th/95th/99th A/B/C/D ms"
    local stream_perf_line=$(grep "^[0-9]*, published " "$file" 2>/dev/null | tail -1)
    
    if [[ -n "$stream_perf_line" ]]; then
        # This is stream-perf-test format
        if [[ "$latency_type" == "confirm" ]]; then
            # Extract: confirm latency median/75th/95th/99th W/X/Y/Z ms
            local pattern="confirm latency median/75th/95th/99th ([0-9./]+) ms"
            if [[ "$stream_perf_line" =~ $pattern ]]; then
                local lat_values="${BASH_REMATCH[1]}"
                IFS='/' read -ra VALS <<< "$lat_values"
                # stream-perf-test format: median/75th/95th/99th (4 values, no min/max)
                # Output format: min|median|75th|90th|95th|99th|max
                echo "|${VALS[0]}|${VALS[1]}||${VALS[2]}|${VALS[3]}|"
                return
            fi
        else
            # Extract: latency median/75th/95th/99th A/B/C/D ms (this is consumer latency for streams)
            # Need to match ", latency median" to avoid matching "confirm latency"
            local pattern=", latency median/75th/95th/99th ([0-9./]+) ms"
            if [[ "$stream_perf_line" =~ $pattern ]]; then
                local lat_values="${BASH_REMATCH[1]}"
                IFS='/' read -ra VALS <<< "$lat_values"
                # stream-perf-test format: median/75th/95th/99th (4 values, no min/max)
                # Output format: min|median|75th|90th|95th|99th|max
                echo "|${VALS[0]}|${VALS[1]}||${VALS[2]}|${VALS[3]}|"
                return
            fi
        fi
        
        # If not found in data lines, try summary line
        local summary=$(grep "^Summary: " "$file" 2>/dev/null | tail -1)
        if [[ -n "$summary" ]]; then
            if [[ "$latency_type" == "confirm" ]]; then
                # Extract: confirm latency 95th X ms
                local pattern="confirm latency 95th ([0-9]+) ms"
                if [[ "$summary" =~ $pattern ]]; then
                    local val="${BASH_REMATCH[1]}"
                    # Only 95th percentile available in summary
                    echo "||||${val}||"
                    return
                fi
            else
                # Extract: latency 95th X ms (with comma before to avoid confirm latency)
                local pattern=", latency 95th ([0-9]+) ms"
                if [[ "$summary" =~ $pattern ]]; then
                    local val="${BASH_REMATCH[1]}"
                    # Only 95th percentile available in summary
                    echo "||||${val}||"
                    return
                fi
            fi
        fi
    fi
    
    # Not stream-perf-test format, try perf-test summary lines (with 75th)
    # Format: "id: baseline, confirm latency min/median/75th/95th/99th/max 2004/3750/4356/5601/7072/23250 µs"
    if [[ "$latency_type" == "confirm" ]]; then
        local summary=$(grep "^id: .*, confirm latency min/median/75th/95th/99th/max" "$file" 2>/dev/null | tail -1)
        if [[ -n "$summary" ]]; then
            local lat=$(echo "$summary" | sed -n 's/.*confirm latency min\/median\/75th\/95th\/99th\/max \([0-9./]*\) \(µs\|ms\).*/\1 \2/p')
            if [[ "$lat" =~ ([0-9./]+)\ (µs|ms) ]]; then
                local lat_values="${BASH_REMATCH[1]}"
                local unit="${BASH_REMATCH[2]}"
                if [[ "$unit" == "µs" ]]; then
                    IFS='/' read -ra VALS <<< "$lat_values"
                    local converted=()
                    for val in "${VALS[@]}"; do
                        if [[ "$val" =~ ^[0-9.]+$ ]]; then
                            converted+=("$(echo "scale=3; $val / 1000" | bc)")
                        else
                            converted+=("$val")
                        fi
                    done
                    # Format: min|median|75th||95th|99th|max (perf-test has 75th, no 90th)
                    echo "${converted[0]}|${converted[1]}|${converted[2]}||${converted[3]}|${converted[4]}|${converted[5]}"
                else
                    IFS='/' read -ra VALS <<< "$lat_values"
                    echo "${VALS[0]}|${VALS[1]}|${VALS[2]}||${VALS[3]}|${VALS[4]}|${VALS[5]}"
                fi
                return
            fi
        fi
    else
        local summary=$(grep "^id: .*, consumer latency min/median/75th/95th/99th/max" "$file" 2>/dev/null | tail -1)
        if [[ -n "$summary" ]]; then
            local lat=$(echo "$summary" | sed -n 's/.*consumer latency min\/median\/75th\/95th\/99th\/max \([0-9./]*\) \(µs\|ms\).*/\1 \2/p')
            if [[ "$lat" =~ ([0-9./]+)\ (µs|ms) ]]; then
                local lat_values="${BASH_REMATCH[1]}"
                local unit="${BASH_REMATCH[2]}"
                if [[ "$unit" == "µs" ]]; then
                    IFS='/' read -ra VALS <<< "$lat_values"
                    local converted=()
                    for val in "${VALS[@]}"; do
                        if [[ "$val" =~ ^[0-9.]+$ ]]; then
                            converted+=("$(echo "scale=3; $val / 1000" | bc)")
                        else
                            converted+=("$val")
                        fi
                    done
                    # Format: min|median|75th||95th|99th|max (perf-test has 75th, no 90th)
                    echo "${converted[0]}|${converted[1]}|${converted[2]}||${converted[3]}|${converted[4]}|${converted[5]}"
                else
                    IFS='/' read -ra VALS <<< "$lat_values"
                    echo "${VALS[0]}|${VALS[1]}|${VALS[2]}||${VALS[3]}|${VALS[4]}|${VALS[5]}"
                fi
                return
            fi
        fi
    fi
    
    # Fallback: try formatted metrics line (OMQ format)
    local last_metric_line=$(grep "^id: .*, time .* s, " "$file" 2>/dev/null | tail -1)
    
    if [[ -n "$last_metric_line" ]]; then
        # OMQ format case 1: with label "min/median/90th/95th/99th/max consumer latency: values"
        local pattern1="min/median/90th/95th/99th/max ${latency_type} latency: ([0-9./]+) (µs|ms)"
        # OMQ format case 2: without label "confirm latency: values" (only for confirm)
        local pattern2="${latency_type} latency: ([0-9./]+) (µs|ms)"
        
        if [[ "$last_metric_line" =~ $pattern1 ]]; then
            local lat_values="${BASH_REMATCH[1]}"
            local unit="${BASH_REMATCH[2]}"
            
            # Convert µs to ms if needed
            if [[ "$unit" == "µs" ]]; then
                IFS='/' read -ra VALS <<< "$lat_values"
                local converted=()
                for val in "${VALS[@]}"; do
                    if [[ "$val" =~ ^[0-9.]+$ ]]; then
                        converted+=("$(echo "scale=3; $val / 1000" | bc)")
                    else
                        converted+=("$val")
                    fi
                done
                # Format: min|median||90th|95th|99th|max (OMQ has 90th instead of 75th)
                echo "${converted[0]}|${converted[1]}||${converted[2]}|${converted[3]}|${converted[4]}|${converted[5]}"
            else
                IFS='/' read -ra VALS <<< "$lat_values"
                echo "${VALS[0]}|${VALS[1]}||${VALS[2]}|${VALS[3]}|${VALS[4]}|${VALS[5]}"
            fi
            return
        elif [[ "$last_metric_line" =~ $pattern2 ]]; then
            # This is the case where there's no label (confirm latency in OMQ)
            local lat_values="${BASH_REMATCH[1]}"
            local unit="${BASH_REMATCH[2]}"
            
            # Convert µs to ms if needed
            if [[ "$unit" == "µs" ]]; then
                IFS='/' read -ra VALS <<< "$lat_values"
                local converted=()
                for val in "${VALS[@]}"; do
                    if [[ "$val" =~ ^[0-9.]+$ ]]; then
                        converted+=("$(echo "scale=3; $val / 1000" | bc)")
                    else
                        converted+=("$val")
                    fi
                done
                # Format: min|median||90th|95th|99th|max (OMQ has 90th instead of 75th)
                echo "${converted[0]}|${converted[1]}||${converted[2]}|${converted[3]}|${converted[4]}|${converted[5]}"
            else
                IFS='/' read -ra VALS <<< "$lat_values"
                echo "${VALS[0]}|${VALS[1]}||${VALS[2]}|${VALS[3]}|${VALS[4]}|${VALS[5]}"
            fi
            return
        fi
    fi
    
    echo "||||||"
}

# Helper to extract average rate from summary line or calculated from metrics
extract_rate() {
    local file="$1"
    local rate_type="$2"  # "sending", "confirmed", or "receiving"
    
    # Check if this is a stream-perf-test file first
    if grep -q "^[0-9]*, published " "$file" 2>/dev/null; then
        # This is stream-perf-test format - use different metric names
        case "$rate_type" in
            sending)
                parse_stream_perf_metrics "$file" "published"
                ;;
            confirmed)
                parse_stream_perf_metrics "$file" "confirmed"
                ;;
            receiving)
                parse_stream_perf_metrics "$file" "consumed"
                ;;
        esac
        return
    fi
    
    # Try summary line first (from perf-test or final OMQ summary)
    local pattern="${rate_type} rate avg: ([0-9]+) msg/s"
    local summary_line=$(grep "${rate_type} rate avg:" "$file" 2>/dev/null | tail -1)
    
    if [[ -n "$summary_line" && "$summary_line" =~ $pattern ]]; then
        echo "${BASH_REMATCH[1]}"
        return
    fi
    
    # If not found, calculate from formatted metrics
    if [[ "$rate_type" == "sending" ]]; then
        parse_formatted_metrics "$file" "sent"
    elif [[ "$rate_type" == "confirmed" ]]; then
        parse_formatted_metrics "$file" "confirmed"
    else
        parse_formatted_metrics "$file" "received"
    fi
}

# Print a metric table (for rates)
print_rate_table() {
    local title="$1"
    local -n data_array=$2
    
    echo ""
    echo "$title:"
    printf "  %-40s | %-15s\n" "Test" "Average Rate"
    printf "  %-40s | %-15s\n" "----------------------------------------" "---------------"
    
    # Sort by test name
    for test in $(echo "${!data_array[@]}" | tr ' ' '\n' | sort); do
        local value="${data_array[$test]}"
        if [[ "$value" == "N/A" ]]; then
            printf "  %-40s | %-15s\n" "$test" "N/A"
        else
            printf "  %-40s | %'15d\n" "$test" "$value"
        fi
    done
}

# Print a latency table with separate columns for each quantile
print_latency_table() {
    local title="$1"
    local -n data_array=$2
    
    echo ""
    echo "$title:"
    printf "  %-40s | %10s | %10s | %10s | %10s | %10s | %10s | %10s\n" \
           "Test" "min" "median" "75th" "90th" "95th" "99th" "max"
    printf "  %-40s | %10s | %10s | %10s | %10s | %10s | %10s | %10s\n" \
           "----------------------------------------" "----------" "----------" "----------" "----------" "----------" "----------" "----------"
    
    # Sort by test name
    for test in $(echo "${!data_array[@]}" | tr ' ' '\n' | sort); do
        local value="${data_array[$test]}"
        
        # Split value by pipe: min|median|75th|90th|95th|99th|max
        IFS='|' read -ra VALS <<< "$value"
        
        # Format each value, use empty string if not available
        local min="${VALS[0]:-}"
        local median="${VALS[1]:-}"
        local p75="${VALS[2]:-}"
        local p90="${VALS[3]:-}"
        local p95="${VALS[4]:-}"
        local p99="${VALS[5]:-}"
        local max="${VALS[6]:-}"
        
        printf "  %-40s | %10s | %10s | %10s | %10s | %10s | %10s | %10s\n" \
               "$test" "$min" "$median" "$p75" "$p90" "$p95" "$p99" "$max"
    done
}

# Collect matching files
FILES=()
for f in "$RESULTS_DIR"/*.txt; do
    # Skip .consumer files (we'll process them with their main file)
    [[ "$f" == *.consumer ]] && continue
    
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

if [[ ${#FILES[@]} -eq 0 ]]; then
    echo "No files found matching filter"
    exit 0
fi

echo "=============================================="
echo "  Performance Test Comparison"
[[ -n "$FILTER" ]] && echo "  Filter: $FILTER"
echo "=============================================="

# First pass: collect all metrics
for f in "${FILES[@]}"; do
    FILENAME=$(basename "$f")
    SCENARIO=$(grep "^# Scenario:" "$f" 2>/dev/null | sed 's/# Scenario: //' | head -1 || echo "unknown")
    DATE=$(grep "^# Date:" "$f" 2>/dev/null | sed 's/# Date: //' | head -1 || echo "")
    ALIAS=$(grep "^# Alias:" "$f" 2>/dev/null | sed 's/# Alias: //' | head -1 || echo "")
    
    HOST=$(grep "^# Host:" "$f" 2>/dev/null | sed 's/# Host: //' | head -1 || true)
    if [[ -z "$HOST" ]]; then
        HOST=$(grep "^# Producer Host:" "$f" 2>/dev/null | sed 's/# Producer Host: //' | head -1 || true)
    fi
    
    # Use alias if available, otherwise use filename without extension as test identifier
    if [[ -n "$ALIAS" ]]; then
        TEST_ID="$ALIAS"
    else
        TEST_ID="${FILENAME%.txt}"
    fi
    
    # Check if there's a consumer file
    CONSUMER_FILE="${f}.consumer"
    
    # Extract sent rate
    SENT_RATE=$(extract_rate "$f" "sending")
    
    # Extract confirmed rate
    CONFIRMED_RATE=$(extract_rate "$f" "confirmed")
    
    # Extract received rate (try main file first, then consumer file)
    RECV_RATE=$(extract_rate "$f" "receiving")
    if [[ "$RECV_RATE" == "N/A" && -f "$CONSUMER_FILE" ]]; then
        RECV_RATE=$(extract_rate "$CONSUMER_FILE" "receiving")
    fi
    
    # Extract confirm latency
    CONFIRM_LAT=$(extract_final_latencies "$f" "confirm")
    
    # Extract consumer latency (try main file first, then consumer file)
    CONSUMER_LAT=$(extract_final_latencies "$f" "consumer")
    if [[ "$CONSUMER_LAT" == "||||||" && -f "$CONSUMER_FILE" ]]; then
        CONSUMER_LAT=$(extract_final_latencies "$CONSUMER_FILE" "consumer")
    fi
    
    # Store in associative arrays
    SENT_RATES["$TEST_ID"]="$SENT_RATE"
    CONFIRMED_RATES["$TEST_ID"]="$CONFIRMED_RATE"
    RECV_RATES["$TEST_ID"]="$RECV_RATE"
    CONFIRM_LATS["$TEST_ID"]="$CONFIRM_LAT"
    CONSUMER_LATS["$TEST_ID"]="$CONSUMER_LAT"
    TEST_DATES["$TEST_ID"]="$DATE"
    TEST_HOSTS["$TEST_ID"]="$HOST"
    TEST_SCENARIOS["$TEST_ID"]="$SCENARIO"
done

# Second pass: print tables organized by metric
print_rate_table "Sent Rate (msg/s)" SENT_RATES
print_rate_table "Confirmed Rate (msg/s)" CONFIRMED_RATES
print_rate_table "Received Rate (msg/s)" RECV_RATES
print_latency_table "Confirm Latency (ms)" CONFIRM_LATS
print_latency_table "Consumer Latency (ms)" CONSUMER_LATS

echo ""
