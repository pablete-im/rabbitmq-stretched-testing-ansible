#!/bin/bash
# OMQ Metrics Collector - Captures Prometheus metrics and formats like perf-test
set -euo pipefail

OMQ_PID=""
METRICS_PORT="8080"
TEST_ID=""
START_TIME=""
OUTPUT_FILE=""
CLEANUP_DONE=false

# Cleanup function
cleanup() {
    if [[ "$CLEANUP_DONE" == "true" ]]; then
        return
    fi
    CLEANUP_DONE=true
    
    echo ""
    echo "🛑 Stopping metrics collection..."
    
    # Kill OMQ if still running
    if [[ -n "$OMQ_PID" ]] && kill -0 "$OMQ_PID" 2>/dev/null; then
        echo "   Stopping OMQ process (PID: $OMQ_PID)..."
        kill "$OMQ_PID" 2>/dev/null || true
        wait "$OMQ_PID" 2>/dev/null || true
    fi
    
    echo "   Metrics collection stopped."
}

# Set up signal handlers
trap cleanup EXIT INT TERM

# Function to get metrics from OMQ Prometheus endpoint
get_omq_metrics() {
    local port="$1"
    curl -s "http://localhost:${port}/metrics" 2>/dev/null || echo ""
}

# Function to extract metric value
extract_metric() {
    local metrics="$1"
    local metric_name="$2"
    local quantile="$3"
    
    if [[ -n "$quantile" ]]; then
        echo "$metrics" | grep "^${metric_name}{quantile=\"${quantile}\"}" | awk '{print $2}' | head -1
    else
        # Special handling for omq_messages_consumed_total which has {priority="0"}
        if [[ "$metric_name" == "omq_messages_consumed_total" ]]; then
            echo "$metrics" | grep "^${metric_name}{priority=" | awk '{print $2}' | head -1
        else
            echo "$metrics" | grep "^${metric_name}" | grep -v '{' | awk '{print $2}' | head -1
        fi
    fi
}

# Function to convert seconds to microseconds
seconds_to_microseconds() {
    local seconds="$1"
    if [[ -n "$seconds" && "$seconds" != "0" ]]; then
        echo "$seconds * 1000000" | bc -l | cut -d. -f1
    else
        echo "0"
    fi
}

# Function to calculate rate per second
calculate_rate() {
    local current="$1"
    local previous="$2"
    local time_diff="$3"
    
    if [[ -n "$current" && -n "$previous" && "$time_diff" -gt 0 ]]; then
        echo "($current - $previous) / $time_diff" | bc -l | cut -d. -f1
    else
        echo "0"
    fi
}

# Function to format metrics like perf-test
format_metrics() {
    local test_id="$1"
    local elapsed_time="$2"
    local sent_rate="$3"
    local confirmed_rate="$4"
    local nacked_rate="$5"
    local received_rate="$6"
    local consumer_latencies="$7"
    local confirm_latencies="$8"
    
    printf "id: %s, time %.3f s, sent: %s msg/s, confirmed: %s msg/s, nacked: %s msg/s, received: %s msg/s, min/median/90th/95th/99th/max consumer latency: %s µs, confirm latency: %s µs\n" \
        "$test_id" "$elapsed_time" "$sent_rate" "$confirmed_rate" "$nacked_rate" "$received_rate" "$consumer_latencies" "$confirm_latencies"
}

# Function to extract min/max latencies from OMQ log output
extract_log_latencies() {
    local log_line="$1"
    local latency_type="$2"  # "pub" or "e2e"
    
    if [[ "$latency_type" == "pub" ]]; then
        # Extract pub_min and pub_max
        local min_val=$(echo "$log_line" | grep -o "pub_min=[0-9.]*[µμ]s\|pub_min=[0-9.]*ms" | sed 's/pub_min=//;s/[µμ]s//;s/ms//')
        local max_val=$(echo "$log_line" | grep -o "pub_max=[0-9.]*[µμ]s\|pub_max=[0-9.]*ms" | sed 's/pub_max=//;s/[µμ]s//;s/ms//')
        
        # Convert to microseconds if needed
        if echo "$log_line" | grep -q "pub_min=[0-9.]*ms"; then
            min_val=$(echo "$min_val * 1000" | bc -l | cut -d. -f1)
        fi
        if echo "$log_line" | grep -q "pub_max=[0-9.]*ms"; then
            max_val=$(echo "$max_val * 1000" | bc -l | cut -d. -f1)
        fi
    else
        # Extract e2e_min and e2e_max
        local min_val=$(echo "$log_line" | grep -o "e2e_min=[0-9.]*[µμ]s\|e2e_min=[0-9.]*ms\|e2e_min=[0-9.]*s" | sed 's/e2e_min=//;s/[µμ]s//;s/ms//;s/s//')
        local max_val=$(echo "$log_line" | grep -o "e2e_max=[0-9.]*[µμ]s\|e2e_max=[0-9.]*ms\|e2e_max=[0-9.]*s" | sed 's/e2e_max=//;s/[µμ]s//;s/ms//;s/s//')
        
        # Convert to microseconds based on unit
        if echo "$log_line" | grep -q "e2e_min=[0-9.]*ms"; then
            min_val=$(echo "$min_val * 1000" | bc -l | cut -d. -f1)
        elif echo "$log_line" | grep -q "e2e_min=[0-9.]*s" && ! echo "$log_line" | grep -q "e2e_min=[0-9.]*[µμ]s"; then
            min_val=$(echo "$min_val * 1000000" | bc -l | cut -d. -f1)
        fi
        
        if echo "$log_line" | grep -q "e2e_max=[0-9.]*ms"; then
            max_val=$(echo "$max_val * 1000" | bc -l | cut -d. -f1)
        elif echo "$log_line" | grep -q "e2e_max=[0-9.]*s" && ! echo "$log_line" | grep -q "e2e_max=[0-9.]*[µμ]s"; then
            max_val=$(echo "$max_val * 1000000" | bc -l | cut -d. -f1)
        fi
    fi
    
    echo "${min_val:-0} ${max_val:-0}"
}

# Function to extract and format latency quantiles with min/max from log
format_latency_quantiles() {
    local metrics="$1"
    local metric_prefix="$2"
    local log_output="$3"
    local latency_type="$4"  # "pub" or "e2e"
    
    local q50=$(extract_metric "$metrics" "$metric_prefix" "0.5")
    local q90=$(extract_metric "$metrics" "$metric_prefix" "0.9")
    local q95=$(extract_metric "$metrics" "$metric_prefix" "0.95")
    local q99=$(extract_metric "$metrics" "$metric_prefix" "0.99")
    
    # Convert to microseconds and format
    local q50_us=$(seconds_to_microseconds "$q50")
    local q90_us=$(seconds_to_microseconds "$q90")
    local q95_us=$(seconds_to_microseconds "$q95")
    local q99_us=$(seconds_to_microseconds "$q99")
    
    # Extract min/max from log output
    local min_max_values=$(extract_log_latencies "$log_output" "$latency_type")
    local min_us=$(echo "$min_max_values" | awk '{print $1}')
    local max_us=$(echo "$min_max_values" | awk '{print $2}')
    
    # Use quantiles as fallback if min/max not found
    min_us=${min_us:-$q50_us}
    max_us=${max_us:-$q99_us}
    
    # Format as min/median/90th/95th/99th/max (includes 90th percentile)
    echo "${min_us}/${q50_us}/${q90_us}/${q95_us}/${q99_us}/${max_us}"
}

# Main metrics collection function
collect_metrics() {
    local test_id="$1"
    local omq_pid="$2"
    local output_file="$3"
    local omq_log_file="$4"
    
    echo "📊 Starting metrics collection for test: $test_id"
    echo "   OMQ PID: $omq_pid"
    echo "   Metrics endpoint: http://localhost:${METRICS_PORT}/metrics"
    echo "   Output: $output_file"
    echo ""
    
    # Wait for OMQ to start and metrics endpoint to be available
    echo "⏳ Waiting for OMQ metrics endpoint..."
    local retries=0
    while [[ $retries -lt 30 ]]; do
        if curl -s "http://localhost:${METRICS_PORT}/metrics" >/dev/null 2>&1; then
            echo "✅ Metrics endpoint available"
            break
        fi
        sleep 1
        ((retries++))
        
        # Check if OMQ process is still running
        if ! kill -0 "$omq_pid" 2>/dev/null; then
            echo "❌ OMQ process died before metrics endpoint became available"
            return 1
        fi
    done
    
    if [[ $retries -eq 30 ]]; then
        echo "❌ Timeout waiting for metrics endpoint"
        return 1
    fi
    
    # Initialize previous values for rate calculation
    local prev_published=0
    local prev_confirmed=0
    local prev_consumed=0
    local prev_returned=0
    local start_time=$(date +%s.%N)
    
    echo "🚀 Starting metrics collection..."
    echo ""
    
    # Metrics collection loop
    while kill -0 "$omq_pid" 2>/dev/null; do
        local current_time=$(date +%s.%N)
        local elapsed_time=$(echo "$current_time - $start_time" | bc -l)
        
        # Get metrics from OMQ
        local metrics=$(get_omq_metrics "$METRICS_PORT")
        
        if [[ -z "$metrics" ]]; then
            echo "⚠️  Failed to get metrics, retrying..."
            sleep 1
            continue
        fi
        
        # Extract current totals
        local published=$(extract_metric "$metrics" "omq_messages_published_total" "")
        local confirmed=$(extract_metric "$metrics" "omq_messages_confirmed_total" "")
        local consumed=$(extract_metric "$metrics" "omq_messages_consumed_total" "")
        local returned=$(extract_metric "$metrics" "omq_messages_returned_total" "")
        
        # Set defaults if empty
        published=${published:-0}
        confirmed=${confirmed:-0}
        consumed=${consumed:-0}
        returned=${returned:-0}
        
        # Calculate rates (messages per second)
        local sent_rate=$(calculate_rate "$published" "$prev_published" 1)
        local confirmed_rate=$(calculate_rate "$confirmed" "$prev_confirmed" 1)
        local nacked_rate=$(calculate_rate "$returned" "$prev_returned" 1)
        local received_rate=$(calculate_rate "$consumed" "$prev_consumed" 1)
        
        # Get latest log output for min/max latencies
        local latest_log=""
        if [[ -f "$omq_log_file" ]]; then
            latest_log=$(tail -1 "$omq_log_file" 2>/dev/null || echo "")
        fi
        
        # Extract latency quantiles with min/max from log
        local consumer_latencies=$(format_latency_quantiles "$metrics" "omq_end_to_end_latency_seconds" "$latest_log" "e2e")
        local confirm_latencies=$(format_latency_quantiles "$metrics" "omq_publishing_latency_seconds" "$latest_log" "pub")
        
        # Format and output metrics
        local formatted_line=$(format_metrics "$test_id" "$elapsed_time" "$sent_rate" "$confirmed_rate" "$nacked_rate" "$received_rate" "$consumer_latencies" "$confirm_latencies")
        
        echo "$formatted_line"
        echo "$formatted_line" >> "$output_file"
        
        # Update previous values
        prev_published=$published
        prev_confirmed=$confirmed
        prev_consumed=$consumed
        prev_returned=$returned
        
        sleep 1
    done
    
    echo ""
    echo "📊 Metrics collection completed"
}

# Main function
main() {
    if [[ $# -lt 4 ]]; then
        echo "Usage: $0 <test_id> <omq_pid> <output_file> <omq_log_file> [metrics_port]"
        echo ""
        echo "Arguments:"
        echo "  test_id      Test identifier for output"
        echo "  omq_pid      PID of running OMQ process"
        echo "  output_file  File to write formatted metrics"
        echo "  omq_log_file File containing OMQ log output"
        echo "  metrics_port Optional metrics port (default: 8080)"
        echo ""
        echo "Example:"
        echo "  $0 baseline 12345 /tmp/metrics.txt /tmp/omq.log 8080"
        exit 1
    fi
    
    TEST_ID="$1"
    OMQ_PID="$2"
    OUTPUT_FILE="$3"
    OMQ_LOG_FILE="$4"
    METRICS_PORT="${5:-8080}"
    
    # Validate OMQ process is running
    if ! kill -0 "$OMQ_PID" 2>/dev/null; then
        echo "Error: OMQ process $OMQ_PID is not running"
        exit 1
    fi
    
    # Check if bc is available for calculations
    if ! command -v bc >/dev/null 2>&1; then
        echo "Error: 'bc' command not found. Please install bc for calculations."
        exit 1
    fi
    
    # Check if curl is available
    if ! command -v curl >/dev/null 2>&1; then
        echo "Error: 'curl' command not found. Please install curl for metrics collection."
        exit 1
    fi
    
    # Start metrics collection
    collect_metrics "$TEST_ID" "$OMQ_PID" "$OUTPUT_FILE" "$OMQ_LOG_FILE"
}

# Run main function
main "$@"