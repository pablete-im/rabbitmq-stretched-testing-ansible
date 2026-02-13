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
    SCENARIO=$(grep "^# Scenario:" "$f" | sed 's/# Scenario: //')
    DATE=$(grep "^# Date:" "$f" | sed 's/# Date: //')
    LABEL=$(grep "^# Label:" "$f" 2>/dev/null | sed 's/# Label: //' || true)
    HOST=$(grep "^# Host:" "$f" | sed 's/# Host: //')

    echo "--- $FILENAME ---"
    echo "  Scenario: $SCENARIO"
    echo "  Date:     $DATE"
    echo "  Host:     $HOST"
    [[ -n "$LABEL" ]] && echo "  Label:    $LABEL"

    # Extract summary stats - perf-test prints periodic stats lines
    # Look for the last summary line (id, sent msg/s, received msg/s, etc.)
    SUMMARY=$(grep -E "^id:" "$f" 2>/dev/null | tail -1 || true)
    if [[ -n "$SUMMARY" ]]; then
        echo "  Summary:  $SUMMARY"
    else
        # Stream perf-test format - look for summary
        STREAM_SUMMARY=$(grep -E "Published|Consumed|Summary" "$f" 2>/dev/null | tail -3 || true)
        if [[ -n "$STREAM_SUMMARY" ]]; then
            echo "$STREAM_SUMMARY" | sed 's/^/  /'
        else
            echo "  (no summary line found - check raw file)"
        fi
    fi
    echo ""
done
