#!/usr/bin/env bash
set -euo pipefail

COMMIT_HASH=$(git rev-parse --short HEAD)

# icons for pass/fail
CORRECT_ICON="\U2705"
WRONG_ICON="\U274C"

mkdir -p benchmarks
mkdir -p flamegraphs

echo "=== Running plans.json under perf ==="
# capture both perf.data and the raw stdout
OUTPUT_FILE="benchmarks/run_${COMMIT_HASH}.txt"
perf record -e cpu-clock:u -F 99 -g \
    --output perf.data -- \
    ./build/run plans.json BENCHMARK_RUNTIME.txt \
  | tee "$OUTPUT_FILE"

# correctness check
if grep -q "Result correct: false" "$OUTPUT_FILE"; then
  echo -e "Overall results: $WRONG_ICON"
else
  echo -e "Overall results: $CORRECT_ICON"
fi

# read total execution time (in microseconds) from file
if [[ -f BENCHMARK_RUNTIME.txt ]]; then
  TOTAL_US=$(<BENCHMARK_RUNTIME.txt)
  TOTAL_MS=$(( TOTAL_US / 1000 ))
  TOTAL_SEC=$(awk -v us="$TOTAL_US" 'BEGIN { printf "%.6f", us/1000000 }')
  echo "Total execution time: ${TOTAL_SEC} s"
else
  echo "Warning: BENCHMARK_RUNTIME.txt not found; skipping total-time summary"
fi

echo "--- Generating perf script ---"
perf script > perf.script

if [[ -s perf.script ]]; then
  echo "--- Building flamegraph ---"
  stackcollapse-perf.pl perf.script \
    | flamegraph.pl \
    > "flamegraphs/run_${COMMIT_HASH}.svg"
  echo "Flamegraph saved to flamegraphs/run_${COMMIT_HASH}.svg"
else
  echo "Error: perf.script is empty, skipping flamegraph"
fi

# Cleanup
rm -f perf.data perf.script

echo "Benchmark complete.  
  • Raw output → $OUTPUT_FILE  
  • Flamegraph   → flamegraphs/run_${COMMIT_HASH}.svg"
