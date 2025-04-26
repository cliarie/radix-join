#!/usr/bin/env bash
set -euo pipefail

COMMIT_HASH=$(git rev-parse --short HEAD)

mkdir -p benchmarks
mkdir -p flamegraphs

# relax perf permissions if "permission denied"
# echo 0 | sudo tee /proc/sys/kernel/perf_event_paranoid

echo "=== Running plans.json under perf ==="
perf record -e cpu-clock:u -F 99 -g \
    --output perf.data -- \
    ./build/run plans.json \
  | tee "benchmarks/run_${COMMIT_HASH}.txt"

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
  • Raw output → benchmarks/run_${COMMIT_HASH}.txt  
  • Flamegraph   → flamegraphs/run_${COMMIT_HASH}.svg"

