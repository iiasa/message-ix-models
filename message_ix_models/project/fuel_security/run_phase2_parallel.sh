#!/usr/bin/env bash
# Run the 9 phase-2 fuel_security sensitivity jobs (MEA + NAM per base) in parallel.
#
# Prerequisite: phase 1 must already have completed successfully, i.e. the 9 base
# scenarios (3 bilateralized + 6 FSU-restricted variants) already exist on the
# platform. This script truncates each job at its base step, so it only loads
# that scenario rather than recomputing it -- running this before phase 1
# finishes will fail to load a nonexistent scenario.
#
# Usage:
#   ./run_phase2_parallel.sh                 # run all 9 bases
#   ./run_phase2_parallel.sh "Baseline.*"     # run only bases matching a regex
#   LOG_DIR=/path/to/logs ./run_phase2_parallel.sh

set -euo pipefail

FILTER="${1:-.*}"
LOG_DIR="${LOG_DIR:-./phase2_logs}"
mkdir -p "$LOG_DIR"

# base step name -> log file slug
declare -a BASES=(
  "Baseline bilateralized"
  "Baseline - FSU2100"
  "Baseline - FSU2040"
  "NPi2030 bilateralized"
  "NPi2030 - FSU2100"
  "NPi2030 - FSU2040"
  "INDC2030i_forever bilateralized"
  "INDC2030i_forever - FSU2100"
  "INDC2030i_forever - FSU2040"
)

declare -a PIDS=()
declare -a NAMES=()

for base in "${BASES[@]}"; do
  if [[ ! "$base" =~ $FILTER ]]; then
    continue
  fi

  slug=$(echo "$base" | tr ' /' '__')
  log_file="$LOG_DIR/${slug}.log"

  echo "Launching: $base  (log: $log_file)"
  mix-models fuel-security run --from "$base" "${base} - .*" \
    > "$log_file" 2>&1 &

  PIDS+=("$!")
  NAMES+=("$base")
done

if [[ "${#PIDS[@]}" -eq 0 ]]; then
  echo "No bases matched filter: $FILTER"
  exit 1
fi

echo
echo "Launched ${#PIDS[@]} job(s). Waiting for completion..."
echo

FAILED=()
for i in "${!PIDS[@]}"; do
  pid="${PIDS[$i]}"
  name="${NAMES[$i]}"
  if wait "$pid"; then
    echo "OK    $name (pid $pid)"
  else
    echo "FAIL  $name (pid $pid) -- see $LOG_DIR/$(echo "$name" | tr ' /' '__').log"
    FAILED+=("$name")
  fi
done

echo
if [[ "${#FAILED[@]}" -eq 0 ]]; then
  echo "All ${#PIDS[@]} phase-2 jobs completed successfully."
else
  echo "${#FAILED[@]} of ${#PIDS[@]} job(s) failed:"
  for name in "${FAILED[@]}"; do
    echo "  - $name"
  done
  exit 1
fi
