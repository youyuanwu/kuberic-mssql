#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 2 || -z "$1" ]]; then
  echo "usage: run_live_nextest.sh <fixture-namespace> <nextest-command...>" >&2
  exit 2
fi
root="$(realpath -m "$1")"
shift
script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
python3 "$script_dir/live_coverage.py" >&2
live_filter="$(python3 "$script_dir/live_coverage.py" --filter)"
mkdir -m 700 -p "$root"
for family in one-replica three-replica lease-fence; do
  if [[ -L "$root/$family" || ( -e "$root/$family" && ! -d "$root/$family" ) ]]; then
    echo "refusing non-directory or symlink fixture family: $root/$family" >&2
    exit 1
  fi
  mkdir -m 700 -p "$root/$family"
done
timing_root="$(mktemp -d "$root/.phase-timings.XXXXXX")"
env -u SQLSERVER_TEST_EULA_ACCEPTED \
  -u KUBERIC_MSSQL_CONFIGURATION_FAULT_FILTER \
  -u KUBERIC_MSSQL_SEED_FAULT_FILTER \
  -u KUBERIC_MSSQL_CONFLICT_FILTER \
  -u KUBERIC_MSSQL_ONE_REPLICA_CHILD_MODE \
  -u KUBERIC_MSSQL_THREE_REPLICA_CHILD_MODE \
  -u SQLSERVER_ONE_REPLICA_FAULT \
  -u KUBERIC_MSSQL_FAULT_CHECKPOINT \
  -u KUBERIC_MSSQL_FAULT_HIT_FILE \
  -u KUBERIC_MSSQL_SIGNAL_READY_FILE \
  -u KUBERIC_MSSQL_LEGACY_PROVISIONED_RESTART \
  -u KUBERIC_MSSQL_PHASE_TIMING_SCENARIO \
  -u KUBERIC_MSSQL_PHASE_TIMING_ATTEMPT \
  -u KUBERIC_MSSQL_TERMINAL_CONTEXT \
  SQLSERVER_ONE_REPLICA_ROOT="$root/one-replica" \
  KUBERIC_MSSQL_THREE_REPLICA_ROOT="$root/three-replica" \
  KUBERIC_MSSQL_LEASE_FENCE_ROOT="$root/lease-fence" \
  KUBERIC_MSSQL_PHASE_TIMING_DIR="$timing_root" \
  CARGO_BUILD_JOBS=1 \
  "$@" --profile live --run-ignored only --test-threads=1 -E "$live_filter" &
child=$!
cancelled=0
forward_signal() {
  cancelled="$1"
  if kill -0 "$child" 2>/dev/null; then
    kill "-$2" "$child" 2>/dev/null || cancelled=125
  fi
}
trap 'forward_signal 130 INT' INT
trap 'forward_signal 143 TERM' TERM
wait_for_child() {
  child_status=0
  while true; do
    wait "$child" && child_status=0 || child_status=$?
    if ! kill -0 "$child" 2>/dev/null; then
      break
    fi
  done
  if [[ "$cancelled" -ne 0 ]]; then
    wait "$child" && child_status=0 || child_status=$?
  fi
  if [[ "$child_status" -eq 0 && "$cancelled" -ne 0 ]]; then
    child_status="$cancelled"
  fi
}
wait_for_child
status="$child_status"
collector_flags=()
if [[ "$status" -ne 0 ]]; then
  collector_flags+=(--failed)
fi
timeout --foreground --signal=TERM --kill-after=1s 5s \
  python3 "$script_dir/collect_phase_timings.py" "$timing_root" "${collector_flags[@]}" &
child=$!
wait_for_child
trap - INT TERM
if [[ "$status" -eq 0 && "$cancelled" -ne 0 ]]; then
  status="$cancelled"
fi
if [[ "$child_status" -eq 0 ]]; then
  exit "$status"
else
  if [[ "$status" -ne 0 ]]; then
    exit "$status"
  fi
  exit "$child_status"
fi
