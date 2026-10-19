#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 2 || -z "$1" ]]; then
  echo "usage: run_live_nextest.sh <fixture-namespace> <nextest-command...>" >&2
  exit 2
fi
root="$(realpath -m "$1")"
shift
mkdir -m 700 -p "$root"
for family in one-replica three-replica lease-fence; do
  if [[ -L "$root/$family" || ( -e "$root/$family" && ! -d "$root/$family" ) ]]; then
    echo "refusing non-directory or symlink fixture family: $root/$family" >&2
    exit 1
  fi
  mkdir -m 700 -p "$root/$family"
done
exec env -u SQLSERVER_TEST_EULA_ACCEPTED \
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
  SQLSERVER_ONE_REPLICA_ROOT="$root/one-replica" \
  KUBERIC_MSSQL_THREE_REPLICA_ROOT="$root/three-replica" \
  KUBERIC_MSSQL_LEASE_FENCE_ROOT="$root/lease-fence" \
  CARGO_BUILD_JOBS=1 \
  "$@" --profile live --run-ignored only --test-threads=1
