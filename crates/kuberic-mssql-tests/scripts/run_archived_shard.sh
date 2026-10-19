#!/usr/bin/env bash
set -euo pipefail

if [[ $# != 4 || ! "$2" =~ ^[1-4]$ ]]; then
  echo "usage: run_archived_shard.sh <archive> <partition:1-4> <extract-directory> <fixture-namespace>" >&2
  exit 2
fi
scripts="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
workspace="$(cd "$scripts/../../.." && pwd)"
archive="$(realpath -e "$1")"
partition="$2"
extract="$(realpath -m "$3")"
fixtures="$(realpath -m "$4")"
cd "$workspace"
if [[ -d "$extract" ]] && find "$extract" -mindepth 1 -maxdepth 1 -print -quit | grep -q .; then
  echo "archive extraction directory must be empty: $extract" >&2
  exit 1
fi
mkdir -p "$extract"
cargo nextest list --archive-file "$archive" --extract-to "$extract" \
  --workspace-remap "$workspace" --profile default --ignore-default-filter \
  --list-type binaries-only --message-format json >/dev/null
for binary in sqlserver-observer-test kuberic-mssql-runtime-test kuberic-release-intake \
  mssql-one-replica-fixture mssql-three-replica-fixture; do
  if [[ ! -x "$extract/target/debug/$binary" ]]; then
    echo "missing executable archive companion: $binary" >&2
    exit 1
  fi
done
cleanup() {
  status=$?
  trap - EXIT
  if ! bash "$scripts/cleanup_nextest_fixtures.sh" "$fixtures" "$extract/target/debug"; then
    echo "archived shard cleanup failed (primary exit $status)" >&2
    if [[ "$status" == 0 ]]; then status=1; fi
  fi
  exit "$status"
}
trap cleanup EXIT
metadata=(
  --cargo-metadata "$extract/target/nextest/cargo-metadata.json"
  --binaries-metadata "$extract/target/nextest/binaries-metadata.json"
  --target-dir-remap "$extract/target"
  --workspace-remap "$workspace"
)
echo "Running archive worker $partition/4: ordinary slice, then serial SQL hash partition."
env -u KUBERIC_MSSQL_ONE_REPLICA_CHILD_MODE -u KUBERIC_MSSQL_THREE_REPLICA_CHILD_MODE \
  cargo nextest run "${metadata[@]}" --profile default --run-ignored default \
  --partition "slice:$partition/4" --test-threads=1
bash "$scripts/run_live_nextest.sh" "$fixtures" cargo nextest run \
  "${metadata[@]}" --partition "hash:$partition/4"
