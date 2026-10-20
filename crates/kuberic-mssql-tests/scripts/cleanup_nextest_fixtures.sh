#!/usr/bin/env bash
set -euo pipefail

if [[ $# == 1 && "$1" == --all ]]; then
  status=0
  for recipe in cleanup-live-one-replica cleanup-live-three-replica cleanup-live-lease-fence cleanup-live-nextest; do
    if ! just "$recipe"; then
      echo "fixture cleanup recipe failed: $recipe" >&2
      status=1
    fi
  done
  exit "$status"
fi

if [[ $# -lt 1 || $# -gt 2 || -z "$1" ]]; then
  echo "usage: cleanup_nextest_fixtures.sh <runner-root> [archive-binary-directory] | --all" >&2
  exit 2
fi
root="$(realpath -m "$1")"
if [[ ! -e "$root" ]]; then
  exit 0
fi
if [[ ! -d "$root" ]]; then
  echo "nextest fixture root is not a directory: $root" >&2
  exit 1
fi
binary_directory=""
if [[ $# == 2 ]]; then
  if [[ -z "$2" || ! -d "$2" ]]; then
    echo "archive binary directory is unavailable: $2" >&2
    exit 1
  fi
  binary_directory="$(realpath -e "$2")"
fi
scratch="$(mktemp "$root/.cleanup-journals.XXXXXX")"
trap 'rm -f -- "$scratch"' EXIT
status=0
for family in one-replica three-replica lease-fence; do
  directory="$root/$family"
  if [[ -L "$directory" || ( -e "$directory" && ! -d "$directory" ) ]]; then
    echo "refusing non-directory or symlink fixture family: $directory" >&2
    status=1
    continue
  fi
  if [[ ! -d "$directory" ]]; then
    continue
  fi
  if ! find -P "$directory" -name ownership.json -print0 > "$scratch"; then
    echo "nextest fixture journal discovery failed: $directory" >&2
    status=1
  fi
  while IFS= read -r -d '' journal; do
    if [[ -L "$journal" || ! -f "$journal" ]]; then
      echo "refusing nonregular fixture journal: $journal" >&2
      status=1
      continue
    fi
    binary=mssql-three-replica-fixture
    if [[ "$family" == one-replica ]]; then
      binary=mssql-one-replica-fixture
    fi
    if [[ -n "$binary_directory" ]]; then
      command=("$binary_directory/$binary")
    else
      command=(cargo run --locked -p kuberic-mssql-tests --bin "$binary" --)
    fi
    if ! CARGO_BUILD_JOBS=1 "${command[@]}" cleanup --root "$(dirname "$journal")"; then
      echo "nextest fixture cleanup failed: $journal" >&2
      status=1
    fi
  done < "$scratch"
done
exit "$status"
