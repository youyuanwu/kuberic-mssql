#!/usr/bin/env bash
set -euo pipefail

repo="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
export PATH="$HOME/.local/bin:$HOME/.cargo/bin:$PATH"
if [[ "$(uname -s)" != Linux || "$(uname -m)" != x86_64 ]]; then
  echo "The shared validation environment requires Linux x86-64." >&2
  exit 1
fi

scratch="$(mktemp -d)"
trap 'rm -f -- "$scratch/just.tar.gz" "$scratch/just" "$scratch/rustup-init.sh"; rmdir -- "$scratch"' EXIT

if ! command -v rustup >/dev/null; then
  curl --fail --silent --show-error --location https://sh.rustup.rs -o "$scratch/rustup-init.sh"
  sh "$scratch/rustup-init.sh" -y --no-modify-path --profile minimal --default-toolchain none
fi
toolchain="$(python3 -c 'import pathlib, sys, tomllib; print(tomllib.loads(pathlib.Path(sys.argv[1]).read_text())["toolchain"]["channel"])' "$repo/rust-toolchain.toml")"
if ! rustup toolchain list | grep -Fq "${toolchain}-x86_64-unknown-linux-gnu"; then
  rustup toolchain install "$toolchain" --profile minimal
fi
installed="$(rustup component list --toolchain "$toolchain" --installed)"
for component in rustfmt clippy; do
  if ! grep -q "^${component}-" <<< "$installed"; then
    rustup component add --toolchain "$toolchain" "$component"
  fi
done

if ! command -v cc >/dev/null; then
  sudo -n apt-get update -qq
  sudo -n env DEBIAN_FRONTEND=noninteractive apt-get install -y -qq build-essential
fi

if ! command -v just >/dev/null || [[ "$(just --version)" != "just 1.21.0" ]]; then
  curl --fail --silent --show-error --location --retry 3 \
    https://github.com/casey/just/releases/download/1.21.0/just-1.21.0-x86_64-unknown-linux-musl.tar.gz \
    -o "$scratch/just.tar.gz"
  printf '%s  %s\n' \
    3292fd257f2e2dfd4cb0d5650aa5e47d2c99cee1233446378eb45a7b045f3b30 \
    "$scratch/just.tar.gz" | sha256sum --check --status
  tar -xzf "$scratch/just.tar.gz" -C "$scratch" just
  install -d "$HOME/.local/bin"
  install -m 755 "$scratch/just" "$HOME/.local/bin/.just-new"
  mv -f "$HOME/.local/bin/.just-new" "$HOME/.local/bin/just"
fi

if [[ -n "${GITHUB_PATH:-}" ]]; then
  printf '%s\n' "$HOME/.local/bin" "$HOME/.cargo/bin" >> "$GITHUB_PATH"
fi
echo "Shared Rust/just validation tools are ready."
