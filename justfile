export CARGO_BUILD_JOBS := env_var_or_default("CARGO_BUILD_JOBS", "1")
export SQLSERVER_TEST_EULA_ACCEPTED := "true"
export PATH := env_var("HOME") + "/.local/bin:" + env_var("HOME") + "/.cargo/bin:" + env_var("PATH")

# Run all server-free checks without provisioning SQL Server.
default: check

# Run the complete CI pipeline locally or on the CI runner.
ci fixture="": setup check (validate-live fixture)

# Install only missing/mismatched shared Rust, just and compiler prerequisites.
setup:
    bash scripts/setup_environment.sh

# Ensure the selected fixture is ready without running tests.
provision fixture="":
    python3 -B scripts/sqlserver_fixture.py provision {{quote(fixture)}}

# Release only owned fixture state; borrowed instances are preserved.
cleanup fixture="":
    python3 -B scripts/sqlserver_fixture.py cleanup {{quote(fixture)}}

# Ensure readiness, run shared live checks, and release owned resources.
validate-live fixture="": build
    python3 -B scripts/sqlserver_fixture.py validate {{quote(fixture)}}
# Check formatting, strict Clippy, Rust tests, and CI fixture helper tests.
check: fmt-check clippy test test-ci-helpers

# Build the observe-only executable.
build:
    cargo build --locked --bin sqlserver-observer

# Check Rust formatting without changing files.
fmt-check:
    cargo fmt -- --check

# Run strict Clippy for every target and feature.
clippy:
    cargo clippy --locked --all-targets --all-features -- -D warnings

# Run ordinary Rust tests; live fixtures remain ignored.
test:
    cargo test --locked --all-features

# Test the CI helper's guards without installing or starting SQL Server.
test-ci-helpers:
    python3 -B -m unittest discover -s scripts -p 'test_*.py'

# Run the three single-instance cases against explicitly provisioned fixtures.
test-live fixture="":
    python3 -B scripts/sqlserver_fixture.py test {{quote(fixture)}}

# Run every live case, including a separately provisioned EXTERNAL AG.
test-live-all fixture="":
    python3 -B scripts/sqlserver_fixture.py test-all {{quote(fixture)}}

# Build the CLI, emit an observation, and verify it against the container fixture.
test-live-cli report: build && (verify-live-cli report)
    target/debug/sqlserver-observer \
        --config "${SQLSERVER_LIVE_ABSENT_CONFIG:?SQLSERVER_LIVE_ABSENT_CONFIG must reference a provisioned fixture}" \
        > {{quote(report)}}

# Verify fresh CLI output from the pinned Enterprise Developer container.
verify-live-cli report:
    python3 -B scripts/sqlserver_fixture.py verify-cli {{quote(report)}}
