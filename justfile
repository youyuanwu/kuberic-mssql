export CARGO_BUILD_JOBS := env_var_or_default("CARGO_BUILD_JOBS", "1")
export PATH := env_var("HOME") + "/.local/bin:" + env_var("HOME") + "/.cargo/bin:" + env_var("PATH")

# Run all server-free checks without provisioning SQL Server.
default: check

# Run the complete CI pipeline locally or on the CI runner.
ci: setup check test-live-one-replica test-live-three-replica test-live-three-replica-restart

# Install only missing/mismatched shared Rust, just and compiler prerequisites.
setup:
    bash crates/kuberic-mssql-tests/scripts/setup_environment.sh

# Release both Rust-owned fixture families.
cleanup: cleanup-live-one-replica cleanup-live-three-replica

# Check formatting, strict Clippy, and ordinary Rust tests.
check: fmt-check clippy test

# Build the observe-only executable.
build:
    cargo build --locked -p kuberic-mssql --bin sqlserver-observer

# Check Rust formatting without changing files.
fmt-check:
    cargo fmt --all -- --check

# Run strict Clippy for every target and feature.
clippy:
    cargo clippy --locked --workspace --all-targets --all-features -- -D warnings

# Run ordinary Rust tests; live fixtures remain ignored.
test:
    cargo test --locked --workspace --all-features

# Launch one owned SQL Server member, validate unique observer/CLI behavior, and clean up.
test-live-one-replica root="target/mssql-one-replica":
    env -u SQLSERVER_TEST_EULA_ACCEPTED \
        SQLSERVER_ONE_REPLICA_ROOT="$(realpath -m {{quote(root)}})" \
        CARGO_BUILD_JOBS=1 \
        cargo test --locked -p kuberic-mssql-tests --test live_one_replica \
        one_replica_mssql_observation_and_cli -- --ignored --exact --test-threads=1 --nocapture

# Recover and remove only the exactly journaled one-member fixture.
cleanup-live-one-replica root="target/mssql-one-replica":
    env -u SQLSERVER_TEST_EULA_ACCEPTED CARGO_BUILD_JOBS=1 \
        cargo run --locked -p kuberic-mssql-tests --bin mssql-one-replica-fixture -- cleanup --root "$(realpath -m {{quote(root)}})"

# Exercise launch/scenario faults, panic, timeout, explicit error, SIGINT and SIGTERM recovery.
test-live-one-replica-recovery root="target/mssql-one-replica-recovery":
    env -u SQLSERVER_TEST_EULA_ACCEPTED \
        SQLSERVER_ONE_REPLICA_ROOT="$(realpath -m {{quote(root)}})" \
        CARGO_BUILD_JOBS=1 \
        cargo test --locked -p kuberic-mssql-tests --test live_one_replica \
        one_replica_recovery_ -- --ignored --test-threads=1 --nocapture

# Launch, validate, and clean up the dedicated real three-member topology.
test-live-three-replica root="target/mssql-three-replica":
    env -u SQLSERVER_TEST_EULA_ACCEPTED \
        KUBERIC_MSSQL_THREE_REPLICA_ROOT="$(realpath -m {{quote(root)}})" \
        CARGO_BUILD_JOBS=1 \
        cargo test --locked -p kuberic-mssql-tests --test live_three_replica three_replica_mssql_happy_path -- --ignored --exact --test-threads=1

# Validate same-root replacement of all three public Kuberic hosts.
test-live-three-replica-restart root="target/mssql-three-replica-restart":
    env -u SQLSERVER_TEST_EULA_ACCEPTED \
        KUBERIC_MSSQL_THREE_REPLICA_ROOT="$(realpath -m {{quote(root)}})" \
        CARGO_BUILD_JOBS=1 \
        cargo test --locked -p kuberic-mssql-tests --test live_three_replica three_replica_mssql_same_root_restart -- --ignored --exact --test-threads=1 --nocapture

# Interrupt a real live subprocess during owned launch, recover, retry, and clean up.
test-live-three-replica-signal root="target/mssql-three-replica-signal":
    env -u SQLSERVER_TEST_EULA_ACCEPTED \
        KUBERIC_MSSQL_THREE_REPLICA_ROOT="$(realpath -m {{quote(root)}})" \
        CARGO_BUILD_JOBS=1 \
        cargo test --locked -p kuberic-mssql-tests --test live_three_replica three_replica_sigterm_during_owned_launch_is_recoverable -- --ignored --exact --test-threads=1 --nocapture

# Exercise handled and uncatchable subprocess interruption with cleanup and retry.
test-live-three-replica-recovery root="target/mssql-three-replica-recovery":
    env -u SQLSERVER_TEST_EULA_ACCEPTED \
        KUBERIC_MSSQL_THREE_REPLICA_ROOT="$(realpath -m {{quote(root)}})" \
        CARGO_BUILD_JOBS=1 \
        cargo test --locked -p kuberic-mssql-tests --test live_three_replica \
        three_replica_sig -- --ignored --test-threads=1 --nocapture

# Exercise post-AG, post-agent panic, and report fault checkpoints with exact recovery.
test-live-three-replica-faults root="target/mssql-three-replica-faults":
    env -u SQLSERVER_TEST_EULA_ACCEPTED \
        KUBERIC_MSSQL_THREE_REPLICA_ROOT="$(realpath -m {{quote(root)}})" \
        CARGO_BUILD_JOBS=1 \
        cargo test --locked -p kuberic-mssql-tests --test live_three_replica \
        three_replica_post_ag_and_report_fault_checkpoints_recover -- --ignored --exact --test-threads=1 --nocapture

# Recover and remove only the exactly journaled real three-member topology.
cleanup-live-three-replica root="target/mssql-three-replica":
    env -u SQLSERVER_TEST_EULA_ACCEPTED CARGO_BUILD_JOBS=1 \
        cargo run --locked -p kuberic-mssql-tests --bin mssql-three-replica-fixture -- cleanup --root "$(realpath -m {{quote(root)}})"
