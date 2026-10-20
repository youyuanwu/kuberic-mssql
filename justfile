export CARGO_BUILD_JOBS := env_var_or_default("CARGO_BUILD_JOBS", "1")
export PATH := env_var("HOME") + "/.local/bin:" + env_var("HOME") + "/.cargo/bin:" + env_var("PATH")

# Run all server-free checks without provisioning SQL Server.
default: check

# Run the complete CI pipeline locally or on the CI runner.
ci: setup check test-live-nextest

# Install only missing/mismatched shared Rust, just and compiler prerequisites.
setup:
    bash crates/kuberic-mssql-tests/scripts/setup_environment.sh

# Attempt every owned fixture cleanup even if an earlier family fails.
cleanup:
    bash crates/kuberic-mssql-tests/scripts/cleanup_nextest_fixtures.sh --all

# Check formatting, strict Clippy, and ordinary Rust tests.
check: fmt-check clippy test test-nextest-partitions

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
test: test-ordinary test-doc

test-ordinary:
    env -u KUBERIC_MSSQL_THREE_REPLICA_CHILD_MODE cargo nextest run --locked --workspace --all-features --test-threads=1 --profile default

test-doc:
    cargo test --locked --workspace --all-features --doc

test-nextest-partitions:
    python3 -B -m unittest discover -s crates/kuberic-mssql-tests/scripts -p 'test_*.py' -q

# Check and compile once; the four CI workers reuse this exact archive.
ci-build-nextest-archive archive="target/nextest/mssql-tests.tar.zst": fmt-check clippy test-doc test-nextest-partitions
    mkdir -p "$(dirname {{quote(archive)}})"
    cargo nextest archive --locked --workspace --all-features --profile default --archive-file {{quote(archive)}}

validate-nextest-archive archive="target/nextest/mssql-tests.tar.zst":
    python3 crates/kuberic-mssql-tests/scripts/validate_nextest_partitions.py --archive-file {{quote(archive)}}

test-nextest-archive archive partition extract root="target/mssql-nextest-live":
    bash crates/kuberic-mssql-tests/scripts/run_archived_shard.sh {{quote(archive)}} {{quote(partition)}} {{quote(extract)}} {{quote(root)}}

cleanup-nextest-archive extract root="target/mssql-nextest-live":
    bash crates/kuberic-mssql-tests/scripts/cleanup_nextest_fixtures.sh {{quote(root)}} "$(realpath -m {{quote(extract)}})/target/debug"

# Run every existing ignored SQL fixture suite, serially, with no case narrowing.
test-live-nextest root="target/mssql-nextest-live":
    bash crates/kuberic-mssql-tests/scripts/run_live_nextest.sh {{quote(root)}} cargo nextest run --locked --workspace --all-features

# Recover exact journals within the nextest runner's three named fixture families.
cleanup-live-nextest root="target/mssql-nextest-live":
    bash crates/kuberic-mssql-tests/scripts/cleanup_nextest_fixtures.sh "$(realpath -m {{quote(root)}})"

# Run the server-free future-readiness group without changing the pinned contract.
test-kuberic-readiness:
    cargo test --locked -p kuberic-mssql-tests --test kuberic_contract readiness:: -- --test-threads=1

# Validate one versioned Kuberic release-intake record.
validate-kuberic-release-intake record:
    cargo run --locked -p kuberic-mssql-tests --bin kuberic-release-intake -- {{quote(record)}}

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
        KUBERIC_MSSQL_THREE_REPLICA_CHILD_MODE=happy-path \
        KUBERIC_MSSQL_THREE_REPLICA_ROOT="$(realpath -m {{quote(root)}})" \
        CARGO_BUILD_JOBS=1 \
        cargo test --locked -p kuberic-mssql-tests --test live_three_replica three_replica_lifecycle_child -- --exact --test-threads=1

# Collect acknowledged-transaction/native-record evidence; coverage can remain explicitly blocked.
test-live-native-commit-coverage root="target/mssql-native-commit-coverage":
    env -u SQLSERVER_TEST_EULA_ACCEPTED \
        KUBERIC_MSSQL_THREE_REPLICA_CHILD_MODE=native-commit \
        KUBERIC_MSSQL_THREE_REPLICA_ROOT="$(realpath -m {{quote(root)}})" \
        CARGO_BUILD_JOBS=1 \
        cargo test --locked -p kuberic-mssql-tests --test live_three_replica \
        three_replica_lifecycle_child -- --exact --test-threads=1 --nocapture

# Evaluate native-only prefix coverage around an exactly owned secondary pause/resume.
test-live-native-prefix-coverage root="target/mssql-native-prefix-coverage":
    env -u SQLSERVER_TEST_EULA_ACCEPTED \
        KUBERIC_MSSQL_THREE_REPLICA_ROOT="$(realpath -m {{quote(root)}})" \
        CARGO_BUILD_JOBS=1 \
        cargo test --locked -p kuberic-mssql-tests --test live_three_replica \
        three_replica_mssql_native_prefix_coverage -- --ignored --exact --test-threads=1 --nocapture

# Validate same-root replacement of all three public Kuberic hosts.
test-live-three-replica-restart root="target/mssql-three-replica-restart":
    env -u SQLSERVER_TEST_EULA_ACCEPTED \
        KUBERIC_MSSQL_THREE_REPLICA_CHILD_MODE=same-root-restart \
        KUBERIC_MSSQL_THREE_REPLICA_ROOT="$(realpath -m {{quote(root)}})" \
        CARGO_BUILD_JOBS=1 \
        cargo test --locked -p kuberic-mssql-tests --test live_three_replica three_replica_lifecycle_child -- --exact --test-threads=1 --nocapture

# Exercise native lease expiry/restart and exact Rust-owned fixture fencing.
test-live-lease-fence root="target/mssql-lease-fence":
    env -u SQLSERVER_TEST_EULA_ACCEPTED \
        KUBERIC_MSSQL_LEASE_FENCE_ROOT="$(realpath -m {{quote(root)}})" \
        CARGO_BUILD_JOBS=1 \
        cargo test --locked -p kuberic-mssql-tests --test live_lease_fence \
        mssql_write_lease_and_exact_fence_experiments -- --ignored --test-threads=1 --nocapture

# Recover only the two exactly journaled lease/fence scenario roots.
cleanup-live-lease-fence root="target/mssql-lease-fence":
    #!/usr/bin/env bash
    root={{quote(root)}}
    if [[ ! -e "$root" && ! -L "$root" ]]; then
      exit 0
    fi
    status=0
    just cleanup-live-three-replica {{quote(root + "/expiry")}} || status=1
    just cleanup-live-three-replica {{quote(root + "/active-restart")}} || status=1
    exit "$status"

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

# Lose callback replies after each classified SQL step and exercise seed/callback seams.
test-live-three-replica-faults root="target/mssql-three-replica-faults":
    env -u SQLSERVER_TEST_EULA_ACCEPTED \
        KUBERIC_MSSQL_THREE_REPLICA_ROOT="$(realpath -m {{quote(root)}})" \
        CARGO_BUILD_JOBS=1 \
        cargo test --locked -p kuberic-mssql-tests --test live_three_replica \
        three_replica_mssql_bootstrap_fault_boundaries -- --ignored --test-threads=1 --nocapture

# Exercise isolated SQL-native cases and callback observation-seam conflicts sequentially.
test-live-three-replica-conflicts root="target/mssql-three-replica-conflicts":
    env -u SQLSERVER_TEST_EULA_ACCEPTED \
        KUBERIC_MSSQL_THREE_REPLICA_ROOT="$(realpath -m {{quote(root)}})" \
        CARGO_BUILD_JOBS=1 \
        cargo test --locked -p kuberic-mssql-tests --test live_three_replica \
        three_replica_mssql_conflict_evidence -- --ignored --test-threads=1 --nocapture

# Reject post-compaction association removal and AG recreation without redispatch.
test-live-three-replica-terminal-seed root="target/mssql-three-replica-terminal-seed":
    env -u SQLSERVER_TEST_EULA_ACCEPTED \
        KUBERIC_MSSQL_THREE_REPLICA_ROOT="$(realpath -m {{quote(root)}})" \
        CARGO_BUILD_JOBS=1 \
        cargo test --locked -p kuberic-mssql-tests --test live_three_replica \
        three_replica_mssql_terminal_seed_conflicts -- --ignored --test-threads=1 --nocapture

# Revalidate target-local lineage and SQL process incarnation at every build window.
test-live-three-replica-target-evidence root="target/mssql-three-replica-target-evidence":
    env -u SQLSERVER_TEST_EULA_ACCEPTED \
        KUBERIC_MSSQL_THREE_REPLICA_ROOT="$(realpath -m {{quote(root)}})" \
        CARGO_BUILD_JOBS=1 \
        cargo test --locked -p kuberic-mssql-tests --test live_three_replica \
        three_replica_mssql_target_host_evidence_faults -- --ignored --test-threads=1 --nocapture

# Recover and remove only the exactly journaled real three-member topology.
cleanup-live-three-replica root="target/mssql-three-replica":
    env -u SQLSERVER_TEST_EULA_ACCEPTED CARGO_BUILD_JOBS=1 \
        cargo run --locked -p kuberic-mssql-tests --bin mssql-three-replica-fixture -- cleanup --root "$(realpath -m {{quote(root)}})"
