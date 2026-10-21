# Kuberic MSSQL

The observe-only runtime connects to an **already provisioned** SQL Server 2025
Linux x86-64 instance and reports native HA evidence. It does not deploy a
container, accept an EULA, start or restart `sqlservr`, create an AG, change a
role, renew a write lease, or execute any mutation command.

An explicit bootstrap-capable construction exists for licensed validation. The
Rust fixture supplies three running SQL Server instances, HADR endpoints,
authentication and administrative credentials; the three standalone
`ReplicaHost` applications then create the managed database and AG, join local
secondaries and perform one build-authorized automatic-seeding dispatch. This
path is not exposed by the production CLI or runtime configuration.

The optional Kuberic runtime implements the fresh-state Public V1 replacement
contract. It publishes exact SQL Server AG `sequence_number` only after an
exact native binding; capability publication also requires the admitted stable
local role. Kuberic epochs and exact decimal database positions remain separate
domains. The root `Cargo.toml` is authoritative for the Kuberic dependency;
release-intake records evaluate proposed changes but do not define a separate
documented pin.
The adapter is not wired into the level-triggered controller and remains
ineligible for lease, reseed, switchover, failover, and data-loss operations.
See the [progress integration guide](docs/kuberic-progress.md); the [support
and safety design](docs/design.md) remains authoritative.
Role validation never publishes a client service address; the configured
replication address is returned only by the custom replicator's open callback.

## Workspace layout

The repository is a two-member Cargo workspace:

- `crates/kuberic-mssql` contains the observe-only runtime library and the
  production `sqlserver-observer` and `kuberic-mssql-runtime` binaries.
- `crates/kuberic-mssql-tests` contains shared test support, integration and
  ignored licensed live tests, and Rust-owned SQL Server fixture lifecycles.

Dependency versions are declared once in the root `Cargo.toml`; member
manifests select only the features they need.

## Run the observer

Provision the instance separately, explicitly accepting the SQL Server EULA
and using a digest-pinned SQL Server 2025 image or an exact native package
version with its SHA-256 recorded. Use Enterprise Developer edition
(`MSSQL_PID=EnterpriseDeveloper`) for non-production fixtures; Standard Developer
does not provide the full-AG profile. Enterprise is the future production profile.
SQL Server 2022 and other engine major versions are rejected.
Enable HADR and install a TLS server certificate whose
SAN matches the configured connection host. Connect directly to each instance,
not an AG listener, load balancer, or read-only routing endpoint.

Copy [the example configuration](observer.example.json)
and set the instance hostname, AG name, expected `@@SERVERNAME`, logical
replica ID, and current Pod UID (or a laboratory incarnation ID). The hostname
and SQL Server name are different concepts and are both checked.

Mount observation credentials as **separate username and password files**.
Use a dedicated observation principal, not `sa` or the future mutation
principal. Files must be nonempty UTF-8 text without a trailing newline or NUL;
password whitespace is preserved, not trimmed. Configure absolute paths,
restrict mount access, and do not embed credentials in JSON, shell arguments,
or connection strings. The observer rereads these files on every connection,
so a subsequent observation sees rotated credentials.

```bash
cargo run --locked -p kuberic-mssql --bin sqlserver-observer -- \
  --config /absolute/path/to/observer.json

cargo run --locked -p kuberic-mssql --bin sqlserver-observer -- \
  --config /absolute/path/to/observer.json --watch
```

There is no mutation flag, arbitrary SQL input, plaintext connection option, or
certificate-verification bypass. Unknown JSON configuration fields and modes
are rejected, including inline passwords and mutation credentials.

## Run the Kuberic application

The non-containerized `kuberic-mssql-runtime` hosts one already provisioned SQL
Server member through Kuberic's public process boundary. It embeds the same
observation engine as the CLI; it never spawns or parses `sqlserver-observer`.

Provide the normal Kuberic resource, replica, Pod, PVC, control, replication
and data-root variables plus three absolute mounted-file paths:

- `KUBERIC_MSSQL_OBSERVER_CONFIG` — the existing observer JSON;
- `KUBERIC_MSSQL_TOPOLOGY_CONFIG` — the stable replica-to-SQL association,
  following [runtime-topology.example.json](runtime-topology.example.json);
- `KUBERIC_AGENT_BEARER_TOKEN_FILE` — a private, bounded token file.

Choose exactly one peer resolver:

- `KUBERIC_NAMESPACE` for Kuberic DNS names; or
- `KUBERIC_PEER_ROUTES` for an exact route document following
  [runtime-peer-routes.example.json](runtime-peer-routes.example.json).

```bash
cargo run --locked -p kuberic-mssql \
  --features kuberic --bin kuberic-mssql-runtime
```

Runtime identity and topology files reject unknown, duplicate, ambiguous,
symlinked or oversized inputs. The application root contains only a private
schema-versioned binding of non-secret identity and topology digests. Kuberic
role and epoch authority can initially arrive before configuration admission;
without native association, the usual compatibility path reports progress `0`
and capability `INVALID_LSN`. Public V1
current configurations describe exact ready remote active secondaries; the
local member is recovered from runtime identity and the configuration
descriptor, while idle build targets remain separate and require
`INVALID_LSN`, exact build/session/storage identity, and an exact address.
After topology admission, role and progress publication reobserve SQL Server.
No client SQL listener or service address is published.

For validated native election metadata, use the typed
`SqlServerReplicator::native_configuration_sequence` API. Public scalar
publication remains hosted compatibility behavior, not proven durable-data
progress or retained history. The [integration guide](docs/kuberic-progress.md#upstream-reporting-and-history-blockers)
records the upstream reporting/history blockers; production remains observe-only.

Accepted higher epochs fence predecessor dispatch and completion, including
same-role changes. Close drains adapter-owned work; Abort fences immediately
without waiting for graceful completion. Retained MSSQL objects expose
`wait_for_containment` for cancellation-safe local drainage. The concrete process
runner retains startup, host completion and local cleanup across waiter
cancellation or bounded shutdown failure while its async runtime remains alive.
This does not certify generic host post-Abort containment. Application-facing
Kuberic/SF traits stay fixed; shutdown budgets and remaining limits are in the
[lifecycle guide](docs/kuberic-progress.md#local-lifecycle-containment).

With empty authorized roots, startup initializes Kuberic metadata and writes a
Public V1 activation-bound application binding. Existing mode reopens only the
exact binding; new mode with any established, old, partial, copied, or
mismatched application state rejects without migration. Licensed seed records
and SQL ownership markers are also activation-bound, so predecessor Stage 3
authority cannot be adopted.
The licensed three-replica validation also stops all public hosts, reopens the
same roots with fresh sessions, rejects commands targeting the superseded
sessions, and reconverges exact reports without restarting SQL Server.

SIGINT and SIGTERM cancel initialization or request bounded runtime shutdown.
Operational and cleanup failures are retained together instead of being
converted to success.

TLS is required for the complete TDS session. By default the client uses the
system trust store. With `ca_certificate_file`, the pinned TDS driver's Rustls
backend uses **that CA instead of the system roots**; supply one PEM/CRT
certificate or a DER certificate, not a PEM bundle. The certificate chain and
connection hostname are still verified. This TDS CA is not the AG endpoint
certificate: AG endpoint authentication and private-key management remain
separate provisioning concerns.

The driver's system-root loader also honors `SSL_CERT_FILE`. Missing,
unreadable, invalid, or empty system roots can make the pinned driver panic.
These panics, like malformed PRELOGIN panics, become sanitized `malformed`
failure records at the `TLS/TDS login` stage; the connection is discarded and
watch mode retries. Check both the peer protocol and TLS trust configuration
when that record reports a driver panic. No trust or encryption check is bypassed.

### Container observation fixtures

Repository live tests use Rust-owned fixtures and run production observation
code on the host. Both fixtures pin
`mcr.microsoft.com/mssql/server:2025-CU9-ubuntu-24.04` by registry digest; the
image reports SQL Server `17.0.5005.3`.

The one-replica fixture owns one loopback-only container and validates the four
behaviors not supplied by the topology test: absent requested AG, partial
metadata permission denial, invalid CA rejection, and fresh production CLI
output with exact provenance. It creates no AG, endpoint, database, Kuberic
runtime, lease, role transition or failover operation.

The three-replica fixture remains the positive topology and Kuberic progress
proof. Both lifecycles use durable exact-ownership journals, bounded cleanup and
recovery. They never borrow, start, stop or preserve a pre-existing container.
Test-only code accepts the EULA for non-production Enterprise Developer
containers; production remains observe-only.

### Configuration

The example contains all configuration fields. Optional values default to:

| Field | Default |
|---|---|
| `mode` | `observe_only` (the only supported mode) |
| `port` | `1433` |
| `ca_certificate_file` | omitted; system trust store |
| `connect_timeout_ms` | `5000`, including Secret loading, TCP, TLS, and login |
| `query_timeout_ms` | `5000`, including reading the complete result |
| `sample_timeout_ms` | `30000`, for the entire multi-query attempt |
| `poll_interval_ms` | `1000`, delay after each completed attempt |
| `max_age_ms` | `60000`, maximum age of evidence |

Durations must be in `1..=300000` milliseconds. Connect and query timeouts must
not exceed the whole-sample timeout. The configuration is limited to 64 KiB,
individual Secret files to 4096 bytes, and query results to 4096 rows with
4096 bytes per text cell. Exceeding a bound produces an explicit error, never
a truncated successful observation. Hostnames and IPv4 addresses are supported;
named-instance discovery and IPv6 literals are not currently supported.

## Permissions and capability checks

Before interpreting catalog absence, every attempt checks the SQL Server
version and edition, HADR capability, platform, expected server name, and
required metadata permissions. Insufficient permissions are a failed attempt,
not evidence that an AG or database does not exist.

The observation principal needs server-level access to the selected HA,
database recovery, host, and seeding DMVs. In particular, SQL Server 2025
requires `VIEW SERVER PERFORMANCE STATE`; metadata visibility also requires
`VIEW ANY DEFINITION`, `VIEW ANY DATABASE`, and `VIEW SERVER STATE`. Grant
only the observation permissions listed by the capability query, not AG
management permissions or `sysadmin`. The runtime never grants itself access.

An absent requested AG is valid evidence on a supported HADR-enabled instance.
An unsupported engine/profile or a changing observation is not. A missing,
resolving, disconnected, suspended, or unhealthy replica is never synthesized
into a healthy secondary.

An EXTERNAL AG must report numeric cluster type `2` and an ASCII
case-insensitive `EXTERNAL` descriptor, including SQL Server's lowercase
`external` value. The original native descriptor is preserved in the snapshot.

## Evidence and output

One-shot mode emits one JSON document. Watch mode emits newline-delimited JSON
containing the latest complete snapshot; a slow consumer can miss intermediate
samples, so this is not an audit journal.

Each report has:

- `schema_version: 1`;
- `source`: endpoint, requested AG, expected SQL Server name, logical replica
  ID, and caller-supplied incarnation;
- `evaluated_at_unix_millis` and `max_age_millis`;
- `fresh`: validity **at that evaluation time**, not a permanent health flag;
- `observation`: a tagged `present`, `absent`, or `failed` value.

An instance snapshot includes the native AG, replicas, databases, local role,
sync/connection health and seeding facts. Native GUIDs identify catalog and
DMV relationships. Hardened-block, redone-record, and committed-record
positions remain separate, exact decimal strings in JSON, including values
larger than `i64` or JavaScript's safe integer range. They are not a generic
scalar election score. Because these lineage-scoped fields have different
meanings and can exceed Public V1's signed 64-bit scalar domain, they cannot be
safely narrowed, hashed, saturated, or substituted into that slot. Future
build, recovery, and handoff decisions must compare the appropriate native
field under exact database and recovery-fork lineage and remain gated on the
Stage 6 lease, fencing, and authenticated mutation design. Local recovery
lineage is not assigned to remote DMV rows; reports about remote replicas are
not that replica's self-observation.
Automatic-seeding history is scoped to the current native database and replica
GUIDs. Physical-seeding rows require an already-associated local database GUID;
pre-join destination processes may be unavailable. Empty seeding lists are not
proof that seeding never ran or that a replica has synchronized.

Each snapshot uses a single connection and brackets its queries with native
identity, role, and configuration evidence. If the anchors change, the whole
attempt fails as `inconsistent`. This detects observable changes; it does
**not** make DMVs transactionally atomic or prove cluster-wide consensus.

A timestamp is captured at the **start** of every attempt, before connecting.
Timeouts, login failures, permission failures, malformed data, inconsistent
snapshots, and unsupported states produce fresh failure records but never
fresh **evidence** (`fresh` is false). A failed attempt replaces the previous
snapshot; the monitor does not silently retain prior successful progress.
Every new attempt reconnects, and an interrupted connection is discarded.

Consumers must recompute freshness from the original attempt timestamp and
their current time, using `ObservationReport::is_fresh_at` in Rust. The exact
age boundary is inclusive; future timestamps and failed attempts are not
fresh. Slow samples can already be stale when published. None of this evidence
alone proves promotion eligibility, fencing, or authority to serve writes.
The source incarnation is caller-provided attribution, not an authenticated
or durable fencing attestation.

The CLI exits zero only after a fresh, successful one-shot observation. This
does not mean the AG is healthy: a valid snapshot can report absence or
unhealthy replication. Failed/stale attempts exit nonzero. Watch mode keeps
observing after failures and exits nonzero on shutdown if any report was failed
or stale at publication, even if a later report overwrote it or shutdown
prevented its output. The monitor retains this aggregate independently of the
latest-value output channel. Cancelling an in-flight attempt before it publishes
does not itself mark watch mode as failed. SIGINT and SIGTERM cancel
in-flight observation even if the stdout consumer stops reading. Normal writes
are acknowledged and flushed, but cancellation may leave a partial final JSON
line. An interrupted one-shot invocation exits with code 130. Output,
configuration, and monitor errors also exit nonzero.

Diagnostics contain adapter-owned text and, when applicable, a numeric SQL
Server error code. Driver/server messages, credentials, and SQL batches are
not echoed. The CLI does not enable driver tracing.

## Library boundary

`SqlExecutor` and `SqlSession` provide a replaceable transport for server-free
tests and future callers. `ReadQuery` exposes only the closed set of
parameterized observation queries. `TdsExecutor` implements that interface
with verified-TLS TDS connections, while `SqlServerInstanceManager` owns
connection/sample lifecycle and `SqlServerMonitor` publishes complete reports
through a watch channel. The monitor starts with no sample, supports explicit
cancellation, and reports subscriber loss rather than silently discarding
results. On cancellation, the monitor returns a completion summary recording
whether any failed/stale report was published. The CLI joins the monitor and
includes that summary in its exit status without draining blocked output.

The TDS boundary contains panics while polling connection and query futures,
never reuses a failed or interrupted session, and requires `panic = "unwind"`
for recovery (the Cargo default). Abort builds and process-level aborts
cannot be recovered. A process-wide delegating panic hook suppresses payloads
only during a driver poll, so panic text and backtraces cannot bypass sanitized
failure reports. The previous hook still handles unrelated panics, including
other tasks between polls. Embedders that replace the panic hook after starting
TDS observation must preserve this delegation.

`runtime_host` owns strict process inputs, peer resolution, non-secret durable
application binding, public `ReplicaHost` assembly, startup cancellation and
shutdown/error composition. Kuberic's private SQLite store and runtime-effect
types are not part of the application API.

Stage 3 did not select operation-envelope serialization or a generic result
journal. Callback-visible authority and Kuberic's retained command/build
evidence are reconciled with exact native SQL postconditions; only the bounded
pre-association seed-dispatch ambiguity uses application metadata because that
is the one external-effect boundary native SQL cannot classify alone across a
callback crash or lost reply. Kuberic's stateless default-replicator style does
not require deleting that bounded file/SQL reconciliation seam. Its durable
file phases distinguish intent, submitting, accepted/ambiguous, and definite
retryable failure. A matching singleton SQL-owned token row and group-scoped
application lock prove acceptance, active ambiguity, or a safe pre-effect
retry; a local `Submitting` phase never claims that SQL accepted the request.
Before temporary file compaction, SQL also retains one scope-named terminal
extended property in `master`. Its exact token binds the resource,
configuration, AG GUID, source database identity/lineage, SQL incarnation and
authorized targets. Once present, later association loss or database/AG
recreation is terminal conflict/unsafe and can never trigger another
`ADD DATABASE`.
Build completion additionally requires a challenge-bound report made from a
fresh direct observation on the target host. The test-host exchange admits only
the exact current Kuberic resource, configuration, build, target identity and
process session, and the primary callback revalidates the target native replica,
AG/database identities, family/recovery-fork lineage, synchronized/healthy/
online/not-suspended state, and the fixture-frozen target SQL start identity.
Primary-side remote DMV rows may advance dispatch polling, but cannot complete
a target build. The exchange is available only to explicit bootstrap testing
construction; it adds no production controller or mutation transport.
The existing
canonical signatures and approval/fence bindings remain contracts for later
command transport. Observation JSON is output, not an authenticated command
protocol.

## Testing

Ordinary tests need neither SQL Server nor Kubernetes. The shared bootstrap
installs or reuses the pinned Rust toolchain, compiler tools, Docker and
checksum-pinned `just` and `cargo-nextest`.

```bash
bash crates/kuberic-mssql-tests/scripts/setup_environment.sh
just check
```

`just check` runs formatting, strict Clippy, locked all-feature nextest tests,
a separate Cargo doctest step and the archive-partition validator's unit tests.
Builds default to one job; an explicitly supplied `CARGO_BUILD_JOBS` is
preserved. Licensed live tests remain ignored during ordinary Cargo execution.
The required GitHub Actions job `Shared CI validation` gates a checked test
archive, exact partition coverage and four isolated test workers at the same
pull-request revision. Each worker always attempts exact archived-binary
cleanup. GitHub Actions job logs are the CI evidence; logs, archives and fixture
state are not committed to this repository.

Future Kuberic candidates have a separate server-free readiness group and a
versioned intake-record validator:

```bash
just test-kuberic-readiness
just validate-kuberic-release-intake /absolute/path/to/intake.json
```

See [Kuberic release intake](docs/kuberic-release-intake.md). A current-pin
baseline can be confirmed without licensed fixtures, but a candidate cannot be
ready to pin while any candidate-required live or target-contract gate remains
pending.

```bash
cargo fmt --all -- --check
cargo test --locked --workspace --all-features
cargo clippy --locked --workspace --all-targets --all-features -- -D warnings
```

The server-free [hosted authority diagnostic](docs/kuberic-progress.md#offline-hosted-authority-association)
uses real public hosts with scripted native witnesses:

```bash
CARGO_BUILD_JOBS=1 cargo test --locked -p kuberic-mssql-tests \
  --test runtime_process_host native_authority -- --test-threads=1
```

It exercises exact accepted configuration/session/storage/history binding and
stale authority rejection, retained partial-startup ownership, and synthetic
quiet-report assembly, not live SQL durability or public catch-up.

The complete local pipeline remains serial:

```bash
just ci
```

It runs server-free checks, then all 59 currently ignored SQL fixture tests
through nextest's `live` profile and shared `live-sql` group. One-replica
observation/recovery, three-replica happy path/restart/fault/signal/conflict/
target-evidence, native commit/prefix, hosted witness and lease/fence suites
run one at a time locally and within each CI worker. The unrelated privileged ACL test is not selected.
Rust ignores keep ordinary Cargo server-free; the explicit live recipe uses
`--run-ignored only`. Live failures stop further scheduling and remain failures.

CI follows Kuberic's build-once nextest archive pattern: one producer checks,
compiles and uploads only the compressed binaries/metadata archive for one day.
Shard 1 first proves exhaustive, disjoint four-way ordinary/live coverage;
shards 2–4 start without waiting for it. All four workers run ordinary
`slice:i/4` and SQL `hash:i/4` partitions. Workers
reuse the archive without rebuilding, remap paths to their matching checkout,
and keep SQL serial on their own isolated Docker host. The final required gate
fails if the producer or any worker fails, skips or is cancelled, including
a coverage-validation failure in shard 1.

To reproduce archive stages locally, run one worker at a time:

```bash
just ci-build-nextest-archive
just validate-nextest-archive
extract="$(mktemp -d "$PWD/target/nextest-extracted.XXXXXXXXXX")"
just test-nextest-archive target/nextest/mssql-tests.tar.zst 1 "$extract"
just cleanup-nextest-archive "$extract"
```

Use the same source revision as the archive producer. Retain the extraction
until cleanup succeeds; archived cleanup invokes its existing fixture binaries
directly, not Cargo. The uploaded archive contains no credentials, journals,
test logs or PAW files.

Long fault/handshake/conflict/target/checkpoint/lease suites use individually
named scenario tests, so nextest reports each case's result and duration.
Fresh fixtures, atomic recovery/replay journeys and serial SQL execution are
retained; splitting improves progress visibility, not total execution time.
Three minutes is a reduction goal, not a per-case deadline; atomic recovery
journeys may take longer. Runtime targets never interrupt required cleanup.
Narrow family recipes select all their named cases by prefix; use the full
`family::case` name with `--exact` to select an individual three-replica case.

Targeted slow scenarios retain private phase snapshots and publish validated
`MSSQL_PHASE` summaries through the same local/archive live runner, without
enabling raw successful SQL output. Scenario, parent/child attempt, phase,
outcome and elapsed milliseconds distinguish setup, native waits, recovery and
cleanup. Nested timings are not additive; unfinished work remains incomplete,
not successful. These diagnostics do not prove containment or authorize cleanup.

Each live-runner invocation retains its snapshots in a fresh
`.phase-timings.*` directory beneath its runner root. Publication has a separate
five-second deadline and one-second forced-stop reserve; reporting failure
cannot hide an existing test failure or indefinitely delay fallback cleanup.
Direct Cargo live runs retain snapshots in `target/.mssql-phase-timings-*`
and print their safe summary only after successful cleanup. Their existing
`--nocapture` output is still raw test output, not a sanitized reporting channel.

Three-member fixtures use a 250ms observation retry cadence and 180-second
observation budget by default, separate from the five-minute SQL member-startup
allowance. The pre-association SQL restart case and its accepted-reply-loss
healthy control instead share a 90-second observation budget and the original
two-second cadence. These are test-only functional budgets, not whole-test
runtime targets. Transport limits, outer operation waits, full recovery
assertions and cleanup grace remain unchanged.

Disposal can overlap stops of eligible members within one already-owned
three-replica fixture; SQL cases and fixtures still run serially per worker.
Every participant needs durable cleanup intent and exact ownership checks.
The ten-second stop grace is unchanged, errors remain failures even after
later recovery, and dependent network/data removal waits for verified absence.
Controlled restart and fencing retain their scalar stop path.

Handled-signal and checkpoint recovery require the actual expected child cause
and completed teardown, not just any nonzero exit. Timeout escalation cannot
count as that expected outcome; SIGKILL requires observed SIGKILL death.
Separate recovery and the existing full positive retries remain mandatory.

Complete happy-path and same-root-restart journeys run inside recovery retries,
not again as standalone CI tests. The hosted witness retains the full native
commit collection. Their existing manual recipes remain available through an
explicitly gated child helper, which runs no SQL in ordinary tests without a
requested mode; canonical checks clear that internal mode.

The same commands run locally when fixture prerequisites are available:

```bash
just test-live-one-replica
just cleanup-live-one-replica
just test-live-one-replica-recovery
just test-live-three-replica
just cleanup-live-three-replica
just test-live-nextest
just cleanup-live-nextest
just cleanup
```

The full nextest run uses absolute fixture-family roots beneath
`target/mssql-nextest-live`; both new recipes accept an optional runner-root
argument. Recovery visits retained nested ownership journals only in its
`one-replica`, `three-replica` and `lease-fence` families, does not traverse
symlinks beneath the runner root, and delegates removal to exact-ownership cleanup.
Failures in discovery or cleanup are reported without suppressing other
families; live/unknown owners remain blocked. Successful cleanup retains
terminal `Removed` journals. CI uses finite 120-minute producer and 90-minute
worker envelopes, with a 30-minute coverage step in shard 1. These are not
worst-case recovery guarantees.
Serialization applies within each worker/invocation; fixture locks still
govern separate invocations.

The one-replica preflight requires at least 5 GiB effective available memory,
6 GiB on its fixture filesystem and two effective CPUs. Its single container
uses the same 3 GiB hard memory, two-CPU and 2 GiB SQL Server memory limits as
each three-replica member.

Override fixture roots with each recipe's optional `root` argument. The
one-replica environment override is `SQLSERVER_ONE_REPLICA_ROOT`; the
three-replica override is `KUBERIC_MSSQL_THREE_REPLICA_ROOT`.

### Legacy Python fixture cleanup

The removed Python fixture is not adopted by the Rust lifecycle. If an older
checkout left `fixture-run.json`, the exact legacy owner marker or its
deterministic container, the Rust command refuses to mutate that root.

Use the immutable pre-migration commit in a temporary worktree:

```bash
git worktree add --detach /tmp/kuberic-mssql-legacy-cleanup \
  b835bd411864dd7697b4f44dde8377940bd8997a
python3 /tmp/kuberic-mssql-legacy-cleanup/crates/kuberic-mssql-tests/scripts/sqlserver_fixture.py \
  cleanup /absolute/path/to/legacy-fixture
git worktree remove /tmp/kuberic-mssql-legacy-cleanup
```

Before removing retained files, independently verify that `fixture-run.json`
and the deterministic legacy container are absent, the root is canonical,
user-owned and not a symlink, and `owner` contains exactly
`kuberic-sqlserver-observer-container-v1`. Ambiguous state must remain
untouched.

### Three-replica licensed happy path

The dedicated three-replica fixture complements the one-replica observer/CLI
path above. It starts three real SQL Server processes and creates
certificate-authenticated endpoints and peer authentication. Explicit
bootstrap-capable public `ReplicaHost` applications then create one managed
database and external AG, join both secondaries, perform one group-level
automatic-seeding dispatch through Kuberic build authority, and prove a marker
is readable from every member. Database create, full recovery, ownership,
marker schema, full/log backup readiness, backup verification, AG intent,
create, GUID binding, local join, seeding grant, and its SQL-owned completion
marker are independently observed typed steps; a retry executes only the next
missing safe step.

The live path is ignored by default and is not part of `just`, `just check`, or
direct Cargo test runs. The full live profile explicitly selects happy-path,
fault, signal, recovery and experiment cases in `just ci`; narrow recipes remain
available. It requires
local Linux x86-64 with cgroup v2, a local Docker engine
that enforces memory/no-additional-swap/CPU limits, `setfacl`/`getfacl`, at
least 10 GiB effective available memory, two effective CPUs, 15 GiB free on the
fixture filesystem, and Docker-root capacity of 8 GiB before an absent-image
pull and 2 GiB after the image is present. Each container is capped at 3 GiB,
two CPUs, and 2 GiB SQL Server memory; compilation uses one Cargo build job.
Before creating run resources, preflight also requires noninteractive `sudo`
for exactly `python3` running the UID-10001 access probe and recursive physical
`setfacl --modify` used to restore host cleanup access. Missing, denied, or
expired authorization fails closed; the member-directory binding is journaled
before the fallible UID probe so exact cleanup remains possible.

The fixture pins:

```text
mcr.microsoft.com/mssql/server@sha256:2b5b581621126574f3d1f75e78d3eebe8d05aedb59ad0cfdf9aa42cb0634d726
```

The root `Cargo.toml` declares the Kuberic dependency source and version.
`Cargo.lock` records the resolver output for reproducible builds; documentation
and release-intake artifacts do not define another dependency pin.

Repository Cargo commands force `ACCEPT_EULA=Y` through `.cargo/config.toml`.
That repository-wide setting authorizes only the Rust-owned licensed fixtures;
their clearly test-only `FixtureConfig` path constructs the affirmative
`SqlServerEulaAcknowledgement` and passes exactly `ACCEPT_EULA=Y` to each owned
SQL Server container. It does not authorize external or borrowed instances.
The production adapter still has no SQL Server launcher, never accepts an EULA,
and never deploys SQL Server. File-backed authorization remains a separate
future-launcher contract.

Run the exact dedicated command with an optional fixture root:

```bash
just test-live-three-replica [root]
just test-live-three-replica-restart [root]
just test-live-native-commit-coverage [root]
just test-live-native-prefix-coverage [root]
just test-live-lease-fence [root]
just cleanup-live-lease-fence [root]
```

The native commit-record recipe uses `target/mssql-native-commit-coverage` by
default. It explicitly runs the fixture-only native diagnostic and emits raw
before/after/quiet evidence. A passing collection run can report **blocked**
coverage; it does not enable catch-up or switchover. See the
[implemented probe and remaining proof gates](docs/kuberic-progress.md#first-native-proof-toward-switchover).
Recover its exact owned resources, if needed, with
`just cleanup-live-three-replica target/mssql-native-commit-coverage`.

The native-prefix recipe uses `target/mssql-native-prefix-coverage`. Its ignored
fixture-only experiment pauses the exact secondary, acknowledges one workload,
resumes it and evaluates fresh lagging/covered/quiet native prefixes. Explicit
native phases do not grant public authority: `public_coverage` remains blocked
and production stays observe-only. See the
[native proof and limits](docs/kuberic-progress.md#native-prefix-coverage).
Recover only that root with
`just cleanup-live-three-replica target/mssql-native-prefix-coverage`.

The separate [live hosted witness](docs/kuberic-progress.md#live-hosted-witness)
has demonstrated one licensed sampled native association using the no-control
commit probe and actual observe-only public hosts. Startup validates exact
accepted lifecycle evidence without requiring serving report roles; public
coverage remains blocked. Its ignored case adds
no SUSPEND/RESUME controls and is included in the explicit nextest live CI
profile; the guide also gives its exact individual selection command.

The lease/fence recipe uses separate `expiry` and `active-restart` scenario
roots and verifies native lease behavior, cancelled-response uncertainty,
fresh-session write readback, and exact owned-container fencing. See the
[foundation design and observed limitations](docs/design.md#lease-and-fence-foundation).
Production lease renewal and failover remain disabled.

For example:

```bash
just test-live-three-replica
just test-live-three-replica target/my-mssql-three-replica
just test-live-three-replica-restart
```

Production and future launchers retain explicit
`SqlServerEulaAcknowledgement` semantics. The strict file-backed constructor
and its negative contract tests remain available for those launchers. The
shipped example intentionally contains `accepted: false` as production-contract
documentation; it is not required by the test command.

The fixture root is private and exclusively locked. An atomic ownership journal
records intent before each resource create and binds exact container, network,
path, native SQL, and process-incarnation evidence. Normal completion, setup
failure, panic, SIGINT, and SIGTERM attempt bounded cleanup; an uncatchable
termination is recovered from that journal on the next command.

Use the actual recovery recipes rather than deleting Docker resources or the
journal manually:

```bash
just cleanup-live-three-replica
just cleanup-live-three-replica target/my-mssql-three-replica
just test-live-three-replica-signal
just test-live-three-replica-signal target/my-mssql-three-replica-signal
just test-live-three-replica-recovery target/my-mssql-three-replica-recovery
just test-live-three-replica-faults target/my-mssql-three-replica-faults
just test-live-three-replica-conflicts target/my-mssql-three-replica-conflicts
just test-live-three-replica-terminal-seed target/my-mssql-three-replica-terminal-seed
just test-live-three-replica-target-evidence target/my-mssql-three-replica-target-evidence
```

Cleanup removes only exactly journaled resources. Foreign, replaced, or
otherwise unverifiable resources remain untouched and block reuse. The signal
recipe interrupts an owned launch with SIGTERM, recovers it, reruns the
complete happy path, and verifies idempotent cleanup. The recovery recipe also
executes real SIGINT and SIGKILL subprocess cases.

The fault recipe uses a test-only observation seam to lose the callback reply
after each separately classified database, backup, AG-create/bind, local-join,
and seeding-permission SQL step. The statements themselves execute through the
normal TDS mutation path. Each configuration fault case requires exactly one
injected reply loss at its named effect, observed mutation dispatch, confirmed
host shutdown and exact idempotent cleanup. Source-recovery also reads back
the completed FULL recovery change. The test makes no post-shutdown restart
attempt: these cases do not establish terminal-versus-pending public state,
fresh-session admission, completed AG/seeding or marker convergence after each
lost reply. Those old per-boundary recovery expectations are removed, not
treated as successful renewal.

Separate seed submission cases retain positive same-session safe retry and
zero duplicate accepted dispatch checks. Seed intent, submission, acceptance,
association, native completion and callback publication use explicit test-only
gates/probes around real callbacks; they are not SQL Server process-crash tests.
The post-native-success case requires effect-free callback rejection after
Abort instead of polling unavailable terminal progress. Its completed-state
replacement executes a real native callback replay with zero duplicate
`ADD DATABASE` dispatch and preserved marker visibility; this is not Public V1
Build completion or interrupted-session renewal.

The conflict recipe separates three real SQL-native cases from callback-seam
coverage. Through normal TDS observation it converges a source that inherits
`SIMPLE` recovery, refuses an unowned same-name database, and refuses an
unowned exact-profile same-name AG. The remaining AG, replica, endpoint,
database-identity/recovery, and lineage cases inject decoded observations into
the real production validators; the SQL-incarnation validator uses a real
process restart plus a test gate.

The terminal-seed recipe performs real SQL association removal and AG
recreation, then proves terminal conflict/unsafe classification, zero
redispatch, no recreated temporary intent, and exact cleanup. The
target-evidence recipe retains three real target SQL restart windows. One
synthetic-lineage journey holds the callback at pre-association, in-flight
seeding and final target evidence, then injects the mismatch only at its final
report-consumption seam. Every scenario performs exact, idempotent
cleanup. Physical restart requires an independently observed new SQL start,
a returned unsafe/unavailable or bounded callback-budget error, and no duplicate
seed submission. An admitted `ADD DATABASE` may remain: callback failure does
not prove rollback, zero side effects or a specific incarnation-check branch.
Lineage mismatch still requires the lineage-conflict error.

Direct test commands remain available:

```bash
cargo test --locked -p kuberic-mssql-tests --test live_one_replica \
  one_replica_mssql_observation_and_cli -- --ignored --exact --test-threads=1
just test-live-three-replica
just test-live-three-replica-restart
```

An explicitly requested live test fails if any of its prerequisites are
missing; it never silently skips. The one-replica fixture uses observation
principals and creates no AG, endpoint or database. The three-replica fixture
owns test-only containers, endpoint security, credentials and cleanup; explicit
bootstrap-capable hosts own the database, AG, joins and seeding effects. Both
lifecycles clean their exact resources, and ordinary production construction
remains observe-only. Local three-node failover, lease expiry and old-primary
fencing remain unsupported and are not claims of this work.
