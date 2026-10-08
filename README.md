# Kuberic MSSQL

The observe-only runtime connects to an **already provisioned** SQL Server 2025
Linux x86-64 instance and reports native HA evidence. It does not deploy a
container, accept an EULA, start or restart `sqlservr`, create an AG, change a
role, renew a write lease, or execute any mutation command.

The library is compatible with the observation side of the level-triggered
contract. Its optional Kuberic runtime publishes validated AG configuration
sequence through the fixed Service Fabric-compatible progress API and hosts the
application through Kuberic's public `ReplicaHost`; it is not wired into the
level-triggered controller. This is not a complete Kubernetes HA integration
or an automatic failover implementation. See the
[progress integration guide](docs/kuberic-progress.md); the
[support and safety design](docs/design.md) remains authoritative.
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
authority can initially arrive before remote peer sessions; missing peer
descriptions may only be completed once, after which replay is exact. Role and
progress callbacks still reobserve SQL Server, and no client SQL listener or
service address is published.

With empty authorized roots, startup initializes Kuberic metadata and writes
the application binding. Reusing those roots reopens established state only
when the supplied identities, bound paths, observer target and topology still
match; the reopened process receives a new session, and mismatches fail closed.
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
scalar election score. Local recovery lineage is not assigned to remote DMV
rows; reports about remote replicas are not that replica's self-observation.
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

Operation-envelope serialization/decoding and the durable result journal
remain stage 3 work. The existing canonical signatures and approval/fence
bindings are unchanged. Observation JSON is an output format, not a new
authenticated command protocol.

## Testing

Ordinary tests need neither SQL Server nor Kubernetes. The shared bootstrap
installs or reuses the pinned Rust toolchain, compiler tools, Docker and
checksum-pinned `just`.

```bash
bash crates/kuberic-mssql-tests/scripts/setup_environment.sh
just check
```

`just check` runs formatting, strict Clippy and locked all-feature Rust tests.
Builds default to one job; an explicitly supplied `CARGO_BUILD_JOBS` is
preserved. Licensed live tests remain ignored during ordinary Cargo execution.

```bash
cargo fmt --all -- --check
cargo test --locked --workspace --all-features
cargo clippy --locked --workspace --all-targets --all-features -- -D warnings
```

The complete local and CI pipeline is:

```bash
just ci
```

It runs server-free checks, the Rust one-replica observer/CLI validation, then
the Rust three-replica topology/Kuberic validation. Both live recipes create
only resources owned by that invocation and clean them before returning.
Aggregate cleanup is always safe to run:

```bash
just test-live-one-replica
just cleanup-live-one-replica
just test-live-one-replica-recovery
just test-live-three-replica
just cleanup-live-three-replica
just cleanup
```

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
path above. It starts three real SQL Server processes, creates
certificate-authenticated endpoints, joins one external AG, automatically
seeds one database, runs three public `ReplicaHost` applications through agent
control RPCs, and proves a marker is readable from every member.

The live path is ignored by default and is not part of `just`, `just check`, or
direct Cargo test runs. The happy path runs after one-replica validation in
`just ci`; fault, signal, and recovery recipes remain explicit. It requires
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

It currently tracks the Kuberic `main` branch. `Cargo.lock` resolves the exact
revision used by reproducible builds; the current revision is
`0124cca382c58cf911eda1c31609c602c52f19b9` from
[Kuberic PR #133](https://github.com/youyuanwu/kuberic/pull/133). It includes
the custom-authority same-root recovery fix and the earlier testing listener
handoff from [Kuberic PR #127](https://github.com/youyuanwu/kuberic/pull/127).

The test crate constructs an affirmative `SqlServerEulaAcknowledgement` through
the clearly test-only `FixtureConfig` path and injects exactly
`ACCEPT_EULA=Y` into each SQL Server container. It does not read an
acknowledgement file or environment variable, and repeated authorization
revalidation is idempotent.

Run the exact dedicated command with an optional fixture root:

```bash
just test-live-three-replica [root]
just test-live-three-replica-restart [root]
```

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
```

Cleanup removes only exactly journaled resources. Foreign, replaced, or
otherwise unverifiable resources remain untouched and block reuse. The signal
recipe interrupts an owned launch with SIGTERM, recovers it, reruns the
complete happy path, and verifies idempotent cleanup. The recovery recipe also
executes real SIGINT and SIGKILL subprocess cases. The fault recipe injects a
post-AG failure, an actual panic after agent startup, a report-stage failure,
and a failure after same-root replacement startup; each is followed by a
separate exact cleanup process and a final same-root restart retry.

Direct test commands remain available:

```bash
cargo test --locked -p kuberic-mssql-tests --test live_one_replica \
  one_replica_mssql_observation_and_cli -- --ignored --exact --test-threads=1
cargo test --locked -p kuberic-mssql-tests --test live_three_replica \
  three_replica_mssql_happy_path -- --ignored --exact --test-threads=1
cargo test --locked -p kuberic-mssql-tests --test live_three_replica \
  three_replica_mssql_same_root_restart -- --ignored --exact --test-threads=1
```

An explicitly requested live test fails if any of its prerequisites are
missing; it never silently skips. The one-replica fixture uses observation
principals and creates no AG, endpoint or database. The three-replica fixture
performs test-only endpoint, AG, database, seeding, and marker mutation. Both
lifecycles own and clean their exact resources; production remains observe-only.
Local three-node failover, lease expiry, old-primary fencing, and fault
injection remain unsupported and are not claims of this work.
