# Kuberic MSSQL

The observe-only runtime connects to an **already provisioned** SQL Server 2025
Linux x86-64 instance and reports native HA evidence. It does not deploy a
container, accept an EULA, start or restart `sqlservr`, create an AG, change a
role, renew a write lease, or execute any mutation command.

The library is compatible with the observation side of the level-triggered
contract. It remained independent when the classic stack was removed and is
not wired into the level-triggered controller. This is not a complete
Kubernetes HA integration or an automatic failover implementation. The
[support and safety design](docs/design.md) remains authoritative.

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
cargo run --locked --bin sqlserver-observer -- \
  --config /absolute/path/to/observer.json

cargo run --locked --bin sqlserver-observer -- \
  --config /absolute/path/to/observer.json --watch
```

There is no mutation flag, arbitrary SQL input, plaintext connection option, or
certificate-verification bypass. Unknown JSON configuration fields and modes
are rejected, including inline passwords and mutation credentials.

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

### Native Ubuntu 24.04 fixtures

SQL Server 2025 CU1 or later supports native installation on Ubuntu 24.04
x86-64. Microsoft's [Ubuntu installation guide](https://learn.microsoft.com/en-us/sql/linux/install-upgrade/quickstart-install-ubuntu?view=sql-server-ver17)
provides the signed `mssql-server-2025` repository. Install a selected exact
`mssql-server` package version, record the package archive's SHA-256, and use
`/opt/mssql/bin/mssql-conf setup` for explicit edition/EULA setup.
The engine executable is `/opt/mssql/bin/sqlservr`; no container is required.

Provisioning remains external to this crate. Configure HADR, verified TLS,
separate observation credentials and permissions before running the observer.
An absent-AG fixture needs no AG creation, join, seeding or failover. A present-AG
fixture must be separately provisioned by its owner. Native installation does
not add process management or Kuberic controller integration to the observer.

Keep local fixture listeners on loopback and restrict credentials/private keys.
SQL Server needs at least 2 GiB of memory to start; set an explicit engine memory
limit and stop laboratory fixtures when not in use. Do not take over an existing
host SQL Server service or assume different database directories alone isolate
multiple instances. A native fixture uses the same JSON configuration, dedicated
principal and verified-TLS observation path as a container fixture.

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

Operation-envelope serialization/decoding and the durable result journal
remain stage 3 work. The existing canonical signatures and approval/fence
bindings are unchanged. Observation JSON is an output format, not a new
authenticated command protocol.

## Testing

Ordinary tests need neither SQL Server nor Kubernetes:

The same bootstrap script is used by CI and local runs. It reuses matching
installed tools and installs only missing/mismatched prerequisites: the pinned
Rust toolchain/components, compiler tools and checksum-pinned `just` 1.21.0.
CI retains `actions-rust-lang/setup-rust-toolchain@v2`; the bootstrap reuses its
prepared toolchain.

```bash
bash scripts/setup_environment.sh
```

On Ubuntu 24.04, `sudo apt-get install just` is also supported. Recipes put the
standard user-local Rust/just directories on `PATH`.

```bash
just --list
just build
just check
just test
just test-ci-helpers
```

Plain `just` runs `check`: formatting, strict Clippy, ordinary Rust tests and
server-free CI helper tests. Builds default to one job; an explicitly supplied
`CARGO_BUILD_JOBS` is preserved. The underlying Cargo commands remain available:

```bash
cargo fmt -- --check
cargo test --locked --all-features
cargo clippy --locked --all-targets --all-features -- -D warnings
```

The `justfile` is the shared interface for every repository-owned CI step.
The workflow checks out the repository, prepares Rust, runs the shared bootstrap,
then invokes `just ci`. Local runs use that same command:

```bash
just ci /absolute/path/to/fixture-directory
# Alternatively configure once:
export SQLSERVER_FIXTURE_DIR=/absolute/path/to/fixture-directory
just ci
```

`ci` runs shared setup, server-free checks, build, fixture readiness, live cases,
CLI verification and ownership-aware cleanup. `just check` remains the
server-free-only command. Individual phases are also shared:

```bash
just provision /absolute/path/to/fixture-directory  # ensure readiness; no tests
just test-live /absolute/path/to/fixture-directory
just validate-live /absolute/path/to/fixture-directory  # ensure + tests + cleanup
just cleanup /absolute/path/to/fixture-directory
```

### Native CI validation

The native SQL Server 2025 observation job runs on every pull request,
main-branch push and manual `CI` dispatch. The shared fixture helper supplies
`SQLSERVER_TEST_EULA_ACCEPTED=true` to live tests and sets `ACCEPT_EULA=Y`
when provisioning its disposable non-production Enterprise Developer fixture.
This CI deployment policy does not enable EULA acceptance or provisioning in the
observer, and does not grant production licensing rights.

The shared job uses a disposable GitHub-hosted `ubuntu-24.04` x86-64 runner, a pinned
`mssql-server` package version/SHA-256 and a checksum-pinned SQL client.
The same provisioning operation runs locally and in CI. An empty host/fixture
can be provisioned; unrelated existing SQL Server installation/storage or an
occupied SQL port is refused. It provisions one loopback-only, 2 GiB-limited engine with
HADR, verified TLS and separate allowed/denied observation principals.
Credentials are generated per run, kept in private temporary files, and never
passed in arguments or published as artifacts.

The job exercises absent-AG observation, permission denial, invalid-CA rejection
and the actual CLI's fresh SQL Server 2025 output. It creates no AG and makes
no join, seeding, role, lease-renewal or failover changes. An always-run cleanup
step stops the owned fixture and removes its credentials; the disposable runner
is the final containment boundary. This is native observation validation, not
three-replica AG or HA validation. CI uses its generated runner-temporary fixture
directory and disposes only files it created there. Local runs retain fixture files
for reuse. The command implementation and test selection are identical; only
lifecycle retention policy differs.

Live observation tests are explicitly ignored by default. They require
externally provisioned, isolated SQL Server fixtures, mounted credentials,
valid TLS configuration, and explicit test-environment/EULA acknowledgement.
Set the following environment variables (only references/acknowledgements,
never credential contents):

| Variable | Required fixture or acknowledgement |
|---|---|
| `SQLSERVER_TEST_EULA_ACCEPTED` | `true`, supplied automatically by shared `just` recipes and the fixture helper; set explicitly only for direct Cargo invocation |
| `SQLSERVER_TEST_IMAGE` | Container fixture engine image reference with `@sha256:` digest; unset for native fixtures |
| `SQLSERVER_TEST_PACKAGE_VERSION` | Native fixture's exact `mssql-server` package version, e.g. `17.0.4006.2-1`; unset for containers |
| `SQLSERVER_TEST_PACKAGE_SHA256` | Native package archive's 64-character lowercase SHA-256; unset for containers |
| `SQLSERVER_LIVE_ABSENT_CONFIG` | Absolute config path for a supported HADR-enabled instance without the requested AG |
| `SQLSERVER_LIVE_AG_CONFIG` | Absolute config path for a preconfigured supported EXTERNAL AG |
| `SQLSERVER_LIVE_DENIED_CONFIG` | Config with a valid login/TLS connection but missing observation permissions |
| `SQLSERVER_LIVE_BAD_TLS_CONFIG` | Config for a reachable instance with a mismatched CA or TLS hostname |

Set exactly one image reference or complete native-package version/SHA-256 pair.
The artifact acknowledgement is fixture-owner metadata, not installation
attestation through TDS. Artifact pinning and EULA acceptance are the provisioning
owner's responsibility. Once those fixtures are configured:

```bash
just test-live      # absent AG, permission denial, invalid CA
just test-live-all  # also requires a preconfigured EXTERNAL AG
just test-live-cli /absolute/path/to/observation.json
just verify-live-cli /absolute/path/to/fresh-observation.json
```

These recipes use the exported fixture variables above and automatically supply
the test EULA acknowledgement under the repository's fixture policy. They do not
install, start or stop SQL Server, change its license setup, or create an AG. Start the fixture
separately and wait for verified TLS/login readiness before running them;
stop laboratory instances afterward. Missing prerequisites fail the requested
live tests instead of silently skipping them.

`test-live-cli` builds the observer, reads `SQLSERVER_LIVE_ABSENT_CONFIG`, writes
the report to the supplied path and runs `verify-live-cli` only after a successful
observation. Both CLI recipes verify the CI-pinned Enterprise Developer engine's
exact version and native output shape; they are not generic verifiers for other
builds. They work locally without GitHub runner variables.

### Reuse a configured local fixture

For an already provisioned native Ubuntu 24.04 fixture, one command runs the same
live cases and CLI validation without GitHub environment variables:

```bash
just validate-live /absolute/path/to/fixture-directory
```

The private fixture directory must contain `absent.json`, `denied.json`,
`bad-tls.json`, their separate credential files, `ca.crt`, `bad-ca.crt`,
`server.crt`, and the pinned `sqlcmd` executable. Configurations must target
`localhost:1433` and reference files within that directory.
The helper supplies the test EULA acknowledgement automatically; no caller
environment-variable prefix is needed. The already provisioned engine's edition,
license setup and credentials are unchanged.

The shared provisioning operation verifies the installed package, service's loopback/HADR/TLS/memory
settings, and the server certificate's binding to this project fixture before any
service change. It then verifies native identity and TLS/login readiness.
If SQL Server is already running, installation, setup, certificate generation and
credential changes are skipped, and the service is left running afterward.
If the verified fixture is stopped, provision starts it and records its captured
systemd invocation/process generation. Validation cleanup stops only that captured
generation after testing, including test failure or
interruption. Replacement generations are refused during cleanup. Credentials,
configuration and database storage are retained.

Concurrent local fixture runners are refused. Trusted administrators must not change
the SQL Server service while validation runs; these fixture checks are not a
production fencing protocol. Already configured fixtures are never reinstalled,
reinitialized or given new credentials. Fresh provisioning on an empty host accepts
the EULA for Enterprise Developer and creates only the observation fixture, never an AG.
Incomplete setup and unrelated installations fail explicitly rather than being
silently adopted or reset.

Without an explicit directory or `SQLSERVER_FIXTURE_DIR`, CI uses
`$RUNNER_TEMP/sqlserver-observer`; local runs use
`~/.local/state/kuberic-mssql/fixture`. If a local engine was previously configured
elsewhere, supply its existing private fixture directory; it is not discovered or
adopted automatically.

Direct test commands remain available:

```bash
cargo nextest list --profile external --run-ignored only
cargo test --locked --test live_observation -- --ignored
```

For a native instance without an AG, run only its provisioned case:

```bash
SQLSERVER_TEST_EULA_ACCEPTED=true \
SQLSERVER_TEST_PACKAGE_VERSION=17.0.4006.2-1 \
SQLSERVER_TEST_PACKAGE_SHA256="$FIXTURE_PACKAGE_SHA256" \
SQLSERVER_LIVE_ABSENT_CONFIG=/absolute/path/to/observer.json \
cargo test --locked --test live_observation live_absent_availability_group -- --ignored
```

An explicitly requested live test fails if any of its prerequisites are
missing; it never silently skips. Tests use observation principals and issue
no setup/mutation SQL. Fixture provisioning and AG management must be done by
the test environment owner. Local three-node failover, lease expiry, old-primary
fencing, and fault injection are stage 4, not claims of this PR.
