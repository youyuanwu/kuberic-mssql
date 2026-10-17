# Kuberic SQL Server Progress Integration

## Purpose

The optional Kuberic integration implements a partial fresh-state Public V1
`Replicator` adapter. Its current progress callbacks publish validated SQL
Server availability-group configuration sequence, not proven database-data
progress. Kuberic lifecycle epochs and SQL-native database positions are
independent authority domains; full Kuberic/Service Fabric semantic conformance
is not yet established.

An explicit native configuration-sequence API separates validated election
metadata from those compatibility callbacks. Existing hosted behavior is
deliberately preserved; this separation does not make the public scalar
publication conforming.

Ordinary construction is deliberately observe-only. It can open as a Kuberic
application, validate a requested native role, and report current progress. An
explicit bootstrap-capable construction used by licensed validation can also
create the managed source database and AG, join local secondaries, and execute
one build-authorized automatic-seeding dispatch. Neither construction can
remove/reseed replicas, grant write access, renew a lease, switch over, fail
over, or handle data loss.

This document records the implemented Public V1 boundary. The root
`Cargo.toml` is authoritative for the Kuberic dependency; no new production
mutation support is announced.

## Two Progress Domains

SQL Server exposes two different categories of progress.

### Availability-group configuration sequence

`sys.availability_groups.sequence_number` is a nonnegative SQL `bigint`. SQL
Server increments it when the primary updates availability-group configuration.
The adapter represents it as `ConfigurationSequence`. Before native association
or configuration admission, the usual compatibility path returns zero for
`current_progress` and `INVALID_LSN` for `catch_up_capability`. Ordinary active
secondaries can discover and bind native evidence before public admission.
After exact AG binding, both callbacks return the current exact
nonnegative signed 64-bit sequence. An observed zero sequence is valid native
metadata but is not promotable. The pre-admission zero is a placeholder, not
proof of an empty durable data history. Configuration epoch is never a fallback
progress value.

This is the adapter's as-built scalar-publication behavior, not an established
Public V1 durable-data history contract. AG sequence can remain unchanged while
customer transactions advance; it does not prove that a transaction has been
committed, hardened, or redone.

Microsoft's public `mssql-server-ha` agent uses the same sequence:

- synchronous-commit and configuration-only replicas publish the local sequence;
- other availability modes publish zero;
- a selected candidate below the maximum observed sequence is rejected;
- sequence zero is not promotable;
- a majority of required sequence reports must be available;
- promotion then renews the external write lease and invokes SQL Server's
  normal external-cluster failover path;
- SQL Server can reject unsynchronized or unjoined databases; matching sequence
  reports do not bypass that native gate; and
- command acceptance is followed by waiting for PRIMARY and monitoring.

The candidate adapter implements only local AG-sequence publication. It does
not implement the quorum, lease, fencing, or promotion steps required for
controller-driven role movement.

### Native election evidence API

With the `kuberic` feature, `SqlServerReplicator::native_configuration_sequence`
returns `Result<Option<ConfigurationSequence>, KubericAdapterError>`:

- `Some(sequence)` is exact validated native configuration metadata, including
  observed zero and the signed-64-bit maximum;
- `None` is the existing pre-admission path without native evidence to publish,
  not a fabricated observation or sequence zero; and
- lifecycle, observation, freshness, identity/profile, and applicable bound
  topology/nonregression failures remain explicit errors.

This shares the existing current-progress read path rather than adding a cache
or another SQL query. Available reads use one fresh observation and recheck
freshness before publication. The usual unassociated pre-admission path reads
no SQL; ordinary active-secondary discovery may observe and establish a native
association, but does not admit public membership or establish readiness.
Without a topology expectation or association, the read validates the native
profile but does not freeze a binding or acquire bound-history guarantees.

The public `current_progress` callback projects this result to the existing
`i64`, including `None -> 0`, solely for compatibility. The capability callback
retains its additional stable-role checks and existing values. Native callers
should use the typed API rather than infer native evidence from the public
placeholder. Neither API result certifies an election quorum, promotion
eligibility, database durability, or retained catch-up coverage.

### Database-native positions

`sys.dm_hadr_database_replica_states` exposes hardened, redone, and committed
positions as lineage-scoped `numeric(25,0)` values. Hardened and related values
can be padded log-block identifiers rather than record LSNs; redone and
committed positions have different meanings. The values are scoped to a
database identity and recovery fork and may exceed the signed 64-bit domain.

The observer retains these fields independently as exact decimal values:

- `hardened_block`;
- `redone_record`;
- `committed_record`.

They are never cast, truncated, narrowed, saturated, hashed, or substituted
for Kuberic progress or Kuberic's signed 64-bit LSN slot. Future build, catch-up,
handoff, and recovery operations must validate the appropriate native field and
lineage explicitly. Database-native values remain outside the current public
scalar callbacks; a conforming progress/history model remains a design blocker.

## Semantic Conformance Boundary

Kuberic's SF-shaped traits require primary quorum-committed progress, secondary
contiguous durable progress, access-closed recovery evidence, and a retained
catch-up floor or `INVALID_LSN` for unknown/inapplicable capability. AG
configuration sequence does not establish those data-progress postconditions.
The public traits remain unchanged; MSSQL must align its supported behavior
rather than redefine their meaning.

### Upstream reporting and history blockers

The dependency selected by root `Cargo.toml` has these concrete integration
constraints. The upstream links identify source locations, not another
dependency pin:

| Boundary | Current constraint |
|---|---|
| [Registered host reporting](https://github.com/youyuanwu/kuberic/blob/main/kuberic-runtime/src/host/hosting.rs#L637-L688) | Status/report construction requires successful custom `current_progress`. Returning an unsupported-progress error would also disable existing registered-host report-dependent bootstrap/restart journeys. |
| [Provider-free report projection](https://github.com/youyuanwu/kuberic/blob/main/kuberic-runtime/src/host/hosting.rs#L644-L687) | The host synthesizes `first = 0` without a provider, or uses provider committed progress otherwise. It does not query custom `catch_up_capability` for that field. Returning `INVALID_LSN` from capability alone would not fix the report. |
| [Completed progress validation](https://github.com/youyuanwu/kuberic/blob/main/kuberic-runtime/src/protocol/public_operations.rs#L2162-L2194) | A completed report needs nonnegative ordered endpoints; it cannot express unknown progress with `INVALID_LSN`. Peer descriptors allowing unknown values do not relax this requirement. |
| [Public history consumers](https://github.com/youyuanwu/kuberic/blob/main/kuberic-controller/src/evaluator/public_v1_topology.rs) | Candidate selection compares report tails within accepted public history; retained rejoin uses history, storage/session/completion evidence, and report endpoints. Peer descriptors receive report `last` as current progress and `first` as capability. These are not Microsoft's native AG-sequence/quorum/failover protocol. |

The remaining adapter obligation is to establish how SQL-native evidence
corresponds to public history, role-specific durable progress, recovery-only
election evidence, and retained catch-up coverage across restart/history changes.
No conforming mapping is established by an AG sequence or by fitting a database
position into `i64`. A wider integer alone would not resolve these semantics.

For now, hosted scalar values and report journeys remain deliberately unchanged
compatibility behavior. This is a known semantic gap, not a successful
conformance gate. No dummy provider, fabricated counter, epoch fallback, or
alternate replication log is introduced to hide the upstream constraints.

The [design](design.md#kuberic-and-service-fabric-semantics) records implemented
local lifecycle containment and the remaining gaps in progress,
configuration/build completion, native endpoint/access ownership, and generic
host integration for custom descendant containment.
The [roadmap](roadmap.md#stage-6a-follow-up-replicator-semantic-conformance)
makes closing those gaps a prerequisite for planned switchover. Existing
observation, bootstrap, and lease/fence evidence remains useful, but does not
certify the missing semantics.

## Local Lifecycle Containment

Supported asynchronous adapter work captures its accepted lifecycle/epoch
authority. A higher validated epoch retires predecessor external polling,
worker/task dispatch and success publication, including same-role changes.
Equal-epoch replay preserves current work. Rejected authority does not weaken
existing native identity, lineage, role, freshness or sequence checks.

Close refuses new admissions and waits for owned callbacks, blocking
seed-store workers and seed-submission task joins. Abort refuses admissions
and requests cancellation without waiting for graceful work or a held
publication guard. Work that has already started remains owned; cancelling a
caller does not cancel a started filesystem closure or prove native SQL
rollback. Submitted seed effects keep their durable Submitting/Ambiguous
evidence and exact-context reconciliation/exclusion.

`SqlServerReplicator::wait_for_containment` and
`SqlServerService::wait_for_containment` are inherent local asynchronous APIs
returning `Result<(), KubericAdapterError>`.
`RuntimeHostApplication::wait_for_containment` forwards the same local boundary
through Kuberic's result type. Call them after Close or Abort, including after
cancelling an earlier Close/drain waiter. Calling before terminal admission is
an explicit invalid-configuration error. Wait cancellation retains ownership
and completion results for retry; it does not reopen admission. Genuine store
I/O/unsafe-state failures and worker panics remain explicit drain failures.
Successfully handled revision/removal contention is not a permanent Close
failure. After successful drainage the cached seed-store owner is released;
durable unresolved records are not discarded.

Application-facing Kuberic/SF traits are fixed and will not be changed or
expanded for MSSQL. Kuberic's internal built-in containment witness/drain
registration is not an application extension point. Generic host post-Abort
or interrupted-terminal completion is not proof that MSSQL-owned descendants
have drained. Callers retaining the actual MSSQL object can use the local wait;
the concrete process runner also retains and drives this local boundary within
the existing lifecycle contracts. No private hook, dummy provider, blocking
graceful shutdown in Abort, or graceful-after-Abort substitute is used.

Server-free coverage sends actual public Epoch, same-role Role and equal-epoch
commands, checking completed operation receipts and the actual adapter's
predecessor behavior. Actual public Close remains pending behind a held adapter
callback and then reaches local drainage and application Close after release.
The host subsequently constructs a progress report, which fails with
`Unavailable`/Closed because the adapter is terminal. This RPC/report limitation
is preserved and asserted separately from successful local callback drainage;
tests do not fabricate terminal progress or assume a successful post-Abort
status report. Existing report/restart compatibility and the broader semantic
conformance blockers remain unchanged.

### Concrete Process Shutdown

`run_runtime_with_shutdown` establishes an independent lifecycle owner before
starting host work. It retains the actual MSSQL application, cooperatively
drives startup shutdown (including a late readiness result), and joins the
returned host completion. Dropping the calling future requests Abort and
shutdown but does not discard those handles or the remaining local cleanup.

The owner observes the application's actual Abort callback, including a
host-initiated Abort. It does not initiate a new graceful Close after Abort.
If Close was already pending, interruption drops that future before the owner
uses `wait_for_containment`; this avoids retaining an Open-attempt lock while
the new wait needs it. Successful concrete process cleanup requires actual
local drainage, not just successful fencing or generic host completion.

`KUBERIC_SHUTDOWN_DEADLINE_MS` remains a **per-stage** budget, not a total
deadline for normal startup or operation. Applicable stages are startup-stop
acknowledgement, host-stop acknowledgement, its post-Abort acknowledgement,
application Close, and local drainage. Each receives one configured budget;
repeated stop requests do not restart it. The conservative composed bounded
acknowledgement limit is five budgets, excluding executor scheduling, and
normally fewer because Abort skips Close.

If a budget expires before actual completion, the caller receives an explicit
failure rather than containment success. The retained owner continues the
remaining joins, preserving available trigger/runtime, Close, timeout and local
drainage errors. A timeout remains a failure even if late drainage succeeds.
An eventual failure without a consuming caller is reported to stderr with the
runtime's existing prefix; observing a test completion does not suppress that
production diagnostic. This is stage-level error composition, not an exhaustive
ledger of every callback or native SQL error.

Continuation requires the asynchronous runtime to remain alive. The executable
returns a failure exit code for bounded failure; subsequent runtime destruction
or OS process termination is not a promise of later joins or diagnostics.
Local drainage never certifies rollback of a submitted SQL effect, SQL Server
process termination, or generic public-host post-Abort containment.

Server-free concrete-runner regressions hold actual started filesystem workers,
an entered seed submission and a registered-but-unpublished Open. They verify
continued root exclusion and actual owner completion after release, terminal
refusal, durable Submitting preservation with one execution, and late-error
reporting after cancellation or a bounded response.

### Fixed-API Assessment

- **Graceful Close already has the required callback boundary.** The host awaits
  replicator Close, then application Close. MSSQL drains its actual owned work
  through those callbacks and retains join ownership if a waiter is cancelled.
  A new application-facing drain callback is not needed for this path.
- **Abort and interrupted-terminal evidence remain limited.** The host's retained
  terminal cleanup owner drains its built-in runtime and any attached internal
  witness; it cannot invoke MSSQL's inherent local wait. MSSQL's synchronous
  Abort fences admission and requests cancellation but does not join already
  started store/submission work. This is a remaining custom ownership boundary,
  not evidence that Kuberic must expose another hook. The concrete process owner
  now retains and awaits that drainage independently of its caller; a generic
  terminal receipt alone still cannot certify it.
- **Terminal reporting is a separate host implementation issue.** After a
  successful terminal program, the execute response still constructs a report.
  Both projection refresh and report projection query the registered custom
  replicator's current progress even though it is closed. MSSQL's Closed result
  is correct; fixing the reporting path must preserve truthful progress/history
  semantics without another trait method, invented progress, or relabeling a
  stale snapshot as a current observation. That fix is not implemented here.

The upstream evidence is the existing
[Close/Abort program](https://github.com/youyuanwu/kuberic/blob/main/kuberic-runtime/src/host/public_lifecycle_program.rs#L438-L479),
[terminal cleanup owner](https://github.com/youyuanwu/kuberic/blob/main/kuberic-runtime/src/host/public_lifecycle_terminal.rs#L41-L76),
[runtime callback binding](https://github.com/youyuanwu/kuberic/blob/main/kuberic-runtime/src/host/hosting.rs#L318-L347),
and [execute response reporting](https://github.com/youyuanwu/kuberic/blob/main/kuberic-runtime/src/host/service.rs#L914-L929).
The concrete MSSQL process wrapper bounds acknowledgement separately from actual
completion and retains the existing local wait after Abort or interrupted Close.
It does not expose that method to the generic host through a new interface.

## Adapter Boundary

The `kuberic` feature enables the service, custom replicator, and factory. The
default observer build remains independent of Kuberic.

Publishing an observed native AG sequence requires a new observation evaluated
at request time. The pre-admission zero/unknown-capability path does not certify
native data progress. Observed sequence publication requires:

- the observation is successful, within the configured maximum age, and not
  future-dated;
- the configured server and availability-group identities match;
- the complete observation bracket retained stable native identities;
- SQL Server 2025 Enterprise-feature Linux x86-64 with HADR is observed;
- the AG is non-basic, non-distributed, and `EXTERNAL`;
- exactly three synchronous-commit, EXTERNAL-failover, automatic-seeding
  replica definitions are present;
- required synchronized secondaries equals one;
- at most one managed database is present;
- the local replica has a stable PRIMARY or SECONDARY native role; and
- the configuration sequence is a valid nonnegative signed 64-bit value.

The general observer intentionally remains less restrictive about replica
count. It can describe partial and empty groups. The exact-three rule exists
only at the Kuberic progress-publication boundary.

Role callbacks never change SQL Server. Before configuration admission, Public
V1 role and epoch authority is recorded independently without native mutation.
After admission, PRIMARY must match native PRIMARY and active or idle secondary
must match native SECONDARY under a fresh observation. Successful role
validation always returns no client service address. The configured replication
address belongs only to `Replicator::open`; it is not a listener, routing
endpoint, or application address.

A strict topology expectation associates three stable Kuberic replica numbers
with expected SQL Server names and AG endpoint URLs. A complete remote-only
primary callback combines those associations with fresh native evidence to
converge and validate the primary-owned source database and AG ownership
foundation, then returns while secondary-local joins remain outstanding.
The local replica and idle or in-build targets stay out of that remote set.
`must_catchup` identifies the successor, and build targets use
invalid/unknown progress rather than synthesized comparable history.

Kuberic can deliver the bootstrap callback before remote sessions are known.
Remote-only foundation convergence does not admit or freeze a partial native
group. A later explicit full projection performs local joins, requires complete
native admission, and freezes the binding. Once frozen, later callbacks may
omit a remote session or address projection without clearing that binding,
while every supplied value must still match exactly. SQL Server process
incarnation is local evidence: each runtime freezes and revalidates its own
start identity, while the three-member validation proves all members
collectively.

`catch_up_capability` is reported under the Public V1 candidate as a fresh
configuration sequence equal to current progress after native topology
admission and publication of the stable matching local role. It does not
depend on remote transport-session readiness. Identity, completed-session,
address, epoch, role, health, synchronization, lineage, incarnation or
freshness drift fails closed.

This is a conservative current-only scalar: the adapter claims no earlier
retained configuration history. It is not a demonstrated retained data-history
floor and does not imply replica build, repair, catch-up, data-loss recovery,
lease, or failover support. The legacy arithmetic test for a member at the
current sequence, one behind, or two behind exercises a locally defined
comparison, not the pinned controller's actual retained-rejoin path. It is not
controller conformance coverage or evidence for a SQL data-repair decision;
the actual public-history consumers and report blockers are described above.

The adapter declares conservative immutable `ReplicatorCapabilities`;
catch-up-specific quorum and write authority remain unsupported. The released
Public V1 transport is not claimed to authenticate production mutation. SQL
Server HADR cannot carry Kuberic authorization tokens, so Stage 6 still needs
an authenticated replica-local adapter/control channel. The existing
challenge-bound test-host exchange remains licensed-validation-only and is not
that production transport.

Public V1 configuration and build DTO interpretation is localized under the
Kuberic adapter. That compatibility component produces local ownership,
member, descriptor, topology-binding, and build-target facts before SQL-native
classification. Native bootstrap classifiers do not consume Kuberic
replica-set DTOs. Application binding schema 2, seed schema 3, and SQL ownership
markers bind licensed effects to the exact Public V1 activation; predecessor
state rejects without migration.

The licensed Stage 3 current-configuration and seed-build callbacks are a
test-only SQL-native convergence seam. They are not end-to-end Public V1 Build
operations and do not prove host Build admission, receipts, or membership
semantics. Public V1 Build evidence here is ordinary/server-free negative
coverage: idle-build DTO validation and effect-free rejection with zero SQL
executor calls. The licensed seed-build translator currently requires its
target in the frozen active-secondary layout. Accepting an idle DTO in that
seam does not prove that the target is outside voting membership or that it
can complete a real public build before admission.

The native evidence model also exposes lineage- and field-scoped history
positions. Exact decimal values can be ordered only when both positions name
the same native field, database identity, and recovery fork. Missing positions
are unavailable; missing/different lineage and cross-field comparisons are
non-orderable. This remains native evidence, not generic Kuberic progress.

Server-free readiness tests cover cancellation and stale-completion fencing,
adapter-owned local/active-peer/build-target facts, role before primary
configuration, same-role epoch renewal, remote-active membership, invalid build
progress, and ordinary zero-mutation behavior.

The canonical version-1 baseline and version-2 candidate intake profiles and
CLI are documented in [Kuberic release intake](kuberic-release-intake.md).
They belong to the unpublished test crate and add no production mutation or
protocol state.

Unbound and ordinary adapters reject bootstrap mutation and build operations.
Only explicitly bootstrap-capable adapters accept initial current-configuration
effects and target builds. Previous/current transitions, catch-up configuration
and quorum, removal, data loss, destructive recovery, lease, switchover, and
failover callbacks remain explicit unsupported errors. Bootstrap configuration
success can mean deferred or source-foundation convergence; it must not be
interpreted as certification of the full public configuration postcondition.

## Ownership Model

Kuberic continues to own generic resource identity, durable authority, role
intent, and lifecycle sequencing. The SQL Server adapter owns native observation
and validates whether SQL evidence satisfies the requested callback.

Future authority-changing work should follow the ownership split demonstrated
by the reviewed operators:

- **DxOperator** provides the closest high-level precedent: the Kubernetes
  operator deploys resources, SQL Server performs AG replication, and a
  replica-local runtime owns tightly coupled cluster transitions.
- **SQL on Kubernetes Operator** demonstrates repeated SQL-state observation,
  retry-friendly object creation, and waiting for native postconditions. Kuberic
  must not copy its controller-side `sqlcmd` protocol, unreachable-primary
  inference, volatile retry state, or `preStop` failover.
- **KubeSQLServer Operator** is useful for straightforward Secret, ConfigMap,
  StatefulSet, Service, and ensure-exists provisioning patterns, but it is
  single-replica and provides no HA precedent.

Kuberic's replica-local runtime now owns the bounded bootstrap/join/initial-seed
slice under durable Kuberic authority. The configured primary owns source
database and AG convergence, each secondary owns only its local join, and one
authorized target build wins a group-level seed dispatch.

Kuberic's command/effect/build receipts and exact SQL Server postconditions
remain the primary durable evidence. The application persists only the
pre-association `Intent`/`Submitting`/`Accepted`/`Ambiguous`/
`FailedRetryable` handshake under its existing application root; exact source
association plus the exact terminal SQL marker removes that temporary record.
This bounded file/SQL marker exists only to reconcile the external-effect
ambiguity around `ADD DATABASE`; Kuberic's stateless default-replicator design
does not require deleting that reconciliation seam. Predecessor seed schemas 1
and 2 reject without migration under fresh Public V1 activation. They are not
reinterpreted as `LegacyAmbiguous` and cannot redispatch.

The owned source database retains one matching singleton dispatch row with a
derived operation token, exact AG/database identities, and SQL-owned phase. A
group-scope SQL application lock distinguishes an active/ambiguous submission
from a session that ended before any native effect. The exact winning
build/storage operation token remains temporary; a separate stable
activation/topology token identifies the v2 terminal marker so both configured
targets can reconcile the same completed group dispatch. An accepted row or exact
association evidence establishes one bounded terminal `master` extended
property before file compaction. Its exact `v1:<operation-token>` value binds
resource/configuration, AG GUID, source identity/lineage and SQL incarnation,
and both authorized targets. Healthy replay is native-only; later association
loss or database/AG recreation is terminal conflict/unsafe and cannot create a
new intent or issue another `ADD DATABASE`. Proven pre-effect or definite SQL
rejection remains retryable instead of degrading into a later deadline. The
native bootstrap path also retains narrow database and AG creation
handshakes. A deterministic scoped DDL trigger is persisted before each
irreversible create; only the exact token-bearing typed statement can
atomically write the generated creation receipt. The source database then
retains resource/configuration ownership and the frozen AG GUID. Replay, join,
and seed reject missing or mismatched markers, including an exact-profile
foreign same-name resource or a later same-name AG with a new GUID. This
follows PostgreSQL v2's separation of Kuberic authority, application-native
evidence and database state without copying PostgreSQL recovery mechanics or
creating a generic operation ledger.

Primary-side remote synchronization and seeding rows now advance dispatch
progress only. Target build completion requires a fresh, challenge-bound direct
observation from the target host. The explicit licensed test-host exchange
authorizes the exact resource/configuration/build, target Kuberic identity and
current process session; the callback independently revalidates native replica,
AG/group-database identity, family/recovery-fork lineage,
synchronized/healthy/online/not-suspended state, and the fixture-frozen target
SQL start. Old sessions, superseded builds, replayed reports and a restarted
target SQL process cannot satisfy the callback. The exchange is confined to bootstrap testing construction and does not
authenticate or enable ordinary runtime mutation.

Source creation, full-recovery configuration, ownership, marker schema, full
backup, log-chain establishment, backup verification, AG intent/create/GUID
binding, each local join, each local seeding grant, and its narrow SQL-owned
grant-completion marker are independently classified. Read-only `msdb` history
and the marker joined to the current AG GUID prevent completed work from being
repeated; a retry dispatches only the next missing safe step.

No new TLS certificate provisioning, production mutation configuration or
plaintext bypass was added. Licensed validation reuses fixture-owned endpoint
security and administrative credentials. Destructive reseed remains deferred
until verified target fencing exists.

## Live Validation Paths

### One-replica observer and CLI fixture

`just ci` first runs one Rust-owned digest-pinned SQL Server 2025 Enterprise
Developer container. It creates no AG, endpoint or database. Separate allowed,
partially privileged and invalid-CA configurations prove absent-group behavior,
permission-denied classification, TLS rejection and fresh production CLI
provenance. This fixture does not publish Kuberic progress.

Its schema-versioned journal records exact intent and immutable identity for
the container, network, files and directories. Cleanup removes only verified
owned resources and refuses legacy Python, foreign, replaced or attached state.

### Three-member native and Kuberic happy path

The dedicated ignored path starts three real containers from:

`mcr.microsoft.com/mssql/server@sha256:2b5b581621126574f3d1f75e78d3eebe8d05aedb59ad0cfdf9aa42cb0634d726`

The SQL Server image reports version `17.0.5005.3`. The fixture requires a
clearly test-only `FixtureConfig` constructor that creates an affirmative
`SqlServerEulaAcknowledgement` in code and passes exactly `ACCEPT_EULA=Y` to
each owned container. Repository Cargo commands force that variable through
`.cargo/config.toml` as repository-wide authorization only for the owned
licensed fixtures; it does not authorize external or borrowed instances.
Production still has no SQL Server launcher, never accepts an EULA, and never
deploys SQL Server. The strict file-backed constructor and negative tests
remain available for a future launcher contract.

Fixture-only preparation creates and proves:

- one certificate-authenticated, started HADR endpoint on every member;
- peer certificate users/logins with exact endpoint `CONNECT`.

Explicit bootstrap-capable hosts then create and prove:

- one three-replica `CLUSTER_TYPE = EXTERNAL` AG;
- synchronous commit, EXTERNAL failover metadata, automatic seeding, and
  readable secondary connections for every replica;
- required synchronized secondaries equal to one;
- both secondary joins and `GRANT CREATE ANY DATABASE`;
- one full-recovery database;
- two completed successful automatic-seeding operations;
- exactly one native primary and two synchronized healthy secondaries; and
- one marker committed on the native primary and read directly from all three
  members.

Production observation code supplies the proof. Each member is observed
directly, and the three observations must agree on AG identity and configuration
sequence, replica identities/profile, group-database identity, family GUID,
recovery fork, roles, synchronization health, and seeding history. A SQL process
restart, container replacement, AG/database recreation, role drift, stale
sample, sequence mismatch, suspended database, or unhealthy synchronization
invalidates the binding.

The licensed path creates three public `ReplicaHost` applications for one shared
Kuberic resource before the database and AG exist, initializes them through
public control RPCs, and admits the exact current configuration and build
authority. Reports are bracketed by fresh direct observations and must show:

- one shared resource and current configuration with no previous
  configuration;
- three exact identities and process sessions;
- contained observe-only report role `None`;
- progress role `Recovery`, with `first = 0` and `last` equal to the
  corresponding fresh native AG sequence;
- read and write access both `NotPrimary`;
- healthy initialized durable state; and
- no client service address.

These contained Public V1 report roles are distinct from SQL-native topology:
direct SQL observation still proves exactly one native primary and two native
secondaries. Recovery `[0, native sequence]` is preserved host compatibility
projection, not proof of a public data-history range or retained floor. Native
marker/lineage/synchronization observations remain separate evidence.
No SQL external write lease is acquired. The replicated marker is
written only by the fixture administrator against the directly observed native
primary.

The licensed same-root restart test stops all three public hosts while retaining
SQL Server and durable roots. It proves contained existing-state Open, fresh
replacement sessions, stale-session rejection, native seed-record compaction,
zero redispatch, exact contained report convergence, and marker continuity.
SQL Server processes are not restarted. It is not Public V1 peer-session or
lifecycle renewal, does not replay an end-to-end Public V1 Build operation, and
does not restore serving access.

The ignored live tests additionally prove handled-signal and SIGKILL recovery.
The fault recipe uses test-only callback seams after each independently
classified database/backup/AG/join/grant statement has executed through normal
TDS, then replaces the hosts and reobserves native state. Seed submission and
callback-publication windows use explicit test-only probes/gates around real
callbacks, including before request submission, after submission, after SQL
acceptance with a lost reply, and after definite SQL rejection.

The conflict recipe has three isolated SQL-native cases observed through the
normal TDS path: convergence from a source database that inherits `SIMPLE`
recovery, refusal of an unowned same-name database, and refusal of an unowned
exact-profile same-name AG. Its remaining AG/replica/endpoint/database/lineage
cases are decoded-observation injections consumed by production validators; its
SQL-incarnation validator uses a real process restart plus a test gate.
Separate terminal-seed cases perform real post-compaction association removal
and AG recreation with zero redispatch. Target-local evidence cases use real
target SQL process restarts and test-injected lineage mismatches at
pre-association, in-flight-seeding and post-native/pre-reply windows. Every
scenario performs exact cleanup.

The required GitHub Actions check `Shared CI validation` runs `just ci`,
including licensed one-replica, three-replica, same-root restart, and isolated
lease/fence validation, and always runs `just cleanup`. Its GitHub Actions job log is the
run evidence; no test logs or evidence artifacts are committed. The dependency
source and version remain defined only by the root `Cargo.toml`.

The required `cancel_before_remove` gate has two separate evidence sources.
Kuberic's release conformance catalog gate KSF-06 proves host-owned
cancellation/drain ordering. The MSSQL readiness test separately proves
effect-free remove rejection after a callback task has settled; it does not
itself claim host ownership.

## Usage

The standalone observer requires no Kuberic dependency:

```bash
cargo build --locked -p kuberic-mssql --bin sqlserver-observer
```

Build or test the adapter through the feature:

```bash
cargo test --locked -p kuberic-mssql-tests --test kuberic_contract
cargo build --locked -p kuberic-mssql --features kuberic \
  --bin kuberic-mssql-runtime
```

Run only the server-free future-readiness group or validate one complete intake
record:

```bash
just test-kuberic-readiness
just validate-kuberic-release-intake /absolute/path/to/intake.json
```

Run the complete server-free and Rust-owned live gates:

```bash
just ci
```

The required exact PR-head GitHub Actions job `Shared CI validation` runs this
same command and always runs `just cleanup`. Licensed gates therefore execute
in CI; their GitHub Actions job log is the evidence, not a checked-in log or
artifact.

Run or clean up the one-member observer/CLI path:

```bash
just test-live-one-replica [root]
just cleanup-live-one-replica [root]
```

Live tests remain ignored for ordinary Cargo invocation.

Run the real three-member path with an optional fixture root:

```bash
just test-live-three-replica [root]
just test-live-three-replica-restart [root]
just test-live-three-replica-faults [root]
just test-live-three-replica-conflicts [root]
just test-live-three-replica-target-evidence [root]
```

Recover the default or a selected journaled root:

```bash
just cleanup-live-three-replica [root]
```

Exercise handled SIGTERM, recovery, retry, and idempotent cleanup:

```bash
just test-live-three-replica-signal [root]
```

The three-replica happy path, same-root restart, and isolated lease/fence
experiments run after the one-replica observer/CLI validation in `just ci`.
Signal, recovery, fault and conflict recipes remain explicit. The shipped
acknowledgement example still contains
`accepted: false` as documentation for the production/file-backed contract, but
tests do not require it.

The [lease/fence foundation](design.md#lease-and-fence-foundation) adds pure
authority contracts and separate licensed SQL expiry/restart observations.
These are not a production renewal actor or a Kuberic failover admission proof.

## Deferred Work

This integration does not yet provide:

- production SQL Server process ownership or restart containment;
- a supported production launcher/transport for managed-database, AG, join or
  initial-seed convergence;
- production endpoint certificate provisioning or rotation;
- destructive reseed before verified target fencing;
- a conforming role-specific public progress and retained-history model;
- generic-host certification of custom-descendant containment after Abort or an
  interrupted terminal operation, and successful terminal reporting;
- end-to-end public nonvoting build admission and database-progress catch-up
  proofs;
- application read/write access fencing;
- production external write-lease renewal and expiry admission proofs;
- sequence quorum collection across exact replica sessions;
- planned switchover, automatic or forced failover, or data-loss recovery;
- Kubernetes Pod, PVC, Secret, Service, listener, routing, or installation
  convergence;
- a Kuberic sidecar;
- Kubernetes fault testing; and
- multi-member failure, replacement, network-partition, or old-primary
  recovery testing, including lease expiry under those faults.

Those stages require separate design and safety review.

The candidate validates contained same-root public-host reopen under real
licensed observation timing. That evidence covers host replacement and native
reconciliation only; it does not prove peer-session/lifecycle renewal, restore
serving access, or cover SQL Server process ownership/restart, Kubernetes
lifecycle, native member replacement, or failover.

## References

- [SQL Server AG catalog view](https://learn.microsoft.com/en-us/sql/relational-databases/system-catalog-views/sys-availability-groups-transact-sql)
- [SQL Server AG replica-state DMV](https://learn.microsoft.com/en-us/sql/relational-databases/system-dynamic-management-objects/sys-dm-hadr-database-replica-states-transact-sql)
- [Microsoft SQL Server HA resource agents](https://github.com/microsoft/mssql-server-ha)
- [Kuberic PostgreSQL design](https://github.com/youyuanwu/kuberic/blob/main/docs/features/postgres/design.md)
- [Kuberic operator comparison](https://github.com/youyuanwu/kuberic/blob/main/docs/background/operator-reconciliation-comparison.md)
