# Kuberic SQL Server Progress Integration

## Purpose

The optional Kuberic integration implements the fresh-state Public V1
replacement `Replicator` contract. It publishes only exact validated SQL Server
availability-group sequence progress; Kuberic lifecycle epochs and SQL-native
database positions are independent authority domains.

Ordinary construction is deliberately observe-only. It can open as a Kuberic
application, validate a requested native role, and report current progress. An
explicit bootstrap-capable construction used by licensed validation can also
create the managed source database and AG, join local secondaries, and execute
one build-authorized automatic-seeding dispatch. Neither construction can
remove/reseed replicas, grant write access, renew a lease, switch over, fail
over, or handle data loss.

This document records the candidate implementation boundary. The exact
candidate lockfile remains feature-branch evaluation until release intake is
`ready_to_pin`; no new production mutation support is announced.

## Two Progress Domains

SQL Server exposes two different categories of progress.

### Availability-group configuration sequence

`sys.availability_groups.sequence_number` is a nonnegative SQL `bigint`. SQL
Server increments it when the primary updates availability-group configuration.
The adapter represents it as `ConfigurationSequence`. Before configuration
admission, `current_progress` is zero and `catch_up_capability` is
`INVALID_LSN`. After exact AG binding, both callbacks return the current exact
nonnegative signed 64-bit sequence. A zero sequence remains valid empty
authority and is never promoted or fabricated. Configuration epoch is never a
fallback progress value.

This is the candidate Public V1 scalar-history behavior: configuration and
promotion-authority progress. It is not evidence that an arbitrary customer
transaction has been hardened or redone.

Microsoft's public `mssql-server-ha` agent uses the same sequence:

- synchronous-commit and configuration-only replicas publish the local sequence;
- other availability modes publish zero;
- a selected candidate below the maximum observed sequence is rejected;
- sequence zero is not promotable;
- a majority of required sequence reports must be available;
- promotion then renews the external write lease and invokes SQL Server's
  external-cluster failover path.

The candidate adapter implements only local AG-sequence publication. It does
not implement the quorum, lease, fencing, or promotion steps required for
controller-driven role movement.

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
for Kuberic progress or Kuberic's signed 64-bit LSN slot. Future build, catch-up, handoff, and recovery operations must validate the
appropriate native field and lineage explicitly. Database-native values remain
outside Public V1's scalar history.

## Adapter Boundary

The `kuberic` feature enables the service, custom replicator, and factory. The
default observer build remains independent of Kuberic.

Every Kuberic progress request performs a new observation and evaluates it at
request time. Progress is returned only when:

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

This is a conservative current-only capability: the adapter claims no earlier
retained configuration history. It is not a database LSN and does not imply
replica build, repair, catch-up, data-loss recovery, lease, or failover support.
The locked controller comparison treats a member at the current sequence or one
behind as not requiring full repair, and a member two or more behind as
requiring it. Contract tests preserve that exact boundary.

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
executor calls.

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
failover callbacks remain explicit unsupported errors. No callback returns a
dummy success or fabricated value.

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
from a session that ended before any native effect. An accepted row or exact
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
`SqlServerEulaAcknowledgement` in code and injects exactly `ACCEPT_EULA=Y`.
It consumes no acknowledgement file or environment variable, and repeated
revalidation is idempotent. Production and future launchers remain responsible
for explicit acknowledgement; the strict file-backed constructor and negative
tests remain available for that purpose.

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
secondaries. No SQL external write lease is acquired. The replicated marker is
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

The feature branch evaluates exact Public V1 candidate
`ceec5887ad97d71222117610428ce715b782777e`. Release-intake records retain
`09ce0a7ba9fdd9d6dcd207606fd0c9496a58c47f` as the accepted current identity
while comparing the reviewed candidate. Exact implementation commit
`bd819a0d866350476a5d04b2306b529dade53c9a` (tree
`4038d31664ae47458d1d80c1e9c001fb022221f6`) passed licensed one-replica,
three-replica, and same-root restart validation. The checked-in evidence
summary and hashed logs retain those runs, and the candidate outcome is
`ready_to_pin`.

The required `cancel_before_remove` gate has two separate evidence sources.
Kuberic candidate exact-head CI run `38844709880` and its KSF-06 catalog prove
host-owned cancellation/drain ordering. The MSSQL readiness test separately
proves effect-free remove rejection after a callback task has settled; it does
not itself claim host ownership.

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

The three-replica happy path and same-root restart run after the one-replica
observer/CLI validation in `just ci`. Signal, recovery, fault and conflict
recipes remain explicit. The shipped acknowledgement example still contains
`accepted: false` as documentation for the production/file-backed contract, but
tests do not require it.

## Deferred Work

This integration does not yet provide:

- SQL Server process ownership or restart containment;
- a supported production launcher/transport for managed-database, AG, join or
  initial-seed convergence;
- production endpoint certificate provisioning or rotation;
- destructive reseed before verified target fencing;
- database-progress build and catch-up proofs;
- application read/write access fencing;
- external write-lease renewal and expiry evidence;
- sequence quorum collection across exact replica sessions;
- planned switchover, automatic or forced failover, or data-loss recovery;
- Kubernetes Pod, PVC, Secret, Service, listener, routing, or installation
  convergence;
- a Kuberic sidecar;
- Kubernetes fault testing; and
- multi-member failure, replacement, network-partition, old-primary, or
  write-lease-expiry testing.

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
