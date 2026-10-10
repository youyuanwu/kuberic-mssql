# Kuberic SQL Server Progress Integration

## Purpose

The optional Kuberic integration exposes SQL Server availability-group progress
through Kuberic's fixed Service Fabric-compatible `Replicator` contract. It does
not change Kuberic's API and does not convert SQL Server database log positions
into a generic scalar.

Ordinary construction is deliberately observe-only. It can open as a Kuberic
application, validate a requested native role, and report current progress. An
explicit bootstrap-capable construction used by licensed validation can also
create the managed source database and AG, join local secondaries, and execute
one build-authorized automatic-seeding dispatch. Neither construction can
remove/reseed replicas, grant write access, renew a lease, switch over, fail
over, or handle data loss.

## Two Progress Domains

SQL Server exposes two different categories of progress.

### Availability-group configuration sequence

`sys.availability_groups.sequence_number` is a nonnegative SQL `bigint`. SQL
Server increments it when the primary updates availability-group configuration.
The adapter represents it as `ConfigurationSequence` and returns its exact
signed 64-bit value from Kuberic `current_progress`.

This is configuration and promotion-authority progress. It is not evidence that
an arbitrary customer transaction has been hardened or redone.

Microsoft's public `mssql-server-ha` agent uses the same sequence:

- synchronous-commit and configuration-only replicas publish the local sequence;
- other availability modes publish zero;
- a selected candidate below the maximum observed sequence is rejected;
- sequence zero is not promotable;
- a majority of required sequence reports must be available;
- promotion then renews the external write lease and invokes SQL Server's
  external-cluster failover path.

The current adapter implements only local sequence publication. It does not
implement the quorum, lease, or promotion steps.

### Database-native positions

`sys.dm_hadr_database_replica_states` exposes hardened, redone, and committed
positions as `numeric(25,0)`. Hardened and related values can be padded log-block
identifiers rather than record LSNs; redone and committed positions have
different meanings. The values are scoped to a database identity and recovery
fork and may exceed the signed 64-bit domain.

The observer retains these fields independently as exact decimal values:

- `hardened_block`;
- `redone_record`;
- `committed_record`.

They are never cast, truncated, saturated, hashed, or substituted for Kuberic
progress. Future build, catch-up, handoff, and recovery operations must validate
the appropriate native field and lineage explicitly.

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

Role callbacks validate observation rather than changing SQL Server. PRIMARY
must match native PRIMARY; active or idle secondary must match native SECONDARY.
The initial no-role state may be observed before Kuberic authority admission.
Successful role validation always returns no client service address. The
configured replication address belongs only to `Replicator::open`; it is not a
listener, routing endpoint, or application address.

A strict topology expectation associates three stable Kuberic replica numbers
with expected SQL Server names and AG endpoint URLs. The first current
configuration callback combines those associations with fresh native evidence
and the callback's full identities, roles, process sessions and replication
addresses.

Kuberic can deliver the bootstrap callback before remote sessions are known.
Empty remote descriptions may be completed in one direction; after completion,
replay is value-identical. SQL Server process incarnation is local evidence:
each runtime freezes and revalidates its own start identity, while the
three-member validation proves all members collectively.

`catch_up_capability` is reported as a fresh configuration sequence equal to
current progress after native topology admission. It does not depend on remote
transport-session readiness. Identity, completed-session, address, epoch, role,
health, synchronization, lineage, incarnation or freshness drift fails closed.

This is a conservative current-only capability: the adapter claims no earlier
retained configuration history. It is not a database LSN and does not imply
replica build, repair, catch-up, data-loss recovery, lease, or failover support.
The locked controller comparison treats a member at the current sequence or one
behind as not requiring full repair, and a member two or more behind as
requiring it. Contract tests preserve that exact boundary.

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
association plus the exact terminal SQL marker removes that temporary record. A
schema-v1 `Dispatched` record is migrated conservatively to `Ambiguous`.

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
target SQL process cannot satisfy the callback. The exchange is confined to
bootstrap testing construction and leaves the pinned Kuberic protocol and
ordinary runtime unchanged.

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
- one Kuberic primary and two active secondaries matching native roles;
- current progress and catch-up capability equal to the corresponding fresh
  native configuration sequence;
- healthy initialized durable state; and
- fenced write access: stable primary and active-secondary roles may report
  read access as granted, but write is never granted and no client service
  address is published.

Role is not write authority. No SQL external write lease is acquired. The
replicated marker is written only by the fixture administrator against the
directly observed native primary.

The licensed same-root restart test stops all three public hosts while retaining
SQL Server and durable roots. It covers every retained seed handshake phase,
fresh replacement sessions, stale-session rejection, restart-driven
compaction, zero duplicate `ADD DATABASE`, exact report convergence and marker
continuity. SQL Server processes are not restarted.

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

The workspace tracks Kuberic `main`, with `Cargo.lock` recording the exact
revision used by locked builds. The current pin
`09ce0a7ba9fdd9d6dcd207606fd0c9496a58c47f` is the merge commit of
[youyuanwu/kuberic#133](https://github.com/youyuanwu/kuberic/pull/133), which
fixes custom-authority recovery and serializes peer replacement across
same-root host replacement. It also contains the listener-adoption API from
[youyuanwu/kuberic#127](https://github.com/youyuanwu/kuberic/pull/127).

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

The current Kuberic pin validates durable same-root public-host restart under
real licensed observation timing. That evidence covers Kuberic host replacement
only; SQL Server process ownership/restart, Kubernetes lifecycle, native member
replacement, and failover remain deferred above.

## References

- [SQL Server AG catalog view](https://learn.microsoft.com/en-us/sql/relational-databases/system-catalog-views/sys-availability-groups-transact-sql)
- [SQL Server AG replica-state DMV](https://learn.microsoft.com/en-us/sql/relational-databases/system-dynamic-management-objects/sys-dm-hadr-database-replica-states-transact-sql)
- [Microsoft SQL Server HA resource agents](https://github.com/microsoft/mssql-server-ha)
- [Kuberic PostgreSQL design](https://github.com/youyuanwu/kuberic/blob/main/docs/features/postgres/design.md)
- [Kuberic operator comparison](https://github.com/youyuanwu/kuberic/blob/main/docs/background/operator-reconciliation-comparison.md)
