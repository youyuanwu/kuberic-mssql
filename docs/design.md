# SQL Server External-Replication Adapter

> **Status:** Safety contract, observe-only production runtime, and explicit
> licensed-test bootstrap convergence. Read-only TDS queries are supported;
> bootstrap-capable test hosts can create the managed database/AG, join
> secondaries and seed once. SQL Server process management and failover effects
> are not enabled.
>
> **Support level:** Experimental. This is not a Microsoft-supported Kubernetes
> high-availability solution.

## Decision

Kuberic will treat SQL Server as an external replication engine. SQL Server
owns log transport, hardening, redo, and automatic seeding. Kuberic may
eventually own the distributed intent and reconciliation around those native
operations.

The first supported target is deliberately narrow:

| Area | Initial contract |
|---|---|
| Engine | SQL Server 2025, major version 17; older majors are rejected |
| Platform | Linux x86-64 containers, or native Ubuntu 24.04 with SQL Server 2025 CU1+ |
| Edition | Enterprise Developer for tests; Enterprise for a future production profile |
| Availability group | Full AG, one managed user database |
| AG naming | `EXTERNAL` limit of 64 characters, enforced as 64 UTF-16 code units |
| Cluster type | `EXTERNAL` |
| Topology | Three synchronous data replicas |
| Failover mode | `EXTERNAL` |
| Seeding | Automatic |
| Commit protection | `REQUIRED_SYNCHRONIZED_SECONDARIES_TO_COMMIT = 1` |
| Write fencing | SQL Server external write lease plus a verified fence receipt |
| Default behavior | Observe only |

Images must be pinned by digest; native `mssql-server` packages must have an exact
build-revision version and a recorded archive SHA-256. `EngineArtifact` keeps
these provisioning profiles distinct; a native fixture does not invent an image
reference. Accepting the SQL Server EULA remains an explicit deployer action and
is never implied by the adapter. Native package installation, edition setup,
HADR/TLS configuration and credential provisioning remain external operations.
The runtime neither installs packages nor manages the host service.

SQL Server 2025 separates Enterprise Developer and Standard Developer editions.
Only the Enterprise-feature profile is accepted: EngineEdition 3 and an
Enterprise Developer or Enterprise display name. Standard Developer, Standard,
Express and Evaluation remain unsupported.

The AG name limit is narrower than the general `sysname` limit.
`CREATE AVAILABILITY GROUP` documents 128 characters for `cluster_type = WSFC`
but 64 for `cluster_type = NONE` and `EXTERNAL`, and the engine enforces it as
error 19544. Before SQL Server 2022 CU23 an over-length name raised an assertion
failure rather than that error, so client-side rejection avoids a crash-class
failure on older builds. Microsoft states the bound in "characters" without
naming a unit; the adapter counts UTF-16 code units, which is never more
permissive than counting scalar values.

`CLUSTER_TYPE = NONE` is intentionally excluded. Microsoft documents it as a
read-scale configuration without high availability. It allows manual T-SQL
failover but has no cluster-manager arbitration, so using it would not
demonstrate the safe failover requested by youyuanwu/kuberic#80. Windows/WSFC,
Basic AGs,
distributed AGs, asynchronous disaster recovery, configuration-only replicas,
multiple managed databases, and mixed major versions are also out of scope for
the initial profile.

## Current Implementation

This repository implements the safety contract, a standalone observe-only
runtime, and an explicit bootstrap-capable host construction used by licensed
validation:

- validation of the supported SQL Server profile;
- immutable container/native-package artifacts and Kubernetes Secret references;
- native AG, database, replica, and incarnation identities;
- separate desired replica identities for bootstrap, before SQL Server generates
  native replica GUIDs;
- exact SQL Server `numeric(25,0)` database progress values without narrowing
  them to Kuberic progress;
- a dedicated signed `ConfigurationSequence` and optional observe-only Kuberic
  adapter that publishes validated AG configuration authority as current
  progress;
- observations that distinguish present, absent, stale, and failed evidence;
- versioned operation envelopes with canonical SHA-256 input signatures;
- explicit destructive approvals and fence references;
- duplicate-operation and operation-ID-reuse classification;
- verified-TLS TDS execution with mounted observation credentials;
- capability-checked, identity-bracketed native DMV snapshots; and
- a freshness-aware monitor and observe-only JSON CLI;
- closed source-database, AG-create, local-join and database-add effects derived
  from validated callback/operation data;
- operation-specific absent, incomplete, complete, conflicting and unsafe
  native classifiers; and
- a source-database-owned AG handshake that retains resource/configuration/name
  intent before creation and then freezes SQL Server's generated AG GUID; and
- bounded seed-dispatch evidence for the pre-association ambiguity only.

The production crate connects to already provisioned SQL Server instances. It
does not create an AG, seed a database, renew a write lease, change a native
role, or integrate with either Kuberic operator through its ordinary
configuration. The optional Kuberic runtime adapter remains observe-only unless
an explicit bootstrap capability and closed mutation executor are injected by
the licensed test harness. That construction lets the configured primary own
source database and AG convergence, each secondary own only its local join, and
Kuberic build authority own one group-level `ADD DATABASE` dispatch. A bound
healthy topology reports `catch_up_capability` equal to freshly validated
current configuration progress; it does not claim earlier retained history,
general repair, destructive recovery, lease, or failover capability.

`SqlServerEulaAcknowledgement` is the shared semantic acknowledgement contract:
it can be constructed only from an explicit affirmative value. The production
crate deliberately provides no container or Pod launcher and never translates
that value into `ACCEPT_EULA=Y`. Only the Rust-owned one- and three-replica fixtures use a clearly test-only
constructor to create the affirmative semantic value in code and inject exactly
`ACCEPT_EULA=Y`; sourceless revalidation is idempotent.
The strict file-backed constructor and negative contract tests remain for future
production launchers.

The licensed one-replica fixture owns one container, TLS assets and test
principals solely to validate absence, permission denial, TLS rejection and CLI
output. It creates no AG, endpoint or database. The licensed three-replica
fixture owns containers, HADR enablement, certificate-authenticated endpoints,
peer authentication, credentials and cleanup. Explicit bootstrap-capable hosts
create the source database and AG, join local secondaries and seed the database
through Kuberic build authority. Ordinary production construction receives no
administrative SQL or container authority.

The crate remained independent when the classic runtime was removed. The
`sqlserver-observer` binary
and runtime remain independently testable without the replication runtime.
See the [observe-only runtime guide](../README.md#configuration) for configuration,
permissions, output semantics, and tests.

Two capabilities named in this design are defined but not yet enforceable, and
each is assigned to a later stage rather than half-built now:

- **Encoding and decoding.** The canonical writer produces the bytes that the
  input signature covers, but there is no operation-envelope reader or `serde`
  support, so an envelope cannot yet be sent between processes. Stage 3 did not
  select operation envelopes for its callback boundary, so no decoder was
  added.
  Decoding does not by itself require a separate application result journal:
  each operation must first be mapped to Kuberic's durable command/effect
  receipts and an exact native SQL postcondition. Application-owned persistence
  is added only for a crash or replay ambiguity that those two sources cannot
  reconstruct safely. `OperationRequest::from_decoded_parts` exists as the seam
  that the decoder will use, and is the only path on which the contract-version
  check is reachable.
- **Proof validity and issuance.** `DestructiveApproval` and `FenceReference`
  currently bind a receipt to an exact operation ID and canonical input
  signature. They carry no issue or expiry time and no issuer verification, so
  the type system cannot yet express an expired or forged proof, and any caller
  able to construct an envelope can construct a receipt for it. Stage 4 owns
  receipt lifetime and issuer authentication, alongside the external lease
  handling that gives those fields their meaning. Until then, no code path may
  treat the presence of a receipt as evidence that fencing actually occurred.

## Why the PostgreSQL Adapter Is Not a Drop-in Template

The retired classic PostgreSQL adapter had a placeholder binary, in-memory
access flags and last-known scalar progress. Those limitations do not describe
the [implemented PostgreSQL v2 contract](https://github.com/youyuanwu/kuberic/blob/main/docs/features/postgres/design.md):
its executable
uses the v2 host, completed access fences drain the owned PostgreSQL process,
and recovery validates fresh, exact-session native lineage and replay evidence.
Workstream 4 is validated with host-local subprocesses; images, deployment and
distribution remain deferred, without PostgreSQL KinD coverage.

That control-plane/data-plane boundary is useful, but PostgreSQL's WAL,
timeline and process-fencing mechanisms are not SQL Server AG protocols.
PostgreSQL LSNs fit a 64-bit scalar; SQL Server DMV positions are
`numeric(25,0)` with field-specific meanings. SQL Server still needs its own
native identity, progress, lease and role evidence, rather than copying
PostgreSQL recovery or relying on generic scalar election ordering.

## Native HA Boundary

For `CLUSTER_TYPE = EXTERNAL`, normal failover belongs to the external cluster
manager. Kuberic must not issue an isolated `ALTER AVAILABILITY GROUP ...
FAILOVER` and then infer success from an RPC response.

Microsoft publishes the `mssql-server-ha` resource-agent source specifically so
other clustering systems can follow the SQL Server protocol. Its AG helper:

- monitors the SQL Server instance and AG;
- reads native configuration sequence numbers;
- computes promotion eligibility;
- promotes and demotes through the external-cluster session contract; and
- maintains the external write lease.

The mutation implementation should invoke a pinned, tested helper build or
faithfully implement that published protocol. Reimplementing only its visible
T-SQL statements is not sufficient because sequence-number arbitration,
required synchronous secondaries, lease renewal, health thresholds, and action
ordering jointly establish safety.

The external write lease is the database-native write fence for direct TDS
clients. Kuberic may renew it only while the accepted primary still has valid
cluster authority. Before promotion, the planner must prove that the old
primary's lease expired or present a verified infrastructure fence that
prevents that exact incarnation from writing or restarting. A Service update,
readiness failure, Pod deletion request, Kubernetes Lease, or in-memory
`PartitionState` flag is not such proof.

## Observation Contract

An observation snapshot must retain provenance and freshness and must
distinguish:

- a catalog object that is absent;
- a replica that is unreachable;
- permission-denied or malformed output;
- a stale last-known observation; and
- an unsupported engine or protocol state.

Every observation records when the attempt was made, including failed attempts,
so that a caller can reason about how long evidence has been unavailable. A
failed observation is never fresh, and an observation stamped in the future is
also reported as not fresh: clock skew must degrade toward refusing to act, not
toward trusting an unverifiable sample.

The observer will read and cross-check at least:

- `SERVERPROPERTY(...)` and `@@SERVERNAME`;
- `sys.availability_groups`;
- `sys.availability_replicas`;
- `sys.dm_hadr_availability_replica_states`;
- `sys.availability_databases_cluster`;
- `sys.dm_hadr_database_replica_states`;
- `sys.databases`; and
- automatic and physical seeding DMVs.

Replica and database rows are joined by their native GUIDs. Local observations
must not be confused with a primary's potentially stale report about a remote
replica. The AG identity and local role are sampled again around a multi-query
observation; a SQL transaction does not make the DMVs a globally atomic
snapshot.

SQL Server exposes multiple progress concepts. Hardened-block, redone-record,
and committed-record positions remain distinct exact decimal values. They
cannot be truncated into the former classic runtime's scalar `i64`, compared across
different databases, or compared across incompatible recovery histories.
Every progress-bearing transition therefore includes both the database GUID
and recovery-fork GUID.

## Command Contract

Each SQL Server operation is bound to:

- contract version;
- Kuberic resource identity;
- operation identity;
- source configuration identity;
- source and target epochs;
- native AG/database/replica identities;
- target Pod incarnation;
- database recovery lineage for progress-bearing transitions;
- all operation-specific safety inputs;
- destructive authorization, when required; and
- a verified fence reference, when required.

Canonical input encoding uses fixed field order, explicit variant and option
tags, big-endian integers, length-prefixed UTF-8 strings, and sorted set-like
collections. The input signature is SHA-256 over that encoding. It is an
idempotency and conflict-detection value, not authentication.

A second digest, the effect signature, covers the same encoding minus the
operation identity under a distinct domain-separation prefix. It answers a
different question: whether two requests ask SQL Server for the same thing. The
input signature cannot answer it, because the operation ID is part of the input
it covers.

Approval and fence receipts are evidence about a request, not request input.
Each carries the exact operation ID and canonical input signature that it
authorizes. They are excluded from that signature to avoid a circular encoding
and to permit replacement of an expired proof without changing the requested
database effect.

An exact duplicate returns the retained result. Reusing an operation ID with
different canonical input is rejected. A planner that crashes after dispatch but
before persisting its intent can regenerate the same native effect under a fresh
operation ID, so a request whose effect signature matches a retained result
under a different operation ID is reported distinctly and requires reobserving
the native postcondition. A lost response is resolved the same way; it is not
permission to issue a conflicting operation.

The initial operation vocabulary is:

- `EnsureAvailabilityGroup`;
- `EnsureReplicaJoined`;
- `EnsureReplicaSeeded`;
- `ReseedReplica`;
- `PlannedSwitchover`; and
- `ForcedFailover`.

Reseeding and forced failover require operation-bound destructive approval.
Reseeding additionally fences the target being replaced. Both switchover and
failover fence the exact old-primary incarnation.

## Security Contract

There are three separate trust boundaries:

1. TDS administration with verified TLS and separate observation and mutation
   principals.
2. AG endpoint encryption and certificate authentication between replicas.
3. Authenticated command authority between the future operator and replica
   agent.

Only Kubernetes Secret references belong in desired state. Passwords,
certificate private keys, connection strings containing credentials, and SQL
batches containing secrets must not be stored in CRD status, operation
signatures, logs, or command-line arguments. Observation permissions and
mutation permissions must use distinct Secret keys. SQL Server 2025 observation
permissions, including `VIEW SERVER PERFORMANCE STATE`, must be validated
explicitly; metadata hidden by insufficient permission must not be treated as
absence.

Endpoint certificates, system databases, and user data all require durable
storage and explicit rotation procedures. Availability Groups do not replicate
server logins or other server-level objects, so those need separate
provisioning.

Stage 3 does not add certificate provisioning, rotation, or a new production
mutation transport. Its executor remains behind an internal boundary and live
validation may reuse the licensed fixture's existing administrative connection.
This deferral does not authorize a plaintext production bypass; production
mutation credentials and transport provisioning remain part of controller and
operations integration.

## Level-Triggered Integration Boundary

youyuanwu/kuberic#79 supplied the durable level-triggered runtime and controller
boundary. This repository now consumes the published runtime for observation
and an explicitly constructed bootstrap/build vertical slice. Controller
deployment and general authority-changing SQL operations still require:

- durable replica and incarnation evidence;
- versioned declarative commands and retained terminal results;
- normalized observation and typed proof transport;
- compact accepted topology and transition intent;
- a pure planner that emits at most one authority-changing command; and
- separate persistence and dispatch reconciliation cycles.

The SQL Server code now includes an independently runnable public
`ReplicaHost` application, but it is not a controller and does not claim
automatic Kubernetes failover.

Each runtime receives a strict stable mapping from three Kuberic replica
numbers to SQL Server names and AG endpoint URLs. The current-configuration
callback supplies dynamic full identities, roles, sessions and addresses.
Fresh SQL evidence freezes native replica GUIDs, AG/database lineage and the
local SQL process start. Bootstrap callbacks may omit remote transport
descriptions; those fields permit only one-way completion, followed by exact
replay.

The application persists a schema-versioned non-secret runtime binding and,
only while native SQL cannot prove whether group seeding was dispatched, one
bounded `seed-dispatch-v1.json` record under the same authorized application
root. Kuberic's private authority store remains behind the public host boundary.
The already-owned source database separately retains narrow SQL extended
properties for AG creation intent and the exact SQL-generated AG GUID. It also
retains one singleton `dbo.kuberic_seed_dispatch` row containing a derived
64-character operation token, the exact AG/database identities, and the native
submission phase. This is bounded operation metadata, not a general command or
result ledger. After SQL accepts the group dispatch, one scope-named
`master` extended property retains `v1:<operation-token>` as the terminal
group marker. The token binds resource/configuration identity, AG name and
GUID, source database identity and lineage, source SQL incarnation, and both
authorized target identities.

The primary's remote DMV rows are dispatch-progress evidence only. Completing a
target build requires a challenge-bound report produced by a fresh direct
observation on that target host. The explicit test-host exchange authenticates
the exact resource, accepted configuration, build ID, target Kuberic identity
and current process session. The callback then validates the target native
replica GUID, exact AG and group-database identities, family/recovery-fork
lineage, local synchronized/healthy/online/not-suspended state, and the target
SQL start identity frozen by the licensed fixture. A report created for an old
challenge, build or process session is not reusable. This exchange exists only
in reviewed bootstrap-testing construction and does not alter the pinned
Kuberic protocol or ordinary read-only runtime.

The file record binds exact authority and native identities and distinguishes
`Intent`, `Submitting`, `Accepted`, genuinely `Ambiguous`, and
`FailedRetryable`. `Submitting` alone never suppresses another attempt after a
restart. The SQL batch takes a group-scope session application lock, publishes
`submitting` before `ALTER AVAILABILITY GROUP ... ADD DATABASE`, and writes
the terminal marker plus `accepted`, or `failed_retryable`, before releasing
that lock. Recovery can acquire the same lock to prove the old submitting
session is gone: exact native association or an accepted row can establish the
terminal marker without redispatch, a held lock is ambiguous, and an absent
effect after lock acquisition is definitely retryable.
Schema-v1 file records migrate conservatively: old `Dispatched` becomes
`Ambiguous`, never accepted by assertion.

Current configuration replay, secondary join, and seed build require native
markers to match the resource, accepted configuration, AG name, and observed
GUID; an unmarked same-name AG or a same-name replacement with another GUID is
a terminal conflict rather than an adoption candidate. The temporary file
record is removed as soon as exact source database-to-AG association proves the
terminal marker and accepted operation. Healthy later builds replay from the
terminal marker and exact native state only. Once terminal, absent or incomplete
source association, source database identity/lineage replacement, or AG GUID
replacement is conflicting or unsafe and cannot create another intent or issue
another `ADD DATABASE`. Previous/current
transitions, catch-up quorum, removal, data loss, destructive recovery, failover
and lease behavior remain explicit unsupported errors.

Role is not write authority. The healthy reports expose one primary and two
active secondaries, but write access is never granted. No client SQL service
address is published and there is no external write lease. The fixture's marker
write uses its private administrator connection to the directly observed native
primary and is not a Kuberic client-access decision.

Stage 3 first identifies what Kuberic's private agent store retains and what
exact SQL Server postcondition can be reobserved after restart. The PostgreSQL
v2 precedent uses three distinct owners: Kuberic agent SQLite metadata,
application-owned checksummed metadata, and PGDATA. MSSQL applies the same
ownership discipline with Kuberic authority, native SQL state, the immutable
runtime binding, the AG identity marker in the owned source database, and one
bounded seed-dispatch file under the existing application root plus one
singleton SQL-owned dispatch row. The file is temporary ambiguity state; one
bounded terminal extended property remains in `master` after compaction so
loss or recreation of the source database or AG cannot reopen dispatch. The AG
marker only binds creation ownership and native identity. No general second
operation database or result ledger was added.

Bootstrap SQL is level-triggered at statement granularity. Scoped native DDL
triggers persist the exact resource/configuration-derived creation intent before
`CREATE DATABASE` and `CREATE AVAILABILITY GROUP`; only the corresponding
token-bearing typed statement can atomically write the generated creation
receipt. Recovery mode, ownership, marker schema, full backup, log backup,
backup verification, AG binding, local join, seeding grant, and a narrow
SQL-owned grant-completion marker then converge as separate classified steps.
Read-only `msdb` history proves the full/log/VERIFY chain, while the local
marker joined to the current AG GUID proves completion of `GRANT CREATE ANY
DATABASE`; retries issue only the next missing safe statement.

The current implementation tracks Kuberic `main`; `Cargo.lock` freezes the
exact revision used by each build. The current pin
`09ce0a7ba9fdd9d6dcd207606fd0c9496a58c47f` is the merge commit of
[youyuanwu/kuberic#133](https://github.com/youyuanwu/kuberic/pull/133), which
fixes custom-authority recovery and serializes peer replacement when public
hosts reopen the same durable roots. It also includes the testing-only
bound-listener handoff from
[youyuanwu/kuberic#127](https://github.com/youyuanwu/kuberic/pull/127).

## Delivery Sequence

1. **Support and safety contract** — implemented: types, validation,
   canonical operation identity, tests, and this design.
2. **Runtime and observation** — implemented: a replaceable TDS executor,
   immutable DMV snapshots, freshness, startup capability checks, an
   observe-only CLI, and Kuberic configuration-progress publication.
3. **Bootstrap, join, and seed convergence** — implemented for explicit
   bootstrap-capable hosts: deterministic statement-level
   database/backup/AG/join/grant convergence, one build-authorized group seed
   dispatch, gap-driven seed metadata, exact native postconditions,
   restart/cancellation/retry/lost-reply recovery, and licensed fault/conflict
   validation. This stage adds neither certificate provisioning nor a
   production mutation transport.
4. **Fencing, reseed, switchover, and failover** — establish verified target and
   old-primary fencing before destructive reseed or role movement, then add
   external lease handling, sequence arbitration, authenticated fence and
   approval receipts, explicit data-loss recovery, and live fault tests.
   Automatic failover remains disabled until every safety gate passes.
5. **Controller integration** — Pod/PVC/Secret/Service convergence through the
   implemented Kuberic command/evidence boundary.
6. **Kubernetes E2E and operations** — pinned licensed test environment,
   three-replica failures, process/operator restarts, client reconnection,
   upgrades, and runbooks.

Stages 1-4 do not require operator2. Stages 5-6 require the minimum
youyuanwu/kuberic#79 vertical slice, but do not need to wait for every
youyuanwu/kuberic#79 operation to be complete.

## Test Gates

Server-free tests
cover profile rejection, exact progress, malformed observations, canonical
operation vectors, duplicate/reused operation IDs, epoch regression,
destructive approval, and fence binding. They run in the ordinary CI job as
`cargo test --workspace --all-features`.
Runtime tests also cover typed DMV decoding, inconsistent snapshots,
permissions, transport and sample deadlines, freshness, cancellation, CLI
output, and credential redaction. A dedicated server-free observer workflow
runs these without provisioning PostgreSQL or Kubernetes.

Live tests run sequentially in the shared validation job after server-free
checks. The Rust one-replica lifecycle owns one digest-pinned SQL Server 2025
Enterprise Developer container and validates absent AG, partial permission
denial, invalid TLS and fresh production CLI provenance. It creates no
availability group and never adopts or borrows an existing container.

The Rust three-replica lifecycle pins
`mcr.microsoft.com/mssql/server@sha256:2b5b581621126574f3d1f75e78d3eebe8d05aedb59ad0cfdf9aa42cb0634d726`
and automatically accepts the EULA only through the test crate's sourceless
authorization constructor. It creates three resource-bounded instances and real
endpoints; explicit bootstrap-capable hosts create one synchronous external AG
and one automatically seeded database. Fresh direct observations must prove one
primary, two synchronized healthy
secondaries, common identities and lineage, completed seeding, and a marker
readable from all members. Three in-process Kuberic agents then admit only the
frozen current topology and publish bracketed, fenced reports. Exact ownership
is persisted before create dispatch, cleanup revalidates immutable IDs and
attributes. Both fixture families persist intent before create dispatch, refuse
foreign or replaced resources, and perform bounded recovery without requiring
SQL Server availability. `just cleanup` invokes both exact cleanup binaries.
The three-runtime validation also performs a host-only same-root restart:
replacement processes receive fresh sessions, stale-session commands are
rejected, exact reports reconverge, and native marker continuity is retained.
Fault, signal and recovery cases remain explicitly invoked, including failure
after replacement-host startup followed by exact cleanup and retry. This does
not advance the production delivery sequence: licensed validation restarts a
target SQL process only to prove build-evidence invalidation; it does not own or
contain that process lifecycle. Write-lease expiry, old-primary fencing,
switchover, failover, replica replacement, Kubernetes lifecycle, and client
routing remain unimplemented.

## References

- [Run SQL Server 2025 containers](https://learn.microsoft.com/en-us/sql/linux/install-upgrade/quickstart-install-docker?view=sql-server-ver17)
- [`SERVERPROPERTY`](https://learn.microsoft.com/en-us/sql/t-sql/functions/serverproperty-transact-sql?view=sql-server-ver17) — SQL Server 2025 edition and EngineEdition identities
- [Issue #80](https://github.com/youyuanwu/kuberic/issues/80)
- [Level-triggered operator proposal](https://github.com/youyuanwu/kuberic/blob/main/docs/proposal/level-triggered-operator-design.md)
- [SQL Server availability groups on Linux](https://learn.microsoft.com/en-us/sql/linux/business-continuity/availability-groups/overview)
- [Configure an availability group for high availability on Linux](https://learn.microsoft.com/en-us/sql/linux/business-continuity/availability-groups/configure)
- [Manage availability group failover on Linux](https://learn.microsoft.com/en-us/sql/linux/business-continuity/availability-groups/failover-high-availability)
- [`sys.dm_hadr_database_replica_states`](https://learn.microsoft.com/en-us/sql/relational-databases/system-dynamic-management-objects/sys-dm-hadr-database-replica-states-transact-sql)
- [`CREATE AVAILABILITY GROUP`](https://learn.microsoft.com/en-us/sql/t-sql/statements/create-availability-group-transact-sql) — `EXTERNAL` AG name limit
- [Microsoft `mssql-server-ha` resource agents](https://github.com/microsoft/mssql-server-ha)
- [Microsoft SQL Server AGs on Kubernetes with DxOperator](https://learn.microsoft.com/en-us/sql/linux/business-continuity/containers/tutorial-kubernetes-dxoperator)
