# SQL Server External-Replication Adapter

> **Status:** Safety contract, observe-only production runtime, and explicit
> licensed-test bootstrap convergence. Read-only TDS queries are supported;
> bootstrap-capable test hosts can create the managed database/AG, join
> secondaries and seed once. SQL Server process management and failover effects
> are not enabled. Full Kuberic/Service Fabric Replicator semantic conformance
> is not yet established.
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
- a dedicated signed `ConfigurationSequence` and optional Public V1 Kuberic
  adapter that publishes validated AG configuration sequence through its
  progress callbacks after exact native binding, while keeping lifecycle epochs
  and native decimal database positions separate;
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
general repair, destructive recovery, lease, or failover capability. These
as-built scalar values do not establish the documented Replicator durable-data
progress or retained-history semantics.

`SqlServerEulaAcknowledgement` is the shared semantic acknowledgement contract:
it can be constructed only from an explicit affirmative value. The production
crate deliberately provides no container or Pod launcher and never translates
that value into `ACCEPT_EULA=Y`. Repository Cargo commands force
`ACCEPT_EULA=Y` through `.cargo/config.toml` as repository-wide authorization
only for the Rust-owned licensed fixtures. Those fixtures use a clearly
test-only constructor to create the affirmative semantic value and pass exactly
`ACCEPT_EULA=Y` to their owned containers; the setting does not authorize
external or borrowed instances. The production adapter never accepts an EULA
or deploys SQL Server. The strict file-backed constructor and negative contract
tests remain for future production launchers.

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
  able to construct an envelope can construct a receipt for it. Production
  receipt lifetime and issuer verification remain future safety work. The
  fixture-issued evidence described below does not upgrade `FenceReference`
  into a verified production proof.

## Kuberic and Service Fabric Semantics

The target is Kuberic's Service Fabric-shaped Public V1
`Replicator`/`PrimaryReplicator` contract, not merely compatible method
signatures or behavior copied from another database adapter. Application-facing
Kuberic/SF traits are fixed: they will not be changed or expanded for MSSQL.
MSSQL must satisfy their documented postconditions for each supported operation.
This does not require a V2 Transactional Replicator or a
dummy `StateProvider`: SQL Server continues to own native replication.

The [trait contracts](https://github.com/youyuanwu/kuberic/blob/main/kuberic-runtime/src/replicator/traits.rs),
[API semantics](https://github.com/youyuanwu/kuberic/blob/main/docs/features/kuberic/service-fabric-api-semantics.md),
and [custom Replicator boundary](https://github.com/youyuanwu/kuberic/blob/main/docs/features/kuberic/replicator-boundary.md)
define the required integration boundary. The root `Cargo.toml` determines
which dependency those contracts must be checked against.

| Area | Required semantics | Current boundary |
|---|---|---|
| Progress and history | Primary progress is locally durable quorum-committed progress; secondary progress is contiguous durably accepted progress. Recovery progress is election-only with access closed. Catch-up capability identifies the retained-history floor for an admitted peer, or is unknown/inapplicable. | A typed native configuration-sequence read distinguishes unavailable evidence from observed zero. Existing public callbacks retain AG-sequence/placeholder projection for hosted compatibility, not proven database-data progress or retained history. |
| Epoch authority | Epoch barriers fence predecessor operations on primaries and secondaries, including same-role changes. | Higher validated epochs retire predecessor dispatch/publication authority, including same-role changes; equal replays preserve it. Submitted effects retain their existing durable uncertainty and exclusion. |
| Configuration and build | Successful callbacks certify the exact supported configuration. An idle build target is outside voting membership; build completion and later admission are separate. | Bootstrap callbacks can defer work or complete only the source foundation. The seed-build translator still requires a target in the frozen active-secondary layout, despite accepting idle build DTOs. |
| Access and catch-up | Serving follows partition read/write status, not role alone. Catch-up satisfies the requested predicate for applicable current/previous configurations and any required successor. | Ordinary hosts publish no client address and do not manage SQL serving access. Catch-up callbacks explicitly reject; native synchronization alone is not a public catch-up receipt. |
| Shutdown | Close drains owned descendants. Abort promptly fences and contains remaining work without relying on graceful completion. | Close drains tracked callbacks and actual store/submission task joins; Abort fences without waiting. Retained objects provide cancellation-safe local drainage, but generic host post-Abort/interrupted-terminal completion does not certify those joins. Ownership must be resolved within the fixed lifecycle contracts, not through a new application hook. |
| Replication endpoint | Open establishes the advertised replication endpoint and its ownership lifetime, using the native transport where appropriate. | The configured replication address is metadata only; Open does not establish or own a transport listener. |

The progress contract remains a design blocker. AG sequence is valid native
configuration/election evidence, but can remain unchanged while database
transactions advance. It cannot stand in for data durability. Native
`numeric(25,0)` fields also cannot be narrowed, hashed, saturated, or compared
across fields or recovery lineages to fill the public signed-64-bit slot.
A valid public progress/history model must be established before using these
callbacks for data-bearing admission; unavailable capability remains
`INVALID_LSN`, and unsupported operations must reject rather than certify an
unproven postcondition. A conservative current-only capability is not by itself
evidence of unsafe replication, but is not a retained-history proof.

`SqlServerReplicator::native_configuration_sequence` now exposes the existing
validated native read as an optional `ConfigurationSequence`, independently of
the public scalar projection. It does not establish public membership,
readiness, election quorum, or promotion authority.
The public callback values and hosted journeys remain deliberately unchanged:
the selected Kuberic host requires successful current progress for registered
reports, synthesizes `first = 0` without a provider, and cannot represent an
unknown completed progress range. It does not project custom capability into
that report field. The [source-backed reporting/history blockers](kuberic-progress.md#upstream-reporting-and-history-blockers)
must be resolved alongside the native/public history model, not concealed with
a dummy provider or fabricated progress. This additive separation is not full
semantic conformance.

Native cancellation is not rollback. Supersession must prevent new predecessor
dispatch and stale completion, while retaining uncertainty and successor
exclusion for effects already submitted. Local epoch or cancellation state
alone cannot prove that an old SQL operation or lease renewal can no longer
apply.

### Local Lifecycle Containment

Adapter admission captures accepted authority, with checked boundaries at
external-future polling, store-worker start, seed-task executor entry and
success publication. Higher valid epochs retire that authority even when role
is unchanged; equal epochs do not. Native identity, history, profile, freshness
and sequence nonregression checks remain independent requirements.

Owned callbacks, blocking store work and seed-submission tasks are accounted
for independently of their caller's waiter. Close first refuses new work and
then drains those obligations. Abort atomically refuses admissions and signals
cancellation without waiting behind a paused publication or invoking Close.
Service terminal states retain the constructed replicator. The inherent
`wait_for_containment` methods on the replicator, service and runtime
application can resume local drainage after waiter cancellation or Abort;
calling them before terminal admission is an explicit error. Genuine store
I/O/unsafe-state failures and worker panics remain errors on repeated drains;
handled revision/removal contention does not poison the terminal result.

This is adapter-local safety, not a complete host contract. Kuberic's internal
built-in witness/drain mechanism is not an application extension point.
Generic host completion after Abort or an interrupted terminal root cannot
certify MSSQL's independently owned descendants. The remaining obligation is
adapter/process ownership under the existing Close and Abort callbacks, not
adding registration or expanding public traits. No private hook, dummy built-in
engine, blocking graceful shutdown in Abort, or graceful-after-Abort substitute
is introduced. Concrete MSSQL owners can use the inherent local wait, but that
does not upgrade the generic host's evidence.

A public Close exercises local callback drainage, but its subsequent
progress-report construction returns `Unavailable`/Closed. Returning Closed
from a terminal replicator is consistent with the existing contract; the
host/reporting implementation must be assessed without requiring a new callback
or fabricated terminal progress. Successful local drainage is not a successful
terminal RPC/report. Existing hosted progress values remain compatibility behavior.
See the [lifecycle guide](kuberic-progress.md#local-lifecycle-containment).

The [semantic-conformance follow-up](roadmap.md#stage-6a-follow-up-replicator-semantic-conformance)
is required before planned switchover. Completed lease/fence experiments remain
native evidence, not public lifecycle or serving-admission proof.

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

The example is an architectural precedent, not a conformance oracle. Kuberic's
[Service Fabric contract audit](https://github.com/youyuanwu/kuberic/blob/main/docs/features/kuberic/service-fabric-contract-audit.md)
also records upstream implementation gaps. MSSQL must verify the required
host/planner behavior and track unresolved upstream blockers rather than
inherit an example's behavior as proof of correctness.

## Native HA Boundary

For `CLUSTER_TYPE = EXTERNAL`, normal failover belongs to the external cluster
manager. Kuberic must not issue an isolated `ALTER AVAILABILITY GROUP ...
FAILOVER` and then infer success from an RPC response.

The comparative [fencing, lease, and failover research](mssql-ha-fencing-lease-failover.md)
documents the published Microsoft Pacemaker protocol, WSFC lease behavior,
Kubernetes operator precedents, and the customer-visible Azure SQL HA boundary.

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

## Lease and Fence Foundation

The `kuberic`-feature [`lease_fence` module](../crates/kuberic-mssql/src/lease_fence.rs#L1)
supplies server-free Stage 6A
contracts, not an executor or permission to promote. `LeaseAuthority` binds the
resource UID, configuration identity, AG name/GUID, full composite Kuberic role
epoch, observed logical/native replica and incarnation, and exact SQL start.
Desired-only replicas cannot satisfy this binding.

[`LeaseModel`](../crates/kuberic-mssql/src/lease_fence.rs#L271) permits one
outstanding request, uses strictly increasing operation sequences, and retains
a monotonic-clock high-water mark even when rejecting a
transition. Requests also carry private model-session identity: recreating a
model cannot admit a predecessor response even if request values are reused.
Revocation/supersession and cancellation remove local acceptance; delayed,
expired, cancelled, duplicate, or mismatching responses cannot restore it.
Cancellation leaves the request unresolved and blocks further dispatch by that
model.

Accepted local authority ends at submission plus validity minus margin, not
response time plus validity. `NativeEffectHorizon` separately accounts for this
actor's possible native submissions. Without a justified bound on actual native
application, a lost response remains unbounded through any finite wait.
Successful acknowledgement can bound that request's application by receipt
time, but revocation is not native cancellation. `NoSubmissions` means only
that this model submitted nothing; it says nothing about predecessor or other
renewers. A fresh model starts without accepted authority or imported native
expiry proof.

[`LeaseBudget`](../crates/kuberic-mssql/src/lease_fence.rs#L129) validates positive
timing components and checked arithmetic. Its
renewal coverage combines observation, reconciliation/scheduling, renewal
interval, request, quorum-loss detection, and margin; native validity must
exceed that coverage. A conditional recovery estimate additionally includes
native application, validity, fencing, and promotion. Without an application
bound that estimate is unavailable. Neither the estimate nor a local deadline
authorizes promotion.

`FenceRequest` and `FenceReceipt` bind one operation to one complete authority
identity. Receipt construction records a trusted provider's verified stop; it
does not authenticate that provider, prevent a later administrative start, or
connect the older operation-envelope `FenceReference` to production execution.

### Licensed Fixture Evidence

The test crate alone can create a lease-enabled external AG and issue
`WRITE_LEASE_VALIDITY` updates in positive integer **seconds**. Ordinary fixture
creation and production SQL transport are unchanged. The fixture fence uses
its own run/configuration identity and a synthetic epoch, not durable Kuberic
role authority.

The [licensed experiments](../crates/kuberic-mssql-tests/tests/live_lease_fence.rs#L82)
against the pinned SQL Server **17.0.5005.3** fixture established:

| Experiment | Observation |
| --- | --- |
| Renewal | A marker write, confirmed from a fresh SQL session, succeeded beyond creation acknowledgement plus the original validity and margin, inside the renewed conservative window. |
| Renewal cessation | Raw remaining lease reached zero and writes failed with SQL error 988, while the replica still reported `PRIMARY`/`ONLINE` and all three replicas remained connected and synchronized. |
| Response cancellation | A delayed renewal response exceeded its client deadline; the session was discarded and fresh observations retained uncertainty rather than assuming rollback. |
| Active-lease restart | The same container acquired fresh Docker/SQL start identities. Within the previous lease window, SQL reported `RESOLVING`/`OFFLINE`, raw remaining lease was null, and fresh-session marker readback did not confirm the attempted write. This is an unconfirmed outcome, not proof of commit, rollback, or lease persistence. |
| Exact fixture fence | A verified stop produced matching evidence; after an explicit completed restart, old requests and a genuinely issued old receipt were rejected without stopping the replacement. |

Error [988](https://learn.microsoft.com/en-us/sql/relational-databases/errors-events/mssqlserver-988-database-engine-error)
means insufficient HA quorum, not uniquely lease expiry. The experiment
corroborates it with unchanged replica/synchronization counts, exact identity,
stopped renewal, and raw lease telemetry. `write_lease_remaining_ticks` is
unsupported informational telemetry; its units are not inferred and it is not
a production expiry API. Reported native role alone is not write admission.

### Fixture Fence Limits

The [fixture provider](../crates/kuberic-mssql-tests/src/three_replica/member/lease.rs#L404)
verifies owned attributes, immutable container ID, Docker
`StartedAt`, native AG/replica, and SQL start before stopping, then verifies the
stopped incarnation before returning a receipt. Exclusive mutable fixture
ownership and the lifetime root lock serialize supported lifecycle changes.
The fixture requires restart policy `no`; arbitrary concurrent Docker
administrators or independent restart actors are outside its guarantees. This
is not Kubernetes, host-power, or storage fencing.

Journal schema 9 persists stop/start exclusions before dispatch and migrates
schemas 5-8. Timeout, cancellation, or ambiguous verification leaves an
exclusion that rejects cooperating restart. Re-entry cleans up and retires the
exact old container before creating a new run. A confirmed stop permits the
explicit fixture restart path; it cannot turn an old receipt into evidence for
the new incarnation.

The ignored experiments run explicitly and sequentially in licensed CI:

```bash
just test-live-lease-fence [root]
just cleanup-live-lease-fence [root]
```

Production renewal, promotion, failover, destructive recovery, and a production
fence provider remain unavailable. Restart and partition-safe native authority,
exclusion of every predecessor renewer, and durable transition/replay evidence
remain admission gates for later Stage 6 work.

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
different databases, or compared across incompatible recovery histories. They
also cannot be safely narrowed, hashed, saturated, or substituted into
Kuberic's signed 64-bit LSN slot. Every progress-bearing transition therefore
includes both the database GUID and recovery-fork GUID. Public V1 uses the AG
configuration sequence in the current adapter callbacks, but that is an
as-built limitation, not the required durable-data history contract. Any future
operation that depends on database-native progress must compare the appropriate
field under exact lineage and satisfy both Replicator semantic conformance and
the Stage 6 lease, fencing, and mutation-authority requirements.

The production crate now exposes repository-owned native history positions for
future adapter integration. A position binds one exact decimal value to one
field and one database/recovery lineage. Comparison returns ordering only when
both positions have the same field and exact lineage. Missing positions remain
unavailable; missing or different lineage and cross-field comparisons are
non-orderable. These types never convert native positions into Kuberic progress.

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

The released Public V1 transport is not claimed to authenticate production
mutation authority. SQL Server HADR cannot carry Kuberic authorization tokens,
so production authority-changing work still needs an authenticated
replica-local adapter/control channel above HADR. The existing challenge-bound
fixture exchange is a licensed validation seam only; it is not the production
mutation transport.

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
numbers to SQL Server names and AG endpoint URLs. Public V1 current
configuration supplies exact ready remote active descriptions, while the local
member is recovered from runtime identity and the configuration descriptor.
Idle or in-build targets stay outside that remote set and carry
`INVALID_LSN`, exact build/session/storage identity, and an exact address.
Fresh SQL evidence freezes native replica GUIDs, AG/database lineage and the
local SQL process start. A complete remote-only primary callback converges the
source database and AG ownership foundation without admitting a partial native
group. A later explicit full projection completes local joins and native
admission before freezing the binding; later omitted projections cannot clear
it, and every supplied value must still match exactly.
These bootstrap completion points do not certify full public configuration
installation or nonvoting build admission.

The adapter publishes conservative immutable `ReplicatorCapabilities`.
Catch-up-specific quorum and all write authority remain unsupported.

Public V1 DTO interpretation is contained in a compatibility component under
the Kuberic adapter. It owns validation, normalization, local bootstrap
ownership, desired SQL descriptors, replay/enrichment, topology admission, and
build-target facts. SQL-native convergence classifiers do not import or
interpret Kuberic replica-set DTOs. Application binding schema 2, seed schema
3, and SQL ownership markers bind licensed behavior to the exact Public V1
activation and reject predecessor state without migration.

The unpublished test crate owns versioned release-intake profiles and a CLI.
Profile v1 preserves the predecessor baseline; profile v2 records executable
Public V1 evidence for role-before-configuration, same-role epoch renewal,
remote-active membership, invalid build progress, cancellation-before-removal,
AG-sequence progress, and the ordinary zero-mutation boundary. See [Kuberic
release intake](kuberic-release-intake.md).

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
authorized target identities. This bounded file/SQL marker exists only to
reconcile the external-effect ambiguity around `ADD DATABASE`; Kuberic's
stateless default-replicator design does not require removing it.

The primary's remote DMV rows are dispatch-progress evidence only. Completing a
target build requires a challenge-bound report produced by a fresh direct
observation on that target host. The explicit test-host exchange authenticates
the exact resource, accepted configuration, build ID, target Kuberic identity
and current process session. The callback then validates the target native
replica GUID, exact AG and group-database identities, family/recovery-fork
lineage, local synchronized/healthy/online/not-suspended state, and the target
SQL start identity frozen by the licensed fixture. A report created for an old
challenge, build or process session is not reusable. This exchange exists only in reviewed bootstrap-testing construction and does
not alter the Kuberic protocol or ordinary read-only runtime. The licensed
Stage 3 configuration and seed callbacks are a SQL-native convergence seam,
not end-to-end Public V1 Build operations. Public V1 Build conformance in this
repository is limited to server-free ordinary-path validation and effect-free
negative coverage; the licensed callbacks do not prove host Build admission,
receipts, or target-membership semantics.

The file record binds exact authority and native identities and distinguishes
`Intent`, `Submitting`, `Accepted`, genuinely `Ambiguous`, and
`FailedRetryable`. `Submitting` alone never suppresses another attempt after a
restart. The SQL batch takes a group-scope session application lock, publishes
`submitting` before `ALTER AVAILABILITY GROUP ... ADD DATABASE`, and writes
the terminal marker plus `accepted`, or `failed_retryable`, before releasing
that lock. The temporary row's operation token remains bound to the exact
winning build/storage authority; the v2 terminal marker uses a separate stable
activation/topology token shared by later target callbacks in the same dispatch
context. Recovery can acquire the same lock to prove the old submitting
session is gone: exact native association or an accepted row can establish the
terminal marker without redispatch, a held lock is ambiguous, and an absent
effect after lock acquisition is definitely retryable.

Predecessor seed schemas 1 and 2 reject without migration under fresh Public V1
activation. They are not converted to `LegacyAmbiguous`, compacted, or
redispatched. Only schema-3 records created for the exact activation can enter
native reconciliation.

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

Role is not write authority. The contained observe-only Public V1 reports use
role `None`, progress role `Recovery`, `first = 0`, `last =` the current native
AG sequence, and read/write access `NotPrimary`. Those report roles are
distinct from direct SQL observation, which proves one native primary and two
native secondaries. No client SQL service address is published and there is no
external write lease. The fixture's marker write uses its private administrator
connection to the directly observed native primary and is not a Kuberic
client-access decision. The reported AG scalar is not proof of durable recovery
progress merely because the report labels its progress role `Recovery`.

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
triggers persist the exact resource/configuration/Public V1 activation-derived
creation intent before
`CREATE DATABASE` and `CREATE AVAILABILITY GROUP`; only the corresponding
token-bearing typed statement can atomically write the generated creation
receipt. Recovery mode, ownership, marker schema, full backup, log backup,
backup verification, AG binding, local join, seeding grant, and a narrow
SQL-owned grant-completion marker then converge as separate classified steps.
Read-only `msdb` history proves the full/log/VERIFY chain, while the local
marker joined to the current AG GUID proves completion of `GRANT CREATE ANY
DATABASE`; retries issue only the next missing safe statement.

The root `Cargo.toml` is authoritative for the Kuberic dependency source and
version. `Cargo.lock` records reproducible resolution; this design does not
define a separate commit pin.

## Future Roadmap

Future implementation stages are maintained in the
[Kuberic MSSQL future roadmap](roadmap.md). The roadmap contains only incomplete
work and defines the Stage 6 lease/fencing slices, later controller integration,
and Kubernetes end-to-end operations. Replicator semantic conformance precedes
planned switchover; public-host sequencing is not deferred to Kubernetes
deployment. This design distinguishes the implemented safety and observation
contract from the required semantic boundary.

## Test Gates

Server-free tests
cover profile rejection, exact progress, malformed observations, canonical
operation vectors, duplicate/reused operation IDs, epoch regression,
destructive approval, lease timing/native uncertainty, fresh-model replay,
fence binding, and gated lifecycle exclusion/re-entry. They run in the ordinary CI job as
`cargo test --workspace --all-features`.
Runtime tests also cover typed DMV decoding, inconsistent snapshots,
permissions, transport and sample deadlines, freshness, cancellation, CLI
output, and credential redaction. A dedicated server-free observer workflow
runs these without provisioning PostgreSQL or Kubernetes.

Live tests run sequentially in the required GitHub Actions job
`Shared CI validation`, which runs `just ci` at the exact pull-request head and
always runs `just cleanup`, after server-free checks. GitHub Actions job logs
are the run evidence; no test logs or evidence artifacts are committed. The
Rust one-replica lifecycle owns one digest-pinned SQL Server 2025
Enterprise Developer container and validates absent AG, partial permission
denial, invalid TLS and fresh production CLI provenance. It creates no
availability group and never adopts or borrows an existing container.

The Rust three-replica lifecycle pins
`mcr.microsoft.com/mssql/server@sha256:2b5b581621126574f3d1f75e78d3eebe8d05aedb59ad0cfdf9aa42cb0634d726`
and is authorized by the repository-wide forced Cargo environment
`ACCEPT_EULA=Y` only for these owned licensed fixtures, together with the test
crate's sourceless authorization constructor. Production remains unable to
accept an EULA or deploy SQL Server. The fixture creates three resource-bounded
instances and real endpoints; explicit bootstrap-capable hosts create one
synchronous external AG and one automatically seeded database. Fresh direct
observations must prove one primary, two synchronized healthy
secondaries, common identities and lineage, completed seeding, and a marker
readable from all members. Three in-process Kuberic agents then admit only the
frozen current topology and publish bracketed, fenced reports. Exact ownership
is persisted before create dispatch, cleanup revalidates immutable IDs and
attributes. Both fixture families persist intent before create dispatch, refuse
foreign or replaced resources, and perform bounded recovery without requiring
SQL Server availability. `just cleanup` invokes both exact cleanup binaries.
The three-runtime validation also performs a host-only same-root restart:
replacement processes receive fresh sessions, stale-session commands are
rejected, existing-state Open succeeds, native seed records compact without
redispatch, exact contained reports reconverge, and native marker continuity is
retained. This is not Public V1 peer-session/lifecycle renewal, does not replay
an end-to-end Public V1 Build, and does not restore serving access.
Fault, signal and recovery cases remain explicitly invoked, including failure
after replacement-host startup followed by exact cleanup and retry. This does
not advance the production delivery sequence: licensed validation restarts a
target SQL process only to prove build-evidence invalidation; it does not own or
contain that process lifecycle. Write-lease expiry, old-primary fencing,
switchover, failover, replica replacement, Kubernetes lifecycle, and client
routing remain unimplemented.

The `cancel_before_remove` intake gate combines upstream and local evidence:
Kuberic release conformance catalog gate KSF-06 proves host-owned
cancellation/drain ordering, while the MSSQL readiness test proves that remove
rejection is effect-free after a callback task has settled. The local test is
not represented as host-owned ordering.

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
