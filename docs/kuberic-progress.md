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

The selected Kuberic traits require primary quorum-committed progress, secondary
contiguous durable progress, access-closed recovery evidence, and a retained
catch-up floor or `INVALID_LSN` for unknown/inapplicable capability. AG
configuration sequence does not establish those data-progress postconditions.
The public traits remain unchanged; MSSQL must align its supported behavior
rather than redefine their meaning.

### Native evidence feasibility

**Decision:** the existing observations support identity-checked native evidence,
not a conforming public progress/history mapping. No new scalar or catch-up
success is justified yet. This is a missing-proof boundary, not a conclusion
that SQL Server or the fixed application interfaces make such a mapping impossible.

The observer preserves exact decimal positions and distinguishes local rows from
primary-reported remote rows. Remote progress does not acquire the primary's
local recovery-fork identity
([observation.rs:873-985](../crates/kuberic-mssql/src/observation.rs#L873-L985)).
The existing comparison helper orders only present values of the same field,
database and recovery fork; missing values remain unavailable and incompatible
fields/lineages are nonorderable
([types.rs:535-608](../crates/kuberic-mssql/src/types.rs#L535-L608)).
Neither helper proves freshness, quorum commitment, complete-prefix coverage,
unique storage ownership or retained transfer capability.

| Obligation | Available evidence and scope | Decision and missing proof |
|---|---|---|
| Primary committed progress | Primary-local aggregate commit observation, synchronous policy and commit participation; a fixture-only exact-transaction numeric commit-record probe. | **Public progress unproven.** The bounded experiment captured one transaction's commit record and subsequent ACK, not a highest quorum-committed public boundary for the applicable configurations and selected successor. |
| Secondary contiguous durable progress | Target-local positions and an executable exclusive flush-prefix predicate, with exact native history/incarnation and evidenced fixture configuration phases. | **Native sample demonstrated; public progress unproven.** One acknowledged record was outside the paused target's observed prefix, then covered after resume and during quiet collection. This is not a highest contiguous public scalar or accepted public authority/history mapping. |
| Access-closed recovery progress | SQL start, database/recovery identity and available local positions; recovery metadata can be unavailable. | **Unproven.** Access-closed compatibility reports do not establish a recovered durable prefix. Missing or terminal evidence must not become an empty history or an access grant. |
| History continuity | Database/family/recovery-fork identities, fork point, public storage/session authority and existing immutable runtime binding. | **Unproven.** Bind recovered native prefix to accepted public history across restart, replacement and forks. Matching GUIDs or public history IDs alone do not prove continuity or unique ownership. |
| Retained catch-up capability | Current positions and seed evidence; no native retained-transfer range is collected. | **Unavailable as a retained floor.** Prove that a source retains a complete required range and can transfer it to the exact admitted peer despite truncation/restart. A current tail or completed seed is not that proof. |
| Configuration and required-successor catch-up | Exact three-replica topology and target-local seed/synchronization observations. | **Unsupported callbacks.** Both catch-up configuration and quorum waits reject. Count-one synchronous commitment does not prove coverage by a specifically marked successor or the applicable current/previous public configurations. |

The raw query and identity bindings are in
[query.rs:296-334](../crates/kuberic-mssql/src/query.rs#L296-L334) and
[kuberic.rs:506-542](../crates/kuberic-mssql/src/kuberic.rs#L506-L542).
The [current callback implementations](../crates/kuberic-mssql/src/kuberic.rs#L4710-L4811)
still project AG sequence and reject catch-up; the matrix does not change them.
Current-configuration admission supports a constrained topology/bootstrap path,
not arbitrary changing current/previous configurations
([kuberic.rs:4813-5013](../crates/kuberic-mssql/src/kuberic.rs#L4813-L5013)).

Microsoft's [database-replica DMV documentation](https://learn.microsoft.com/en-us/sql/relational-databases/system-dynamic-management-objects/sys-dm-hadr-database-replica-states-transact-sql?view=sql-server-ver17)
qualifies these observations: primary remote rows may be out of date;
`last_commit_lsn` is the last commit processed on a primary and the last commit
redone on a secondary. `last_hardened_lsn` is a padded block value, not a record
LSN; on a synchronous primary it can describe the minimum secondary hardening
rather than the local durable tail. The explanatory flush boundary covers
records strictly below it, so equality or an invented subtract-one conversion
is not a demonstrated coverage test. The DMV's `quorum_commit_lsn` and
`quorum_commit_time` are explicitly unsupported informational fields, not a
supported shortcut.

[Synchronous commit](https://learn.microsoft.com/en-us/sql/database-engine/availability-groups/windows/availability-modes-always-on-availability-groups?view=sql-server-ver17)
uses log-hardening acknowledgements, independently of redo.
[Recovery metadata](https://learn.microsoft.com/en-us/sql/relational-databases/system-catalog-views/sys-database-recovery-status-transact-sql?view=sql-server-ver17)
can be unavailable, and database GUIDs can be duplicated by copy/attach.
[Log start/end diagnostics](https://learn.microsoft.com/en-us/sql/relational-databases/system-dynamic-management-objects/sys-dm-db-log-stats-transact-sql?view=sql-server-ver17)
are potential source-retention inputs, not a peer-transfer guarantee; the
documented secondary result is limited. These vendor descriptions are not
build-specific live proof. An exact bounded scalar representation would still
need supported ordering, history and recovery semantics; numeric width alone
does not decide feasibility.

### First native proof toward switchover

The fixture-only **record probe and selected-target native prefix experiment
are implemented**. Sampled native coverage has been demonstrated on owned
replicas; public catch-up, promotion authority and switchover are not certified.

#### Record acquisition

On an explicitly authorized, qualified licensed fixture host, run:

```bash
just test-live-native-commit-coverage
# Recover only this experiment's exactly journaled resources if interrupted:
just cleanup-live-three-replica target/mssql-native-commit-coverage
```

The exact test remains ignored by default
([live_three_replica.rs:80-159](../crates/kuberic-mssql-tests/tests/live_three_replica.rs#L80-L159)).
It launches only owned disposable replicas through existing license/resource
preflight, inserts one parameterized marker, captures its transaction/database
ID inside the transaction, and samples
[`database_transaction_commit_lsn`](https://learn.microsoft.com/en-us/sql/relational-databases/system-dynamic-management-objects/sys-dm-tran-database-transactions-transact-sql?view=sql-server-ver17)
on a second primary connection around COMMIT. Effective delayed durability
must be disabled. The field identifies that transaction's commit log record;
an aggregate primary `last_commit_lsn` is not substituted for it.

The report preserves exact decimal positions, transaction state, ACK timing,
and raw direct native snapshots of all three members before, after ACK and in
a quiet phase with no additional experiment workload write
([commit_coverage.rs:36-107](../crates/kuberic-mssql-tests/src/three_replica/commit_coverage.rs#L36-L107)).
Owned container/image, SQL start, AG/configuration, replica/database/family/fork,
local provenance and freshness checks bracket collection. These independent
observations are not an atomic snapshot or proof against change-and-revert.
Normal completion, failures, caught panic and handled signals use the existing
bounded consuming cleanup; a dropped SQL connection is not certified rollback.
The report is serialized in memory before cleanup and emitted only after
successful teardown, so a stalled output sink cannot retain owned SQL resources.
Failed cleanup prevents emission and remains an execution failure.

**Measured result:** an authorized three-replica run captured the exact matching
transaction's nonzero numeric commit LSN in state 12 (being committed), followed
by successful client ACK. Nineteen collection attempts included one before
COMMIT, sixteen in the overlap-sampling phase (which can extend past ACK), and
two in the dedicated post-ACK phase. The dedicated post-ACK rows were absent:
the run demonstrates a usable collection window, not deterministic retention
of completed transaction rows. Quiet collection completed without another
marker INSERT. This record-only run used no replication suspension or redo control.

Output is structured JSON through the test harness with `--nocapture`.
`record_probe.status` distinguishes `observed` from
`not_observed_in_bounded_window`; successful queries without a usable row are
a collection limit, not proof that SQL Server has no supported mechanism.
SQL/transport/parsing/deadline/output/cleanup failures remain test failures.
In the record-only report, even when observed, `coverage.status` is **`blocked`**:
that command does not collect accepted public configuration/storage/
session authority or native/public history mapping, and does not evaluate
selected-target durable-prefix coverage
([commit_coverage.rs:821-853](../crates/kuberic-mssql-tests/src/three_replica/commit_coverage.rs#L821-L853)).
A passing collection harness is not a passing coverage gate.

#### Native prefix coverage

The separate ignored prefix experiment uses one reviewed, owned-secondary
SUSPEND/workload/RESUME sequence:

```bash
just test-live-native-prefix-coverage
just cleanup-live-three-replica target/mssql-native-prefix-coverage
```

It confirms the selected secondary is paused, acknowledges one primary
transaction using the other synchronous secondary, and collects fresh
post-ACK target-local hardening evidence. After bounded resume and healthy
synchronization it evaluates the same record again, then repeats quietly
without another workload write. Primary and other-secondary health are not
waived; data-movement suspension is not a redo-only control.

The dedicated predicate applies Microsoft's explanatory exclusive flush-prefix
rule **`R < H`**, where R is the exact acknowledged commit record and H is the
selected secondary's local hardened boundary. Equality is not covered. Native
values remain exact; no encoding arithmetic, aggregate latest-commit
substitution, redo test or public `i64` projection is used
([prefix.rs:215-321](../crates/kuberic-mssql-tests/src/three_replica/commit_coverage/prefix.rs#L215-L321)).
The documentation's column/explanation discrepancy remains as noted above.
Even fresh local DMV data is not a linearizable disk oracle: `R >= H` means
**outside the observed prefix**, not proven current physical non-durability,
permanent absence or data loss.

**Measured result:** the authorized experiment observed
`outside_observed_prefix` while the exact selected secondary was paused, then
`native_sampled_coverage` after resume and in the quiet phase for the same
acknowledged record. Both native control transitions retained fresh direct
three-member agreement. Missing exact record/boundary evidence instead yields
`unavailable`, without hidden workload retries; invalid evidence and
SQL/control/release/output/cleanup failures remain execution failures.

Native AG sequence can advance across controls. The experiment keeps the
initial journal binding unchanged and admits an explicit fixture phase only
after acknowledged control, fresh three-member agreement and the target's
native postcondition, with container/SQL incarnation, roles, AG/database/
family/fork, membership/profile and commit policy unchanged
([sequence.rs:118-218](../crates/kuberic-mssql-tests/src/three_replica/commit_coverage/prefix/sequence.rs#L118-L218)).
Pre-dispatch drift still fails. Workload and scenario bindings are retained
separately; earlier evidence is not relabeled. No exact sequence increment or
unique native control-causation receipt is assumed.

This design assumes one isolated fixture operator. Its root lock does not
fence arbitrary SQL/Docker administrators, and these observation brackets
cannot exclude change-and-revert. Native phase admission is not public
configuration/storage/session authority or general restart/history continuity.
Final dispatch rechecks current health/freshness after owner inspection and
preserves the release reserve. Uncertain controls cannot grant a phase;
failed safe release remains visible and exact owned disposal contains the
effects. Successful teardown precedes report output.

The prefix report explicitly uses `scope = fixture_native_only`; native
decisions are separate from **`public_coverage.status = blocked`**
([prefix.rs:15-47](../crates/kuberic-mssql-tests/src/three_replica/commit_coverage/prefix.rs#L15-L47),
[prefix.rs:323-334](../crates/kuberic-mssql-tests/src/three_replica/commit_coverage/prefix.rs#L323-L334)).
The record-only command/report above remains unchanged.

Remaining live/public work:

1. Establish native/public history semantics beyond the sampled live hosted
   association below. Do not manufacture accepted public authority or history
   semantics from fixture labels, phase admissions or matching numeric values.
2. Demonstrate hardening with redo still behind; that live proof remains
   `not_exercised`. Offline redo independence is not such a demonstration.
   Any additional control requires design/safety review and live authorization.
3. Demonstrate actual accepted storage/public-history replacement journeys,
   beyond altered-evidence rejection, and establish the role-specific public
   progress, history and retained-transfer semantics still missing from the matrix.

The successful live witness below proves only that sampled coverage. It does not
establish a highest public progress scalar, a retained floor, restart
continuity, arbitrary configuration catch-up, or safe switchover. Those remain
separate gates, alongside SQL access, lease/fence and role-transition authority.

#### Offline hosted authority association

The server-free diagnostic now associates **scripted native evidence with
actual public `ReplicaHost` authority**. It uses real initialization, the
primary's successful `InitialPrimary` lifecycle, and successful current-only
Configuration programs on both secondaries. Frozen inputs must match exact
completion ID/revision/kind, digest/replay, configuration identity and accepted
history/storage. Actual reports retain process session, creation identity,
Pod/PVC storage generation and public history; an old successful completion is
not treated as a current-configuration oracle
([hosted_authority.rs:224-382](../crates/kuberic-mssql-tests/src/three_replica/hosted_authority.rs#L224-L382)).

The observe-only adapter accepts a secondary configuration containing exactly
its two complete remote descriptions. Its own identity/session/address comes
from the bound runtime context, with remote role, completeness and local
session/address collision checks. Native eligibility and frozen replay remain
required; partial bootstrap deferral and SQL mutation paths are unchanged
([compatibility.rs:834-884](../crates/kuberic-mssql/src/kuberic/compatibility.rs#L834-L884)).

Each member is linked through its actual admitted adapter topology. Independent
database identity anchors are collected from the owned observation source;
coherently changing a witness's local DBID/database GUID or shared family/fork
cannot replace that baseline. Local DBIDs/GUIDs may legitimately differ between
members. The native facade reuses the existing exact prefix/phase evaluators
and rejects future transitions, a same-phase transaction predating initial
admission, or final observations predating their admitted phase
([hosted.rs:28-115](../crates/kuberic-mssql-tests/src/three_replica/commit_coverage/prefix/hosted.rs#L28-L115)).

Collection has a 15-second bound. Its exclusively owned command stream brackets
native collection with fresh same-source reports; command admission and result
publication share a short lock, never held across network/native awaits.
Verified higher Epoch or same-role Role retires predecessor collection; equal
and exact replay preserve the lifetime. Pending, timed-out, caller-cancelled,
terminal or restarting authority cannot publish predecessor success. A later
successful host receipt does not revive an unknown-outcome lifetime
([hosted_authority.rs:425-598](../crates/kuberic-mssql-tests/src/three_replica/hosted_authority.rs#L425-L598)).

Run the diagnostic and focused secondary replay cases without SQL Server:

```bash
CARGO_BUILD_JOBS=1 cargo test --locked -p kuberic-mssql-tests \
  --test runtime_process_host native_authority -- --test-threads=1
```

The offline journeys inspect an in-memory serializable association with
`scope = offline_hosted_native_association`, `native_input = scripted_no_live_sql`
and **`public_coverage.status = blocked`**. This is not a new fixture CLI or
durable authority certificate. Actual hosted journeys cover epoch supersession,
same-root fresh sessions, terminal Close, pending publication, caller drop and
an awaited nonterminal timeout. Storage/public-history replacements are
**altered-evidence rejection cases**, not accepted replacement journeys.

The diagnostic excludes out-of-band administrators and cannot rule out
undetected change-and-revert. It does not join public history to a SQL recovery
history as a semantic equivalence, establish exclusive production storage
ownership, prove new live SQL durability, or authorize catch-up/promotion.
The licensed native-only commands/reports above are unchanged. The sampled live
hosted binding below does not close the redo-lag, public progress/retention,
restart continuity or reporting gates.

#### Live hosted witness

An ignored live case connects the existing **no-control** native commit
collector to real observe-only `ReplicaHost` applications. A separately
authorized licensed execution on 2026-10-18 proved one `NativeSampledCoverage`
association: acknowledged commit record `44000000055200028` was strictly below
the selected secondary's exclusive hardened boundary `44000000056000001`.
This is one sampled live association, not semantic public/native history
equivalence or general catch-up proof.

The initial run exposed a test-support startup gate that waited for serving
`ActiveSecondary` reports. Ordinary provider-free hosts instead remain contained
as `None`/Recovery with `[0, native sequence]` compatibility progress and
`NotPrimary` access. Fresh existing-native startup now validates its frozen
`InitialPrimary`/`SecondaryEpochAdvance` inputs against exact successful
receipts and an independent post-Open/Role, pre-lifecycle report baseline.
It rejects changed identities, sessions, storage/history, replayed reports and
different command/peer provenance rather than treating containment as missing
authority. Bootstrap retains its serving-role prerequisite; established-store
reopen is unchanged. Startup acceptance is not serving eligibility or the later
complete-secondary-configuration proof
([startup gate](../crates/kuberic-mssql-tests/src/three_replica/public_kuberic_group.rs#L3906-L3948),
[startup evidence](../crates/kuberic-mssql-tests/src/three_replica/hosted_authority.rs#L394-L445)).
The corrected live run completed host shutdown and exact fixture teardown
before publishing output.

The live path preserves the primary's exact `InitialPrimary` input captured
before dispatch and installs complete remote-only Configuration programs on
both secondaries. It obtains real successful receipts, admitted adapter bindings
and independent native database anchors from each owned SQL observation source.
The raw `CommitCoverageReport` is collected inside the shared authority
coordinator's fresh same-source public-report bracket, then adapted without
changing its ACK, exact record, bindings or observation timestamps
([hosted_witness.rs:31-222](../crates/kuberic-mssql-tests/src/three_replica/public_kuberic_group/hosted_witness.rs#L31-L222)).

The result retains that original raw report under `native_collection` and its
association under `association`, with `scope = live_hosted_native_association`
and `native_input = live_owned_quiet_sql`. The original raw report's standalone
blocked conclusion is not rewritten. Association **public coverage remains
blocked** for progress/history mapping, retained capability, restart continuity
and redo-lag proof. The ignored test requires `NativeSampledCoverage`:
missing exact records/boundaries, equality, and outside-prefix results cannot
pass. Row retention and a covered quiet sample are not guaranteed.

Live publication refreshes the evaluation clock after final host reports,
so those report requests cannot preserve stale native samples. The live
collection allowance is bounded by the configured convergence timeout and
remaining original fixture deadline, rather than the offline 15-second budget;
RPC/anchor requests have five-second allowances. The existing transaction
sub-budget remains 30 seconds. No fresh parent deadline, new SQL control,
SUSPEND/RESUME or redo-lag workload is introduced
([hosted_authority.rs](../crates/kuberic-mssql-tests/src/three_replica/hosted_authority.rs)).

Host construction completes before the first spawn. An owned startup/shutdown
guard retains pending tasks and already-started handles across cancellation
or unwind. Both startup-result consumers preserve panicked/cancelled task and
unconfirmed startup errors as termination-unconfirmed even when other hosts
drain successfully. The live runner blocks fixture disposal on uncertain
startup or shutdown; a retained waiter or MSSQL-local containment is not generic
host termination confirmation. Successful output follows confirmed host
shutdown and successful exact fixture teardown
([startup.rs](../crates/kuberic-mssql-tests/src/three_replica/public_kuberic_group/startup.rs),
[live runner](../crates/kuberic-mssql-tests/tests/live_three_replica/hosted_witness.rs)).

Only after explicit licensed-execution authorization and the
[three-replica fixture prerequisites](../README.md#three-replica-licensed-happy-path),
select exactly this case:

```bash
KUBERIC_MSSQL_THREE_REPLICA_ROOT="$PWD/target/mssql-live-hosted-witness" \
CARGO_BUILD_JOBS=1 cargo test --locked -p kuberic-mssql-tests \
  --test live_three_replica hosted_witness::three_replica_mssql_live_hosted_native_witness \
  -- --exact --ignored --nocapture --test-threads=1
```

The existing test-only fixture authorization accepts the SQL Server EULA for
owned disposable containers; this is not authorization to run this new
experiment or use borrowed deployments. The case remains ignored by default
for raw Cargo and is explicitly selected by nextest's live profile in `just ci`.
It provisions via existing fixture APIs and observes
through ordinary non-bootstrap hosts, not production SQL deployment behavior.
A blocked disposal result retains ownership evidence and is not successful
cleanup; resolve exact host ownership before using the existing fixture recovery
procedure.

The server-free selector above now also exercises frozen input versus later
report reconstruction, complete secondary programs, raw quiet-report retention,
source-specific output assembly and publication-clock staleness. Those cases
use **synthetic native inputs**, including when inspecting the live output
schema; they are not live evidence. Partial-startup cases use actual hosts
with normal, caller-drop, timeout and both panicked-startup-consumer paths.
The contained-startup regression invokes the actual fixture configuration
startup path on ordinary real hosts with synthetic observations, proves
receipt-based completion without serving access, rejects altered receipt and
independent-baseline evidence, and joins the owned hosts.
Neither these checks nor the single live association establishes
semantic history equivalence, retained progress, accepted replacement journeys,
redo-lag proof or lossless switchover.

### Upstream reporting and history blockers

The dependency selected by root `Cargo.toml` has these concrete integration
constraints. The upstream links identify source locations, not another
dependency pin:

| Boundary | Current constraint |
|---|---|
| [Registered host reporting](https://github.com/youyuanwu/kuberic/blob/main/kuberic-runtime/src/host/hosting.rs#L62-L83) | Status/report construction requires successful custom `current_progress`. Returning an unsupported-progress error would also disable existing registered-host report-dependent bootstrap/restart journeys. |
| [Provider-free report projection](https://github.com/youyuanwu/kuberic/blob/main/kuberic-runtime/src/host/hosting.rs#L665-L712) | The host synthesizes `first = 0` without a provider, or uses provider committed progress otherwise. It does not query custom `catch_up_capability` for that field. Returning `INVALID_LSN` from capability alone would not fix the report. |
| [Completed progress validation](https://github.com/youyuanwu/kuberic/blob/main/kuberic-runtime/src/protocol/public_operations.rs#L2168-L2195) | A completed report needs nonnegative ordered endpoints; it cannot express unknown progress with `INVALID_LSN`. Peer descriptors allowing unknown values do not relax this requirement. |
| [Public history consumers](https://github.com/youyuanwu/kuberic/blob/main/kuberic-controller/src/evaluator/public_v1_topology.rs) | Candidate selection compares report tails within accepted public history; retained rejoin uses history, storage/session/completion evidence, and report endpoints. Peer descriptors receive report `last` as current progress and `first` as capability. These are not Microsoft's native AG-sequence/quorum/failover protocol. |

The remaining adapter obligation is to establish how SQL-native evidence
corresponds to public history, role-specific durable progress, recovery-only
election evidence, and retained catch-up coverage across restart/history changes.
No conforming mapping is established by an AG sequence or by fitting a database
position into `i64`. A wider integer alone would not resolve these semantics.

The built-in engine's [completed-copy prefix proof](https://github.com/youyuanwu/kuberic/blob/main/kuberic-runtime/src/replicator/stateless/mod.rs#L1625-L1764)
handles quiet snapshot catch-up using exact live build/storage/session evidence.
MSSQL registers a custom replicator without a StateProvider or StateReplicator
([kuberic.rs:925-932](../crates/kuberic-mssql/src/kuberic.rs#L925-L932),
[kuberic.rs:1007-1039](../crates/kuberic-mssql/src/kuberic.rs#L1007-L1039));
it does not inherit that proof or gain native prefix/catch-up support from it.

The upstream [SF V1 semantic-alignment plan](https://github.com/youyuanwu/kuberic/blob/main/docs/features/kuberic/sf-v1-semantic-alignment-plan.md#L35-L84)
also distinguishes native SF queue semantics from the current local durability
and catch-up definitions. Its [reporting work](https://github.com/youyuanwu/kuberic/blob/main/docs/features/kuberic/sf-v1-semantic-alignment-plan.md#L240-L266)
calls for a coherent report/consumer correction rather than reinterpreting the
two existing integers in place. This is a future port plan, not an implemented
custom-adapter fix. Assess the selected contract and eventual native target
separately; neither certifies MSSQL's missing native evidence or permits an
application trait expansion.

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

The one-replica member of `just ci`'s serial live profile runs one Rust-owned
digest-pinned SQL Server 2025 Enterprise Developer container. It creates no AG,
endpoint or database. Separate allowed,
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
TDS. Its configuration-interruption cases require exactly one injected reply
loss at the named effect, observed mutation dispatch, confirmed host
termination and exact idempotent cleanup; source-recovery also reads FULL
recovery. They deliberately make no post-drain startup attempt or public
terminal-state inference: neither fresh-session rejection/admission nor
AG/seeding/marker convergence is proved after each lost reply. Those former
recovery assertions are removed, not supplied by an unrelated happy path.
Genuine interrupted-bootstrap renewal remains a separate unsupported gate.
Separate seed submission cases retain positive
same-session safe retry. Callback-publication windows use explicit probes/gates around real
callbacks, including before request submission, after submission, after SQL
acceptance with a lost reply, and after definite SQL rejection.
The post-native-success case stops requesting progress after Abort: it requires
the exact closed-source callback rejection, then confirms host drainage.
Completed-state replacement performs the actual native callback replay and
checks no duplicate `ADD DATABASE`, resolved temporary seed state and unchanged
three-member marker visibility. That is native callback evidence, not Public V1
Build success or renewal of an interrupted operation.

The conflict recipe has three isolated SQL-native cases observed through the
normal TDS path: convergence from a source database that inherits `SIMPLE`
recovery, refusal of an unowned same-name database, and refusal of an unowned
exact-profile same-name AG. Its remaining AG/replica/endpoint/database/lineage
cases are decoded-observation injections consumed by production validators; its
SQL-incarnation validator uses a real process restart plus a test gate.
Separate terminal-seed cases perform real post-compaction association removal
and AG recreation with zero redispatch. Target-local evidence cases use real
target SQL process restarts at three distinct windows. One synthetic-lineage
callback retains all three held-checkpoint/no-success observations before the
single final report-seam injection; it does not model three physical fork
changes. Every scenario performs exact cleanup. Physical restart requires an
independently observed new SQL start, a returned narrowly classified unsafe/unavailable or
bounded callback-budget error, and at most one seed submission. This proves
bounded callback failure without duplicate submission, not rollback, zero
effects or a specific incarnation-check branch; an admitted `ADD DATABASE` may
remain. Lineage mismatch still requires its strict conflict error.

The required GitHub Actions check `Shared CI validation` runs `just ci`,
including every existing ignored SQL fixture test through nextest's serial
live profile, and always runs `just cleanup`. This includes one-replica
recovery, three-replica fault/signal/conflict/target evidence, native commit/
prefix, the hosted witness and isolated lease/fence cases, not just happy
path/restart. The unrelated privileged ACL test is not selected.
Long aggregate loops are exposed as individually named scenario tests while
preserving fresh-fixture isolation and the single-slot SQL group. Each case
gets its own nextest result/timing; duplicate bootstrap handshake execution is
removed without dropping the separately selected handshake cases. The last
restart-checkpoint case retains its same-root positive retry as one atomic
journey, rather than relying on another test's order. Splitting does not
promise lower total runtime or impose a five-minute kill deadline.
Complete happy/restart executions are retained inside recovery retries rather
than scheduled a second time. Hosted witness retains the complete native
commit report; the native-only manual diagnostic keeps its separate blocked-
coverage acceptance semantics. Manual recipes and subprocesses use one gated
ordinary child helper, which starts no SQL without an explicit mode.
Its GitHub Actions job log is the
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

Run all existing SQL fixture suites serially, or recover their nested journals
within a selected runner namespace:

```bash
just test-live-nextest [runner-root]
just cleanup-live-nextest [runner-root]
```

The default runner namespace is `target/mssql-nextest-live`, with separate
one-replica, three-replica and lease-fence families. `just check` preserves
ordinary tests in those binaries and runs Cargo doctests separately.
The live recipe clears case-narrowing environments and uses ignored-only
selection; a live failure stops scheduling. Cleanup retains Removed journals
and refuses live/unknown owners or unverifiable resources. Per-invocation
nextest serialization is not a substitute for fixture ownership locks.

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

All these suites are selected by the explicit serial nextest live stage after
server-free checks in `just ci`; no inter-suite ordering is promised.
Narrow signal, recovery, fault and conflict recipes remain available. The shipped
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
