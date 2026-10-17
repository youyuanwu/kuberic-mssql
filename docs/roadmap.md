# Kuberic MSSQL Future Roadmap

> **Status:** Future work only.
>
> Completed implementation is documented in [the design](design.md) and
> [progress integration guide](kuberic-progress.md). This roadmap intentionally
> does not repeat completed stages.

## Planning Principles

- Ordinary production construction remains observe-only until every safety gate
  for a mutation is implemented and tested.
- Preserve Kuberic's Service Fabric-shaped public traits and satisfy their
  documented semantics. Matching signatures or another adapter's behavior is
  not conformance evidence.
- SQL Server's published external-cluster protocol is the native reference for
  lease, promotion, demotion, and failover behavior.
- Kuberic authority, SQL Server write authority, exact process incarnation,
  infrastructure fencing, and client routing remain separate evidence domains.
- Normal failover, destructive replacement, and forced data-loss recovery are
  separate operations with separate admission criteria.
- Authentication is not part of the immediate roadmap. Deferring it does not
  weaken the lease, fencing, replay, identity, or native-postcondition
  requirements.

## Stage 6: SQL-Native Lease, Fencing, and Role Movement

Stage 6 establishes safe SQL Server authority transitions. Production failover
remains disabled until the complete normal-failover path has a real
exact-incarnation fence provider and passes the live partition matrix.

The implementation should follow the findings in
[SQL Server HA fencing, lease, and failover research](mssql-ha-fencing-lease-failover.md).

The implemented [lease and fence foundation](design.md#lease-and-fence-foundation)
defines the starting boundary. Before enabling a production transition, connect
that model to durable current authority, exclude outstanding predecessor
renewals across restart, and supply a production exact-incarnation fence.
Fixture observations and local deadlines are not those admission proofs.

### Stage 6A Follow-up: Replicator Semantic Conformance

Close the gap between the implemented observe-only/bootstrap adapter and
[Kuberic's documented Replicator semantics](design.md#kuberic-and-service-fabric-semantics)
before proceeding to planned switchover. The completed lease/fence foundation
remains implemented; this follow-up does not replace its native evidence.
Application-facing Kuberic/SF trait APIs are fixed and must not be changed or
expanded for this work.

Planned work:

- establish the public progress/history model for primary committed progress,
  secondary contiguous durable progress, access-closed recovery, and retained
  catch-up capability, including restart, history changes and unavailable
  evidence;
- resolve the [host reporting/history constraints](kuberic-progress.md#upstream-reporting-and-history-blockers):
  mandatory successful current progress, provider-free synthetic `first = 0`,
  custom capability not projected into reports, and no unknown completed
  progress range; preserve compatibility only as an explicit interim boundary;
- use separate native AG election evidence without treating configuration
  freshness as transaction durability or narrowing native `numeric(25,0)`
  positions to the public signed-64-bit slot;
- resolve custom descendant ownership under the existing Close/Abort contracts
  and the concrete MSSQL process owner, without adding an application-facing
  witness/drain hook; generic host completion after Abort or an interrupted
  terminal root must not be treated as proof that custom descendants drained;
- resolve terminal reporting without fabricated progress: the current public
  Close drains the adapter but its subsequent report returns
  `Unavailable`/Closed; assess the host/reporting implementation rather than
  requiring terminal progress or another application callback;
- separate idle nonvoting build targets from active membership, prove exact
  build completion before admission, and replace partial bootstrap success
  with the documented public configuration postconditions;
- define native replication endpoint ownership and partition read/write
  admission independently of role and routing;
- exercise the supported configuration/build/lifecycle path through real
  public `ReplicaHost` operations, not only direct test-only callbacks; and
- verify upstream host/planner conformance, recording any unresolved blockers
  from Kuberic's Service Fabric audit rather than copying example behavior.

Exit criteria:

- the progress/history model satisfies the documented semantics without
  fabricated progress or comparisons across incompatible histories;
- actual hosted reports and peer descriptors preserve the proven custom
  progress/capability meaning, including unavailable evidence, rather than
  relying on compatibility placeholders or provider-free synthetic history;
- a superseded callback cannot dispatch new work or publish success, and
  unresolved predecessor effects cannot authorize successor writes;
- hosted Close, Abort and shutdown satisfy the fixed lifecycle contracts, and
  any claim of completed custom-descendant containment is backed by actual joins,
  without requiring graceful work after Abort;
- an idle build target does not vote, and public build/configuration completion
  certifies the exact supported postconditions;
- unsupported operations remain explicit errors and ordinary production
  construction remains observe-only; and
- hosted conformance coverage passes, with upstream blockers resolved before
  enabling any dependent operation.

### Stage 6B: Planned Switchover

Implement the first controlled role movement while all three replicas and the
current primary are reachable. The semantic-conformance follow-up is a
prerequisite, not an optional part of later Kubernetes integration.

Planned work:

- select a synchronized and healthy target at the accepted maximum AG
  configuration sequence, with separate valid data-progress/history evidence;
- implement the supported current/previous-configuration catch-up predicates
  and required-successor condition without claiming unsupported quorum modes;
- perform initial catch-up, revoke write access, and complete final catch-up
  before demoting the source; keep its primary role until that work completes;
- stop old-primary lease renewal under durable authority;
- verify lease revocation and the required exact-incarnation fence;
- execute the published external-cluster demotion and promotion ordering;
- wait for exact native role, configuration, synchronization, and lineage
  postconditions;
- grant target lease renewal only after those postconditions hold;
- honor partition read/write admission throughout native serving and
  revocation; role, lease renewal, or routing alone is not an access grant;
- retain durable replay evidence for every authority-changing step; and
- leave listener and client-routing changes outside the role transition until
  native authority is proven.

Exit criteria:

- planned switchover survives cancellation, lost replies, and runtime restart;
- the actual public-host operation sequence proves catch-up before and after
  write revocation, correct role ordering, and access refusal until admission;
- no step can make two replicas writable;
- replay converges from durable Kuberic and native SQL evidence; and
- the old primary cannot regain write authority after the target is admitted.

### Stage 6C: Automatic Failover and Partitions

Extend normal failover to an unavailable or partitioned primary without using
the data-loss path.

Planned work:

- collect sequence and health evidence from exact current replica sessions;
- require the selected candidate to satisfy synchronization, topology,
  configuration-sequence, lineage, and incarnation rules;
- distinguish controller partition, SQL partition, node failure, and stale
  observation;
- stop or supersede every old-primary lease-renewal actor;
- require lease-expiry and exact fence evidence before replacement writes;
- handle ambiguous failover responses by reobserving native postconditions;
- test delayed fencing, failed fencing, stale leaders, and asymmetric
  partitions; and
- keep failover unavailable when quorum or fencing evidence is insufficient.

Exit criteria:

- all supported single-failure and partition cases either converge to one
  writable primary or remain safely unavailable;
- no missing observation is interpreted as a completed fence;
- a delayed actor cannot restore old-primary authority; and
- the complete live fault matrix passes with licensed three-replica fixtures.

### Stage 6D: Destructive Reseed and Member Replacement

Add destructive convergence only after target fencing is proven independently
of failover.

Planned work:

- define replacement identity and storage ownership for a removed or recreated
  member;
- prove the target cannot still serve or rejoin with superseded data;
- bind destructive approval to the exact operation, input signature, target
  incarnation, AG identity, database identity, and recovery lineage;
- classify safe retry, ambiguous completion, native conflict, and terminal
  unsafe states;
- implement removal, cleanup, rejoin, and reseed as separate observable steps;
- preserve current-primary write authority throughout target replacement; and
- test process restart, storage reuse, copied roots, partial cleanup, and
  interrupted seeding.

Exit criteria:

- destructive work cannot target the current primary;
- superseded storage or process identity cannot satisfy a new replacement;
- retries do not repeat an already completed destructive effect; and
- healthy surviving replicas retain one unambiguous write authority.

### Stage 6E: Forced Failover and Data-Loss Recovery

Treat `FORCE_FAILOVER_ALLOW_DATA_LOSS` as an exceptional recovery workflow, not
an automatic-failover fallback.

Planned work:

- require explicit selection of the forced recovery mode;
- record why normal failover is unavailable;
- identify the best surviving candidate using exact native evidence without
  claiming nonexistent losslessness;
- record the last known source and target progress and the possible data-loss
  boundary;
- require old-primary lease expiry and exact fence evidence;
- execute and reconcile the external-cluster forced-failover protocol;
- prevent an old primary from rejoining without explicit destructive recovery;
  and
- expose the resulting degraded/recovery-required state to operators.

Exit criteria:

- normal automation cannot enter the forced path;
- the possible data-loss boundary is retained durably;
- the old primary cannot rejoin as current data; and
- recovery tests demonstrate both acknowledged loss and safe refusal.

## Stage 7: Controller and Kubernetes Resource Integration

Integrate the completed native safety model with Kuberic's public controller
contract for Kubernetes resource orchestration. Public-host operation and
access-sequencing conformance is required earlier, not deferred to this stage.

Planned work:

- converge Pods, PVCs, Secrets, Services, and stable replica identities through
  the public-only contract;
- provide a production fence provider appropriate to the selected Kubernetes
  and infrastructure environment;
- preserve exact SQL Server incarnation across controller and Pod lifecycle
  events;
- publish client routing only after native write authority is established;
- coordinate listener or Service movement with role postconditions without
  treating routing as fencing;
- add upgrade and replacement behavior that cannot adopt copied or stale
  application roots; and
- keep deployment, EULA acceptance, and credential provisioning explicit
  operator responsibilities.

Exit criteria:

- controller restart and leader change preserve native authority;
- Pod deletion and recreation cannot bypass fencing;
- PVC reuse and replacement follow explicit identity rules; and
- routing never advertises a replica before native write admission.

## Stage 8: Kubernetes End-to-End and Operations

Validate and document the complete operational system in an isolated licensed
environment.

Planned work:

- create a pinned three-replica licensed Kubernetes fixture;
- test process, Pod, node, network, controller, and storage failures;
- test planned switchover, automatic failover, replacement, reseed, and forced
  recovery independently;
- validate client reconnect and listener/DNS timing;
- validate rolling SQL Server and Kuberic upgrades;
- measure the complete failure-detection, lease-expiry, fence, promotion, and
  client-recovery budget;
- provide backup, restore, certificate, secret-rotation, upgrade, and incident
  runbooks; and
- keep live licensed and destructive tests explicitly provisioned and ignored
  by default outside their dedicated environment.

Exit criteria:

- the supported fault matrix passes without dual-write authority;
- operational timing matches documented bounds;
- upgrades and rollback preserve exact authority and lineage; and
- runbooks cover safe refusal as well as successful recovery.
