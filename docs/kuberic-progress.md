# Kuberic SQL Server Progress Integration

## Purpose

The optional Kuberic integration exposes SQL Server availability-group progress
through Kuberic's fixed Service Fabric-compatible `Replicator` contract. It does
not change Kuberic's API and does not convert SQL Server database log positions
into a generic scalar.

The integration is deliberately observe-only. It can open as a Kuberic
application, validate a requested native role, and report current progress. It
cannot build or remove replicas, change availability-group configuration, wait
for catch-up, handle data loss, seed a database, grant write access, renew a
lease, switch over, or fail over.

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

An exactly bound healthy topology enables two additional current-only
operations. `HealthyTopologyBinding` freezes the shared resource, local and peer
Kuberic identities, process sessions, replication addresses, stable roles,
current configuration, effective policy, SQL Server process incarnations,
native replica GUIDs, AG identity, and database lineage.

`update_current_replica_set_configuration` accepts only the frozen current
descriptor and exact peer descriptions; after admission, only value-identical
replay is accepted. `catch_up_capability` is then reported as a fresh
configuration sequence equal to current progress. Both paths reobserve SQL
Server and revalidate the durable runtime authority and effective policy. Any
identity, session, epoch, role, health, synchronization, lineage, incarnation,
freshness, or policy drift fails closed.

This is a conservative current-only capability: the adapter claims no earlier
retained configuration history. It is not a database LSN and does not imply
replica build, repair, catch-up, data-loss recovery, lease, or failover support.
The locked controller comparison treats a member at the current sequence or one
behind as not requiring full repair, and a member two or more behind as
requiring it. Contract tests preserve that exact boundary.

Unbound adapters still reject both operations. Previous/current transitions,
catch-up configuration and quorum, build, removal, data loss, recovery, lease,
switchover, and failover callbacks remain explicit observe-only errors. No
callback returns a dummy success or fabricated value.

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

Kuberic's replica-local runtime, rather than the Kubernetes controller, should
eventually own SQL bootstrap, join, seeding, lease, and role transitions under
durable Kuberic authority.

## Live Validation Paths

### Legacy single-container observation fixture

`just ci` retains one digest-pinned SQL Server 2025 Enterprise Developer
container. It creates one metadata-only EXTERNAL AG with one local primary
definition and two configured but unstarted peers. It has no managed database,
running HADR endpoint, join, seeding, write lease, or failover. This fixture
proves real-engine observation and single-runtime configuration-progress
publication, not data replication or HA.

Its private ownership record persists the nonce-bearing AG name before create,
then binds the exact group ID and profile. Cleanup revalidates those values in
the destructive batch. This fixture also retains its ambient
`SQLSERVER_TEST_EULA_ACCEPTED` compatibility gate.

### Three-member native and Kuberic happy path

The dedicated ignored path starts three real containers from:

`mcr.microsoft.com/mssql/server@sha256:2b5b581621126574f3d1f75e78d3eebe8d05aedb59ad0cfdf9aa42cb0634d726`

The SQL Server image reports version `17.0.5005.3`. The fixture requires a
strict version-one affirmative acknowledgement file; it does not accept the
legacy ambient variable.

Fixture-only administration creates and proves:

- one certificate-authenticated, started HADR endpoint on every member;
- peer certificate users/logins with exact endpoint `CONNECT`;
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

`MssqlGroup` then creates three isolated SQLite stores, services, replicators,
runtimes, process sessions, and agent servers for one shared Kuberic resource.
Every ordered peer pair is registered and described before the exact current
configuration is admitted. Reports are bracketed by fresh direct observations
and must show:

- one shared resource and current configuration with no previous
  configuration;
- three exact identities and process sessions;
- one Kuberic primary and two active secondaries matching native roles;
- current progress and catch-up capability equal to the corresponding fresh
  native configuration sequence;
- healthy initialized durable state; and
- fenced access: read remains reconfiguration-pending and write is never
  granted.

Role is not write authority. No SQL external write lease is acquired. The
replicated marker is written only by the fixture administrator against the
directly observed native primary.

The pre-implementation feasibility run completed the native path on the same
pinned image with one primary, two synchronized healthy secondaries, common
configuration sequence `4294967307`, and the same marker visible on all three.
The final ignored test additionally proves the three fenced Kuberic reports and
exact cleanup/recovery behavior.

The workspace temporarily pins Kuberic commit
`301d7f364744aea4dd2513dcc8179d3588fd7dd7`, tracked by open
[Kuberic PR #124](https://github.com/youyuanwu/kuberic/pull/124), so custom
authority validation completes before durable publication. The pin remains
until a suitable upstream merge or release is available and does not predict
the pull request's outcome.

## Usage

The standalone observer requires no Kuberic dependency:

```bash
cargo build --locked -p kuberic-mssql --bin sqlserver-observer
```

Build or test the adapter through the feature:

```bash
cargo test --locked -p kuberic-mssql-tests --test kuberic_contract
```

Run the complete server-free and single-container gate:

```bash
just ci /absolute/path/to/fixture-directory
```

Targeted prepared-fixture commands are:

```bash
just test-live
just test-live-all
just test-live-kuberic
just test-live-shared
```

Live tests remain ignored for direct Cargo invocation and require explicit
licensed fixture provisioning.

Run the real three-member path with a reviewed acknowledgement file:

```bash
just test-live-three-replica <file> [root]
```

Recover the default or a selected journaled root:

```bash
just cleanup-live-three-replica [root]
```

Exercise handled SIGTERM, recovery, retry, and idempotent cleanup:

```bash
just test-live-three-replica-signal <file> [root]
```

The shipped acknowledgement example contains `accepted: false`; it parses but
cannot authorize launch until a contributor reviews the license and explicitly
changes the value to `true`.

## Deferred Work

This integration does not yet provide:

- a production executable hosting SQL Server through `ReplicaHost`;
- SQL Server process ownership or restart containment;
- production managed-database creation;
- production endpoint certificate provisioning or rotation;
- production replica join, automatic seeding, rebuild, or reseed;
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

## References

- [SQL Server AG catalog view](https://learn.microsoft.com/en-us/sql/relational-databases/system-catalog-views/sys-availability-groups-transact-sql)
- [SQL Server AG replica-state DMV](https://learn.microsoft.com/en-us/sql/relational-databases/system-dynamic-management-objects/sys-dm-hadr-database-replica-states-transact-sql)
- [Microsoft SQL Server HA resource agents](https://github.com/microsoft/mssql-server-ha)
- [Kuberic PostgreSQL design](https://github.com/youyuanwu/kuberic/blob/main/docs/features/postgres/design.md)
- [Kuberic operator comparison](https://github.com/youyuanwu/kuberic/blob/main/docs/background/operator-reconciliation-comparison.md)
