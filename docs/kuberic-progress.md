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

All topology callbacks return explicit observe-only errors. No callback returns
a dummy success or a fabricated catch-up value.

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

## Container Happy Path

`just ci` uses one digest-pinned SQL Server 2025 Enterprise Developer container.
The Rust tests, Kuberic testing runtime, observer, and SQL client remain host
processes.

The fixture creates one metadata-only EXTERNAL availability group:

- one local PRIMARY replica definition;
- two configured but unstarted peer definitions;
- synchronous commit, EXTERNAL failover, and automatic seeding for all three;
- required synchronized secondaries equal to one;
- no managed database; and
- no database-mirroring endpoint, endpoint certificate, database master key,
  write lease operation, join, seeding, or failover.

SQL Server accepts endpoint URL metadata without a running endpoint. This is
enough to expose and test the AG configuration sequence, but it proves no data
replication or HA behavior.

The fixture ownership record binds the exact container ID and created AG group
ID/profile. A same-name AG without that record is refused. Cleanup removes the
exact AG before preserving a borrowed container. Removing a fixture-created
container removes its writable SQL metadata. Interrupted cleanup retains the
record and fails rather than adopting or deleting ambiguous state.

The required live Kuberic test:

1. opens the observe-only service through Kuberic's published testing runtime;
2. obtains current progress through the Kuberic runtime snapshot;
3. makes a fresh direct SQL observation; and
4. verifies both paths report the same positive configuration sequence.

## Usage

The standalone observer requires no Kuberic dependency:

```bash
cargo build --locked --bin sqlserver-observer
```

Build or test the adapter through the feature:

```bash
cargo test --locked --features kuberic-testing --test kuberic_contract
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

## Deferred Work

This integration does not yet provide:

- a production executable hosting SQL Server through `ReplicaHost`;
- SQL Server process ownership or restart containment;
- a managed user database;
- endpoint certificate provisioning or rotation;
- replica join, automatic seeding, rebuild, or reseed;
- database-progress build and catch-up proofs;
- application read/write access fencing;
- external write-lease renewal and expiry evidence;
- sequence quorum collection across exact replica sessions;
- planned switchover, automatic or forced failover, or data-loss recovery;
- Kubernetes Pod, PVC, Secret, Service, listener, routing, or installation
  convergence; or
- multi-container and Kubernetes fault testing.

Those stages require separate design and safety review.

## References

- [SQL Server AG catalog view](https://learn.microsoft.com/en-us/sql/relational-databases/system-catalog-views/sys-availability-groups-transact-sql)
- [SQL Server AG replica-state DMV](https://learn.microsoft.com/en-us/sql/relational-databases/system-dynamic-management-objects/sys-dm-hadr-database-replica-states-transact-sql)
- [Microsoft SQL Server HA resource agents](https://github.com/microsoft/mssql-server-ha)
- [Kuberic PostgreSQL design](https://github.com/youyuanwu/kuberic/blob/main/docs/features/postgres/design.md)
- [Kuberic operator comparison](https://github.com/youyuanwu/kuberic/blob/main/docs/background/operator-reconciliation-comparison.md)
