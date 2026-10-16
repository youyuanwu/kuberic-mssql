# SQL Server HA Fencing, Lease, and Failover Research

> **Research date:** 2026-10-16
>
> **Scope:** Fencing, lease, health detection, and failover behavior. Authentication
> and mutation transport are intentionally outside this research.
>
> **Status:** Design input only. This document does not enable SQL Server role
> changes, lease renewal, failover, or destructive recovery in this repository.

## Conclusion

The closest public reference for SQL Server failover authority is
Microsoft's [`mssql-server-ha`](https://github.com/microsoft/mssql-server-ha)
repository. It is a source snapshot of the Linux Pacemaker resource agents for
availability groups and failover cluster instances, not a Kubernetes operator.
Its availability-group agent combines three different safety mechanisms:

1. Pacemaker quorum and resource ownership decide which node may act.
2. SQL Server's external write lease limits how long a primary can continue
   writing without renewed cluster authority.
3. Pacemaker fencing (STONITH) ensures an unresponsive old node cannot keep
   accessing resources or restart behind the cluster's decision.

These mechanisms are complementary. A Kubernetes leader-election lease alone
does not fence SQL Server, SQL Server's write lease does not prove that an old
host is powered off, and changing a Service or listener only redirects new
client connections.

For Kuberic MSSQL, the strongest precedent is therefore to follow the published
SQL Server external-cluster protocol and retain an explicit exact-incarnation
fence. The adapter must not reduce failover to an isolated `ALTER AVAILABILITY
GROUP ... FAILOVER` statement.

## Microsoft `mssql-server-ha`

The repository README describes the project as a snapshot of the agents shipped
in the `mssql-server-ha` package. The currently published implementations are:

- `ocf:mssql:ag` for Always On availability groups; and
- `ocf:mssql:fci` for failover cluster instances.

The README says Kubernetes agents are future work. The repository is therefore
the protocol reference for SQL Server on Linux under Pacemaker, not evidence of
a Microsoft-supported SQL Server Kubernetes HA controller.

Relevant repository locations:

- `README.md`
- `ag/ag`
- `ag/docs/ag_metadata`
- `go/src/ag-helper/main.go`
- `go/src/mssqlcommon/ag/lib.go`

### Health and promotion eligibility

The AG agent delegates native work to `ag-helper`. The published implementation
uses `sp_server_diagnostics` for instance health and SQL catalog/DMV evidence
for AG health. Its monitor policy can react at several severities, from an
unresponsive or stopped server through qualified moderate or critical errors.

The agent also publishes and compares SQL Server AG configuration sequence
numbers. Promotion is not based only on a replica reporting `SECONDARY` and
`SYNCHRONIZED`: a candidate behind the maximum accepted configuration sequence
is rejected, sequence zero is not promotable, and the cluster needs enough
sequence reports to establish current authority.

This matches Kuberic MSSQL's current use of
`sys.availability_groups.sequence_number` as signed 64-bit configuration
progress. It does not make database-native `numeric(25,0)` hardened, redone, or
committed positions interchangeable with configuration authority.

### External write lease

The resource-agent metadata exposes `primary_write_lease_duration`. The shell
agent defaults it to the monitor interval plus monitor timeout plus two seconds.
The helper renews the SQL Server external write lease with:

```sql
ALTER AVAILABILITY GROUP ... SET (WRITE_LEASE_VALIDITY = ...)
```

The helper reports the resulting lease-expiry timestamp back to Pacemaker
attributes. If availability-group configuration updates cannot commit for the
configured quorum timeout, the primary monitor path stops renewing the lease
and attempts to take the AG offline.

The important property is not the exact default duration. The safety property
is that lease renewal remains coupled to current cluster authority and bounded
monitor timing. The monitor interval, action timeout, lease duration, cluster
recheck timing, and worst-case scheduling delay form one failover budget and
cannot be tuned independently.

### Promotion and demotion

The published helper's normal promotion sequence renews the external write
lease, requests failover through SQL Server's external-cluster session context,
and waits for the local role to become `PRIMARY`.

The SQL library sets the session context used by an external cluster manager
before issuing either:

```sql
ALTER AVAILABILITY GROUP ... FAILOVER
```

or the explicitly destructive:

```sql
ALTER AVAILABILITY GROUP ... FORCE_FAILOVER_ALLOW_DATA_LOSS
```

Demotion prefers taking the old primary offline. If the normal offline path
cannot complete, the agent can wait for the external write lease to expire.
This is materially different from treating a successful SQL command response
as proof that the distributed role transition is complete.

### Quorum loss

The agent exposes `disable_primary_on_quorum_timeout_after`. Its primary monitor
uses this threshold when required synchronous or configuration-only replicas
cannot acknowledge configuration changes. Once the threshold is exceeded, the
agent stops extending write authority rather than allowing the primary to
continue indefinitely without the configured cluster evidence.

Microsoft's Linux guidance recommends either:

- three synchronous replicas; or
- a primary, secondary, and configuration-only replica.

The current Kuberic MSSQL profile uses three synchronous data replicas and
`REQUIRED_SYNCHRONIZED_SECONDARIES_TO_COMMIT = 1`, which is compatible with the
first topology but does not yet implement the corresponding lease and failover
protocol.

## Pacemaker Fencing

Microsoft's Linux AG documentation requires proper quorum and fencing for
production Pacemaker clusters. Pacemaker's own documentation explains fencing
as the mechanism that makes an unresponsive node known to be offline before
resources are activated elsewhere.

Pacemaker distinguishes power fencing from fabric fencing. A fencing device
must remain reachable through failure conditions that may make the target node
unreachable; a nominal fence implemented through the same failed host or path
does not establish safety.

For SQL Server AGs, fencing and the SQL external write lease protect different
boundaries:

| Mechanism | Protects |
|---|---|
| Pacemaker quorum/resource ownership | Which cluster partition may direct the resource |
| SQL external write lease | How long SQL Server may retain write authority without renewal |
| STONITH or fabric fencing | Whether the exact old node can continue or regain access |
| Listener/VIP movement | Where new client connections are directed |

Listener movement is therefore not a fence. Existing TDS sessions can remain
connected to an old primary, and direct instance connections bypass the
listener entirely.

## Windows Server Failover Clustering

Windows Server Failover Clustering uses the same separation between failure
detection and write authority, although its implementation differs from
Pacemaker.

The SQL Server instance and the WSFC resource host maintain a lease. Lease
expiry causes the availability group to transition away from normal primary
operation and exists specifically to avoid split brain when the resource DLL or
cluster service cannot complete a normal offline.

WSFC separately exposes:

- **health-check timeout**, driven by `sp_server_diagnostics`; and
- **failure-condition level**, which controls which classes of server failure
  are severe enough to trigger automatic failover.

The default health-check timeout is 30 seconds, with diagnostics evaluated at
one-third of that interval. Failure-condition levels progressively include
server-down/lease expiry, unresponsive server, critical errors, moderate
errors, and any qualified error.

The lesson for Kuberic is that "unhealthy," "lease expired," and "eligible to
promote this specific replica" are separate states. A single readiness Boolean
cannot safely represent them.

## Kubernetes Operators

### Microsoft support boundary

No current Microsoft-maintained open-source SQL Server Kubernetes HA operator
was identified. The `mssql-server-ha` README still describes Kubernetes agents
as future work. Microsoft's SQL Server container HA guidance primarily
describes Kubernetes restarting a single SQL Server Pod and reattaching
persistent storage, which is instance recovery rather than multi-replica AG
arbitration.

### KubeDB

KubeDB publishes an MSSQL `AvailabilityGroup` topology. Its examples use three
replicas, and its API includes leader-election configuration and an arbiter
default for even replica counts.

This demonstrates useful Kubernetes packaging and control-plane patterns, but
the public CRD shape is not itself evidence of SQL Server-native fencing. A
Kubernetes Lease establishes controller leadership; it does not by itself:

- expire SQL Server's external write authority;
- terminate direct TDS writes to an old primary;
- fence an old node or storage path; or
- prove SQL Server's external-cluster failover protocol completed.

KubeDB is therefore useful as an operator topology comparison, not as a
replacement for the Microsoft resource-agent protocol.

## Azure SQL Database and Managed Instance

Azure SQL Database and Azure SQL Managed Instance expose managed HA behavior
rather than customer-controlled cluster internals.

Microsoft publicly documents:

- built-in local high availability and optional zone redundancy;
- Service Fabric health management and compute failover for General Purpose
  architectures;
- Always On availability-group technology underneath Managed Instance Business
  Critical;
- stable read-write and read-only failover-group listener endpoints;
- planned and forced failover behavior; and
- a configurable grace period before Microsoft-managed geo-failover may accept
  data loss.

Managed Instance also documents recurring internal connectivity tests.

Microsoft does not publicly expose a customer-tunable Pacemaker/WSFC-style
write lease or STONITH setting for these managed services. Azure comparisons
must therefore remain behavioral:

- clients use stable listener names and retry transient failures;
- planned failover aims for no data loss;
- forced failover can lose data;
- zone placement and service health influence availability; and
- the platform, not the tenant, owns replica fencing.

It would be an unsupported inference to claim that Azure SQL Database uses the
same public lease or fencing implementation as `mssql-server-ha`.

## Comparison

| Environment | Authority owner | Failure detection | Write/fence mechanism | Failover interface |
|---|---|---|---|---|
| Linux AG with Pacemaker | Pacemaker | Resource-agent monitor, `sp_server_diagnostics`, SQL state, timeouts | SQL external write lease plus production STONITH/fabric fencing | External-cluster `FAILOVER`; forced data-loss path is separate |
| Windows AG with WSFC | WSFC and AG resource DLL | Health-check timeout and failure-condition level | SQL/WSFC lease plus cluster node/resource management | WSFC-coordinated automatic or planned failover |
| KubeDB MSSQL AG | Kubernetes operator/controller | Operator and Kubernetes observations, leader election | Public CRD/controller leadership is visible; equivalent SQL/node fencing was not established by this research | Operator-managed AG topology |
| Azure SQL Database | Azure platform | Managed service health and replica monitoring | Internal platform mechanism is not customer exposed | Service failover and failover-group listeners |
| Azure SQL Managed Instance | Azure platform | Managed service health, replica monitoring, connectivity tests | Internal platform mechanism is not customer exposed | Service failover and failover-group listeners |

## Implications for Kuberic MSSQL

### 1. Preserve separate authority domains

Kuberic should model at least:

- durable Kuberic role/configuration authority;
- current SQL Server AG configuration sequence;
- exact SQL Server process incarnation;
- local native role and synchronization health;
- external write-lease expiry and renewal ownership;
- old-primary fence status; and
- client listener/routing status.

None of these should be inferred from another. In particular, listener movement
does not prove fencing, and native role alone does not prove current cluster
authority.

### 2. Couple lease renewal to exact current authority

A future lease renewer should act only while the exact local SQL Server
incarnation remains the accepted primary under current durable Kuberic
authority. Losing quorum, superseding the lifecycle/configuration epoch, or
starting shutdown must stop further renewal.

Renewal needs stale-completion fencing: a delayed renewal started under an old
epoch must not extend write authority after that epoch has been revoked.

### 3. Require an old-primary fence before replacement writes

Before making a replacement primary writable, Kuberic should require one of the
design-approved proofs for the exact old-primary incarnation, such as:

- observed expiry of SQL Server's external write lease with no actor still
  authorized to renew it; or
- a verified infrastructure/process fence that prevents that exact incarnation
  from writing or restarting.

A Pod deletion request, failed readiness probe, missing observation, Kubernetes
Lease expiry, Service update, or role-change command response is not sufficient
proof.

### 4. Follow the full external-cluster protocol

The implementation should use a pinned and tested Microsoft helper or faithfully
reproduce the published protocol, including:

- sequence-number arbitration;
- required synchronized-secondary policy;
- health and monitor timing;
- write-lease renewal and expiry;
- external-cluster session context;
- promotion/demotion ordering;
- native postcondition checks; and
- normal versus forced data-loss failover.

Copying only the visible T-SQL statements would omit the distributed safety
conditions around them.

### 5. Separate normal failover from data-loss recovery

Normal failover should require the selected secondary to satisfy exact current
configuration and synchronization requirements. `FORCE_FAILOVER_ALLOW_DATA_LOSS`
must remain a separately approved recovery operation with explicit evidence
that normal failover is unavailable and with the possible data-loss boundary
recorded.

Azure failover-group grace periods reinforce the same distinction: delayed
forced recovery is a policy decision, not an ordinary automatic failover.

### 6. Treat timing as one calculated budget

The following durations interact:

- observation and diagnostics interval;
- monitor timeout;
- Kuberic reconciliation and scheduling delay;
- write-lease duration;
- quorum-loss timeout;
- fence execution timeout;
- promotion timeout; and
- client DNS/listener retry behavior.

The design should calculate and test a coherent upper bound. Independent
defaults can create either premature failover or an unexpectedly long
unavailable-but-still-authoritative primary.

### 7. Test partitions and stale actors, not only clean shutdown

The minimum future failover validation should include:

- clean planned switchover;
- old primary alive but controller path partitioned;
- old primary SQL reachable while Kuberic is unreachable;
- Kuberic leader loss during lease renewal;
- delayed renewal completion after authority revocation;
- candidate behind the maximum AG configuration sequence;
- loss of required synchronized replicas;
- SQL process restart with a new incarnation;
- failed or delayed infrastructure fence;
- normal failover command ambiguity; and
- explicitly approved forced failover with measurable data loss.

## Recommended Design Direction

Authentication can be deferred without changing the fencing model. The next
design should first specify:

1. the durable Kuberic lease/epoch that authorizes one exact replica to renew
   SQL Server's external write lease;
2. the SQL-visible and application-visible lease-expiry evidence;
3. the exact-incarnation process or infrastructure fence;
4. the candidate eligibility calculation using AG configuration sequence,
   synchronization, health, topology, and lineage;
5. the ordered normal promotion/demotion state machine and its replay rules;
6. the separate forced-data-loss recovery state machine; and
7. the combined timing budget and partition test matrix.

This keeps the current runtime observe-only until the full authority and
failure model is reviewable.

## Sources

### Microsoft SQL Server on Linux

- [`microsoft/mssql-server-ha`](https://github.com/microsoft/mssql-server-ha)
- [Always On availability groups on Linux](https://learn.microsoft.com/en-us/sql/linux/business-continuity/availability-groups/overview?view=sql-server-ver17)
- [Configure a Pacemaker cluster for SQL Server availability groups](https://learn.microsoft.com/en-us/sql/linux/business-continuity/availability-groups/cluster-pacemaker?view=sql-server-ver17)
- [Failover and failover modes for availability groups on Linux](https://learn.microsoft.com/en-us/sql/linux/business-continuity/availability-groups/failover-high-availability?view=sql-server-ver17)
- [SQL Server containers and Kubernetes high availability](https://learn.microsoft.com/en-us/sql/linux/business-continuity/containers/high-availability-overview?view=sql-server-ver17)

### Pacemaker and WSFC

- [Pacemaker fencing](https://github.com/ClusterLabs/pacemaker/blob/main/doc/sphinx/Clusters_from_Scratch/fencing.rst)
- [Availability-group lease, health-check timeout, and lease timeout](https://learn.microsoft.com/en-us/sql/database-engine/availability-groups/windows/availability-group-lease-healthcheck-timeout?view=sql-server-ver17)
- [Flexible automatic failover policy](https://learn.microsoft.com/en-us/sql/database-engine/availability-groups/windows/configure-flexible-automatic-failover-policy?view=sql-server-ver17)

### Kubernetes operator

- [KubeDB SQL Server availability-group guide](https://kubedb.com/docs/v2026.10.12/guides/mssqlserver/clustering/ag_cluster/)
- [`kubedb/apimachinery` MSSQL API](https://github.com/kubedb/apimachinery/blob/master/apis/kubedb/v1alpha2/mssqlserver_types.go)
- [`kubedb/apimachinery` MSSQL defaults](https://github.com/kubedb/apimachinery/blob/master/apis/kubedb/v1alpha2/mssqlserver_helpers.go)

### Azure SQL

- [Azure SQL Database high availability and zone redundancy](https://learn.microsoft.com/en-us/azure/azure-sql/database/high-availability-sla-local-zone-redundancy?view=azuresql)
- [Azure SQL Database failover groups](https://learn.microsoft.com/en-us/azure/azure-sql/database/failover-group-sql-db?view=azuresql)
- [Azure SQL Managed Instance high availability and zone redundancy](https://learn.microsoft.com/en-us/azure/azure-sql/managed-instance/high-availability-sla-local-zone-redundancy?view=azuresql)
- [Azure SQL Managed Instance failover groups](https://learn.microsoft.com/en-us/azure/azure-sql/managed-instance/failover-group-sql-mi?view=azuresql)
