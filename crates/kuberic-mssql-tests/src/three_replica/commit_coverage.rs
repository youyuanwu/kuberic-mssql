use std::fmt;
use std::future::Future;
use std::time::Duration;

use kuberic_mssql::instance::{SqlServerInstanceManager, unix_millis};
use kuberic_mssql::observation::{InstanceSnapshot, NativeProvenance, RecoveryLineageObservation};
use kuberic_mssql::runtime_config::ObserverConfig;
use kuberic_mssql::tds::TdsExecutor;
use kuberic_mssql::{DecimalProgress, NativeRole, Observation};
use serde::Serialize;
use tiberius::ToSql;
use tokio::time::{Instant, sleep};

use crate::fixture::admin::{AdminError, AdminSession, required_text, single_row};
use crate::fixture::deadline::{complete_before, earlier};

use super::availability_group::AvailabilityGroupError;
use super::data::{self, DataContext, DataError};
use super::member::ReadyMember;
use super::model::NativeTopologyBinding;

const PROBE_BUDGET: Duration = Duration::from_secs(30);
const SAMPLE_WINDOW: Duration = Duration::from_secs(2);
const MAX_OVERLAP_SAMPLES: usize = 16;
const MAX_SAMPLE_AGE: Duration = Duration::from_secs(30);

pub(super) mod prefix;

#[derive(Clone, Copy)]
pub(super) enum NativeSamplePolicy {
    Healthy,
    ControlledSecondary(u8),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct CommitCoverageReport {
    pub fixture_run_id: String,
    pub fixture_resource_uid: String,
    pub native_binding: NativeTopologyBinding,
    pub primary_ordinal: u8,
    pub selected_target_ordinal: u8,
    pub acknowledgement: TransactionAcknowledgement,
    pub transaction_samples: Vec<TransactionSample>,
    pub record_probe: RecordProbeOutcome,
    pub before: NativeSampleSet,
    pub after_ack: NativeSampleSet,
    pub quiet: NativeSampleSet,
    pub coverage: CoverageConclusion,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct TransactionAcknowledgement {
    pub marker_id: String,
    pub marker_value: String,
    pub transaction_id: String,
    pub database_id: i32,
    pub delayed_durability: String,
    pub commit_requested_at_unix_millis: u64,
    pub acknowledged_at_unix_millis: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct TransactionSample {
    pub phase: &'static str,
    pub requested_at_unix_millis: u64,
    pub completed_at_unix_millis: u64,
    pub row: Option<TransactionRecordRow>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct TransactionRecordRow {
    pub transaction_id: String,
    pub database_id: i32,
    pub state: i32,
    pub commit_lsn: Option<DecimalProgress>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum RecordProbeOutcome {
    Observed { commit_lsn: DecimalProgress },
    NotObservedInBoundedWindow,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct NativeSampleSet {
    pub requested_at_unix_millis: u64,
    pub completed_at_unix_millis: u64,
    pub members: Vec<NativeSample>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct NativeSample {
    pub ordinal: u8,
    pub collection_completed_at_unix_millis: u64,
    pub snapshot: InstanceSnapshot,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum CoverageConclusion {
    Blocked {
        missing_evidence: Vec<&'static str>,
        not_exercised: [&'static str; 3],
    },
}

#[derive(Debug)]
pub enum CommitCoverageError {
    Data(DataError),
    Ownership(AvailabilityGroupError),
    InvalidNativeSample,
    NativeSample {
        ordinal: u8,
        stage: &'static str,
    },
    Observation {
        ordinal: u8,
        detail: String,
    },
    RejectedNativeSample {
        reason: Box<Self>,
        snapshot: Box<InstanceSnapshot>,
        expected_binding: Box<NativeTopologyBinding>,
    },
    InvalidTransactionSample,
    DelayedDurability,
    Clock,
    Stage {
        name: &'static str,
        source: Box<Self>,
    },
    Concurrent {
        commit: DataError,
        sampling: Option<Box<Self>>,
    },
    Release {
        primary: Option<Box<Self>>,
        release: Box<Self>,
    },
    Control(&'static str),
}

impl fmt::Display for CommitCoverageError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Data(error) => write!(f, "native commit probe failed: {error}"),
            Self::Ownership(error) => write!(f, "native commit probe ownership failed: {error}"),
            Self::InvalidNativeSample => {
                f.write_str("native commit probe sample is stale, unavailable or not exactly bound")
            }
            Self::NativeSample { ordinal, stage } => {
                write!(
                    f,
                    "native commit probe member {ordinal} failed {stage} validation"
                )
            }
            Self::Observation { ordinal, detail } => {
                write!(
                    f,
                    "native commit probe member {ordinal} observation failed: {detail}"
                )
            }
            Self::RejectedNativeSample { reason, .. } => reason.fmt(f),
            Self::InvalidTransactionSample => {
                f.write_str("native commit probe transaction sample is malformed or mismatched")
            }
            Self::DelayedDurability => {
                f.write_str("native commit probe requires disabled delayed durability")
            }
            Self::Clock => {
                f.write_str("native commit probe clock is unavailable or moved backward")
            }
            Self::Stage { name, source } => write!(f, "native commit probe {name}: {source}"),
            Self::Concurrent { commit, sampling } => {
                write!(
                    f,
                    "native commit probe concurrent failure (commit: {commit:?}, sampling: {sampling:?})"
                )
            }
            Self::Release { primary, release } => {
                write!(
                    f,
                    "native prefix release failed: {release}; original failure: {primary:?}"
                )
            }
            Self::Control(reason) => write!(f, "native prefix control rejected: {reason}"),
        }
    }
}

impl std::error::Error for CommitCoverageError {}

impl From<DataError> for CommitCoverageError {
    fn from(error: DataError) -> Self {
        Self::Data(error)
    }
}

pub(super) fn now() -> Result<u64, CommitCoverageError> {
    unix_millis().map_err(|_| CommitCoverageError::Clock)
}

fn bounded_deadline(parent: Instant, limit: Duration) -> Instant {
    earlier(parent, Instant::now() + limit)
}

pub(super) async fn collect_native_samples(
    context: &DataContext<'_>,
    binding: &NativeTopologyBinding,
) -> Result<NativeSampleSet, CommitCoverageError> {
    collect_native_samples_for(context, binding, NativeSamplePolicy::Healthy).await
}

pub(super) async fn collect_native_samples_for(
    context: &DataContext<'_>,
    binding: &NativeTopologyBinding,
    policy: NativeSamplePolicy,
) -> Result<NativeSampleSet, CommitCoverageError> {
    let requested_at_unix_millis = now()?;
    let mut members = Vec::with_capacity(3);
    for member in context.members {
        let sample = observe_native_member(context, member).await?;
        validate_native_sample_for(
            &sample.snapshot,
            member,
            binding,
            requested_at_unix_millis,
            sample.collection_completed_at_unix_millis,
            policy,
        )
        .map_err(|reason| CommitCoverageError::RejectedNativeSample {
            reason: Box::new(reason),
            snapshot: Box::new(sample.snapshot.clone()),
            expected_binding: Box::new(binding.clone()),
        })?;
        members.push(sample);
    }
    let completed_at_unix_millis = now()?;
    for sample in &members {
        validate_native_sample_for(
            &sample.snapshot,
            &context.members[usize::from(sample.ordinal - 1)],
            binding,
            requested_at_unix_millis,
            completed_at_unix_millis,
            policy,
        )?;
    }
    Ok(NativeSampleSet {
        requested_at_unix_millis,
        completed_at_unix_millis,
        members,
    })
}

pub(super) async fn observe_native_member(
    context: &DataContext<'_>,
    member: &ReadyMember,
) -> Result<NativeSample, CommitCoverageError> {
    let parent = Instant::from_std(context.complete_deadline);
    let config = complete_before(
        parent,
        context.deadlines.sql_batch,
        ObserverConfig::read(&member.observer_config),
    )
    .await
    .map_err(|_| DataError::Deadline)?
    .map_err(|error| CommitCoverageError::Observation {
        ordinal: member.ordinal,
        detail: error.to_string(),
    })?;
    let limit = config.sample_timeout();
    let manager =
        SqlServerInstanceManager::new(TdsExecutor::new(config.connection().clone()), config);
    let observation = complete_before(parent, limit, manager.observe())
        .await
        .map_err(|_| DataError::Deadline)?
        .map_err(|error| CommitCoverageError::Observation {
            ordinal: member.ordinal,
            detail: error.to_string(),
        })?;
    let Observation::Present {
        value: snapshot, ..
    } = observation
    else {
        return Err(CommitCoverageError::Observation {
            ordinal: member.ordinal,
            detail: format!("{observation:?}"),
        });
    };
    Ok(NativeSample {
        ordinal: member.ordinal,
        collection_completed_at_unix_millis: now()?,
        snapshot,
    })
}

#[cfg(test)]
fn validate_native_sample(
    sample: &InstanceSnapshot,
    member: &ReadyMember,
    binding: &NativeTopologyBinding,
    requested: u64,
    completed: u64,
) -> Result<(), CommitCoverageError> {
    validate_native_sample_for(
        sample,
        member,
        binding,
        requested,
        completed,
        NativeSamplePolicy::Healthy,
    )
}

pub(super) fn validate_native_sample_for(
    sample: &InstanceSnapshot,
    member: &ReadyMember,
    binding: &NativeTopologyBinding,
    requested: u64,
    completed: u64,
    policy: NativeSamplePolicy,
) -> Result<(), CommitCoverageError> {
    let invalid = CommitCoverageError::InvalidNativeSample;
    let expected = binding
        .members
        .iter()
        .find(|expected| expected.ordinal == member.ordinal)
        .ok_or(CommitCoverageError::InvalidNativeSample)?;
    if sample.observed_at_unix_millis < requested
        || sample.observed_at_unix_millis > completed
        || completed - sample.observed_at_unix_millis > MAX_SAMPLE_AGE.as_millis() as u64
        || member.container_id != expected.container_id
        || member.server_name != expected.server_name
        || member.sql_start_time != expected.sql_start_time
        || sample.instance.sqlserver_start_time != expected.sql_start_time
        || sample.instance.server_name.as_str() != expected.server_name
        || sample.instance.property_server_name.as_str() != expected.server_name
    {
        return Err(CommitCoverageError::NativeSample {
            ordinal: member.ordinal,
            stage: "freshness/instance",
        });
    }
    let Observation::Present {
        value: group,
        observed_at_unix_millis,
    } = &sample.availability_group
    else {
        return Err(CommitCoverageError::NativeSample {
            ordinal: member.ordinal,
            stage: "present availability group",
        });
    };
    let expected_role = match expected.role.as_str() {
        "PRIMARY" => NativeRole::Primary,
        "SECONDARY" => NativeRole::Secondary,
        _ => return Err(invalid),
    };
    for (matches, stage) in [
        (
            *observed_at_unix_millis == sample.observed_at_unix_millis,
            "nested timestamp",
        ),
        (
            group.identity.group_id.as_str() == binding.availability_group_id
                && group.identity.name.as_str() == binding.availability_group_name,
            "availability-group identity",
        ),
        (
            group.configuration_sequence.value() == binding.configuration_sequence,
            "configuration sequence",
        ),
        (
            group.cluster_type.eq_ignore_ascii_case("EXTERNAL")
                && group.required_synchronized_secondaries_to_commit == 1
                && !group.basic_features
                && !group.is_distributed,
            "availability-group profile",
        ),
        (
            group.local_replica.state_available
                && group.local_replica.role.as_ref() == Some(&expected_role),
            "local role",
        ),
        (
            group
                .local_replica
                .identity
                .native_replica_id()
                .map(|id| id.as_str())
                == Some(expected.native_replica_id.as_str()),
            "local replica identity",
        ),
        (
            group.replicas.len() == 3
                && binding.members.iter().all(|bound| {
                    group
                        .replicas
                        .iter()
                        .filter(|replica| {
                            replica.replica_id.as_str() == bound.native_replica_id
                                && replica.server_name.as_str() == bound.server_name
                                && replica.endpoint_url.as_deref()
                                    == Some(bound.endpoint_url.as_str())
                                && replica.availability_mode == "SYNCHRONOUS_COMMIT"
                                && replica.failover_mode == "EXTERNAL"
                                && replica.seeding_mode == "AUTOMATIC"
                        })
                        .count()
                        == 1
                }),
            "native membership/profile",
        ),
        (group.databases.len() == 1, "database count"),
    ] {
        if !matches {
            return Err(CommitCoverageError::NativeSample {
                ordinal: member.ordinal,
                stage,
            });
        }
    }
    let database = &group.databases[0];
    let local = database
        .local
        .as_ref()
        .ok_or(CommitCoverageError::InvalidNativeSample)?;
    let recovery = local
        .recovery
        .as_ref()
        .ok_or(CommitCoverageError::InvalidNativeSample)?;
    let local_rows = database
        .replicas
        .iter()
        .filter(|row| row.replica_id.as_str() == expected.native_replica_id)
        .collect::<Vec<_>>();
    if local_rows.len() != 1 {
        return Err(CommitCoverageError::NativeSample {
            ordinal: member.ordinal,
            stage: "unique local database row",
        });
    }
    let row = local_rows[0];
    let controlled_secondary = matches!(
        policy, NativeSamplePolicy::ControlledSecondary(ordinal)
            if ordinal == member.ordinal && expected.role == "SECONDARY"
    );
    let RecoveryLineageObservation::Local { value: lineage } = &row.lineage else {
        return Err(CommitCoverageError::NativeSample {
            ordinal: member.ordinal,
            stage: "local lineage",
        });
    };
    if database.identity.name.as_str() != binding.database_name
        || database.identity.group_database_id.as_str() != binding.group_database_id
        || local.database_id != expected.local_database_id
        || local.replica_id.as_str() != expected.native_replica_id
        || local.state.as_deref() != Some("ONLINE")
        || local.recovery_model.as_deref() != Some("FULL")
        || recovery.database_guid.as_ref().map(|id| id.as_str())
            != Some(expected.database_guid.as_str())
        || recovery.family_guid.as_ref().map(|id| id.as_str()) != Some(binding.family_guid.as_str())
        || recovery.recovery_fork_guid.as_ref().map(|id| id.as_str())
            != Some(binding.recovery_fork_id.as_str())
        || lineage.database != database.identity
        || lineage.recovery_fork_id.as_str() != binding.recovery_fork_id
        || row.group_database_id.as_str() != binding.group_database_id
        || row.database_id != expected.local_database_id
        || row.provenance != NativeProvenance::Local
        || row.is_primary_replica != Some(expected_role == NativeRole::Primary)
        || row.is_suspended.is_none()
        || (!controlled_secondary
            && (row.is_suspended != Some(false)
                || row.synchronization_state.as_deref() != Some("SYNCHRONIZED")
                || row.synchronization_health.as_deref() != Some("HEALTHY")))
        || row.database_state.as_deref() != Some("ONLINE")
    {
        return Err(CommitCoverageError::NativeSample {
            ordinal: member.ordinal,
            stage: "database/lineage/health",
        });
    }
    Ok(())
}

async fn sample_transaction(
    context: &DataContext<'_>,
    session: &mut AdminSession,
    transaction_id: &str,
    database_id: i32,
    phase: &'static str,
    parent: Instant,
) -> Result<TransactionSample, CommitCoverageError> {
    let requested_at_unix_millis = now()?;
    let rows = data::query_rows_diagnostic(
        context,
        session,
        "SELECT CONVERT(nvarchar(20), transaction_id), CONVERT(int, database_id), \
         CONVERT(int, database_transaction_state), \
         CONVERT(nvarchar(25), database_transaction_commit_lsn) \
         FROM sys.dm_tran_database_transactions \
         WHERE transaction_id = CONVERT(bigint, @P1) AND database_id = @P2;",
        &[&transaction_id as &dyn ToSql, &database_id],
        bounded_deadline(parent, Duration::from_secs(1)),
    )
    .await?;
    if rows.len() > 1 {
        return Err(CommitCoverageError::InvalidTransactionSample);
    }
    let row = rows
        .into_iter()
        .next()
        .map(|row| {
            let malformed = || CommitCoverageError::InvalidTransactionSample;
            parse_transaction_row(
                row.try_get::<&str, _>(0).map_err(|_| malformed())?,
                row.try_get::<i32, _>(1).map_err(|_| malformed())?,
                row.try_get::<i32, _>(2).map_err(|_| malformed())?,
                row.try_get::<&str, _>(3).map_err(|_| malformed())?,
                transaction_id,
                database_id,
            )
        })
        .transpose()?;
    let completed_at_unix_millis = now()?;
    if completed_at_unix_millis < requested_at_unix_millis {
        return Err(CommitCoverageError::Clock);
    }
    Ok(TransactionSample {
        phase,
        requested_at_unix_millis,
        completed_at_unix_millis,
        row,
    })
}

fn parse_transaction_row(
    transaction_id: Option<&str>,
    database_id: Option<i32>,
    state: Option<i32>,
    commit_lsn: Option<&str>,
    expected_transaction: &str,
    expected_database: i32,
) -> Result<TransactionRecordRow, CommitCoverageError> {
    let invalid = CommitCoverageError::InvalidTransactionSample;
    if transaction_id != Some(expected_transaction)
        || database_id != Some(expected_database)
        || expected_transaction
            .parse::<i64>()
            .ok()
            .is_none_or(|id| id <= 0)
        || expected_database <= 0
    {
        return Err(invalid);
    }
    Ok(TransactionRecordRow {
        transaction_id: expected_transaction.to_owned(),
        database_id: expected_database,
        state: state
            .filter(|state| *state >= 0)
            .ok_or(CommitCoverageError::InvalidTransactionSample)?,
        commit_lsn: commit_lsn
            .map(DecimalProgress::parse)
            .transpose()
            .map_err(|_| invalid)?,
    })
}

async fn join_commit_and_sampling<C, S, T>(
    commit: C,
    sampling: S,
) -> Result<(u64, T), CommitCoverageError>
where
    C: Future<Output = Result<u64, DataError>>,
    S: Future<Output = Result<T, CommitCoverageError>>,
{
    let (commit, sampling) = tokio::join!(commit, sampling);
    match (commit, sampling) {
        (Ok(ack), Ok(samples)) => Ok((ack, samples)),
        (Ok(_), Err(error)) => Err(error),
        (Err(commit), sampling) => Err(CommitCoverageError::Concurrent {
            commit,
            sampling: sampling.err().map(Box::new),
        }),
    }
}

async fn complete_probe<C, S, P, F, T, U>(
    commit: C,
    sampling: S,
    post_ack: P,
) -> Result<(u64, T), CommitCoverageError>
where
    C: Future<Output = Result<u64, DataError>>,
    S: Future<Output = Result<(T, U), CommitCoverageError>>,
    P: FnOnce(T, U) -> F,
    F: Future<Output = Result<T, CommitCoverageError>>,
{
    let (acknowledged, (samples, session)) = join_commit_and_sampling(commit, sampling).await?;
    Ok((acknowledged, post_ack(samples, session).await?))
}

pub(super) async fn probe_acknowledged_transaction(
    original_context: &DataContext<'_>,
    binding: &NativeTopologyBinding,
    primary_index: usize,
) -> Result<(TransactionAcknowledgement, Vec<TransactionSample>), CommitCoverageError> {
    let parent = bounded_deadline(
        Instant::from_std(original_context.complete_deadline),
        PROBE_BUDGET,
    );
    let mut deadlines = original_context.deadlines;
    deadlines.sql_connect = deadlines.sql_connect.min(Duration::from_secs(5));
    deadlines.sql_batch = deadlines.sql_batch.min(Duration::from_secs(2));
    let context = DataContext {
        run: original_context.run,
        members: original_context.members,
        credentials: original_context.credentials,
        tls: original_context.tls,
        deadlines,
        complete_deadline: parent.into_std(),
    };
    let mut primary = data::connect(
        &context,
        primary_index,
        &binding.database_name,
        false,
        parent,
    )
    .await?;
    let mut sampler = data::connect(
        &context,
        primary_index,
        &binding.database_name,
        false,
        parent,
    )
    .await?;
    let policy = single_row(
        data::query_rows_diagnostic(
            &context,
            &mut primary,
            "SELECT CONVERT(nvarchar(60), delayed_durability_desc), CONVERT(int, DB_ID()) \
         FROM sys.databases WHERE database_id = DB_ID();",
            &[],
            bounded_deadline(parent, Duration::from_secs(2)),
        )
        .await
        .map_err(|error| CommitCoverageError::Stage {
            name: "durability policy query",
            source: Box::new(error.into()),
        })?,
    )
    .map_err(DataError::Admin)?;
    let delayed_durability = required_text(&policy, 0)
        .map_err(DataError::Admin)?
        .to_owned();
    if delayed_durability != "DISABLED" {
        return Err(CommitCoverageError::DelayedDurability);
    }
    let database_id = policy
        .try_get::<i32, _>(1)
        .map_err(|_| CommitCoverageError::InvalidTransactionSample)?
        .ok_or(CommitCoverageError::InvalidTransactionSample)?;
    if u32::try_from(database_id).ok() != Some(binding.members[primary_index].local_database_id) {
        return Err(CommitCoverageError::InvalidTransactionSample);
    }
    let marker_id = data::random_guid()?;
    let marker_value = format!("native-commit-{}", context.run.run_id);
    data::execute_batch(
        &context,
        &mut primary,
        "SET NOCOUNT ON; SET XACT_ABORT ON; BEGIN TRANSACTION;",
        bounded_deadline(parent, Duration::from_secs(2)),
    )
    .await?;
    let identity = single_row(
        data::query_rows_diagnostic(
            &context,
            &mut primary,
            "SET NOCOUNT ON; SET XACT_ABORT ON; \
         IF @@SERVERNAME <> @P3 OR DB_NAME() <> @P7 \
             OR ISNULL(sys.fn_hadr_is_primary_replica(DB_NAME()), 0) <> 1 \
             OR NOT EXISTS (SELECT 1 FROM sys.dm_os_sys_info \
                 WHERE CONVERT(nvarchar(33), sqlserver_start_time, 126) = @P4) \
             OR NOT EXISTS (SELECT 1 FROM sys.availability_groups \
                 WHERE group_id = CONVERT(uniqueidentifier, @P5) AND sequence_number = @P6) \
             THROW 51000, 'Native commit probe identity changed before INSERT', 1; \
         INSERT INTO dbo.kuberic_marker(id, value) VALUES (CONVERT(uniqueidentifier, @P1), @P2); \
         SELECT CONVERT(nvarchar(20), transaction_id), CONVERT(int, DB_ID()) \
         FROM sys.dm_tran_current_transaction;",
            &[
                &marker_id.as_str() as &dyn ToSql,
                &marker_value.as_str(),
                &binding.members[primary_index].server_name.as_str(),
                &binding.members[primary_index].sql_start_time.as_str(),
                &binding.availability_group_id.as_str(),
                &binding.configuration_sequence,
                &binding.database_name.as_str(),
            ],
            bounded_deadline(parent, Duration::from_secs(2)),
        )
        .await
        .map_err(|error| CommitCoverageError::Stage {
            name: "begin owned workload transaction",
            source: Box::new(error.into()),
        })?,
    )
    .map_err(DataError::Admin)?;
    let transaction_id = required_text(&identity, 0)
        .map_err(DataError::Admin)?
        .to_owned();
    let captured_database = identity
        .try_get::<i32, _>(1)
        .map_err(|_| CommitCoverageError::InvalidTransactionSample)?
        .ok_or(CommitCoverageError::InvalidTransactionSample)?;
    parse_transaction_row(
        Some(&transaction_id),
        Some(captured_database),
        Some(0),
        None,
        &transaction_id,
        database_id,
    )?;
    let first = sample_transaction(
        &context,
        &mut sampler,
        &transaction_id,
        database_id,
        "before_commit",
        parent,
    )
    .await?;
    if first.row.is_none() {
        return Err(CommitCoverageError::InvalidTransactionSample);
    }
    let commit_requested_at_unix_millis = now()?;
    let context_ref = &context;
    let transaction_ref = transaction_id.as_str();
    let (acknowledged_at_unix_millis, samples) = complete_probe(
        async {
            data::execute_batch(
                &DataContext {
                    deadlines: original_context.deadlines,
                    ..context
                },
                &mut primary,
                "COMMIT TRANSACTION WITH (DELAYED_DURABILITY = OFF);",
                bounded_deadline(parent, Duration::from_secs(10)),
            )
            .await?;
            unix_millis().map_err(|_| DataError::Admin(AdminError::MalformedResult))
        },
        async move {
            let deadline = bounded_deadline(parent, SAMPLE_WINDOW);
            let mut samples = vec![first];
            for _ in 0..MAX_OVERLAP_SAMPLES {
                if Instant::now() >= deadline {
                    break;
                }
                samples.push(
                    sample_transaction(
                        context_ref,
                        &mut sampler,
                        transaction_ref,
                        database_id,
                        "overlap",
                        deadline,
                    )
                    .await?,
                );
                sleep(
                    Duration::from_millis(10)
                        .min(deadline.saturating_duration_since(Instant::now())),
                )
                .await;
            }
            Ok((samples, sampler))
        },
        |mut samples, mut sampler| async move {
            if parent.saturating_duration_since(Instant::now()) < SAMPLE_WINDOW {
                return Err(DataError::Deadline.into());
            }
            let post_deadline = Instant::now() + SAMPLE_WINDOW;
            for _ in 0..2 {
                samples.push(
                    sample_transaction(
                        context_ref,
                        &mut sampler,
                        transaction_ref,
                        database_id,
                        "after_ack",
                        post_deadline,
                    )
                    .await?,
                );
            }
            Ok(samples)
        },
    )
    .await?;
    if acknowledged_at_unix_millis < commit_requested_at_unix_millis {
        return Err(CommitCoverageError::Clock);
    }
    Ok((
        TransactionAcknowledgement {
            marker_id,
            marker_value,
            transaction_id,
            database_id,
            delayed_durability,
            commit_requested_at_unix_millis,
            acknowledged_at_unix_millis,
        },
        samples,
    ))
}

pub(super) fn record_probe_outcome(
    samples: &[TransactionSample],
) -> Result<RecordProbeOutcome, CommitCoverageError> {
    let mut observed = None;
    for row in samples.iter().filter_map(|sample| sample.row.as_ref()) {
        if matches!(row.state, 10 | 12)
            && let Some(lsn) = row.commit_lsn.filter(|lsn| lsn.value() != 0)
        {
            if observed.is_some_and(|previous| previous != lsn) {
                return Err(CommitCoverageError::InvalidTransactionSample);
            }
            observed = Some(lsn);
        }
    }
    Ok(match observed {
        Some(commit_lsn) => RecordProbeOutcome::Observed { commit_lsn },
        None => RecordProbeOutcome::NotObservedInBoundedWindow,
    })
}

pub(super) fn coverage_conclusion(record: &RecordProbeOutcome) -> CoverageConclusion {
    let mut missing_evidence = vec![
        "accepted public configuration/storage/session authority and native/public history mapping not collected",
        "acknowledged record to selected-target durable-prefix coverage not evaluated",
    ];
    if matches!(record, RecordProbeOutcome::NotObservedInBoundedWindow) {
        missing_evidence.push("transaction-specific commit record not observed in bounded window");
    }
    CoverageConclusion::Blocked {
        missing_evidence,
        not_exercised: ["covered", "not_yet_covered", "hardening_with_redo_lag"],
    }
}

#[cfg(test)]
pub(super) mod tests;
