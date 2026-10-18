use super::startup::{StartupHosts, cancel_attempts};
use super::*;

#[doc(hidden)]
#[derive(Clone, Copy)]
pub enum PublicStartupProbeMode {
    Drain,
    ReadinessFailure,
    ReportFailure,
}

#[doc(hidden)]
pub enum PublicStartupProbeHandle {
    Started(RunningReplica),
    Pending(tokio::task::JoinHandle<kuberic_runtime::host::Result<Option<RunningReplica>>>),
}

#[doc(hidden)]
pub struct PublicStartupProbeMember {
    pub config: RuntimeHostConfig,
    pub application: Arc<RuntimeHostApplication>,
    pub source: Arc<dyn SqlServerObservationSource>,
    pub report: PublicV1AgentReport,
    pub handle: PublicStartupProbeHandle,
}

#[doc(hidden)]
pub async fn exercise_partial_public_startup_cleanup(
    members: [PublicStartupProbeMember; 3],
    budget: Duration,
    drained: tokio::sync::oneshot::Sender<()>,
    mode: PublicStartupProbeMode,
) -> Result<(), MssqlGroupError> {
    let mut attempts = Vec::with_capacity(3);
    let mut pods = Vec::with_capacity(3);
    for (index, member) in members.into_iter().enumerate() {
        let (shutdown, _) = tokio::sync::watch::channel(false);
        let (running, task) = match member.handle {
            PublicStartupProbeHandle::Started(running) => (Some(running), None),
            PublicStartupProbeHandle::Pending(task) => (None, Some(task)),
        };
        attempts.push(HostAttempt { shutdown, task });
        pods.push(PublicMssqlPod {
            ordinal: index as u8 + 1,
            identity: member.report.replica.clone(),
            session: member.report.process_session_id,
            stable_role: if index == 0 {
                ReplicaRole::Primary
            } else {
                ReplicaRole::ActiveSecondary
            },
            pod_uid: member.config.pod_uid().clone(),
            pvc_uid: member.config.pvc_uid().clone(),
            resource_uid: member.config.resource_uid().clone(),
            control_address: member.config.control_address(),
            replication_address: member.config.replication_address(),
            running,
            source: member.source,
            application: member.application,
            application_root: member.config.application_root().to_owned(),
            observer_config_path: PathBuf::new(),
            mutation: None,
            conflict_injection: PublicConflictInjection::default(),
        });
    }
    let pods = pods
        .try_into()
        .unwrap_or_else(|_| unreachable!("three probe members supplied"));
    let mut owner = StartupHosts::new(pods);
    owner.attempts = attempts;
    owner.drained = Some(drained);
    let cleanup = CleanupCoordinator::new(SystemCleanupClock::default(), budget);
    let primary = match mode {
        PublicStartupProbeMode::Drain => None,
        PublicStartupProbeMode::ReadinessFailure => Some(
            await_started_hosts(
                owner.pods.as_mut().expect("probe owner armed"),
                &mut owner.attempts,
                budget,
            )
            .await
            .expect_err("probe supplies a failed startup"),
        ),
        PublicStartupProbeMode::ReportFailure => Some(
            startup_reports(
                owner.pods.as_mut().expect("probe owner armed"),
                &mut owner.attempts,
                budget,
            )
            .await
            .expect_err("probe supplies a failed startup"),
        ),
    };
    let shutdown = cancel_attempts(
        &mut owner.attempts,
        owner.pods.as_mut().expect("probe owner armed"),
        &cleanup,
    )
    .await;
    match primary {
        Some(primary) => Err(startup_failure(primary, shutdown)),
        None => shutdown,
    }
}
