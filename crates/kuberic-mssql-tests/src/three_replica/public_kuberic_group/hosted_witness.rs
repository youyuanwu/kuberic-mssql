use super::*;

use serde::Serialize;

use crate::three_replica::hosted_authority::{
    HostedAssociation, HostedAuthority, NativeDatabaseAnchor, NativeInput, accepted_command,
    complete_remote_configuration,
};
use crate::three_replica::hosted_native_prefix::NativePrefixEvidence;
use crate::three_replica::{CommitCoverageReport, LaunchedMembers};

#[derive(Debug, Serialize)]
pub struct LiveHostedWitness {
    pub native_collection: CommitCoverageReport,
    pub association: HostedAssociation,
}

pub(super) fn operation_input(
    pod: &PublicMssqlPod,
    resource: &ResourceUid,
    request: &wire::ExecuteCommandRequest,
) -> Result<PublicOperationIntent, MssqlGroupError> {
    crate::three_replica::hosted_authority::operation_input(
        &pod.identity,
        resource.as_str(),
        request,
    )
    .map_err(MssqlGroupError::new)
}

pub async fn associate_quiet_hosted_witness<F, R, RF>(
    authority: &HostedAuthority,
    members: &[ReadyMember; 3],
    collect: F,
    reports: R,
    budget: Duration,
) -> Result<LiveHostedWitness, String>
where
    F: std::future::Future<Output = Result<CommitCoverageReport, String>>,
    R: FnMut() -> RF,
    RF: std::future::Future<Output = Result<Vec<PublicV1AgentReport>, String>>,
{
    let mut native_collection = None;
    let native = async {
        let report = collect.await?;
        let evidence =
            NativePrefixEvidence::from_quiet_report(&report, authority.evaluation_time()?);
        native_collection = Some(report);
        Ok(evidence)
    };
    let association = authority
        .collect(
            native,
            reports,
            members,
            NativeInput::LiveQuietFixture,
            budget,
        )
        .await?;
    Ok(LiveHostedWitness {
        native_collection: native_collection.ok_or("live native collection missing")?,
        association,
    })
}

impl PublicMssqlGroup {
    pub async fn collect_live_native_witness(
        &mut self,
        launched: &LaunchedMembers,
        target: u8,
    ) -> Result<LiveHostedWitness, MssqlGroupError> {
        if self.resource_uid.as_str() != launched.run.resource_uid
            || self.native_binding.as_ref() != launched.journal().native_binding.as_ref()
            || self.native_binding.is_none()
        {
            return Err(MssqlGroupError::new(
                "live witness fixture/native owner differs",
            ));
        }
        let budget = self
            .operation_timeout
            .min(remaining(self.complete_deadline, "live hosted witness")?);
        timeout(
            budget,
            "live hosted witness",
            self.collect_live_native_witness_inner(launched, target),
        )
        .await?
    }

    async fn collect_live_native_witness_inner(
        &mut self,
        launched: &LaunchedMembers,
        target: u8,
    ) -> Result<LiveHostedWitness, MssqlGroupError> {
        let mut inputs = Vec::with_capacity(3);
        for index in 0..3 {
            if self.pods[index].stable_role == ReplicaRole::Primary {
                let input = self
                    .configuration_inputs
                    .iter()
                    .find(|(member, input)| {
                        *member == index
                            && input.lifecycle.as_ref().is_some_and(|lifecycle| {
                                lifecycle.recipe == PublicLifecycleRecipe::InitialPrimary
                            })
                    })
                    .ok_or_else(|| {
                        MssqlGroupError::new("live witness lacks frozen initial-primary input")
                    })?;
                inputs.push(input.1.clone());
                continue;
            }
            let reports = public_reports(&self.pods, self.witness_rpc_budget()?).await?;
            let configuration = complete_remote_configuration(&self.configuration, &reports, index)
                .map_err(MssqlGroupError::new)?;
            let request = program_request(
                &self.pods,
                &reports,
                index,
                &self.resource_uid,
                &reports[index].process_session_id,
                &format!("live-native-witness-configuration-{}", index + 1),
                PublicOperationProgram::Configuration(configuration),
            )
            .await?;
            let input = operation_input(&self.pods[index], &self.resource_uid, request.get_ref())?;
            let (mut client, _) = status(&self.pods[index], self.witness_rpc_budget()?).await?;
            timeout(
                self.witness_rpc_budget()?,
                "live witness configuration",
                client.execute(request),
            )
            .await?
            .map_err(display_error)?;
            inputs.push(input);
        }
        let baseline = public_reports(&self.pods, self.witness_rpc_budget()?).await?;
        let accepted = inputs
            .iter()
            .enumerate()
            .map(|(index, input)| {
                accepted_command(index, input, &baseline[index]).map_err(MssqlGroupError::new)
            })
            .collect::<Result<Vec<_>, _>>()?;
        let mut bindings = Vec::with_capacity(3);
        let mut anchors = Vec::with_capacity(3);
        let mut observers = Vec::with_capacity(3);
        for (index, pod) in self.pods.iter().enumerate() {
            let binding = pod
                .application
                .testing_replicator()
                .and_then(|replicator| replicator.admitted_topology())
                .ok_or_else(|| {
                    MssqlGroupError::new("live witness actual admitted topology missing")
                })?;
            let Observation::Present {
                value: snapshot, ..
            } = timeout(
                self.witness_rpc_budget()?,
                "live witness independent observation",
                pod.source.observe(),
            )
            .await?
            .map_err(display_error)?
            else {
                return Err(MssqlGroupError::new(
                    "live witness independent observation unavailable",
                ));
            };
            binding
                .validate_snapshot(
                    pod.source.observer_config(),
                    &snapshot,
                    kuberic_mssql::instance::unix_millis().map_err(display_error)?,
                    true,
                    Some(pod.stable_role),
                )
                .map_err(display_error)?;
            anchors.push(
                NativeDatabaseAnchor::from_snapshot(baseline[index].replica.clone(), &snapshot)
                    .map_err(MssqlGroupError::new)?,
            );
            observers.push(pod.source.observer_config().clone());
            bindings.push(binding);
        }
        let authority = HostedAuthority::new(
            self.configuration.clone(),
            baseline.into(),
            accepted,
            bindings,
            anchors,
            observers,
            Arc::new(SystemObservationClock),
        )
        .map_err(MssqlGroupError::new)?;
        let native = async {
            launched
                .probe_native_commit_coverage(target)
                .await
                .map_err(|error| error.to_string())
        };
        let association = associate_quiet_hosted_witness(
            &authority,
            &launched.members,
            native,
            || async {
                public_reports(
                    &self.pods,
                    self.witness_rpc_budget()
                        .map_err(|error| error.to_string())?,
                )
                .await
                .map(Vec::from)
                .map_err(|error| error.to_string())
            },
            self.operation_timeout.min(remaining(
                self.complete_deadline,
                "live witness collection",
            )?),
        )
        .await;
        authority.retire();
        association.map_err(MssqlGroupError::new)
    }

    fn witness_rpc_budget(&self) -> Result<Duration, MssqlGroupError> {
        Ok(Duration::from_secs(5).min(remaining(self.complete_deadline, "live witness RPC")?))
    }
}
