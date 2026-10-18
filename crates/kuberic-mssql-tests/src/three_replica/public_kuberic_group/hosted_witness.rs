use super::*;

use kuberic_runtime::control::normalize_execute_request;
use kuberic_runtime::protocol::command::ProtocolCommand;
use kuberic_runtime::protocol::public_operations::PublicConfiguration;
use serde::Serialize;

use crate::three_replica::hosted_authority::{
    HostedAssociation, HostedAuthority, NativeDatabaseAnchor, NativeInput, accepted_command,
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
    let envelope = normalize_execute_request(request.clone()).map_err(display_error)?;
    let ProtocolCommand::PublicV1Operation(operation) = envelope.command else {
        return Err(MssqlGroupError::new("witness requires a public operation"));
    };
    if operation.target != pod.identity || request.resource_uid != resource.as_str() {
        return Err(MssqlGroupError::new(
            "witness operation target/resource differs",
        ));
    }
    Ok(operation.intent)
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
            let peers = self
                .configuration
                .members
                .iter()
                .filter(|member| member.identity != self.pods[index].identity)
                .map(|member| {
                    let report = reports
                        .iter()
                        .find(|report| report.replica == member.identity)
                        .ok_or_else(|| {
                            MssqlGroupError::new("live witness exact peer report missing")
                        })?;
                    Ok(PublicReplicaDescriptor {
                        build_id: OperationId::default(),
                        identity: report.replica.clone(),
                        replication_address: report.replication_address.clone().ok_or_else(
                            || MssqlGroupError::new("live witness peer address missing"),
                        )?,
                        process_session_id: report.process_session_id.clone(),
                        storage_incarnation: Some(report.storage_incarnation.clone()),
                        role: member.role,
                        current_progress: report.progress.last,
                        catch_up_capability: report.progress.first,
                        must_catchup: false,
                    })
                })
                .collect::<Result<Vec<_>, MssqlGroupError>>()?;
            let request = program_request(
                &self.pods,
                &reports,
                index,
                &self.resource_uid,
                &reports[index].process_session_id,
                &format!("live-native-witness-configuration-{}", index + 1),
                PublicOperationProgram::Configuration(PublicConfiguration {
                    current: self.configuration.clone(),
                    previous: None,
                    replicas: peers,
                }),
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
        )
        .map_err(MssqlGroupError::new)?;
        let mut native_collection = None;
        let native = async {
            let report = launched
                .probe_native_commit_coverage(target)
                .await
                .map_err(|error| error.to_string())?;
            let evidence = NativePrefixEvidence::from_quiet_report(
                &report,
                kuberic_mssql::instance::unix_millis().map_err(|error| error.to_string())?,
            );
            native_collection = Some(report);
            Ok(evidence)
        };
        let association = authority
            .collect(
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
                &launched.members,
                NativeInput::LiveQuietFixture,
                self.operation_timeout.min(remaining(
                    self.complete_deadline,
                    "live witness collection",
                )?),
            )
            .await;
        authority.retire();
        let association = association.map_err(MssqlGroupError::new)?;
        Ok(LiveHostedWitness {
            native_collection: native_collection
                .ok_or_else(|| MssqlGroupError::new("live native collection missing"))?,
            association,
        })
    }

    fn witness_rpc_budget(&self) -> Result<Duration, MssqlGroupError> {
        Ok(Duration::from_secs(5).min(remaining(self.complete_deadline, "live witness RPC")?))
    }
}
