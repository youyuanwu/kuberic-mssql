//! Primary-only native fixture. Its identities, sessions and addresses are not accepted PublicHost authority.

use super::super::member::LaunchedMembers;
use super::*;
use kuberic_mssql::kuberic::{NativeSourceFoundationEvidence, SqlServerReplicator};

pub struct NativeSourceFoundation {
    replicator: SqlServerReplicator,
    current: ReplicaSetConfiguration,
}

impl NativeSourceFoundation {
    pub async fn new(
        launched: &mut LaunchedMembers,
        prepared: &PreparedAvailabilityGroup,
        faults: PublicFaultInjection,
    ) -> Result<Self, MssqlGroupError> {
        let seed_root = launched
            .native_foundation_state_directory()
            .map_err(display_error)?;
        let run = &launched.run;
        let members = &launched.members;
        let observer = ObserverConfig::read(&members[0].observer_config)
            .await
            .map_err(display_error)?;
        let resource = ResourceUid::new(run.resource_uid.clone());
        let identities = identities(run, &resource);
        let configuration = configuration(
            None,
            &identities,
            [
                ReplicaRole::Primary,
                ReplicaRole::ActiveSecondary,
                ReplicaRole::ActiveSecondary,
            ],
        )?;
        let topology = SqlServerTopologyExpectation::new(
            identities[0].replica_id,
            observer.target().replica.logical_id(),
            (0..3)
                .map(|index| {
                    let endpoint = &prepared.endpoint_evidence[index];
                    if endpoint.ordinal != members[index].ordinal
                        || endpoint.state != "STARTED"
                        || endpoint.port != super::super::availability_group::HADR_ENDPOINT_PORT
                    {
                        return Err(MssqlGroupError::new(
                            "native foundation requires exact prepared endpoints",
                        ));
                    }
                    SqlServerTopologyMemberExpectation::new(
                        identities[index].replica_id,
                        ServerName::new(run.members[index].server_name.clone())
                            .map_err(display_error)?,
                        format!("TCP://{}:{}", run.members[index].server_name, endpoint.port),
                    )
                    .map_err(display_error)
                })
                .collect::<Result<Vec<_>, _>>()?,
        )
        .map_err(display_error)?;
        let settings = observer.connection().testing_with_secret_files(
            members[0].admin_username.clone(),
            members[0].admin_password.clone(),
        );
        let executor = Arc::new(PublicMutationExecutor::new(
            TdsMutationExecutor::testing_new_with_seed_probe(
                settings.clone(),
                Arc::new(faults.clone()),
            ),
            TdsMutationExecutor::testing_new(settings),
            members[0].ordinal,
            faults,
            PublicConflictInjection::default(),
        ));
        let source = Arc::new(SqlServerInstanceManager::new(
            TdsExecutor::new(observer.connection().clone()),
            observer,
        ));
        let native_address = |index: usize| format!("native-fixture-{}.invalid:5022", index + 1);
        let native_session = |index: usize| {
            ProcessSessionId::new(format!(
                "native-source-foundation-{}-{}",
                run.run_id,
                index + 1
            ))
        };
        let replicator = SqlServerReplicator::new_bootstrap_with_topology(
            SqlServerServiceConfig::new(resource, native_address(0)).map_err(display_error)?,
            source,
            Arc::new(SystemObservationClock),
            identities[0].clone(),
            topology,
            SqlServerBootstrapConfig::new(
                SqlIdentifier::new(format!("km_db_{}", run.run_id)).map_err(display_error)?,
            ),
            executor,
            seed_root,
        )
        .map_err(display_error)?;
        replicator
            .testing_bind_process_session(native_session(0))
            .map_err(display_error)?;
        let replicas = (1..3)
            .map(|index| {
                let mut replica = ReplicaInformation::new(
                    OperationId::default(),
                    identities[index].clone(),
                    native_address(index),
                );
                replica.process_session_id = native_session(index);
                replica.role = ReplicaRole::ActiveSecondary;
                replica
            })
            .collect();
        Ok(Self {
            replicator,
            current: ReplicaSetConfiguration {
                configuration,
                replicas,
            },
        })
    }

    pub async fn open(&self) -> KubericResult<()> {
        self.replicator.open().await.map(|_| ())
    }

    pub async fn prepare(&self) -> Result<NativeSourceFoundationEvidence, KubericAdapterError> {
        self.replicator
            .testing_prepare_native_source_foundation(self.current.clone())
            .await
    }

    pub async fn close(&self) -> KubericResult<()> {
        self.replicator.close().await
    }

    pub fn abort(&self) {
        self.replicator.abort();
    }

    pub async fn wait_for_containment(&self) -> Result<(), KubericAdapterError> {
        self.replicator.wait_for_containment().await
    }
}
