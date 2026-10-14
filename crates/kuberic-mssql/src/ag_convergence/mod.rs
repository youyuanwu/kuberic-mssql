//! Opt-in operation-envelope convergence, separate from Kuberic host callbacks.

pub mod adapter;
pub mod codec;
pub mod convergence;
pub mod journal;
pub mod mutation;
pub mod operation;

pub use crate::{
    AvailabilityGroupIdentity, AvailabilityGroupName, AvailabilityMode, ClusterType,
    ConfigurationSequence, ContractError, DatabaseIdentity, DatabaseLineage, DecimalProgress,
    Edition, Endpoint, EngineArtifact, FailoverMode, Guid, MutationMode, NativeProgress,
    NativeRole, Observation, ObservationFailure, ObservationFailureKind, OpaqueId, PinnedImage,
    ReplicaDescriptor, ReplicaIdentity, SUPPORTED_DATABASE_COUNT, SUPPORTED_ENGINE_MAJOR,
    SUPPORTED_REPLICA_COUNT, SUPPORTED_REPLICA_COUNT_TEXT, SUPPORTED_REQUIRED_SECONDARIES,
    SecretRef, SeedingMode, ServerName, SqlIdentifier, SqlServerSupportConfig,
};
pub use crate::{
    config, error, executor, instance, observation, runtime_config, runtime_error, tds, types,
};
pub use operation::{
    DestructiveApproval, EffectSignature, FenceReference, InputSignature,
    OPERATION_CONTRACT_VERSION, OperationEnvelope, OperationPayload, OperationRecord,
    OperationRequest, ReplayDisposition,
};

/// Allows paired MANUAL seeding metadata during the guarded secondary-join
/// handshake. Ordinary observer and Kuberic host observations remain strict.
pub async fn observe_session(
    session: &mut dyn crate::executor::SqlSession,
    target: &crate::observation::ObservationTarget,
    observed_at_unix_millis: u64,
) -> Result<crate::observation::InstanceSnapshot, crate::runtime_error::RuntimeError> {
    crate::observation::observe_convergence_session(session, target, observed_at_unix_millis).await
}
