mod bootstrap;
pub mod config;
pub mod error;
pub mod executor;
pub mod instance;
#[cfg(feature = "kuberic")]
pub mod kuberic;
pub mod monitor;
mod mutation;
pub mod observation;
pub mod observer;
pub mod operation;
mod output;
pub mod query;
pub mod runtime_config;
pub mod runtime_error;
#[cfg(feature = "kuberic")]
pub mod runtime_host;
pub mod tds;
#[cfg(feature = "kuberic")]
pub mod topology_config;
pub mod types;

#[cfg(feature = "kuberic-testing")]
pub use bootstrap::{
    AvailabilityGroupOwnership, NativeConvergenceState, StandaloneDatabaseIdentity,
    StandaloneDatabaseOwnership, StandaloneDatabaseSnapshot, classify_availability_group,
    classify_owned_availability_group, classify_replica_join, classify_replica_seed,
    classify_standalone_database, source_database_creation_token,
};
#[cfg(feature = "kuberic-testing")]
pub use bootstrap::{BootstrapOwnership, BootstrapOwnershipError, classify_bootstrap_ownership};
pub use config::{
    AvailabilityMode, ClusterType, Edition, FailoverMode, MutationMode, SUPPORTED_DATABASE_COUNT,
    SUPPORTED_ENGINE_MAJOR, SUPPORTED_REPLICA_COUNT, SUPPORTED_REPLICA_COUNT_TEXT,
    SUPPORTED_REQUIRED_SECONDARIES, SeedingMode, SqlServerEulaAcknowledgement,
    SqlServerSupportConfig,
};
pub use error::ContractError;
#[cfg(feature = "kuberic")]
pub use kuberic::{
    HealthyTopologyBinding, HealthyTopologyMemberBinding, SqlServerStartIncarnation,
};
#[cfg(feature = "kuberic-testing")]
pub use mutation::{
    BootstrapMutationPlan, MutationContractError, MutationEffect, STANDALONE_DATABASE_QUERY,
    SqlMutationExecutor,
};
pub use operation::{
    DestructiveApproval, EffectSignature, FenceReference, InputSignature,
    OPERATION_CONTRACT_VERSION, OperationEnvelope, OperationPayload, OperationRecord,
    OperationRequest, ReplayDisposition,
};
#[cfg(feature = "kuberic")]
pub use topology_config::{
    SqlServerTopologyExpectation, SqlServerTopologyMemberExpectation, TopologyConfigError,
};
pub use types::{
    AvailabilityGroupIdentity, AvailabilityGroupName, ConfigurationSequence, DatabaseIdentity,
    DatabaseLineage, DecimalProgress, Endpoint, EngineArtifact, Guid, NativeProgress, NativeRole,
    Observation, ObservationFailure, ObservationFailureKind, OpaqueId, PinnedImage, PinnedPackage,
    ReplicaDescriptor, ReplicaIdentity, SecretRef, ServerName, SqlIdentifier,
};
