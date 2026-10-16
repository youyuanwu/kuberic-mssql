//! Server-free authority accounting, not a lease executor or promotion permit.
//!
//! Times are monotonic durations from one runtime's origin. Native-effect
//! horizons cover only this model's submissions, never other or prior actors.

use std::{error::Error, fmt, time::Duration};

use kuberic_runtime::protocol::types::Epoch;

use crate::{
    AvailabilityGroupIdentity, ContractError, OpaqueId, ReplicaIdentity, SqlServerStartIncarnation,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LeaseFenceError {
    Identity(ContractError),
    MissingNativeReplica,
    InvalidTiming(&'static str),
    TimeOverflow,
    ClockRegression,
    AuthorityMismatch,
    OperationMismatch,
    RequestPending,
    StaleSequence,
    Revoked,
    Expired,
    Cancelled,
    NoPendingRequest,
}

impl fmt::Display for LeaseFenceError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Identity(error) => write!(f, "invalid lease/fence identity: {error}"),
            Self::MissingNativeReplica => {
                write!(f, "lease authority requires a native replica GUID")
            }
            Self::InvalidTiming(field) => write!(f, "invalid lease timing budget: {field}"),
            Self::TimeOverflow => write!(f, "lease timing arithmetic overflow"),
            Self::ClockRegression => write!(f, "lease monotonic clock regressed"),
            Self::AuthorityMismatch => write!(f, "lease/fence authority binding mismatch"),
            Self::OperationMismatch => write!(f, "lease/fence operation mismatch"),
            Self::RequestPending => write!(f, "a native lease request remains unresolved"),
            Self::StaleSequence => write!(f, "lease operation sequence must strictly increase"),
            Self::Revoked => write!(f, "local lease authority was revoked or superseded"),
            Self::Expired => write!(f, "local lease acceptance deadline expired"),
            Self::Cancelled => write!(
                f,
                "lease response was cancelled; native effect remains possible"
            ),
            Self::NoPendingRequest => write!(f, "no matching pending lease request"),
        }
    }
}

impl Error for LeaseFenceError {}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LeaseAuthority {
    resource_uid: OpaqueId,
    configuration_id: OpaqueId,
    availability_group: AvailabilityGroupIdentity,
    role_epoch: Epoch,
    replica: ReplicaIdentity,
    sql_start: SqlServerStartIncarnation,
}

impl LeaseAuthority {
    pub fn new(
        resource_uid: impl Into<String>,
        configuration_id: impl Into<String>,
        availability_group: AvailabilityGroupIdentity,
        role_epoch: Epoch,
        replica: ReplicaIdentity,
        sql_start: SqlServerStartIncarnation,
    ) -> Result<Self, LeaseFenceError> {
        if replica.native_replica_id().is_none() {
            return Err(LeaseFenceError::MissingNativeReplica);
        }
        Ok(Self {
            resource_uid: OpaqueId::new("lease resource UID", resource_uid)
                .map_err(LeaseFenceError::Identity)?,
            configuration_id: OpaqueId::new("lease configuration identity", configuration_id)
                .map_err(LeaseFenceError::Identity)?,
            availability_group,
            role_epoch,
            replica,
            sql_start,
        })
    }

    pub fn availability_group(&self) -> &AvailabilityGroupIdentity {
        &self.availability_group
    }

    pub fn replica(&self) -> &ReplicaIdentity {
        &self.replica
    }

    pub fn sql_start(&self) -> &SqlServerStartIncarnation {
        &self.sql_start
    }

    pub fn matches(&self, observed: &Self) -> Result<(), LeaseFenceError> {
        if self == observed {
            Ok(())
        } else {
            Err(LeaseFenceError::AuthorityMismatch)
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LeaseTiming {
    pub observation: Duration,
    pub reconciliation: Duration,
    pub renewal_interval: Duration,
    pub request: Duration,
    pub quorum_loss: Duration,
    pub validity: Duration,
    pub margin: Duration,
    pub fencing: Duration,
    pub promotion: Duration,
    /// A justified bound on actual native application, not a client timeout.
    pub native_application: Option<Duration>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LeaseBudget {
    timing: LeaseTiming,
    renewal_coverage: Duration,
    recovery_bound: Option<Duration>,
}

fn sum(values: &[Duration]) -> Result<Duration, LeaseFenceError> {
    values.iter().try_fold(Duration::ZERO, |total, value| {
        total
            .checked_add(*value)
            .ok_or(LeaseFenceError::TimeOverflow)
    })
}

impl LeaseBudget {
    pub fn new(timing: LeaseTiming) -> Result<Self, LeaseFenceError> {
        if timing
            .native_application
            .is_some_and(|value| value.is_zero())
        {
            return Err(LeaseFenceError::InvalidTiming("native application"));
        }
        for (name, value) in [
            ("observation", timing.observation),
            ("reconciliation", timing.reconciliation),
            ("renewal interval", timing.renewal_interval),
            ("request", timing.request),
            ("quorum loss", timing.quorum_loss),
            ("validity", timing.validity),
            ("margin", timing.margin),
            ("fencing", timing.fencing),
            ("promotion", timing.promotion),
        ] {
            if value.is_zero() {
                return Err(LeaseFenceError::InvalidTiming(name));
            }
        }
        let renewal_coverage = sum(&[
            timing.observation,
            timing.reconciliation,
            timing.renewal_interval,
            timing.request,
            timing.quorum_loss,
            timing.margin,
        ])?;
        if renewal_coverage >= timing.validity {
            return Err(LeaseFenceError::InvalidTiming(
                "validity must exceed the complete renewal coverage",
            ));
        }
        let recovery_tail = sum(&[
            renewal_coverage,
            timing.validity,
            timing.fencing,
            timing.promotion,
        ])?;
        let recovery_bound = timing
            .native_application
            .map(|application| sum(&[recovery_tail, application]))
            .transpose()?;
        Ok(Self {
            timing,
            renewal_coverage,
            recovery_bound,
        })
    }

    pub fn renewal_coverage(&self) -> Duration {
        self.renewal_coverage
    }

    /// Conditional timing estimate only; it does not admit promotion.
    pub fn recovery_bound(&self) -> Option<Duration> {
        self.recovery_bound
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NativeEffectHorizon {
    /// No submissions by this model; predecessor/other actors are not covered.
    NoSubmissions,
    Until(Duration),
    Unbounded,
}

impl NativeEffectHorizon {
    fn merge(self, other: Self) -> Self {
        match (self, other) {
            (Self::Unbounded, _) | (_, Self::Unbounded) => Self::Unbounded,
            (Self::NoSubmissions, value) | (value, Self::NoSubmissions) => value,
            (Self::Until(left), Self::Until(right)) => Self::Until(left.max(right)),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LeaseRequest {
    authority: LeaseAuthority,
    operation_id: OpaqueId,
    sequence: u64,
    submitted_at: Duration,
    accept_before: Duration,
}

impl LeaseRequest {
    pub fn authority(&self) -> &LeaseAuthority {
        &self.authority
    }

    pub fn accept_before(&self) -> Duration {
        self.accept_before
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LocalLeaseState {
    Inactive,
    ActiveUntil(Duration),
    Expired,
    Revoked,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct PendingLease {
    request: LeaseRequest,
    prior_native: NativeEffectHorizon,
    cancelled: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LeaseModel {
    authority: LeaseAuthority,
    budget: LeaseBudget,
    last_time: Duration,
    last_sequence: u64,
    accepted_until: Option<Duration>,
    revoked: bool,
    pending: Option<PendingLease>,
    native: NativeEffectHorizon,
}

impl LeaseModel {
    pub fn new(authority: LeaseAuthority, budget: LeaseBudget, now: Duration) -> Self {
        Self {
            authority,
            budget,
            last_time: now,
            last_sequence: 0,
            accepted_until: None,
            revoked: false,
            pending: None,
            native: NativeEffectHorizon::NoSubmissions,
        }
    }

    fn check_time(&mut self, now: Duration) -> Result<(), LeaseFenceError> {
        if now < self.last_time {
            Err(LeaseFenceError::ClockRegression)
        } else {
            self.last_time = now;
            Ok(())
        }
    }

    /// Call immediately before dispatch. Even a lost response may have an effect.
    pub fn begin(
        &mut self,
        operation_id: OpaqueId,
        sequence: u64,
        now: Duration,
    ) -> Result<LeaseRequest, LeaseFenceError> {
        self.check_time(now)?;
        if self.revoked {
            return Err(LeaseFenceError::Revoked);
        }
        if self.pending.is_some() {
            return Err(LeaseFenceError::RequestPending);
        }
        if sequence <= self.last_sequence {
            return Err(LeaseFenceError::StaleSequence);
        }
        let accept_before = sum(&[now, self.budget.timing.validity])?
            .checked_sub(self.budget.timing.margin)
            .ok_or(LeaseFenceError::TimeOverflow)?;
        let possible = match self.budget.timing.native_application {
            Some(application) => NativeEffectHorizon::Until(sum(&[
                now,
                application,
                self.budget.timing.validity,
                self.budget.timing.margin,
            ])?),
            None => NativeEffectHorizon::Unbounded,
        };
        let request = LeaseRequest {
            authority: self.authority.clone(),
            operation_id,
            sequence,
            submitted_at: now,
            accept_before,
        };
        self.pending = Some(PendingLease {
            request: request.clone(),
            prior_native: self.native,
            cancelled: false,
        });
        self.native = self.native.merge(possible);
        self.last_sequence = sequence;
        self.last_time = now;
        Ok(request)
    }

    fn matching_pending(&self, request: &LeaseRequest) -> Result<&PendingLease, LeaseFenceError> {
        self.authority.matches(&request.authority)?;
        let pending = self
            .pending
            .as_ref()
            .ok_or(LeaseFenceError::NoPendingRequest)?;
        if &pending.request != request {
            return Err(LeaseFenceError::OperationMismatch);
        }
        Ok(pending)
    }

    pub fn acknowledge(
        &mut self,
        request: &LeaseRequest,
        now: Duration,
    ) -> Result<(), LeaseFenceError> {
        self.check_time(now)?;
        let pending = self.matching_pending(request)?;
        if self.revoked {
            return Err(LeaseFenceError::Revoked);
        }
        if pending.cancelled {
            return Err(LeaseFenceError::Cancelled);
        }
        if now >= request.accept_before {
            return Err(LeaseFenceError::Expired);
        }
        let known_upper = sum(&[now, self.budget.timing.validity, self.budget.timing.margin])?;
        self.native = pending
            .prior_native
            .merge(NativeEffectHorizon::Until(known_upper));
        self.accepted_until = Some(request.accept_before);
        self.pending = None;
        self.last_time = now;
        Ok(())
    }

    pub fn cancel(&mut self, request: &LeaseRequest, now: Duration) -> Result<(), LeaseFenceError> {
        self.check_time(now)?;
        self.matching_pending(request)?;
        let pending = self
            .pending
            .as_mut()
            .ok_or(LeaseFenceError::NoPendingRequest)?;
        pending.cancelled = true;
        self.accepted_until = None;
        self.last_time = now;
        Ok(())
    }

    /// Revocation/supersession ends local acceptance, never native effects.
    pub fn revoke(&mut self, now: Duration) -> Result<(), LeaseFenceError> {
        self.check_time(now)?;
        self.revoked = true;
        self.accepted_until = None;
        self.last_time = now;
        Ok(())
    }

    pub fn local_state(&mut self, now: Duration) -> Result<LocalLeaseState, LeaseFenceError> {
        self.check_time(now)?;
        self.last_time = now;
        Ok(if self.revoked {
            LocalLeaseState::Revoked
        } else {
            match self.accepted_until {
                Some(until) if now < until => LocalLeaseState::ActiveUntil(until),
                Some(_) => LocalLeaseState::Expired,
                None => LocalLeaseState::Inactive,
            }
        })
    }

    pub fn native_effect_horizon(&self) -> NativeEffectHorizon {
        self.native
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FenceRequest {
    operation_id: OpaqueId,
    authority: LeaseAuthority,
}

impl FenceRequest {
    pub fn new(operation_id: OpaqueId, authority: LeaseAuthority) -> Self {
        Self {
            operation_id,
            authority,
        }
    }

    pub fn authority(&self) -> &LeaseAuthority {
        &self.authority
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FenceReceipt {
    request: FenceRequest,
    provider: OpaqueId,
}

impl FenceReceipt {
    /// Records a trusted provider's verified stop, not authentication or proof
    /// that no external administrator can start a new incarnation.
    pub fn from_verified_stop(
        request: FenceRequest,
        observed: &LeaseAuthority,
        provider: OpaqueId,
    ) -> Result<Self, LeaseFenceError> {
        request.authority.matches(observed)?;
        Ok(Self { request, provider })
    }

    pub fn validate(&self, request: &FenceRequest) -> Result<(), LeaseFenceError> {
        self.request.authority.matches(&request.authority)?;
        if self.request.operation_id != request.operation_id {
            return Err(LeaseFenceError::OperationMismatch);
        }
        Ok(())
    }

    pub fn provider(&self) -> &str {
        self.provider.as_str()
    }
}
