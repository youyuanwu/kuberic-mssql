use std::time::Duration;

use kuberic_mssql::{
    AvailabilityGroupIdentity, AvailabilityGroupName, Guid, OpaqueId, ReplicaIdentity,
    SqlServerStartIncarnation,
    lease_fence::{
        FenceReceipt, FenceRequest, LeaseAuthority, LeaseBudget, LeaseFenceError, LeaseModel,
        LeaseTiming, LocalLeaseState, NativeEffectHorizon,
    },
};
use kuberic_runtime::protocol::types::Epoch;

fn seconds(value: u64) -> Duration {
    Duration::from_secs(value)
}

fn guid(value: u8) -> Guid {
    Guid::parse("test", format!("00000000-0000-0000-0000-{value:012}")).unwrap()
}

fn authority_variant(field: usize) -> LeaseAuthority {
    LeaseAuthority::new(
        if field == 1 {
            "other-resource"
        } else {
            "resource"
        },
        if field == 2 { "other-config" } else { "config" },
        AvailabilityGroupIdentity {
            name: AvailabilityGroupName::new(if field == 3 { "other_ag" } else { "ag" }).unwrap(),
            group_id: guid(if field == 4 { 2 } else { 1 }),
        },
        Epoch::new(
            if field == 5 { 8 } else { 7 },
            if field == 6 { 43 } else { 42 },
        ),
        ReplicaIdentity::observed(
            if field == 7 {
                "other-replica"
            } else {
                "replica"
            },
            guid(if field == 8 { 4 } else { 3 }),
            if field == 9 {
                "other-incarnation"
            } else {
                "incarnation"
            },
        )
        .unwrap(),
        SqlServerStartIncarnation::new(if field == 10 { "start-2" } else { "start-1" }).unwrap(),
    )
    .unwrap()
}

fn timing() -> LeaseTiming {
    LeaseTiming {
        observation: seconds(1),
        reconciliation: seconds(2),
        renewal_interval: seconds(3),
        request: seconds(4),
        quorum_loss: seconds(5),
        validity: seconds(30),
        margin: seconds(1),
        fencing: seconds(6),
        promotion: seconds(7),
        native_application: None,
    }
}

fn model() -> LeaseModel {
    LeaseModel::new(
        authority_variant(0),
        LeaseBudget::new(timing()).unwrap(),
        Duration::ZERO,
    )
}

fn operation(value: &str) -> OpaqueId {
    OpaqueId::new("test operation", value).unwrap()
}

fn at(mut state: LeaseModel, now: Duration) -> LeaseModel {
    state.local_state(now).unwrap();
    state
}

#[test]
fn authority_requires_observed_replica_and_nonempty_identity() {
    let desired = ReplicaIdentity::desired("replica", "incarnation").unwrap();
    let group = authority_variant(0).availability_group().clone();
    let start = SqlServerStartIncarnation::new("start-1").unwrap();
    assert_eq!(
        LeaseAuthority::new(
            "resource",
            "config",
            group.clone(),
            Epoch::new(7, 42),
            desired,
            start.clone()
        ),
        Err(LeaseFenceError::MissingNativeReplica)
    );
    for (resource, config) in [("", "config"), ("resource", "")] {
        assert!(matches!(
            LeaseAuthority::new(
                resource,
                config,
                group.clone(),
                Epoch::new(7, 42),
                authority_variant(0).replica().clone(),
                start.clone()
            ),
            Err(LeaseFenceError::Identity(_))
        ));
    }
}

#[test]
fn every_binding_field_rejects_completion_and_fence_transfer() {
    let original = authority_variant(0);
    let request = FenceRequest::new(operation("fence"), original.clone());
    let receipt =
        FenceReceipt::from_verified_stop(request.clone(), &original, operation("fixture-provider"))
            .unwrap();
    receipt.validate(&request).unwrap();
    for field in 1..=10 {
        let changed = authority_variant(field);
        assert_eq!(
            original.matches(&changed),
            Err(LeaseFenceError::AuthorityMismatch)
        );
        assert_eq!(
            receipt.validate(&FenceRequest::new(operation("fence"), changed.clone())),
            Err(LeaseFenceError::AuthorityMismatch),
            "field {field}"
        );
        assert_eq!(
            FenceReceipt::from_verified_stop(request.clone(), &changed, operation("provider")),
            Err(LeaseFenceError::AuthorityMismatch)
        );
        let mut current = model();
        current.begin(operation("grant"), 1, seconds(0)).unwrap();
        let before = at(current.clone(), seconds(1));
        let mut other = LeaseModel::new(changed, LeaseBudget::new(timing()).unwrap(), seconds(0));
        let wrong = other.begin(operation("grant"), 1, seconds(0)).unwrap();
        assert_eq!(
            current.acknowledge(&wrong, seconds(1)),
            Err(LeaseFenceError::AuthorityMismatch)
        );
        assert_eq!(current, before);
    }
    assert_eq!(
        receipt.validate(&FenceRequest::new(
            operation("different-operation"),
            original
        )),
        Err(LeaseFenceError::OperationMismatch)
    );
}

#[test]
fn grant_renewal_and_exact_deadline_do_not_use_response_time_as_local_start() {
    let mut state = model();
    let first = state.begin(operation("grant"), 1, seconds(10)).unwrap();
    assert_eq!(first.accept_before(), seconds(39));
    assert_eq!(first.authority(), &authority_variant(0));
    state.acknowledge(&first, seconds(12)).unwrap();
    assert_eq!(
        state.local_state(seconds(38)).unwrap(),
        LocalLeaseState::ActiveUntil(seconds(39))
    );
    let renewal = state.begin(operation("renew"), 2, seconds(38)).unwrap();
    state.acknowledge(&renewal, seconds(40)).unwrap();
    assert_eq!(
        state.local_state(seconds(66)).unwrap(),
        LocalLeaseState::ActiveUntil(seconds(67))
    );
    assert_eq!(
        state.local_state(seconds(67)).unwrap(),
        LocalLeaseState::Expired
    );
    assert_eq!(
        state.native_effect_horizon(),
        NativeEffectHorizon::Until(seconds(71))
    );
}

#[test]
fn duplicate_and_out_of_order_completions_preserve_the_pending_request() {
    let mut state = model();
    let first = state.begin(operation("one"), 1, seconds(0)).unwrap();
    let before = at(state.clone(), seconds(1));
    assert_eq!(
        state.begin(operation("two"), 2, seconds(1)),
        Err(LeaseFenceError::RequestPending)
    );
    assert_eq!(state, before);
    state.acknowledge(&first, seconds(1)).unwrap();
    let before = at(state.clone(), seconds(2));
    assert_eq!(
        state.acknowledge(&first, seconds(2)),
        Err(LeaseFenceError::NoPendingRequest)
    );
    assert_eq!(state, before);
    assert_eq!(
        state.begin(operation("one"), 1, seconds(2)),
        Err(LeaseFenceError::StaleSequence)
    );
    let second = state.begin(operation("two"), 2, seconds(2)).unwrap();
    let before = at(state.clone(), seconds(3));
    assert_eq!(
        state.acknowledge(&first, seconds(3)),
        Err(LeaseFenceError::OperationMismatch)
    );
    assert_eq!(state, before);
    state.acknowledge(&second, seconds(3)).unwrap();
}

#[test]
fn expired_delayed_completion_cannot_resurrect_local_authority() {
    for completion in [seconds(29), seconds(30), seconds(1_000)] {
        let mut state = model();
        let request = state.begin(operation("grant"), 1, seconds(0)).unwrap();
        let before = at(state.clone(), completion);
        assert_eq!(
            state.acknowledge(&request, completion),
            Err(LeaseFenceError::Expired)
        );
        assert_eq!(state, before);
        assert_eq!(
            state.acknowledge(&request, seconds(1)),
            Err(LeaseFenceError::ClockRegression)
        );
        assert_eq!(
            state.local_state(completion).unwrap(),
            LocalLeaseState::Inactive
        );
        assert_eq!(
            state.native_effect_horizon(),
            NativeEffectHorizon::Unbounded
        );
    }
}

#[test]
fn cancellation_preserves_unbounded_native_effect_and_blocks_later_dispatch() {
    let mut state = model();
    let request = state.begin(operation("cancelled"), 1, seconds(0)).unwrap();
    state.cancel(&request, seconds(1)).unwrap();
    let before = at(state.clone(), seconds(2));
    assert_eq!(
        state.acknowledge(&request, seconds(2)),
        Err(LeaseFenceError::Cancelled)
    );
    assert_eq!(state, before);
    assert_eq!(
        state.begin(operation("later"), 2, seconds(2)),
        Err(LeaseFenceError::RequestPending)
    );
    assert_eq!(state, before);
    for now in [seconds(30), seconds(100_000), Duration::MAX] {
        assert_eq!(state.local_state(now).unwrap(), LocalLeaseState::Inactive);
        assert_eq!(
            state.native_effect_horizon(),
            NativeEffectHorizon::Unbounded
        );
    }
}

#[test]
fn supersession_revokes_local_authority_but_does_not_cancel_native_effect() {
    let mut state = model();
    let grant = state.begin(operation("grant"), 1, seconds(0)).unwrap();
    state.acknowledge(&grant, seconds(1)).unwrap();
    let renewal = state
        .begin(operation("old-epoch-renewal"), 2, seconds(2))
        .unwrap();
    state.revoke(seconds(3)).unwrap();
    let before = at(state.clone(), seconds(4));
    assert_eq!(
        state.acknowledge(&renewal, seconds(4)),
        Err(LeaseFenceError::Revoked)
    );
    assert_eq!(state, before);
    assert_eq!(
        state.begin(operation("new"), 3, seconds(4)),
        Err(LeaseFenceError::Revoked)
    );
    assert_eq!(
        state.native_effect_horizon(),
        NativeEffectHorizon::Unbounded
    );
    assert_eq!(
        state.local_state(seconds(100)).unwrap(),
        LocalLeaseState::Revoked
    );
}

#[test]
fn finite_native_horizon_requires_application_bound_or_successful_acknowledgement() {
    let mut configured = timing();
    configured.native_application = Some(seconds(5));
    let budget = LeaseBudget::new(configured).unwrap();
    assert_eq!(budget.renewal_coverage(), seconds(16));
    assert_eq!(budget.recovery_bound(), Some(seconds(64)));
    let mut state = LeaseModel::new(authority_variant(0), budget, seconds(0));
    let request = state.begin(operation("bounded"), 1, seconds(10)).unwrap();
    assert_eq!(
        state.native_effect_horizon(),
        NativeEffectHorizon::Until(seconds(46))
    );
    state.cancel(&request, seconds(11)).unwrap();
    state.revoke(seconds(12)).unwrap();
    assert_eq!(
        state.native_effect_horizon(),
        NativeEffectHorizon::Until(seconds(46))
    );

    let mut unknown = model();
    assert_eq!(LeaseBudget::new(timing()).unwrap().recovery_bound(), None);
    let request = unknown.begin(operation("unknown"), 1, seconds(0)).unwrap();
    assert_eq!(
        unknown.native_effect_horizon(),
        NativeEffectHorizon::Unbounded
    );
    unknown.acknowledge(&request, seconds(2)).unwrap();
    assert_eq!(
        unknown.native_effect_horizon(),
        NativeEffectHorizon::Until(seconds(33))
    );
}

#[test]
fn native_observation_loss_and_controller_restart_do_not_prove_global_expiry() {
    let mut predecessor = model();
    predecessor.begin(operation("lost"), 1, seconds(0)).unwrap();
    predecessor.local_state(seconds(100)).unwrap();
    assert_eq!(
        predecessor.native_effect_horizon(),
        NativeEffectHorizon::Unbounded
    );
    let mut successor = model();
    assert_eq!(
        successor.local_state(seconds(100)).unwrap(),
        LocalLeaseState::Inactive
    );
    // Empty actor accounting explicitly says nothing about predecessor requests.
    assert_eq!(
        successor.native_effect_horizon(),
        NativeEffectHorizon::NoSubmissions
    );
    assert_eq!(
        predecessor.native_effect_horizon(),
        NativeEffectHorizon::Unbounded
    );
}

#[test]
fn every_zero_timing_component_and_insufficient_coverage_is_rejected() {
    let mut candidate = timing();
    candidate.native_application = Some(Duration::ZERO);
    assert_eq!(
        LeaseBudget::new(candidate),
        Err(LeaseFenceError::InvalidTiming("native application"))
    );
    for field in 0..9 {
        let mut candidate = timing();
        let value = match field {
            0 => &mut candidate.observation,
            1 => &mut candidate.reconciliation,
            2 => &mut candidate.renewal_interval,
            3 => &mut candidate.request,
            4 => &mut candidate.quorum_loss,
            5 => &mut candidate.validity,
            6 => &mut candidate.margin,
            7 => &mut candidate.fencing,
            _ => &mut candidate.promotion,
        };
        *value = Duration::ZERO;
        assert!(matches!(
            LeaseBudget::new(candidate),
            Err(LeaseFenceError::InvalidTiming(_))
        ));
    }
    let mut candidate = timing();
    candidate.validity = seconds(16);
    assert!(matches!(
        LeaseBudget::new(candidate),
        Err(LeaseFenceError::InvalidTiming(_))
    ));
}

#[test]
fn arithmetic_and_clock_failures_preserve_state() {
    let mut candidate = timing();
    candidate.observation = Duration::MAX;
    assert_eq!(
        LeaseBudget::new(candidate),
        Err(LeaseFenceError::TimeOverflow)
    );
    candidate = timing();
    candidate.native_application = Some(Duration::MAX);
    assert_eq!(
        LeaseBudget::new(candidate),
        Err(LeaseFenceError::TimeOverflow)
    );

    let mut state = model();
    let before = at(state.clone(), Duration::MAX);
    assert_eq!(
        state.begin(operation("overflow"), 1, Duration::MAX),
        Err(LeaseFenceError::TimeOverflow)
    );
    assert_eq!(state, before);
    let mut state = model();
    let request = state.begin(operation("clock"), 1, seconds(10)).unwrap();
    let before = state.clone();
    assert_eq!(
        state.acknowledge(&request, seconds(9)),
        Err(LeaseFenceError::ClockRegression)
    );
    assert_eq!(
        state.cancel(&request, seconds(9)),
        Err(LeaseFenceError::ClockRegression)
    );
    assert_eq!(
        state.revoke(seconds(9)),
        Err(LeaseFenceError::ClockRegression)
    );
    assert_eq!(
        state.local_state(seconds(9)),
        Err(LeaseFenceError::ClockRegression)
    );
    assert_eq!(state, before);
}
