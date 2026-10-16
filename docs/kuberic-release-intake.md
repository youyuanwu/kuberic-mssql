# Kuberic Release Intake

## Purpose

Use this procedure before changing the pinned Kuberic revision. It evaluates a
specific dependency identity against the MSSQL adapter's current contract,
native evidence, persistence boundary, target Service Fabric requirements, and
licensed validation gates.

The intake command is unpublished test tooling. It does not change
`Cargo.toml`, `Cargo.lock`, SQL Server, runtime state, or Kubernetes resources.
It validates one complete JSON record and computes a deterministic outcome.

## Outcomes

The profile owns gate applicability and requiredness. A record cannot add,
remove, promote, demote, or reclassify gates.

Outcomes use this precedence:

1. **blocked** — an unresolved blocker exists or an applicable required gate
   failed;
2. **incomplete** — no blocker or required failure exists, but an applicable
   required gate is pending;
3. **ready_to_pin** — candidate mode, different current/reviewed dependency
   identities, and every candidate-required gate passed;
4. **baseline_confirmed** — baseline mode, identical current/reviewed
   dependency identities, and every baseline-required gate passed.

Optional gate status is retained but does not affect the outcome. Baseline mode
can never produce `ready_to_pin`; candidate mode cannot treat the current
dependency identity as a new candidate.

The CLI uses distinct exit codes:

| Exit | Meaning |
|---:|---|
| 0 | `ready_to_pin` |
| 2 | invalid record or invocation |
| 3 | `blocked` |
| 4 | `incomplete` |
| 5 | `baseline_confirmed` |

## Profile Version 1

Every record contains exactly one result for every gate below.

| Gate | Baseline | Candidate |
|---|---|---|
| `dependency_identity` | required | required |
| `semantic_contract_review` | required | required |
| `persistence_contract_review` | required | required |
| `fresh_state_activation_review` | required | required |
| `native_history_contract` | required | required |
| `pinned_adapter_contract` | required | required |
| `sf_readiness_contract` | required | required |
| `release_intake_contract` | required | required |
| `formatting` | required | required |
| `strict_clippy` | required | required |
| `full_locked_tests` | required | required |
| `live_one_replica` | optional | required |
| `live_three_replica` | optional | required |
| `live_three_replica_restart` | optional | required |
| `external_progress_contract` | optional | required |
| `role_before_primary_configuration` | optional | required |
| `same_role_secondary_epoch` | optional | required |
| `remote_active_membership` | optional | required |
| `invalid_build_progress` | optional | required |
| `cancel_before_remove` | optional | required |
| `signed_authorization_contract` | optional | required |

The last seven gates remain release-blocked under the current pin. Current
rejection or unsupported behavior is evidence of the gap, not future
conformance.

## Profile Version 2

Profile version 2 is the Public V1 candidate profile. It retains the same
21-gate cardinality, ordering, and baseline/candidate requiredness as version
1, with exactly two semantic replacements:

| Version 1 gate | Version 2 gate | Public V1 meaning |
|---|---|---|
| `external_progress_contract` | `ag_sequence_history_contract` | Public scalar progress is the exact nonnegative SQL availability-group `sequence_number`; Kuberic epoch and SQL-native decimal database positions remain separate domains. |
| `signed_authorization_contract` | `ordinary_mutation_boundary` | Ordinary construction cannot reach any SQL executor path. The released Public V1 transport is not described as authenticated; licensed bootstrap remains a separate bounded test-only construction. |

All other gate identities are unchanged. A version-2 record containing a
version-1 replacement, or a version-1 record containing a version-2
replacement, is invalid rather than downgraded or reinterpreted.

The candidate-only feature-branch exception permits `Cargo.lock` to resolve
the exact reviewed candidate while intake is in progress. It does not accept
the candidate as the production pin. The branch must not be proposed for
merge, described as pin adoption, or used to update the accepted dependency
identity until the exact candidate record computes `ready_to_pin`.

The current candidate record is
[kuberic-release-intake-public-v1-candidate.json](kuberic-release-intake-public-v1-candidate.json).
It deliberately remains `incomplete` while final locked verification or
licensed evidence is pending.

## Intake Procedure

1. Record the current locked Kuberic revision and the exact reviewed candidate.
   Baseline mode requires the same identity; candidate mode requires different
   identities. Identity syntax is canonical
   `<lowercase-package-name>@<40-lowercase-hex-git-revision>` with no
   surrounding whitespace or control characters.
2. Compare protected lifecycle ordering, configuration/build values, progress
   meanings, capabilities, signed authorization, reporting, and error
   semantics against the target Kuberic contract.
3. Compare agent/application persistence and activation. A candidate requiring
   fresh public-only state must not reinterpret current runtime binding,
   topology, seed-dispatch, SQL marker, or fixture-journal records.
4. Confirm the future authenticated replica-local adapter/control channel
   owns signed build and peer authorization validation. SQL Server HADR cannot
   carry Kuberic authorization tokens.
5. Run server-free gates with bounded resources:

   ```bash
   CARGO_BUILD_JOBS=1 just test-kuberic-readiness
   CARGO_BUILD_JOBS=1 cargo test --locked -p kuberic-mssql-tests \
     --test release_intake -- --test-threads=1
   CARGO_BUILD_JOBS=1 cargo fmt --all -- --check
   CARGO_BUILD_JOBS=1 cargo clippy --locked --workspace \
     --all-targets --all-features -- -D warnings
   CARGO_BUILD_JOBS=1 cargo test --locked --workspace --all-features \
     -- --test-threads=1
   ```

6. Only with explicitly provisioned licensed fixtures, run sequentially:

   ```bash
   CARGO_BUILD_JOBS=1 just test-live-one-replica
   CARGO_BUILD_JOBS=1 just test-live-three-replica
   CARGO_BUILD_JOBS=1 just test-live-three-replica-restart
   ```

   Unavailable or unrun candidate-required licensed gates remain `pending`;
   they are never inferred from server-free success.
7. Fill one JSON record for the applicable profile version with every
   canonical gate and nonempty evidence. Validate it:

   ```bash
   just validate-kuberic-release-intake /absolute/path/to/intake.json
   ```

8. Do not update the dependency pin unless the candidate outcome is
   `ready_to_pin`.

## Record Shape

```json
{
  "profile_version": 2,
  "mode": "candidate",
  "current_dependency_identity": "kuberic-runtime@0000000000000000000000000000000000000000",
  "reviewed_dependency_identity": "kuberic-runtime@1111111111111111111111111111111111111111",
  "semantic_contract_finding": "reviewed public lifecycle and value semantics",
  "persistence_contract_finding": "reviewed schemas and migration/cutover rules",
  "fresh_state_finding": "reviewed fresh-state activation requirements",
  "gates": [
    {
      "gate_id": "dependency_identity",
      "status": "passed",
      "evidence": "current and candidate identities are exact and different"
    }
  ],
  "blockers": [],
  "outcome": "incomplete"
}
```

The abbreviated example is not valid because all profile gates are mandatory
record entries. Use the profile table and the validation error output to
complete the record; requiredness and applicability are not JSON fields.

## Current-Pin Baseline

The readiness implementation was exercised against locked Kuberic revision
`09ce0a7ba9fdd9d6dcd207606fd0c9496a58c47f`.

The complete machine-readable record is
[kuberic-release-intake-baseline.json](kuberic-release-intake-baseline.json).
It is validated by the release-intake integration tests and can also be
inspected directly:

```bash
just validate-kuberic-release-intake \
  docs/kuberic-release-intake-baseline.json
```

| Evidence | Status |
|---|---|
| dependency identity and semantic review | passed |
| persistence and fresh-state review | passed |
| native history, pinned adapter, readiness, and intake tests | passed |
| formatting, strict Clippy, and full locked ordinary tests | passed |
| licensed one-replica, three-replica, and restart gates | pending; not run for the server-free readiness change |
| external progress contract | pending release |
| role before primary configuration | pending release; current pin rejects pre-admission role |
| newer same-role secondary epoch | pending release; current pin requires the frozen epoch |
| remote-active membership and invalid build progress | pending release |
| cancel-before-remove | pending release; removal remains unsupported |
| signed authorization contract | pending release |

Because all baseline-required gates passed and optional gates are accurately
pending, the dry-run outcome is `baseline_confirmed`, not `ready_to_pin`.

## Stop Conditions

Stop intake as `blocked` when any required gate fails, an existing persisted
format could be reinterpreted ambiguously, a target-only behavior is presented
as current conformance, production mutation becomes reachable through ordinary
construction, or an unresolved safety question remains.

Stop intake as `incomplete` when required evidence is unavailable, including
licensed fixtures, signed authorization behavior, external progress/history,
fresh-state activation, or controller/runtime conformance.
