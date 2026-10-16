# Kuberic Release Intake

## Purpose

Use this procedure before changing the Kuberic dependency declared in the root
`Cargo.toml`. It evaluates a specific dependency identity against the MSSQL
adapter's current contract, native evidence, persistence boundary, target
Service Fabric requirements, and licensed validation gates.

`Cargo.toml` is the dependency authority. `Cargo.lock` records reproducible
resolution, while intake records are review inputs and do not define or
document a separate repository pin.

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

Profile version 2 is the Public V1 intake profile. It retains the same
21-gate cardinality, ordering, and baseline/candidate requiredness as version
1, with exactly two semantic replacements:

| Version 1 gate | Version 2 gate | Public V1 meaning |
|---|---|---|
| `external_progress_contract` | `ag_sequence_history_contract` | Public scalar progress is the exact nonnegative SQL availability-group `sequence_number`; Kuberic epoch and SQL-native decimal database positions remain separate domains. |
| `signed_authorization_contract` | `ordinary_mutation_boundary` | Ordinary construction cannot reach any SQL executor path. The released Public V1 transport is not described as authenticated; licensed bootstrap remains a separate bounded test-only construction. |

All other gate identities are unchanged. A version-2 record containing a
version-1 replacement, or a version-1 record containing a version-2
replacement, is invalid rather than downgraded or reinterpreted.

A candidate record may describe the exact current and reviewed resolutions
during intake, but it is not checked in as dependency authority. A change to
`Cargo.toml` must not merge until the candidate computes `ready_to_pin` and the
required exact PR-head GitHub Actions check `Shared CI validation` passes. That
job runs `just ci`, including formatting, strict Clippy, full locked ordinary
tests, licensed one-replica, three-replica, and same-root restart validation,
and always runs `just cleanup`. The GitHub Actions job log is the run evidence.
Test logs and evidence artifacts are not committed to this repository.

Repository Cargo commands force `ACCEPT_EULA=Y` through `.cargo/config.toml`
as repository-wide authorization only for the Rust-owned licensed fixtures.
It does not authorize external or borrowed SQL Server instances. The production
adapter has no launcher, never accepts an EULA, and never deploys SQL Server.

The licensed Stage 3 configuration and seed callbacks remain a SQL-native test
seam, not end-to-end Public V1 Build operations. Public V1 Build evidence is
ordinary/server-free negative coverage for valid idle-build DTOs and
effect-free rejection. The licensed happy path's contained reports use role
`None`, progress role `Recovery`, `first = 0`, `last =` the current native AG
sequence, and read/write access `NotPrimary`; direct SQL observation separately
proves one native primary and two native secondaries.

The same-root run proves contained existing-state Open, fresh sessions,
stale-session rejection, native seed-record compaction, zero redispatch, and
marker continuity. It does not prove Public V1 peer-session/lifecycle renewal,
replay a Public V1 Build operation, or restore serving access.

`cancel_before_remove` uses two-part evidence. Kuberic release conformance
catalog gate KSF-06 proves host-owned cancellation/drain ordering. The MSSQL
local readiness test separately proves effect-free remove rejection after a
callback task has settled; it is not host-ordering evidence.

## Intake Procedure

1. Resolve the current `Cargo.toml` dependency and the exact reviewed candidate.
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
5. Run the complete exact-head pipeline through the required GitHub Actions
   job `Shared CI validation`, which executes:

   ```bash
   just ci
   ```

   The command performs setup, ordinary checks, licensed one-replica,
   licensed three-replica, and licensed same-root restart validation
   sequentially. The workflow always follows it with `just cleanup`. The
   exact-head check must pass; local runs are useful before pushing but do not
   replace it. Job logs remain in GitHub Actions and are not checked in or
   uploaded as evidence artifacts. Unavailable or unrun candidate-required
   licensed gates remain `pending`; they are never inferred from server-free
   success.
6. Fill one JSON record for the applicable profile version with every
   canonical gate and nonempty evidence. Validate it:

   ```bash
   just validate-kuberic-release-intake /absolute/path/to/intake.json
   ```

7. Do not merge the `Cargo.toml` dependency change unless the candidate outcome
   is `ready_to_pin` and the required exact PR-head
   `Shared CI validation` check passes.

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

## Stop Conditions

Stop intake as `blocked` when any required gate fails, an existing persisted
format could be reinterpreted ambiguously, a target-only behavior is presented
as current conformance, production mutation becomes reachable through ordinary
construction, or an unresolved safety question remains.

Stop intake as `incomplete` when required evidence is unavailable, including
licensed fixtures, signed authorization behavior, external progress/history,
fresh-state activation, or controller/runtime conformance.
