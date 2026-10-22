#!/usr/bin/env python3
"""Account explicitly for licensed journeys blocked by public SQL semantics."""

import argparse
import sys


BINARY = "kuberic-mssql-tests::live_three_replica"
REASON = (
    "SQL-native/public history mapping is unimplemented; public current progress "
    "and catch-up capability refuse unsupported semantics."
)
BLOCKED_FAMILIES = {
    "three_replica_mssql_bootstrap_fault_boundaries": (
        "source_recovery",
        "source_database_creation_intent",
        "source_database_completion",
        "source_database_creation_intent_removal",
        "source_ownership",
        "source_marker_table",
        "source_full_backup",
        "source_log_backup",
        "source_backup_verification",
        "availability_group_ownership_intent",
        "availability_group_creation_intent",
        "availability_group_creation",
        "availability_group_binding",
        "availability_group_creation_intent_removal",
        "primary_seeding_grant",
        "primary_seeding_grant_evidence",
        "first_secondary_join",
        "first_secondary_seeding_grant",
        "first_secondary_seeding_grant_evidence",
        "second_secondary_join",
        "second_secondary_seeding_grant",
        "second_secondary_seeding_grant_evidence",
        "seed_boundaries",
        "post_native_success",
    ),
    "three_replica_mssql_seed_dispatch_handshake_faults": (
        "before_request_submission",
        "after_request_submission",
        "accepted_reply_lost",
        "definite_sql_rejection",
    ),
    "three_replica_mssql_conflict_evidence": (
        "native_source_recovery_partial",
        "native_unowned_source_database",
        "native_unowned_availability_group",
        "availability_group_identity_profile",
        "logical_replica_native_identity",
        "endpoint_member_profile",
        "standalone_database_identity_recovery",
        "associated_database_identity_recovery",
        "target_lineage",
        "sql_process_incarnation",
    ),
    "three_replica_mssql_terminal_seed_conflicts": (
        "association_removal",
        "ag_recreation",
    ),
    "three_replica_mssql_target_host_evidence_faults": (
        "pre_association_sql_process_restart",
        "in_flight_seeding_sql_process_restart",
        "post_native_pre_reply_sql_process_restart",
        "lineage_mismatch_across_checkpoints",
    ),
    "three_replica_post_ag_and_report_fault_checkpoints_recover": (
        "fail_after_ag",
        "panic_after_agent_start",
        "fail_during_report",
        "fail_after_agent_restart_and_retry",
    ),
    "hosted_witness": ("three_replica_mssql_live_hosted_native_witness",),
}
BLOCKED = frozenset(
    f"{family}::{case}"
    for family, cases in BLOCKED_FAMILIES.items()
    for case in cases
)
if sum(map(len, BLOCKED_FAMILIES.values())) != len(BLOCKED):
    raise ValueError("duplicate blocked licensed test")


def live_filter():
    cases = " | ".join(f"test(={name})" for name in sorted(BLOCKED))
    return f"not (binary_id(={BINARY}) & ({cases}))"


def report():
    print(f"BLOCKED licensed coverage: {len(BLOCKED)} journeys (not executed or qualified).")
    print(REASON)
    for family, cases in BLOCKED_FAMILIES.items():
        print(f"  {family}: {len(cases)} blocked")
    print("Native observation/prefix, owned-launch recovery and lease/fence remain separate evidence.")
    print("Passing active CI tests does not complete Stage 6A or qualify these blocked journeys.")
    print("Reactivation needs semantic proof and revised scenario assertions, not just ledger removal.")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    modes = parser.add_mutually_exclusive_group()
    modes.add_argument("--filter", action="store_true", help="print the exact nextest exclusion")
    modes.add_argument("--require-public", action="store_true", help="reject a blocked public recipe")
    args = parser.parse_args()
    if args.filter:
        print(live_filter())
        return 0
    report()
    if args.require_public:
        print("Requested public journey is unsupported; no fixture was launched.", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
