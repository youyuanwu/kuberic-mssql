# Copilot Instructions - Kuberic MSSQL

This repository contains the standalone SQL Server availability-group observer
for Kuberic. It is observe-only: do not add SQL Server deployment, mutation,
failover, lease-renewal, or EULA-acceptance behavior without an explicit design
and safety review.

Run `cargo test --locked --workspace --all-features` and strict workspace Clippy
for routine changes.
Live observation tests require explicitly provisioned licensed fixtures and
must remain ignored by default.
