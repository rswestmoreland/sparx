# Architecture

sparx is a single-owner embedded runtime for sparse-matrix log analysis. It is
built around deterministic ingestion, sparse row construction, baseline updates,
and explainable alert creation.

## System goals

- process heterogeneous log collections across many tenants and devices
- keep storage practical by storing only observed sparse features
- preserve deterministic behavior for IDs, ordering, tie-breaks, and output
- retain provenance so alerts can drill back to source file spans
- fail closed for DB-backed runtime and CLI flows
- keep diagnostics bounded and safe for multi-tenant operation

## Runtime flow

1. Discover active tenants and device directories.
2. Read plain-text or gzip logs using stable cursor handling.
3. Tokenize supported formats and emit canonical features plus entity sketches.
4. Aggregate events into open windows.
5. Finalize mature windows into sparse rows.
6. Update per-device and per-tenant baselines.
7. Score sparse-row anomalies and volume-loss conditions.
8. Persist `AlertV1` records and secondary alert indexes.
9. Surface status, metrics, health, and alert workflows for operators.

## Tenant isolation

Tenant data is stored under tenant-specific roots and DB namespaces. Feature
dictionaries, baselines, alert objects, alert indexes, expected-source state,
source-stream catalogs, and source-stream stats stay tenant scoped.

## Determinism

sparx relies on deterministic ordering for scans, finalized windows, alert IDs,
secondary indexes, diagnostics, and exported output. Stable hashing uses the
first 16 bytes of a BLAKE3 digest rendered as lowercase hexadecimal.

## Storage boundary

Fjall is the active embedded DB backend. It must stay behind the thin adapter
boundary under `src/db/`. Higher-level runtime and CLI code should use internal
repository helpers instead of binding directly to storage-engine details.
