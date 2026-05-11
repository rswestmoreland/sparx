# Phase 33D - Validation and EPS Cleanup Checkpoint

## Purpose

Prepare the checkpoint for external Codex validation before signal-processing
implementation begins.

## Scope

- Keep runtime changes narrow and limited to validation blockers.
- Preserve AlertV1, DeviceStatsV1, SourceStreamStatsV1, storage layout, Fjall
  adapter boundaries, and provenance semantics.
- Keep signal-processing work documentation-only until Rust validation and EPS
  benchmark cleanup are complete.
- Keep task notes under `docs/roadmap/`; the parent `docs/` directory is for
  active public-facing guides.

## Changes in this checkpoint

- Tightened alert list/query loading so complete secondary indexes are used
  directly and incomplete or unreadable indexed paths fall back to primary alert
  scanning.
- Left primary alert fallback scans tolerant of malformed primary alert payloads
  so list/filter operations can continue across readable records.
- Moved validation, benchmark, handoff, and hardening review notes from the
  public docs root into phase-oriented files under `docs/roadmap/`.
- Updated active documentation indexes to distinguish public guides from task
  and roadmap notes.
- Updated the current checklist to state that external Rust validation remains
  required before signal-processing feature work.

## Expected Codex validation flow

Run under the external Rust toolchain environment:

```bash
cargo fmt --check
cargo check
cargo test
cargo clippy --all-targets --all-features -- -D warnings
cargo bench --bench tenant_device_eps
SPARX_BENCH_TENANTS=2 SPARX_BENCH_DEVICES_PER_TENANT=10 SPARX_BENCH_FILES_PER_DEVICE=5 SPARX_BENCH_EVENTS_PER_FILE=1000 cargo bench --bench tenant_device_eps
SPARX_BENCH_DURABLE_ONESHOT=1 cargo bench --bench tenant_device_eps
```

The durable oneshot benchmark is optional because it includes embedded DB and
runtime durability costs in addition to ingestion and detection timing.

## Acceptance gate

Signal-processing implementation should not begin until Codex provides green
format, check, test, and clippy logs, plus fresh default and 100000-event EPS
benchmark output.

## Notes

No local Rust build, test, clippy, or benchmark result is claimed for this
checkpoint.
