# Phase 32 - Final hardening and release-readiness review

Status: complete as a documentation-only hardening/release-readiness checkpoint.

## Goal

Phase 32 reviews the completed Phase 31 source-stream V_DROP sequence and records the
remaining release-readiness gates for sparx v1. This phase does not add runtime behavior,
configuration, metrics, alert schema fields, storage encodings, replay behavior, or
recovery behavior.

## Review scope

Reviewed surfaces:

- root README and phase history
- docs README and current plan/checklist
- Phase 26 through Phase 31 planning and closeout documents
- Contracts 10, 11, 13, 25, 28, 33, 34, 36, 37, 38, and 39 for V_DROP, config, policy,
  diagnostics, source-stream scope, and release-gate consistency
- `src/` source tree with emphasis on source-stream, V_DROP, alert, policy, config,
  observability, and runtime routing modules
- `tests/` coverage names and Phase 31 source-stream test surfaces

## Findings

No blocking contract drift was found in the static review. The Phase 31 sequence is
closed through source-stream diagnostics, validation, and closeout.

The review identified stale planning wording only:

- `docs/README.md` still described the current checklist as only through the Phase 31e
  checkpoint.
- `contracts/39_source_stream_vdrop_implementation_plan_v0_1.md` still described its
  status as updated only through Phase 31e.
- top-level current-status wording still pointed at Phase 31f as the current phase.

Those wording issues were reconciled in this checkpoint.

## Current v1 status after Phase 32

Active v1 behavior now includes:

- device hard-silence V_DROP
- tenant aggregate hard-silence V_DROP
- device sharp-drop V_DROP
- tenant aggregate sharp-drop V_DROP
- source-stream hard-silence V_DROP behind the default-off source-stream gate
- source-stream sharp-drop V_DROP behind the default-off source-stream gate
- bounded V_DROP diagnostics for device, tenant aggregate, and source-stream subjects
- source-stream diagnostics bounded to aggregate and per-tenant metrics only

The following remain deferred and outside v1 release scope unless explicitly pulled in:

- parser-class V_DROP subjects
- vendor-event-family V_DROP subjects
- source-stream-specific threshold knobs
- heartbeat checks
- maintenance-window calendars
- cross-tenant outage correlation
- AlertV1 schema changes

## Preserved boundaries

Phase 32 changes no:

- runtime source behavior
- tests
- config schema
- tenant-policy schema
- AlertV1 schema
- AlertV1 provenance semantics
- DeviceStatsV1 layout
- SourceStreamStatsV1 layout
- tenant/global DB key encodings
- metrics names or labels
- replay behavior
- recovery behavior
- hashed-fallback FeatureId behavior

## Release-readiness gates still required

The project should not be called v1 finished until an external user-run validation pass
produces green logs for at least:

```text
cargo fmt --check
cargo check
cargo test
cargo clippy --all-targets --all-features -- -D warnings
```

If clippy is unavailable in the local toolchain, record that explicitly and run the other
gates.

Recommended scenario checks before v1 release:

- representative syslog, key/value, JSON, CSV, CEF, gzip, and plaintext fixtures
- run and oneshot with default source-stream gate disabled
- run and oneshot with source-stream gate enabled
- tenant-policy source-stream override true/false/inherit
- alert query/export/show/drill/extract for anomaly and V_DROP alerts
- replay-spool compatibility and fail-closed stdout behavior
- status, JSON status, metrics, and health output with and without V_DROP activity
- restart/recovery over open windows, output spool, alert persistence, and open-state keys

## Completion definition

sparx v1 can be considered complete when:

1. supported ingest/tokenization paths are stable and externally validated
2. sparse row construction, baselines, and stable storage encodings are externally validated
3. AlertV1 query/export/show/drill/extract paths are externally validated
4. V_DROP hard-silence and sharp-drop behavior is externally validated for device, tenant
   aggregate, and source-stream subjects under the default-off source-stream gate
5. operator workflows are documented and externally validated
6. final packaging, install, config, tenant-policy, migration, purge, and operations docs
   are complete
7. final cargo fmt/check/test/clippy logs are green or documented with an approved local
   tooling exception

## Recommended next phase

Phase 33 should be final validation packaging, driven by user-provided cargo/toolchain
logs and any failures found in local Windows validation. Fix build/test failures first
before adding new features.
