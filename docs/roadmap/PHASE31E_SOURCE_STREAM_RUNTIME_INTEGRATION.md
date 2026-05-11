# Phase 31e - Source-stream runtime integration

Status: complete as a runtime-integration checkpoint.

Phase 31e wires source-stream V_DROP into `run` and `oneshot` behind the
source-stream gate added in Phase 31d. The gate remains default disabled. Runtime
source-stream evaluation is active only when global V_DROP is enabled and the
resolved source-stream gate is true for the tenant.

## Scope

Implemented:

- collect per-source-stream observations from applied lines only when the resolved
  source-stream gate is enabled
- persist source-stream catalog, stats, expected-source state, and per-source
  provenance when a finalized window is committed
- evaluate source-stream hard-silence candidates during the tenant V_DROP pass
- evaluate source-stream sharp-drop candidates from current finalized windows using
  `SourceStreamStatsV1` expected-volume summaries
- build and persist source-stream hard-silence and sharp-drop `AlertV1` objects
- persist source-stream `silence_open/*` and `drop_open/*` state for emitted alerts
- preserve hard-silence priority over sharp-drop for each source stream
- emit source-stream V_DROP alerts through the existing alert sink and recovery-safe
  emission flow

Not implemented in this phase:

- no new metrics
- no status output changes
- no health output changes
- no config schema changes
- no tenant-policy schema changes
- no AlertV1 schema changes
- no DeviceStatsV1 layout changes
- no replay behavior changes
- no recovery behavior changes
- no parser-class subject behavior
- no vendor-event-family subject behavior

## Runtime behavior

When source-stream V_DROP is disabled, `run` and `oneshot` do not collect source-stream
observations, do not write source-stream catalog/stats/state records, and do not emit
source-stream V_DROP alerts.

When source-stream V_DROP is enabled for a tenant:

1. Each applied line is attributed to a canonical source stream derived from tenant id,
   device key, and canonical relative source path.
2. Finalized windows update source-stream catalog, stats, and expected-source state
   independently from `DeviceStatsV1`.
3. The existing tenant V_DROP pass evaluates source-stream hard-silence candidates from
   source-stream expected-source state.
4. The same pass evaluates source-stream sharp-drop candidates for current finalized
   source-stream windows when a mature `SourceStreamStatsV1` baseline exists.
5. Source-stream alerts are persisted as normal `AlertV1` records and emitted through the
   existing sink path.

## Preserved contracts

- Source-stream IDs remain subject identifiers, not FeatureIds.
- Source-stream expected volume uses `SourceStreamStatsV1`, not `DeviceStatsV1`.
- `stats/v1/<device_key>/<bucket>` remains the fixed 68-byte `DeviceStatsV1` layout.
- `AlertV1.provenance` remains authoritative.
- Source-stream hard-silence uses `V_DROP` with `drop_kind=hard_silence`.
- Source-stream sharp-drop uses `V_DROP` with `drop_kind=sharp_drop`.
- `observed_expected_ratio = observed_lines / expected_lines`.
- `drop_ratio = 1.0 - observed_expected_ratio`.
- Hard silence has priority over sharp-drop for the same source stream.
- Source-stream open hard-silence state uses source-stream-specific `silence_open/*` keys.
- Source-stream open sharp-drop state uses source-stream-specific `drop_open/*` keys.
- Metrics remain bounded and low-cardinality.

## Tests added

Targeted `oneshot` and `run` tests were added for:

- default-off source-stream gate does not write source-stream runtime state
- enabled source-stream gate emits source-stream hard-silence V_DROP in oneshot and run
- tenant policy can disable source-stream runtime behavior even when global config enables it
- source-stream sharp-drop runtime emission from a seeded `SourceStreamStatsV1` baseline

## Validation notes

No local cargo/build/test/rustfmt/clippy run was performed in this sandbox. External
validation should run the normal project gates before accepting the checkpoint.

Static/hygiene checks performed in the sandbox:

- ASCII-only source/docs scan
- path-length scan
- stale-marker scan
- checkpoint zip integrity check

## Next phase

Phase 31f source-stream diagnostics, validation, and closeout was completed after this checkpoint.
