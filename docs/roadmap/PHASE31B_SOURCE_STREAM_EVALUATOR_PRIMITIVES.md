# Phase 31b Source-Stream Evaluator Primitives

Status: complete as an implementation-primitives checkpoint.

Phase 31b adds storage-agnostic source-stream evaluator helpers for `V_DROP` hard
silence and sharp drop. It uses the Phase 31a source-stream identity and stats-state
primitives, but it does not activate source-stream runtime evaluation or alert emission.

No local cargo/build/test/rustfmt run was performed in this sandbox.

## Scope

Implemented in Phase 31b:

- `SourceStreamSubjectV1` as the evaluator subject wrapper
- `SourceStreamCurrentWindowV1` as the source-stream current-window wrapper
- subject validation for tenant id, device key, source-stream id, and canonical source path
- expected-volume derivation from `SourceStreamStatsV1`
- source-stream hard-silence evaluator wrapper over the existing storage-agnostic
  hard-silence evaluator
- source-stream sharp-drop evaluator wrapper over the existing storage-agnostic
  sharp-drop evaluator
- deterministic source-stream reason-detail decoration for candidate output
- deterministic tests for expected volume, invalid stats, hard silence, sharp drop,
  hard-silence priority, maturity suppression, and expected-line floor suppression

Out of scope for Phase 31b:

- source-stream runtime scans or finalized-window integration
- source-stream AlertV1 construction
- source-stream open-state dedup helpers
- source-stream policy/config fields
- source-stream diagnostics/metrics/status/health output
- replay or recovery changes

## Expected-volume primitive

Phase 31b adds:

```text
sharp_drop_expected_volume_from_source_stream_stats_v1(SourceStreamStatsV1)
```

The helper validates the source-stream stats record and derives:

- maturity count from `line_count.n`
- expected lines from `line_count.mean`
- expected bytes from `byte_count.mean`
- line standard deviation from the source-stream line-count Welford state

`SourceStreamStatsV1` remains separate from `DeviceStatsV1`. The locked 68-byte
`DeviceStatsV1` layout is unchanged.

Malformed source-stream stats fail closed through `SourceStreamErrorV1` and do not panic.

## Hard-silence evaluator primitive

Phase 31b adds:

```text
evaluate_source_stream_hard_silence_candidate_v1(...)
```

The helper validates the source-stream subject, requires source-stream expected-source
state, and then reuses the existing storage-agnostic hard-silence evaluator. Candidate
output is decorated with source-stream subject details in deterministic order:

```text
subject_kind=source_stream
tenant_id=<tenant_id>
device_key=<device_key>
source_stream_id=<source_stream_id>
source_path=<canonical_relative_source_path>
```

Hard silence remains the full-drop case:

```text
observed_lines = 0
drop_ratio = 1.0
```

## Sharp-drop evaluator primitive

Phase 31b adds:

```text
evaluate_source_stream_sharp_drop_candidate_v1(...)
```

The helper validates the source-stream subject, creates a source-stream
`SharpDropCurrentWindowV1`, and reuses the existing storage-agnostic sharp-drop evaluator.
Candidate output is decorated with the same deterministic source-stream subject details.

Sharp-drop ratio semantics remain unchanged:

```text
observed_expected_ratio = observed_lines / expected_lines
drop_ratio = 1.0 - observed_expected_ratio
```

Zero observed lines are suppressed by `HardSilencePriority`, preserving the existing rule
that hard silence takes priority over sharp drop for the same source stream/window.

## Preserved behavior

Phase 31b preserves:

- source-stream runtime `V_DROP` remains inactive
- source-stream AlertV1 emission remains inactive
- config and tenant-policy schemas are unchanged
- metrics/status/health output is unchanged
- replay and recovery behavior are unchanged
- `AlertV1` schema and provenance authority are unchanged
- `DeviceStatsV1` 68-byte layout is unchanged
- no hashed-fallback FeatureId behavior is introduced
- no device/source-path/source-stream Prometheus labels are introduced

## Tests added

Phase 31b updates `tests/source_stream.rs` with deterministic coverage for:

- source-stream expected-volume derivation from `SourceStreamStatsV1`
- malformed source-stream stats fail closed without panic
- source-stream hard-silence candidate creation with full-drop semantics
- source-stream hard-silence maturity suppression
- wrong subject-kind suppression for the source-stream hard-silence helper
- source-stream sharp-drop candidate creation for reduced nonzero activity
- sharp-drop zero-observed suppression through hard-silence priority
- sharp-drop low expected-volume suppression
- deterministic reason-detail ordering for source-stream candidate output

## Validation notes

Performed in this sandbox:

- source and test files were edited for Phase 31b evaluator primitives
- docs/contracts/history were updated
- ASCII-only scan passed for text/source/docs files
- repo-relative path length remained within the 260-character cap
- checkpoint zip integrity was verified

Not performed in this sandbox:

- cargo fmt
- cargo check
- cargo test
- rustfmt

## Next recommended phase

Recommended next phase: Phase 31c source-stream AlertV1 construction and subject-specific
open-state/dedup primitives.

Phase 31c should remain runtime-inactive. Policy/config gating, run/oneshot integration,
and diagnostics should remain later phases.
