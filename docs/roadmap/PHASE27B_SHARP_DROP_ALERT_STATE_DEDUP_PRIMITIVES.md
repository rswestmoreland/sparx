# Phase 27b Sharp-Drop State, AlertV1, and Dedup Primitives

Phase 27b is the second sharp-drop implementation subphase. It adds storage-neutral
state, key, alert-construction, and duplicate-suppression primitives for future
sharp-drop runtime integration.

This phase does not activate runtime sharp-drop alert emission. It does not add run or
oneshot integration, config fields, tenant-policy fields, diagnostics, replay changes,
recovery changes, or AlertV1 schema changes.

## Scope implemented

Phase 27b adds:

- `OpenDropStateV1` as a separate semantic state type from `OpenSilenceStateV1`
- `OpenDropStateV1` encode/decode helpers using the locked 30-byte fixed header plus
  variable lowercase-hex alert id bytes
- open, closed-by-recovery, and closed-by-hard-silence-supersession flags
- `drop_open/*` tenant DB key builders
- storage-neutral open-drop duplicate suppression helpers
- storage-neutral open-drop closure helpers
- `build_sharp_drop_alert_v1()` for deterministic AlertV1 construction
- deterministic state/key/alert tests

## Boundaries preserved

Phase 27b preserves:

- runtime sharp-drop alert emission remains inactive
- no tenant DB read/write helpers for `drop_open/*`
- no automatic open-drop persistence
- no runtime hard-silence or sharp-drop interaction changes
- no config or tenant-policy changes
- no diagnostics or metrics changes
- no AlertV1 schema change
- no replay or recovery behavior change
- `AlertV1.provenance` remains the only authoritative drilldown field model

## OpenDropStateV1

`OpenDropStateV1` is separate from `OpenSilenceStateV1`.

Fields:

- `schema_version_u16`
- `subject_kind_u8`
- `state_flags_u8`
- `drop_start_ts_i64`
- `last_alert_window_start_ts_i64`
- `last_alert_window_end_ts_i64`
- `last_alert_id`

The encoded fixed header is 30 bytes before variable alert id bytes:

- bytes 0..2: schema version u16 LE
- byte 2: subject kind
- byte 3: state flags
- bytes 4..12: drop start i64 LE
- bytes 12..20: last alert window start i64 LE
- bytes 20..28: last alert window end i64 LE
- bytes 28..30: alert id length u16 LE
- remaining bytes: lowercase ASCII hex alert id

Flags:

- bit 0: open sharp-drop interval exists
- bit 1: interval closed by recovery
- bit 2: interval closed by hard-silence supersession

The state intentionally does not store expected lines, observed lines, ratios, z-scores,
thresholds, or byte baselines. Those values are recomputed from finalized windows and
baseline stats during evaluation and alert construction.

## drop_open keys

Phase 27b adds canonical key builders for the future tenant-scoped drop state:

- `drop_open/v1/device/<device_key>`
- `drop_open/v1/tenant`

These key builders are available for later runtime phases, but Phase 27b does not add
runtime persistence.

## AlertV1 construction

`build_sharp_drop_alert_v1()` constructs a deterministic AlertV1 from a
`SharpDropCandidateV1`.

Locked behavior:

- existing AlertV1 schema is reused
- reason code is `V_DROP`
- first reason detail must be `drop_kind=sharp_drop`
- reason details preserve deterministic evaluator order
- label is `Info`
- confidence is `Medium`
- `score_volume = clamp01(drop_ratio)`
- `score_total = score_volume`
- `score_rarity = 0.0`
- `score_drift = 0.0`
- `top_features = []`
- alert id input includes `sharp_drop`
- device alerts preserve capped current-row provenance when provided
- tenant aggregate alerts use empty provenance for the first implementation

## Tests added

Phase 27b adds deterministic tests for:

- `drop_open/*` key builders
- `OpenDropStateV1` encode/decode roundtrip
- malformed `OpenDropStateV1` payload rejection
- open-drop duplicate suppression
- recovery closure state
- hard-silence supersession closure state
- sharp-drop AlertV1 field mapping
- deterministic reason detail preservation
- device provenance preservation
- tenant aggregate empty provenance
- hard-silence and sharp-drop alert id non-collision
- invalid sharp-drop alert candidate rejection

## Validation note

No local cargo/build/test run was performed in this environment. Rustfmt was attempted but
is not installed in this sandbox. Validation should be run externally with the project
standard cargo checks.

## Next recommended phase

Phase 27c should integrate sharp-drop evaluation and primitives into `run` and `oneshot`
while preserving hard-silence priority, replay behavior, recovery behavior, and bounded
metrics rules.
