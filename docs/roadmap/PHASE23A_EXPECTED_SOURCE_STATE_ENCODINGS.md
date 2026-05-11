# Phase 23a - Expected-Source State Structs and Encodings

Phase 23a starts the implementation sequence for future `V_DROP` / sudden loss-of-log
detection by adding inactive state structs, deterministic encodings, tenant DB key
helpers, and focused codec/key tests.

## Boundary

Phase 23a does not activate `V_DROP` scoring behavior.

It adds:

- `ExpectedSourceStateV1`
- `OpenSilenceStateV1`
- fixed-size `ExpectedSourceStateV1` byte encoding
- variable-size `OpenSilenceStateV1` byte encoding
- canonical tenant DB key builders for `silence_subject/*` and `silence_open/*`
- unit tests for encode/decode and key formatting

It does not add:

- expected-source state updates from finalized windows
- missing-window candidate evaluation
- `AlertV1` construction for `V_DROP`
- dedup/open-silence runtime persistence
- metrics or health output
- recovery, replay, or output-sink changes

## Added module

`src/db/silence.rs` defines the inactive state primitives for future silence detection.
The module is intentionally independent of runtime/scoring paths in Phase 23a.

## ExpectedSourceStateV1

`ExpectedSourceStateV1` is the future per-subject state record for expected activity.
It supports the first planned subject kinds:

- device subject: `subject_kind_u8 = 1`
- tenant aggregate subject: `subject_kind_u8 = 2`

The encoding is a fixed 68-byte little-endian structure matching Contract 35.
Reserved fields must be zero. Decode rejects unknown schema versions, invalid subject
kinds, nonzero reserved fields, and invalid lengths.

## OpenSilenceStateV1

`OpenSilenceStateV1` is the future open/last-emitted silence dedup record. It stores the
silence interval start, last alert window, and last alert id.

The encoding has a 30-byte fixed header followed by `last_alert_id` bytes. The alert id
must be ASCII lowercase hex when present. Decode rejects unknown schema versions, invalid
subject kinds, invalid lengths, trailing bytes, and non-lowercase-hex alert id bytes.

## Key helpers

Phase 23a adds key builders for the future tenant DB records:

- `silence_subject/v1/device/<device_key>/state`
- `silence_subject/v1/tenant/state`
- `silence_open/v1/device/<device_key>`
- `silence_open/v1/tenant`

These key helpers were canonical and unused at Phase 23a. Phase 23b later starts writing
`silence_subject/*` expected-source state from finalized windows; `silence_open/*` remains
unused until dedup is implemented.

## Tests

Added coverage includes:

- `ExpectedSourceStateV1` 68-byte roundtrip
- tenant and device subject kinds
- invalid schema version rejection
- invalid subject kind rejection
- nonzero reserved field rejection
- invalid length rejection
- `OpenSilenceStateV1` variable-length alert id roundtrip
- closed tenant subject with empty alert id
- invalid alert id byte rejection
- declared/actual alert id length mismatch rejection
- deterministic key builder output for silence state keys

## Next phase

Phase 23b later added state update from finalized windows. The current next recommended
phase later became Phase 23c: `V_DROP` candidate evaluator. Phase 23d later added deterministic `V_DROP` `AlertV1` construction and open-silence dedup state helpers. Phase 23e later activated first runtime hard-silence V_DROP integration and operator surfacing. Phase 23f later closed the first hard-silence path with validation hardening. Phase 25a later added the V_DROP config and tenant-policy surfaces. Phase 25b through Phase 25d later completed policy resolution, diagnostics, and diagnostics validation. Phase 26a now scopes future sharp-drop detection planning.


Phase 24 later locked V_DROP policy controls and diagnostics scope as planning-only work.
