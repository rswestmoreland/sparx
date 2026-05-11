# Phase 23d V_DROP AlertV1 Construction and Open-Silence Dedup State

Phase 23d implements the next bounded piece of the health/silence roadmap. It turns an already-approved `VDropCandidateV1` into deterministic alert artifacts and adds tenant DB helpers for the matching open-silence dedup state.

## Implemented

- `build_vdrop_alert_v1(...)` constructs a deterministic `AlertV1` from `VDropCandidateV1`.
- The constructed alert uses reason code `V_DROP`.
- The constructed alert uses the existing primary alert key/value path through `alert/v1/<alert_id>`.
- The constructed alert has hard-silence absence-of-data fields:
  - `lines = 0`
  - `bytes = 0`
  - empty `top_features`
  - empty `provenance`
  - `score_rarity = 0.0`
  - `score_drift = 0.0`
  - `score_volume = drop_ratio`
- The constructed alert uses deterministic reason details copied from the candidate in contract order.
- The constructed alert id is derived from tenant id, subject kind, subject key, silence interval, reason code, and missed-window count.
- The helper returns the matching `OpenSilenceStateV1` for dedup state.
- Tenant DB helpers can now read/write:
  - `silence_open/v1/device/<device_key>`
  - `silence_open/v1/tenant`

## Not implemented in this phase

- No runtime missing-window scanner is active.
- No automatic `V_DROP` alert emission is active.
- No runtime code writes open-silence dedup state unless a caller explicitly uses the new helper.
- No `V_DROP` metrics or health fields are active.
- No sharp-drop detection is active.
- No observed-recovery close/supersede behavior is active.
- No replay or recovery behavior changed.

## Acceptance coverage added

- Device `V_DROP` candidate builds one deterministic `AlertV1`.
- Tenant aggregate `V_DROP` candidate maps to the sentinel alert device key `__tenant__`.
- Alert primary key/value output decodes back to the constructed alert.
- The returned open-silence state references the constructed alert id.
- Invalid candidates fail closed before alert construction.
- Tenant DB open-silence read/write helpers roundtrip device and tenant state.

## Handoff

Phase 23e later connected expected-source state, the pure candidate evaluator, `AlertV1` construction, and open-silence dedup state through the first runtime hard-silence path. Phase 23f later closed that path with validation hardening. Phase 25a later added the V_DROP config and tenant-policy surfaces. Phase 25b through Phase 25d later completed policy resolution, diagnostics, and diagnostics validation. Phase 26a now scopes future sharp-drop detection planning.


Phase 24 later locked V_DROP policy controls and diagnostics scope as planning-only work.
