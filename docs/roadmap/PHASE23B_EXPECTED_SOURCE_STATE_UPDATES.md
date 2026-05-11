# Phase 23b - Expected-Source State Updates from Finalized Windows

Phase 23b wires the Phase 23a expected-source state primitives into finalized-window
persistence. It is still not a `V_DROP` scoring phase.

## Boundary

Phase 23b activates expected-source state updates only:

- device expected-source state is updated when a device window finalizes
- tenant aggregate expected-source state is updated from the same finalized window
- observed-window counters advance for each finalized window update
- mature-window counters advance only when the finalized window meets the configured
  `scoring.min_lines_per_window` floor
- last-seen timestamps, observed lines, observed bytes, and bucket move forward only for
  non-regressive finalized windows

Phase 23b does not add:

- missing-window candidate evaluation
- `V_DROP` alert construction
- open-silence dedup persistence
- metrics or health fields
- replay, recovery, or sink behavior changes

## Active keys

Phase 23b starts writing the expected-source subject keys introduced in Phase 23a:

- `silence_subject/v1/device/<device_key>/state`
- `silence_subject/v1/tenant/state`

The open-silence dedup keys remain unused until a later phase:

- `silence_open/v1/device/<device_key>`
- `silence_open/v1/tenant`

## Update rules

For every finalized window that successfully reaches the existing finalize path:

1. compute the window size from `window_end_ts - window_start_ts`
2. update the device subject state
3. update the tenant aggregate subject state
4. increment `observed_windows_total_u64`
5. increment `mature_windows_total_u64` only when `lines >= min_lines_per_window`, or
   when `min_lines_per_window` is zero
6. update last-seen window fields only when the finalized window end timestamp is at or
   after the currently stored last-seen end timestamp
7. preserve non-regressive `last_update_ts_i64`

The non-regressive rule prevents replayed or delayed older windows from moving a subject
backward and later creating misleading silence candidates.

## Tests

Phase 23b adds focused coverage for:

- expected-source update initialization
- maturity floor counting
- non-regressive last-seen behavior for older replayed windows
- invalid window input rejection
- tenant DB read/write/update helpers for expected-source state
- oneshot finalized-window integration creating device and tenant aggregate state

## Current status

`V_DROP` remains inactive current scoring behavior. The system now learns expected-source
activity state from finalized windows, but it does not yet scan for missing windows or
emit loss-of-log alerts.

## Next phase

The next recommended phase later became Phase 23c: `V_DROP` candidate evaluator. Phase 23d later added deterministic `V_DROP` `AlertV1` construction and open-silence dedup state helpers. Phase 23e later activated first runtime hard-silence V_DROP integration and operator surfacing. Phase 23f later closed the first hard-silence path with validation hardening. Phase 25a later added the V_DROP config and tenant-policy surfaces. Phase 25b through Phase 25d later completed policy resolution, diagnostics, and diagnostics validation. Phase 26a now scopes future sharp-drop detection planning.


Phase 24 later locked V_DROP policy controls and diagnostics scope as planning-only work.
