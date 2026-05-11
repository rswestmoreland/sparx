# Phase 23f V_DROP Validation Hardening and Closeout

Phase 23f closes the first hard-silence `V_DROP` implementation arc. It does not add a
new silence-detection scope. The active `V_DROP` behavior remains limited to mature
expected-source state for:

- tenant/device subjects
- tenant aggregate subjects

The active runtime path remains:

1. finalized windows update `silence_subject/*` expected-source state
2. `run` and `oneshot` evaluate mature expected-source state after finalized-window
   processing
3. hard-silence candidates construct deterministic `AlertV1` objects with reason code
   `V_DROP`
4. emitted alerts are persisted through the primary alert object path and emitted through
   the configured alert sink
5. matching `silence_open/*` state suppresses duplicate alerts during the same silence
   interval
6. later observations close matching open-silence state so a future independent silence
   interval can alert again

## Validation hardening added in Phase 23f

Phase 23f adds explicit coverage for two important lifecycle edges:

- a closed open-silence state does not suppress a later independent candidate
- a later observation closes both device and tenant aggregate open-silence state without
  creating duplicate `V_DROP` alerts

The second case is covered through the `oneshot` runtime path by appending a later
observation after the initial hard-silence alert and verifying that:

- the original two `V_DROP` alerts remain the only `V_DROP` alerts
- the device open-silence state no longer has the open flag
- the tenant aggregate open-silence state no longer has the open flag
- both states retain the closed flag

## Active behavior after closeout

After Phase 23f, hard-silence `V_DROP` is active for the first scoped runtime path.
The implementation supports deterministic AlertV1 construction, open-silence duplicate
suppression, and open-state closure after later observation.

## Still deferred

The following remain intentionally out of scope:

- sharp-drop detection where observed volume is far below expected volume but nonzero
- per-file or source-path silence subjects
- parser-class or vendor-event-family silence subjects
- external heartbeat or reachability checks
- maintenance-window calendars
- cross-tenant outage correlation
- public silence policy knobs (implemented through Phase 25b)
- dedicated `V_DROP` metrics or health diagnostics

## External validation

Cargo/build/test execution was not performed in the ChatGPT sandbox. External validation
should include at least:

```text
cargo fmt --check
cargo test
cargo test --test db_silence
cargo test --test alert_scoring
cargo test --test db_tenant
cargo test --test oneshot_mode
cargo test --test run_mode
```

Optional stricter validation:

```text
cargo clippy --all-targets --all-features -- -D warnings
```

## Next recommended phase

Phase 24 should be a scope-lock phase for `V_DROP` policy controls and diagnostics before
expanding the detection model. It should decide whether the next implementation work is:

- public policy/config knobs for hard-silence thresholds (implemented through Phase 25b)
- operator metrics and health diagnostics for silence evaluation
- sharp-drop candidate planning
- richer subject scopes


Phase 24 later locked V_DROP policy controls and diagnostics scope in `docs/PHASE24_VDROP_POLICY_DIAGNOSTICS_SCOPE_LOCK.md` and `contracts/36_vdrop_policy_diagnostics_scope_v0_1.md`.

Phase 25a later added the V_DROP config and tenant-policy surfaces. Current next recommended phase: Phase 25b V_DROP policy resolution and runtime evaluator integration.
