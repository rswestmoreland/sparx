# sparx Docs

Key documents in this folder:
- `CURRENT_PLAN_CHECKLIST.md`: current implementation plan and progress through the Phase 15d closeout checkpoint.
- `FJALL_STORAGE_RUNTIME_DESIGN_NOTE.md`: storage/runtime design note for the implemented Fjall-backed DB foundation, reconciled through the Phase 12.5 closeout.

Additional history:
- `../PHASE_HISTORY.txt`: consolidated phase and fix checkpoint history.

Contracts remain under `../contracts/`.

---

- Phases 0 through 12 are complete
- Phase 12e completed tenant lifecycle runtime reconciliation for the daemon path,
  including disable/terminating enforcement without restart, deterministic active-index
  reconciliation across discovered and known tenants, and tenant `last_seen_ts` updates
  during observed inventory cycles
- Phase 12.5 completed contract/config/docs closure before Phase 13
- Phase 12.5a completed the scope lock and checklist insertion for that closure work
- Phase 12.5b completed hashed-fallback retirement across contracts and stale config surface
- Phase 12.5c completed config contract reconciliation and validator hardening
- Phase 12.5d completed observability contract narrowing to the current status-centered v0.1 surface
- Phase 12.5e completed output sink and spool reconciliation against the narrowed active runtime/config surface
- Phase 12.5f completed Fjall note and doc closure, including removal of stale planning wording and dead doc artifacts
- Phase 12.5g completed the final consistency sweep and closeout across contracts, docs, config wording, and tests
- Phase 13a completed observability expansion
- Phase 13b completed release hardening and final operator ergonomics
- Phase 14a completed output recovery automation
- Phase 14b completed recovery visibility and tuning
- Phase 15a completed scoring policy activation
- Phase 15b completed secondary alert index persistence
- Phase 15c completed secondary alert index query activation
- Phase 15d completed structured alert filter activation
- next recommended phase: 16a replay cadence and spool-cap tuning

## Current implementation priorities

- Phase 15b activated deterministic secondary `alert_idx_*` persistence alongside the primary alert object
- Phase 15c activates the persisted `alert_idx_time` path for list/search/export candidate selection when coverage is complete
- Phase 15d activates structured category/entity alert filters on top of the persisted secondary indexes while preserving backward-safe fallback to primary scans
- keep the primary `AlertV1` object authoritative for show/export/drill flows
- next focus: carefully scoped replay cadence and spool-cap tuning for output recovery
