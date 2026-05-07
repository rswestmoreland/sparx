# Current Plan / Checklist

This document is the current working implementation plan and progress summary through the
Phase 15d closeout. Phases 0 through 12e are complete. Phase 12.5 is complete. Phase 13 is complete. Phase 14 is complete. Phase 15a is complete. Phase 15b is complete. Phase 15c is complete. Phase 15d is complete and Phase 16a is next.

Legend:
- done
- next
- planned

## Phase 0 - Repo scaffold
- done 0a Layout scaffold + contracts copied
  - checkpoint: `sparx_phase0a_scaffold.zip`
- done 0b Types-only module skeleton
  - checkpoint: `sparx_phase0b_types_skeleton.zip`

## Phase 1 - Config + CLI plumbing
- done 1a Config loader + precedence + validation
  - checkpoint: `sparx_phase1a_config_loader.zip`
- done 1b CLI parsing + routing stubs
  - checkpoint: `sparx_phase1b_cli_routing.zip`
- done 1c Status + config check implemented
  - checkpoints:
    - `sparx_phase1c_status_check.zip`
    - `sparx_phase1c_status_check_fix1.zip`
    - `sparx_phase1c_status_check_fix2.zip`
    - `sparx_phase1c_status_check_fix3.zip`

## Phase 2 - DB keys + encodings
- done 2a DB key builder module
  - checkpoint: `sparx_phase2a_db_key_builders.zip`
- done 2b Tenant simple value encodings + tests
  - checkpoint: `sparx_phase2b_tenant_simple_value_encodings.zip`
- done 2c Open-window checkpoint encoding + tests
  - checkpoint: `sparx_phase2c_open_window_checkpoint_encoding.zip`
- done 2d Baseline sketch encoding + tests
  - checkpoint: `sparx_phase2d_baseline_sketch_encoding.zip`

## Phase 3 - Directory discovery + cursors + readers
- done 3a Directory discovery per contract 08
  - checkpoint: `sparx_phase3a_directory_discovery.zip`
- done 3b Cursor state machine per contract 31
  - checkpoint: `sparx_phase3b_cursor_state_machine.zip`
- done 3c Reader abstraction (plain + gzip)
  - checkpoints:
    - `sparx_phase3c_reader_abstraction.zip`
    - `sparx_phase3c_reader_abstraction_fix1.zip`

## Phase 4 - Tokenization boundary
- done 4a Syslog envelope split (BSD + ISO variants)
  - checkpoints:
    - `sparx_phase4a_syslog_envelope_split.zip`
    - `sparx_phase4a_syslog_envelope_split_fix1.zip`
- done 4b Generic tokenizer (kv/json/csv + plaintext fallback)
  - checkpoints:
    - `sparx_phase4b_generic_tokenizer.zip`
    - `sparx_phase4b_generic_tokenizer_fix1.zip`
- done 4c CEF reverse parse
  - checkpoints:
    - `sparx_phase4c_cef_reverse_parse.zip`
    - `sparx_phase4c_cef_reverse_parse_fix1.zip`

## Phase 5 - Feature emission + entity sketches
- done 5a Feature dictionary (tenant-scoped `feat_dict/v1/*`)
  - checkpoints:
    - `sparx_phase5a_feature_dictionary.zip`
    - `sparx_phase5a_feature_dictionary_fix1.zip`
- done 5b Feature emission per catalog
  - checkpoint: `sparx_phase5b_feature_emission_catalog.zip`
- done 5c Entity sketches (top-K)
  - checkpoint: `sparx_phase5c_entity_sketches.zip`

## Phase 6 - Windowing + open-window persistence
- done 6a Window aggregator + checkpointing
  - checkpoints:
    - `sparx_phase6a_window_checkpointing.zip`
    - `sparx_phase6a_window_checkpointing_fix1.zip`
- done 6b Finalize windows
  - checkpoint: `sparx_phase6b_finalize_windows.zip`

## Phase 7 - Baselines + scoring + alert creation
- done 7a DF ring update
  - checkpoints:
    - `sparx_phase7a_df_ring_update.zip`
    - `sparx_phase7a_df_ring_update_fix1.zip`
    - `sparx_phase7a_df_ring_update_fix2.zip`
    - `sparx_phase7a_df_ring_update_fix3.zip`
    - `sparx_phase7a_df_ring_update_fix4.zip`
    - `sparx_phase7a_df_ring_update_fix5.zip`
- done 7b Centroid/stats update
  - checkpoints:
    - `sparx_phase7b_centroid_stats_update.zip`
    - `sparx_phase7b_centroid_stats_update_fix1.zip`
- done 7c Scoring + `AlertV1` + reasons + top features
  - checkpoints:
    - `sparx_phase7c_scoring_alerts.zip`
    - `sparx_phase7c_scoring_alerts_fix1.zip`

## Phase 8 - Output sinks + spool/replay helpers
- done 8a JSONL + stdout sink
  - checkpoint: `sparx_phase8a_output_sinks.zip`
- done 8b Spool + replay + caps helpers
  - checkpoint: `sparx_phase8b_spool_replay_caps.zip`

## Phase 9 - Fixtures + E2E smoke
- done 9a `validate-fixtures` implementation
  - checkpoint: `sparx_phase9a_validate_fixtures.zip`
- done 9b E2E smoke + restart recovery
  - checkpoints:
    - `sparx_phase9b_e2e_smoke_restart_recovery.zip`
    - `sparx_phase9b_e2e_smoke_restart_recovery_fix1.zip`

## Phase 10 - Operational foundation + real DB/runtime layer
- done 10a CLI dispatch hardening + fail-closed behavior
  - checkpoint: `sparx_phase10a_cli_dispatch_hardening.zip`
- done 10a docs/contracts rebase for Fjall + roadmap update
  - checkpoint: `sparx_phase10a_fjall_design_docs_contracts_fix1.zip`
- done 10b Filesystem layout helper + canonical path derivation
  - checkpoint: `sparx_phase10b_layout_helper.zip`
- done 10c Real global DB layer (Fjall)
  - checkpoints:
    - `sparx_phase10c_global_db_fjall.zip`
    - `sparx_phase10c_global_db_fjall_fix1.zip`
- done 10d Real tenant DB layer (Fjall)
  - checkpoint: `sparx_phase10d_tenant_db_fjall.zip`
- done 10e Tenant DB handle cache + lifecycle
  - checkpoint: `sparx_phase10e_tenant_cache.zip`
- done 10f Runtime context + repository helpers
  - checkpoint: `sparx_phase10f_runtime_context.zip`

## Phase 11 - Operational CLI commands
- done 11a Tenant purge implementation
  - checkpoint: `sparx_phase11a_tenant_purge.zip`
- done 11b Migrate implementation
  - checkpoints:
    - `sparx_phase11b_migrate.zip`
    - `sparx_phase11b_migrate_fix1.zip`
- done 11c Tenant policy show/check
  - checkpoints:
    - `sparx_phase11c_tenant_policy.zip`
    - `sparx_phase11c_tenant_policy_fix1.zip`
- done 11d Alerts query/show/search/export
  - checkpoints:
    - `sparx_phase11d_alerts_query_export.zip`
    - `sparx_phase11d_alerts_query_export_fix1.zip`
- done 11e Alert drill/extract
  - checkpoint: `sparx_phase11e_alert_drill_extract.zip`

## Phase 12 - Remaining service/runtime contract surfaces
- done 12a Replay-spool command
  - checkpoint: `sparx_phase12a_replay_spool.zip`
  - locked decisions now in effect:
    - `replay-spool` is filesystem/config based and does not open Fjall
    - deterministic spool ordering is by filename
    - successful replay deletes spool file
    - partial success returns exit `6`
    - `output.sink=stdout` fails closed for replay

- done 12b Status command backed by real persisted/runtime state
  - checkpoints:
    - `sparx_phase12b_status_runtime.zip`
    - `sparx_phase12b_status_runtime_fix1.zip`
  - current behavior:
    - real status via runtime/global DB path
    - supports text and `--json`
    - deterministic empty/populated output
    - DB/runtime open/read failures return exit `4`
    - single-owner DB test behavior accounted for in fix1

- done 12c Oneshot mode
  - checkpoints:
    - `sparx_phase12c_oneshot.zip`
    - `sparx_phase12c_oneshot_fix1.zip`
    - `sparx_phase12c_oneshot_fix2.zip`
    - `sparx_phase12c_oneshot_fix3.zip`
    - `sparx_phase12c_oneshot_fix4.zip`
  - implemented scope:
    - real oneshot parser/model:
      - required `--tenant`
      - optional `--since`
      - optional `--until`
      - optional `--device`
      - optional `--migrate auto|off|require`
    - real single-pass tenant processing through runtime/repositories
    - deterministic device ordering
    - deterministic file ordering
    - device/time filtering
    - cursor restore/reconciliation/advancement
    - open-window restore/checkpoint/finalize path
    - baseline update and alert emission through configured sink
    - partial-success exit `6` for mixed device outcomes
  - fixes absorbed:
    - newline literal compile fix in `route.rs`
    - removed `unused_mut` warning in `drilldown.rs`
    - corrected file ordering so plain files process before gzip when required by fixture chronology
    - hardened tests to avoid over-assuming alert emission under cold-start scoring
    - fixed final cursor persistence so successful gzip processing advances to actual source EOF

- done 12d Run daemon mode
  - checkpoints:
    - `sparx_phase12d_run_daemon.zip`
    - `sparx_phase12d_run_daemon_fix1.zip`
    - `sparx_phase12d_run_daemon_fix2.zip`
    - `sparx_phase12d_run_daemon_fix3.zip`
    - `sparx_phase12d_run_daemon_fix4.zip`
    - `sparx_phase12d_run_daemon_fix5.zip`
  - implemented scope:
    - real `run` parser/model:
      - `run [--migrate auto|off|require]`
    - daemon startup/shutdown runtime path
    - global process-state start/end bookkeeping
    - global schema handling at run startup
    - per-tenant schema handling for active tenants
    - deterministic tenant discovery
    - deterministic device processing order
    - disabled tenant skip behavior
    - final checkpoint flush path
    - best-effort final spool replay on shutdown
    - bounded-cycle test hook via `SPARX_TEST_RUN_MAX_CYCLES`
  - fixes absorbed:
    - newline literal compile fix in `route.rs`
    - corrected `ensure_run_signal_handler_v1()` result shape
    - removed unreachable fallback match arm
    - updated stale CLI dispatch test assumptions now that `run` is implemented
    - fixed malformed test config string generation
    - fixed Windows-safe TOML path quoting in `cli_dispatch.rs`
    - removed unused `route_unimplemented_v1`

- done 12e Tenant lifecycle runtime reconciliation
  - checkpoint: `sparx_phase12e_tenant_lifecycle_runtime_reconciliation.zip`
  - implemented scope:
    - daemon-cycle reconciliation of discovered tenants plus known global-DB tenants
    - disabled, terminating, and terminated tenants skipped without restart
    - deterministic active-index reconciliation
    - tenant `last_seen_ts` updates during run cycles
    - lifecycle transition tests covering live runtime behavior


## Phase 12.5 - Contract, config, and documentation closure
- done 12.5a Scope lock and checklist insertion
  - checkpoint: `sparx_phase12_5a_scope_lock.zip`
  - implemented scope:
    - Phase 12.5 added to the working plan/checklist and history surfaces
    - closure targets frozen from the Phase 12e review:
      - hashed-fallback drift
      - config validation gaps
      - loaded-but-unused config ambiguity
      - observability contract narrowing
      - spool contract/config mismatch
      - stale Fjall note wording
      - stray dead doc artifacts
    - observability decision locked for this phase:
      - narrow the contract now
      - keep richer observability work for Phase 13

- done 12.5b Hashed-fallback retirement
  - checkpoint: `sparx_phase12_5b_hashed_fallback_retirement.zip`
  - implemented scope:
    - reconciled Contracts 04, 05, 25, and 28 with the locked dictionary-only FeatureId implementation
    - retired the stale hybrid/hashed-fallback FeatureId contract language
    - removed the stale `hash_fallback_enabled` config field from the active config structs, defaults, file/env loader, and tests
    - kept `hash_space_bits` and `dict_gc_interval_s` explicitly documented as reserved compatibility/metadata fields, with their final config status closed in 12.5c

- done 12.5c Config contract reconciliation and validator hardening
  - checkpoint: `sparx_phase12_5c_config_reconciliation.zip`
  - implemented scope:
    - classified the remaining config surface in Contract 28 as active, reserved continuity, or deferred
    - added validator coverage for active enum fields `sparx.log_level` and `sparx.log_format`
    - kept reserved/deferred continuity fields parseable without promoting them to active runtime behavior
    - added config tests for enum rejection and deterministic config-file/env/CLI precedence

- done 12.5d Observability contract narrowing
  - checkpoint: `sparx_phase12_5d_observability_narrowing.zip`
  - implemented scope:
    - narrowed Contract 10 to the current `status`-centered v0.1 observability surface
    - classified `[metrics]` config as deferred placeholder continuity only in Contract 28
    - removed stale Prometheus-exporter wording from the tenant key-map contract
    - kept richer observability explicitly deferred to Phase 13

- done 12.5e Output sink and spool reconciliation
  - checkpoint: `sparx_phase12_5e_sink_spool_reconciliation.zip`
  - implemented scope:
    - reconciled Contract 28, Contract 29, and actual sink/runtime behavior
    - locked spool controls as helper/internal behavior rather than active v0.1 config
    - narrowed the contract so replay-spool remains the active filesystem/config replay surface without opening Fjall
    - removed the stale helper-only `spool_replay_interval_s` field from `SpoolConfigV1` and updated tests

- done 12.5f Fjall note and doc closure
  - checkpoint: `sparx_phase12_5f_docs_closure.zip`
  - implemented scope:
    - updated the Fjall/runtime design note to current implementation reality
    - removed stale planning-era wording and stale module-path references
    - removed stray dead doc artifacts
    - aligned README/docs status surfaces with the new current state

- done 12.5g Final consistency sweep and closeout
  - checkpoint: `sparx_phase12_5g_closeout_consistency.zip`
  - implemented scope:
    - completed a final consistency sweep across contracts, docs, config wording, and tests
    - removed the last stale config-contract wording around reserved `hash_space_bits` handling
    - finalized history/checklist/readme surfaces so Phase 13 starts clean

## Phase 13 - Observability expansion and release hardening
- done 13a Observability expansion
  - checkpoint: `sparx_phase13a_observability_expansion.zip`
  - implemented scope:
    - activated `/metrics` and `/healthz` during `run` when enabled
    - kept endpoint data grounded in real runtime/process/schema state plus persisted run-cycle totals
    - added persisted global metrics counters/gauges for run-cycle observability continuity
    - expanded `status` and `status --json` to report the implemented observability surface
  - tests:
    - endpoint enable/disable behavior
    - endpoint absence handled cleanly when disabled
    - status integration only for metrics that actually exist

- done 13b Release hardening and final operator ergonomics
  - checkpoint: `sparx_phase13b_release_hardening_and_operator_ergonomics.zip`
  - implemented scope:
    - added derived observability URLs to `status` and `status --json`
    - hardened observability startup so partial endpoint bind failures shut down any listener already started in the same run attempt
    - added endpoint behavior coverage for wrong-path `404` and non-`GET` `405`
    - completed the final observability docs/contracts/readme consistency pass after Phase 13a landed
  - tests:
    - status integration reflects derived endpoint URLs deterministically
    - endpoint wrong-path and wrong-method handling is deterministic
    - partial observability startup failure releases any previously bound listener before returning the startup error


## Phase 14 - Output recovery automation and follow-on tuning
- done 14a Output recovery automation
  - checkpoint: `sparx_phase14a_output_recovery_automation.zip`
  - implemented scope:
    - activated automatic jsonl-failure-to-spool fallback for the active `run` and `oneshot` jsonl sink path
    - activated bounded deterministic automated replay passes for `run` and `oneshot` while keeping manual `replay-spool` as the full-drain operator command
    - kept spool tuning as internal/default behavior rather than expanding the active config surface mid-phase
  - tests:
    - helper-level bounded replay coverage
    - `run` automatic replay integration
    - bounded replay leaves deterministic backlog in place
    - `oneshot` automatic replay integration
- done 14b Recovery visibility and tuning
  - checkpoint: `sparx_phase14b_recovery_visibility_and_tuning.zip`
  - implemented scope:
    - added recovery backlog visibility to `status`, `/metrics`, and `/healthz`
    - activated carefully scoped output config tuning for deterministic automated replay max-files-per-pass
    - kept replay cadence and spool-cap tuning deferred so the active runtime/config surface stays narrow
  - tests:
    - status text now includes recovery tuning/backlog fields
    - status json now covers nonzero spool backlog deterministically
    - config validation rejects zero automated replay max-files-per-pass
    - observability endpoint coverage includes the active recovery metrics/health view
    - `run` bounded replay coverage now exercises the config-controlled per-pass limit

## Known intentionally unresolved items still carried forward
These remain intentionally unresolved or deferred after the Phase 15d closeout:
- configurable replay cadence and config-exposed spool-cap tuning remain deferred beyond the active v0.1 runtime/config surface
- per-tenant recovery backlog visibility remains deferred beyond the active global recovery view

## Current progress summary
Completed:
- Phases `0` through `12e`
- Phase `12.5a`
- Phase `12.5b`
- Phase `12.5c`
- Phase `12.5d`
- Phase `12.5e`
- Phase `12.5f`
- Phase `12.5g`
- Phase `13a`
- Phase `13b`
- Phase `14a`
- Phase `14b`
- Phase `15a`
- Phase `15b`
- Phase `15c`
- Phase `15d`

Next recommended phase:
- **16a Replay cadence and spool-cap tuning**

Done 15c Secondary alert index query activation
- activated the persisted `alert_idx_time` path for list/search/export candidate selection when tenant index coverage is complete
- kept older tenants and mixed-history DBs correct by falling back to primary alert scans when the time index is absent or incomplete
- preserved deterministic list/search/export ordering and the primary `AlertV1` drill/export model

Done 15d Structured alert filter activation
- activated structured `--category` and `--entity-kind/--entity-value` alert filters for list/search/export
- used `alert_idx_cat` and `alert_idx_ent` candidate selection only when those indexes were complete enough to preserve correctness
- kept deterministic ordering and the primary `AlertV1` show/drill/export model authoritative while preserving mixed-history fallback to primary scans

Main focus for 16a:
- expose carefully scoped output-recovery cadence and spool-cap tuning without widening the active runtime/config surface too far
- keep fail-closed recovery behavior and deterministic replay ordering intact

