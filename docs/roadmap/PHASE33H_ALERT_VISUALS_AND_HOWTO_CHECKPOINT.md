# Phase 33H - Alert Visuals and HOWTO Checkpoint

This checkpoint is documentation and repository-asset only.

## Scope

- Add a realistic illustrative CLI-style alert screenshot asset.
- Add the annotated AlertV1 diagram asset to the repository.
- Link both assets from the public README in an alert-focused section.
- Add a public HOWTO with build, directory layout, configuration, ingest, alert,
  replay, and benchmark usage instructions.
- Reconcile documentation indexes and the current checklist.

## Files changed

- `README.md`
- `HISTORY.md`
- `docs/HOWTO.md`
- `docs/README.md`
- `docs/CURRENT_PLAN_CHECKLIST.md`
- `docs/images/sparx_alert_cli_screenshot.png`
- `docs/images/sparx_alertv1_annotated_diagram.png`
- `docs/roadmap/README.md`
- `docs/roadmap/PHASE33H_ALERT_VISUALS_AND_HOWTO_CHECKPOINT.md`

## Non-scope

- No runtime source changes.
- No tests or fixtures changed.
- No storage layout changes.
- No alert schema changes.
- No benchmark target changes.
- No signal-processing feature implementation.

## Validation note

No Rust build, test, clippy, or benchmark validation is claimed for this
checkpoint. The retained phase33f Rust 1.90 validation and benchmark report
remains the current runtime validation baseline until a later source-changing or
release-candidate checkpoint is revalidated.
