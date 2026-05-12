# Signal Processing MVP Plan

This plan carries forward the lean signal-processing work for sparx. The goal
is to improve baseline quality and reduce false positives while preserving the
sparse matrix model and existing storage contracts.

## Work order

1. Lock exact EWMA and periodic state details before coding.
2. Add EWMA state primitives.
3. Add periodic volume stats primitives.
4. Integrate mature-slot periodic expected volume into existing spike/drop
   evaluation.
5. Add bounded diagnostics and validation coverage.
6. Review performance impact with the ingest/detection EPS benchmark.

## Current validation and benchmark baseline

The current checkpoint carries forward a retained Rust 1.90 report with green
formatting, check, test, clippy, and tenant/device benchmark results. The
current planning estimates are about 58000 to 70000 split-path ingestion EPS,
about 740000 to 1390000 detection event EPS over finalized sparse rows, and
about 3100 durable oneshot total EPS on the documented default workload.

Any signal-processing implementation checkpoint should repeat the validation
flow after source or test changes and compare default plus 100000-event EPS
results against this baseline.

## MVP scope

The MVP includes:

- EWMA volume smoothing for device, tenant aggregate, and source-stream subjects
- hour-of-week periodic volume baselines for device, tenant aggregate, and
  source-stream subjects
- conservative mature-slot fallback into existing volume-based detection
- deterministic tests for state update, maturity, fallback, and alert behavior
- bounded diagnostics without per-subject metric fanout

The MVP does not include dense per-feature periodic baselines.

## EWMA implementation checklist

- Add a compact fixed-layout EWMA state record.
- Add key builders under the tenant DB key namespace.
- Add tenant DB read/write helpers.
- Add deterministic update helpers.
- Add tests for first update, repeated update, maturity, invalid state handling,
  and deterministic encoding.
- Keep EWMA as a supporting expected-volume signal.
- Do not introduce a new alert type.

## Periodic baseline implementation checklist

- Add a compact fixed-layout periodic volume stats record.
- Add hour-of-week slot calculation.
- Add key builders for device, tenant aggregate, and source-stream subjects.
- Add tenant DB read/write helpers.
- Add update helpers using finalized window volume.
- Add maturity rules for slot-specific expected volume.
- Add fallback to existing general baseline when the slot is immature.
- Add tests for slot calculation, maturity, fallback, deterministic encoding,
  and source-stream gating.

## Detection integration checklist

- Start with spike, extreme volume, and sharp-drop expected-volume inputs.
- Use periodic expected volume only when the slot is mature.
- Preserve hard-silence priority over sharp-drop.
- Preserve existing ratio semantics:
  - `observed_expected_ratio = observed_lines / expected_lines`
  - `drop_ratio = 1.0 - observed_expected_ratio`
- Preserve all existing alert IDs and AlertV1 schema semantics unless a change is
  explicitly approved.
- Add targeted alert scoring tests for business-hour spikes and scheduled quiet
  periods.

## Diagnostics checklist

- Add aggregate status counters only.
- Do not add source-path, source-stream-id, device, parser-class, vendor-family,
  per-subject, or suppression-reason Prometheus labels.
- Track bounded counts such as mature periodic slots, immature fallback count,
  and EWMA updated subject count.

## Performance checklist

- Measure default EPS before and after changes.
- Measure 100000-event EPS before and after changes.
- Keep new state updates O(1) per subject/window.
- Avoid per-feature periodic state in the hot path.
- Avoid unnecessary allocation while updating compact state records.

## Completion criteria

The MVP is complete when:

- validation commands pass under Rust 1.90 or newer
- default and 100000-event EPS results are recorded
- EWMA and periodic state tests pass
- alert scoring tests show false-positive reduction paths without contract drift
- docs and contracts reflect the implemented behavior

