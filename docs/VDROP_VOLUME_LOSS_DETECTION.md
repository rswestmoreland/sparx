# V_DROP Volume-Loss Detection

`V_DROP` detects loss of expected log activity. It complements sparse-row anomaly
scoring by finding windows that are unexpectedly quiet.

## Subject families

Active subject families:

- device hard silence
- tenant aggregate hard silence
- device sharp drop
- tenant aggregate sharp drop
- source-stream hard silence and sharp drop behind the default-off source-stream
  gate

Deferred subject families:

- parser class
- vendor event family
- external heartbeat subjects
- maintenance-calendar-aware suppression
- cross-tenant outage correlation

## Hard silence

Hard silence is the full-drop case. A mature subject with expected activity has
zero observed lines across the required missed-window interval. Open hard-silence
state is stored under `silence_open/*`.

## Sharp drop

Sharp drop covers reduced-but-nonzero activity. The active ratio semantics are:

```text
observed_expected_ratio = observed_lines / expected_lines
drop_ratio = 1.0 - observed_expected_ratio
```

Hard silence takes priority over sharp drop. Open sharp-drop state is stored
under `drop_open/*` and is separate from hard-silence state.

## Expected volume

Device and tenant aggregate sharp-drop detection uses the locked 68-byte
`DeviceStatsV1` line-count bucket baseline as the primary expected-volume signal.
Source-stream detection uses separate source-stream catalog, stats, state, and
provenance structures. Source-stream behavior must not mutate device-level stats.

## Source-stream identity

A source stream is identified by tenant id, device key, and canonical relative
source path. The `source_stream_id` is a subject identifier, not a `FeatureId`.
It uses the existing stable BLAKE3 lowercase hex128 rule.

## Diagnostics boundaries

Diagnostics must remain bounded and low-cardinality. Prometheus labels must not
fan out by device, source path, source-stream id, parser class, vendor family,
per-subject state, or suppression reason.
