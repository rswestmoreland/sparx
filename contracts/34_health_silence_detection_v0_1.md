# Health and Silence Detection Contract v0.1

This contract defines `V_DROP`, the sparx sudden loss-of-log detection model.
It complements sparse-row anomaly scoring by detecting expected activity that is
missing or much lower than learned cadence.

## Active scope

Active subjects:

- device hard silence
- tenant aggregate hard silence
- device sharp drop
- tenant aggregate sharp drop
- source-stream hard silence and sharp drop behind the default-off source-stream
  gate

Active output:

- existing `AlertV1` primary alert path
- configured alert sink
- existing alert query/export/drill workflows

Active dedup:

- `silence_open/*` for hard-silence intervals
- `drop_open/*` for sharp-drop intervals

## Purpose

Detect when an expected tenant/device/source log stream stops or falls sharply
below learned cadence. This fills a gap in sparse-row scoring: when no row is
produced, row-based rarity/drift/volume scoring has nothing to score.

## Non-goals

- not a general host uptime monitor
- not an external heartbeat probe
- not a network reachability system
- not a replacement for recovery backlog, replay-rate, or health checks
- not cross-tenant outage correlation
- no replay ordering or alert delivery semantic changes

## Expected-source model

A subject is eligible only after enough observed history exists. Eligibility
should suppress alerting on sources that were never established as active.

Eligibility controls include:

- maturity count
- current bucket baseline
- expected line-volume floor
- tenant/device lifecycle state
- valid timestamps and counters
- existing open-state dedup checks

## Hard silence

Hard silence is the full-drop case. A candidate exists when a mature subject has
expected activity and misses the configured number of expected windows.

Hard-silence alerts may have empty provenance because absence of data can have no
current source span.

## Sharp drop

Sharp drop is the reduced-but-nonzero case. It uses the ratio semantics:

```text
observed_expected_ratio = observed_lines / expected_lines
drop_ratio = 1.0 - observed_expected_ratio
```

Hard silence takes priority over sharp drop for the same subject/window.

## Source-stream behavior

Source-stream `V_DROP` uses separate source-stream catalog, stats, expected-source
state, provenance, and open-state keys. It must not mutate `DeviceStatsV1` or use
source-stream IDs as feature IDs.

## Alert output shape

`V_DROP` uses the existing `AlertV1` path.

Allowed absence-of-data fields:

- `lines = 0`
- `bytes = 0`
- empty `top_features`
- empty `provenance`
- reason code `V_DROP`
- deterministic expected-vs-observed details

If provenance is empty, drill/extract should fail closed with a clear message that
the alert represents absence of expected data and has no raw source span.

## Deferred scope

Deferred until separately approved:

- parser-class silence/drop subjects
- vendor-event-family silence/drop subjects
- external heartbeat checks
- maintenance-window calendars
- cross-tenant outage correlation
- source-stream-specific threshold knobs
- suppression-reason metric labels
