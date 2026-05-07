# Worker Partitioning Contract v0.1

## Model
- Tenant-scoped processing: only one worker mutates a tenant’s state at a time.

## Stages
A) IO + decode (pollers): scan/tail/decompress -> batches of raw lines
B) Parse + aggregate + score (tenant workers): tokenize -> windows -> baselines -> alerts

## Workers
- IO workers (IO bound)
- Tenant workers (CPU+DB bound), process one tenant at a time via ready queue

## Scheduling
- tenant ready queue with slice limits (lines or ms)
- fairness across tenants, DB-cache friendly

## Backpressure
- per-tenant queue caps (bytes/lines)
- poll skipping when tenant buffers exceed caps
- metrics counters for backpressure

## Determinism
- window-aggregated scoring should tolerate minor ordering differences
