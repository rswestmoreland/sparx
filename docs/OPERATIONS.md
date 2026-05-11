# Operations

sparx is designed for deterministic operator workflows on Enterprise Linux.

## Runtime commands

- `run`: daemon-style processing for active tenants
- `oneshot`: bounded single-pass processing for a tenant or filtered device/time
- `status`: text status
- `status --json`: machine-readable status
- `/metrics`: Prometheus text metrics when enabled
- `/healthz`: health endpoint when enabled

## Tenant and maintenance commands

- `tenant policy show`
- `tenant policy check`
- `purge`
- `migrate`

## Alert workflows

- query alerts by time, category, entity, or search fields
- export alerts deterministically
- show an alert object
- drill or extract source spans using `AlertV1.provenance`

## Replay workflow

`replay-spool` replays filesystem spool files for replay-compatible sinks. It
sorts spool files deterministically and fails closed for stdout.

## Operator expectations

- failures should be visible and deterministic
- partial success should be reported explicitly
- diagnostics should be bounded
- sensitive high-cardinality subject names should not become metric labels
