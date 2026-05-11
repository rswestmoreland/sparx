# Configuration and Policy

sparx uses a global configuration file plus environment overrides and tenant
policy overrides. Missing tenant-policy fields inherit from global config and
defaults.

## Core policy areas

- tenant enable/disable lifecycle state
- scoring thresholds and maturity floors
- output sink and recovery settings
- Prometheus and health endpoint settings
- `V_DROP` volume-loss controls
- source-stream `V_DROP` gate, disabled by default

## V_DROP controls

The `[vdrop]` configuration surface controls hard-silence and sharp-drop
volume-loss evaluation for device and tenant aggregate subjects. Source-stream
volume-loss evaluation has its own explicit gate and defaults to disabled.

Tenant policy can override the global source-stream gate using the inherited
source-stream policy field. Invalid tenant policy fails closed for that tenant's
volume-loss pass.

## Fail-closed behavior

DB-backed and runtime-sensitive flows should report failure instead of silently
pretending success. This includes invalid configuration, invalid tenant policy,
DB open/read/write failures, and replay modes that cannot guarantee durable
behavior.
