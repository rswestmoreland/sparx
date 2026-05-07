# Fixtures

Fixture corpus layout is defined by Fixture Corpus Contract v0.1.

Repo fixture contents for Phase 9b:
- tenants/smoke/devices/edge01.log
- tenants/smoke/devices/edge01.gz
- golden/smoke_alert_subset.json
- golden/smoke_status.json
- gen/smoke_scenario.toml

The Phase 9b E2E smoke test copies the handwritten smoke log fixture into a temporary watch root that matches ingest discovery layout and then verifies single-pass and restart-recovery equivalence.
