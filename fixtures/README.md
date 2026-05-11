# Fixtures

Fixture corpus layout is defined by Fixture Corpus Contract v0.1.

Repository fixture contents:
- tenants/smoke/devices/edge01.log
- tenants/smoke/devices/edge01.gz
- golden/smoke_alert_subset.json
- golden/smoke_status.json
- gen/smoke_scenario.toml

The end-to-end smoke test copies the handwritten smoke log fixture into a temporary watch root that matches ingest discovery layout and then verifies single-pass and restart-recovery equivalence.


## License and author

Fixtures in this repository are distributed with sparx under the MIT License.

Author: Richard S. Westmoreland  
Contact: dev@rswestmore.land  
Copyright (c) 2026 Richard S. Westmoreland.

See `../LICENSE` for the full license text.
