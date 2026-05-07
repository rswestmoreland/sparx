# Contract Consistency Checklist v0.1

This checklist confirms the v0.1 contract set is complete and internally consistent before implementation.

## A) Persistence coverage

1. Every key prefix has:
   - a Key Prefix Map entry, AND
   - a byte-level encoding contract for its values.

Global DB:
- Global DB Key Prefix Map v0.1 exists.

Tenant DB:
- Tenant DB Key Prefix Map v0.1 exists.
- Open-window keys are covered by Open-Window Checkpoint Encoding v0.1.
- Baseline keys are covered by Baseline Sketch Encoding v0.1.
- Remaining keys are covered by Tenant DB Simple Value Encodings v0.1.

2. Alert persistence:
- Alert Object Schema v0.1 exists.
- Output Sink Contract v0.1 exists.
- Drilldown model uses `AlertV1.provenance: Vec<FileSpanV1>`.

3. Migration:
- Both global and tenant DB include:
  - schema version keys
  - migration journal prefixes

---

## B) Feature pipeline coverage

4. Tokenization boundary:
- Syslog envelope rules exist (BSD + ISO variants).
- CEF reverse extension parse rule exists.
- Plaintext fallback feature emission rules exist.

5. Feature emission:
- Feature families enumerated.
- Caps exist per line and per window.
- Deterministic drop ordering exists.

6. Identity handling:
- Explicit "no redaction" statement exists.
- UserId normalization exists (email/user/domain backslash forms).
- Domain extracted separately as metadata (not merged into UserId).

---

## C) Baselines and scoring coverage

7. Windowing:
- window_size_s and max_emit_latency_s exist.
- bucket scheme (48) exists.

8. Baselines:
- DF ring sizing and retention exist.
- centroid/stats persistence exists.

9. Scoring:
- score components and thresholds exist.
- cold start behavior exists.

---

## D) Operational coverage

10. Config:
- Config Schema v0.1 exists with:
  - sources + precedence
  - defaults
  - bounds/whitelists

11. Deployment:
- Service/Deployment Contract v0.1 exists:
  - systemd expectations
  - permissions/paths
  - tenant purge behavior
  - single-process embedded DB ownership rule

12. CLI:
- CLI Contract v0.1 exists:
  - commands
  - exit codes
  - outputs
  - config-free command behavior
  - fail-closed rule for partial checkpoints

13. Fixtures:
- Fixture Corpus Contract v0.1 exists and includes:
  - layout rules
  - deterministic expected outputs

---

## E) Test plan coverage

14. Encoding roundtrips:
- open-window
- simple tenant values
- baseline sketches
- alert object

15. Determinism tests:
- ordering ties
- caps drop priority
- stable ids/signatures

16. Operational tests:
- tenant purge
- spool replay
- DB ownership failure path

17. E2E smoke test exists:
- ingest -> tokenize -> feature -> window -> score -> alert -> sink -> restart recovery.

---

## F) No-coding gate

Do not begin implementation if any of these are missing:
- Tokenization boundary contract
- Feature emission catalog + caps
- Baseline sketch encoding
- Open-window checkpoint encoding
- Alert object schema
- Tenant and global DB prefix maps + value encodings
- Config schema
- Output sink contract
- Fixture corpus contract
