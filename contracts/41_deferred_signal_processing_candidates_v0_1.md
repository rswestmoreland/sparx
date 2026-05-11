# Contract 41: Deferred Signal Processing Candidates v0.1

## Purpose

This contract records signal-processing candidates that are intentionally outside
the lean MVP. These items require separate review before implementation.

## Deferred candidate: autocorrelation-lite

Autocorrelation-lite may be useful for detecting repeated intervals, retry
loops, heartbeat-like behavior, and beacon-like signals.

It is deferred because it requires additional recent-sample rings, lag scoring,
state maturity rules, diagnostics, and false-positive controls.

Before implementation, a future design must lock:

- target subjects
- sample history length
- supported lag range
- maturity requirements
- scoring formula
- storage encoding
- diagnostic surface
- alert interaction rules
- performance gates

Autocorrelation-lite must not be added implicitly as part of EWMA or periodic
baseline work.

## Deferred candidate: frequency-domain analysis

DFT/FFT-style analysis may be useful for offline periodicity review, but it is
outside the MVP. The MVP uses hour-of-week periodic volume baselines instead.

Frequency-domain analysis must not be added to the hot path without an explicit
contract, benchmark plan, and storage plan.

