// Copyright (c) 2026 Richard S. Westmoreland
// SPDX-License-Identifier: MIT

use chrono::{TimeZone, Utc};
use sparx::tokenize::peel_syslog_envelope_v1;

#[test]
fn parses_rfc5424_with_pri_and_structured_data() {
    let ingest_ts = 1_710_000_000i64;
    let parsed = peel_syslog_envelope_v1(
        "<34>1 2026-03-20T12:34:56Z host1 app1 123 ID47 [exampleSDID@32473 iut=3 eventSource=Application] hello world",
        ingest_ts,
    );

    assert_eq!(parsed.envelope.pri, Some(34));
    assert_eq!(parsed.envelope.version, Some(1));
    assert_eq!(parsed.envelope.host.as_deref(), Some("host1"));
    assert_eq!(parsed.envelope.app.as_deref(), Some("app1"));
    assert_eq!(parsed.envelope.procid.as_deref(), Some("123"));
    assert_eq!(parsed.envelope.msgid.as_deref(), Some("ID47"));
    assert_eq!(
        parsed.envelope.structured_data.as_deref(),
        Some("[exampleSDID@32473 iut=3 eventSource=Application]")
    );
    assert_eq!(
        parsed.envelope.ts_guess,
        Some(Utc.with_ymd_and_hms(2026, 3, 20, 12, 34, 56).unwrap().timestamp())
    );
    assert_eq!(parsed.msg, "hello world");
}

#[test]
fn parses_bsd_with_tag_pid_and_infers_year() {
    let ingest_ts = Utc.with_ymd_and_hms(2026, 3, 20, 15, 0, 0).unwrap().timestamp();
    let parsed = peel_syslog_envelope_v1(
        "Mar 20 12:34:56 web01 sshd[1234]: Accepted password for user",
        ingest_ts,
    );

    assert_eq!(parsed.envelope.host.as_deref(), Some("web01"));
    assert_eq!(parsed.envelope.app.as_deref(), Some("sshd"));
    assert_eq!(parsed.envelope.procid.as_deref(), Some("1234"));
    assert_eq!(
        parsed.envelope.ts_guess,
        Some(Utc.with_ymd_and_hms(2026, 3, 20, 12, 34, 56).unwrap().timestamp())
    );
    assert_eq!(parsed.msg, "Accepted password for user");
}

#[test]
fn bsd_more_than_24h_future_uses_previous_year() {
    let ingest_ts = Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap().timestamp();
    let parsed = peel_syslog_envelope_v1("Dec 31 23:00:00 fw01 kernel: previous year event", ingest_ts);

    assert_eq!(
        parsed.envelope.ts_guess,
        Some(Utc.with_ymd_and_hms(2025, 12, 31, 23, 0, 0).unwrap().timestamp())
    );
    assert_eq!(parsed.envelope.app.as_deref(), Some("kernel"));
}

#[test]
fn parses_iso_timestamp_with_host_and_app_heuristics() {
    let ingest_ts = 1_710_000_000i64;
    let parsed = peel_syslog_envelope_v1(
        "2026-03-20T12:34:56Z db01 postgres: checkpoint complete",
        ingest_ts,
    );

    assert_eq!(parsed.envelope.host.as_deref(), Some("db01"));
    assert_eq!(parsed.envelope.app.as_deref(), Some("postgres"));
    assert_eq!(parsed.msg, "checkpoint complete");
}

#[test]
fn peels_cisco_style_embedded_prefix_and_prefers_embedded_timestamp() {
    let ingest_ts = Utc.with_ymd_and_hms(2026, 3, 20, 12, 0, 0).unwrap().timestamp();
    let parsed = peel_syslog_envelope_v1(
        "Mar 20 11:59:00 edge01 syslogd: 2026-03-20T11:58:59Z: %ASA-6-302013: Built outbound TCP connection",
        ingest_ts,
    );

    assert_eq!(parsed.envelope.peeled_prefixes, vec!["2026-03-20T11:58:59Z"]);
    assert_eq!(
        parsed.envelope.embedded_ts_guess,
        Some(Utc.with_ymd_and_hms(2026, 3, 20, 11, 58, 59).unwrap().timestamp())
    );
    assert_eq!(
        parsed.envelope.ts_guess,
        Some(Utc.with_ymd_and_hms(2026, 3, 20, 11, 58, 59).unwrap().timestamp())
    );
    assert_eq!(parsed.msg, "%ASA-6-302013: Built outbound TCP connection");
}

#[test]
fn falls_back_to_plaintext_without_hard_failure() {
    let ingest_ts = 1_710_000_000i64;
    let parsed = peel_syslog_envelope_v1("not a syslog header just text", ingest_ts);

    assert_eq!(parsed.envelope.pri, None);
    assert_eq!(parsed.envelope.ts_guess, None);
    assert_eq!(parsed.msg, "not a syslog header just text");
}

#[test]
fn keeps_pri_even_if_no_known_header_matches() {
    let ingest_ts = 1_710_000_000i64;
    let parsed = peel_syslog_envelope_v1("<13>payload without recognizable envelope", ingest_ts);

    assert_eq!(parsed.envelope.pri, Some(13));
    assert_eq!(parsed.msg, "payload without recognizable envelope");
}
