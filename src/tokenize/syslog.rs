use crate::tokenize::{ParsedLineV1, SyslogEnvelopeV1};
use crate::types::UnixSec;
use chrono::{Datelike, NaiveDate, NaiveTime, TimeZone, Timelike, Utc};

pub fn parse_syslog_envelope_v1(line: &str, ingest_ts: UnixSec) -> ParsedLineV1 {
    peel_syslog_envelope_v1(line, ingest_ts)
}

pub fn peel_syslog_envelope_v1(line: &str, ingest_ts: UnixSec) -> ParsedLineV1 {
    let (pri, rest) = parse_pri_prefix_v1(line);

    if let Some(mut parsed) = parse_rfc5424_like_v1(rest, pri, ingest_ts) {
        peel_vendor_prefixes_v1(&mut parsed.envelope, &mut parsed.msg, ingest_ts, false);
        return parsed;
    }
    if let Some(mut parsed) = parse_bsd_v1(rest, pri, ingest_ts) {
        peel_vendor_prefixes_v1(&mut parsed.envelope, &mut parsed.msg, ingest_ts, true);
        return parsed;
    }
    if let Some(mut parsed) = parse_iso_heuristic_v1(rest, pri, ingest_ts) {
        peel_vendor_prefixes_v1(&mut parsed.envelope, &mut parsed.msg, ingest_ts, false);
        return parsed;
    }

    ParsedLineV1 {
        envelope: SyslogEnvelopeV1 {
            pri,
            ..SyslogEnvelopeV1::default()
        },
        msg: rest.to_string(),
    }
}

fn parse_pri_prefix_v1(line: &str) -> (Option<u32>, &str) {
    if !line.starts_with('<') {
        return (None, line);
    }
    let bytes = line.as_bytes();
    let mut idx = 1usize;
    while idx < bytes.len() && bytes[idx].is_ascii_digit() {
        idx += 1;
    }
    if idx == 1 || idx >= bytes.len() || bytes[idx] != b'>' {
        return (None, line);
    }
    let pri = match line[1..idx].parse::<u32>() {
        Ok(v) => v,
        Err(_) => return (None, line),
    };
    let mut rest = &line[idx + 1..];
    rest = trim_ascii_start_v1(rest);
    (Some(pri), rest)
}

fn parse_rfc5424_like_v1(line: &str, pri: Option<u32>, _ingest_ts: UnixSec) -> Option<ParsedLineV1> {
    let s = trim_ascii_start_v1(line);
    let (version_token, rest) = take_space_token_v1(s)?;
    let version = version_token.parse::<u32>().ok()?;
    let (ts_token, rest) = take_space_token_v1(rest)?;
    let ts_guess = parse_iso_timestamp_v1(ts_token)?;
    let (host_token, rest) = take_space_token_v1(rest)?;
    let (app_token, rest) = take_space_token_v1(rest)?;
    let (procid_token, rest) = take_space_token_v1(rest)?;
    let (msgid_token, rest) = take_space_token_v1(rest)?;
    let (structured_data, msg) = parse_structured_data_prefix_v1(rest)?;

    Some(ParsedLineV1 {
        envelope: SyslogEnvelopeV1 {
            pri,
            version: Some(version),
            ts_guess: Some(ts_guess),
            host: dash_is_none_v1(host_token),
            app: dash_is_none_v1(app_token),
            procid: dash_is_none_v1(procid_token),
            msgid: dash_is_none_v1(msgid_token),
            structured_data,
            embedded_ts_guess: None,
            peeled_prefixes: Vec::new(),
        },
        msg,
    })
}

fn parse_bsd_v1(line: &str, pri: Option<u32>, ingest_ts: UnixSec) -> Option<ParsedLineV1> {
    let s = trim_ascii_start_v1(line);
    let (month_token, rest) = take_space_token_v1(s)?;
    let month = parse_bsd_month_v1(month_token)?;
    let (day_token, rest) = take_space_token_v1(rest)?;
    let day = day_token.parse::<u32>().ok()?;
    let (time_token, rest) = take_space_token_v1(rest)?;
    let time = NaiveTime::parse_from_str(time_token, "%H:%M:%S").ok()?;
    let (host_token, rest) = take_space_token_v1(rest)?;
    let rest = trim_ascii_start_v1(rest);
    let colon_idx = rest.find(':')?;
    let tag = rest[..colon_idx].trim();
    if tag.is_empty() {
        return None;
    }
    let msg = trim_ascii_start_v1(&rest[colon_idx + 1..]).to_string();
    let (app, procid) = parse_bsd_tag_v1(tag);
    let ts_guess = infer_bsd_timestamp_v1(ingest_ts, month, day, time)?;

    Some(ParsedLineV1 {
        envelope: SyslogEnvelopeV1 {
            pri,
            version: None,
            ts_guess: Some(ts_guess),
            host: Some(host_token.to_string()),
            app,
            procid,
            msgid: None,
            structured_data: None,
            embedded_ts_guess: None,
            peeled_prefixes: Vec::new(),
        },
        msg,
    })
}

fn parse_iso_heuristic_v1(line: &str, pri: Option<u32>, _ingest_ts: UnixSec) -> Option<ParsedLineV1> {
    let s = trim_ascii_start_v1(line);
    let (ts_guess, consumed) = parse_iso_prefix_v1(s)?;
    let rest = trim_ascii_start_v1(&s[consumed..]);

    let mut host: Option<String> = None;
    let mut app: Option<String> = None;
    let mut msg = rest.to_string();

    if let Some((first, after_first)) = take_space_token_v1(rest) {
        if let Some(name) = first.strip_suffix(':') {
            if !name.is_empty() {
                app = Some(name.to_string());
                msg = trim_ascii_start_v1(after_first).to_string();
            }
        } else if let Some((second, after_second)) = take_space_token_v1(after_first) {
            if let Some(name) = second.strip_suffix(':') {
                if !name.is_empty() && is_host_like_v1(first) {
                    host = Some(first.to_string());
                    app = Some(name.to_string());
                    msg = trim_ascii_start_v1(after_second).to_string();
                }
            }
        }
    }

    Some(ParsedLineV1 {
        envelope: SyslogEnvelopeV1 {
            pri,
            version: None,
            ts_guess: Some(ts_guess),
            host,
            app,
            procid: None,
            msgid: None,
            structured_data: None,
            embedded_ts_guess: None,
            peeled_prefixes: Vec::new(),
        },
        msg,
    })
}

fn peel_vendor_prefixes_v1(envelope: &mut SyslogEnvelopeV1, msg: &mut String, ingest_ts: UnixSec, allow_embedded_override: bool) {
    let mut current = msg.as_str();
    let mut peeled: Vec<String> = Vec::new();

    for _ in 0..2 {
        let (prefix, remainder) = match split_vendor_prefix_v1(current) {
            Some(v) => v,
            None => break,
        };
        if prefix.is_empty() || prefix.len() > 96 || remainder.is_empty() {
            break;
        }
        if !should_peel_prefix_v1(prefix, remainder, ingest_ts) {
            break;
        }
        if envelope.embedded_ts_guess.is_none() {
            if let Some(ts) = parse_iso_timestamp_v1(prefix) {
                envelope.embedded_ts_guess = Some(ts);
            } else if let Some(ts) = parse_embedded_bsd_timestamp_v1(prefix, ingest_ts) {
                envelope.embedded_ts_guess = Some(ts);
            }
        }
        peeled.push(prefix.to_string());
        current = remainder;
    }

    if !peeled.is_empty() {
        envelope.peeled_prefixes = peeled;
        if allow_embedded_override && envelope.embedded_ts_guess.is_some() {
            envelope.ts_guess = envelope.embedded_ts_guess;
        }
        *msg = current.to_string();
    }
}

fn should_peel_prefix_v1(prefix: &str, remainder: &str, ingest_ts: UnixSec) -> bool {
    if remainder.starts_with('%') {
        return true;
    }
    parse_iso_timestamp_v1(prefix).is_some() || parse_embedded_bsd_timestamp_v1(prefix, ingest_ts).is_some()
}

fn split_vendor_prefix_v1(s: &str) -> Option<(&str, &str)> {
    let s = trim_ascii_start_v1(s);
    if s.is_empty() {
        return None;
    }

    for (idx, ch) in s.char_indices() {
        if ch != ':' {
            continue;
        }
        let next = &s[idx + ch.len_utf8()..];
        if next.is_empty() || !next.starts_with(|c: char| c.is_ascii_whitespace()) {
            continue;
        }
        let prefix = s[..idx].trim();
        if prefix.is_empty() {
            return None;
        }
        let remainder = trim_ascii_start_v1(next);
        if remainder.is_empty() {
            return None;
        }
        return Some((prefix, remainder));
    }
    None
}

fn parse_embedded_bsd_timestamp_v1(s: &str, ingest_ts: UnixSec) -> Option<i64> {
    let (month_token, rest) = take_space_token_v1(s)?;
    let month = parse_bsd_month_v1(month_token)?;
    let (day_token, rest) = take_space_token_v1(rest)?;
    let day = day_token.parse::<u32>().ok()?;
    let (time_token, _) = take_space_token_v1(rest)?;
    let time = NaiveTime::parse_from_str(time_token, "%H:%M:%S").ok()?;
    infer_bsd_timestamp_v1(ingest_ts, month, day, time)
}

fn parse_iso_prefix_v1(s: &str) -> Option<(i64, usize)> {
    let s = trim_ascii_start_v1(s);
    let (first, _) = take_space_token_v1(s)?;
    if let Some(ts) = parse_iso_timestamp_v1(first) {
        return Some((ts, first.len()));
    }

    let mut iter = s.char_indices();
    let mut first_end: Option<usize> = None;
    let mut second_start: Option<usize> = None;
    let mut second_end: Option<usize> = None;
    let mut in_token = false;
    let mut token_count = 0usize;
    for (idx, ch) in iter.by_ref() {
        if ch.is_ascii_whitespace() {
            if in_token {
                token_count += 1;
                if token_count == 1 {
                    first_end = Some(idx);
                } else if token_count == 2 {
                    second_end = Some(idx);
                    break;
                }
                in_token = false;
            }
        } else if !in_token {
            in_token = true;
            if token_count == 1 && second_start.is_none() {
                second_start = Some(idx);
            }
        }
    }
    if in_token {
        token_count += 1;
        if token_count == 1 {
            first_end = Some(s.len());
        } else if token_count == 2 {
            second_end = Some(s.len());
        }
    }
    let first_end = first_end?;
    let second_start = second_start?;
    let second_end = second_end?;
    let candidate = format!("{} {}", &s[..first_end], &s[second_start..second_end]);
    let ts = parse_iso_timestamp_v1(&candidate)?;
    Some((ts, second_end))
}

fn parse_iso_timestamp_v1(s: &str) -> Option<i64> {
    if let Ok(dt) = chrono::DateTime::parse_from_rfc3339(s) {
        return Some(dt.timestamp());
    }

    const OFFSET_PATTERNS: [&str; 4] = [
        "%Y-%m-%d %H:%M:%S%.f%:z",
        "%Y-%m-%d %H:%M:%S%:z",
        "%Y-%m-%dT%H:%M:%S%.f%:z",
        "%Y-%m-%dT%H:%M:%S%:z",
    ];
    for pattern in OFFSET_PATTERNS {
        if let Ok(dt) = chrono::DateTime::parse_from_str(s, pattern) {
            return Some(dt.timestamp());
        }
    }

    const NAIVE_PATTERNS: [&str; 4] = [
        "%Y-%m-%d %H:%M:%S%.f",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%dT%H:%M:%S%.f",
        "%Y-%m-%dT%H:%M:%S",
    ];
    for pattern in NAIVE_PATTERNS {
        if let Ok(dt) = chrono::NaiveDateTime::parse_from_str(s, pattern) {
            return Some(Utc.from_utc_datetime(&dt).timestamp());
        }
    }

    if let Some(ts) = parse_iso_basic_utc_v1(s) {
        return Some(ts);
    }
    None
}

fn parse_iso_basic_utc_v1(s: &str) -> Option<i64> {
    if !s.ends_with('Z') {
        return None;
    }
    let base = &s[..s.len() - 1];
    let bytes = base.as_bytes();
    if bytes.len() < 15 {
        return None;
    }
    if bytes.get(4) != Some(&b'-') || bytes.get(7) != Some(&b'-') {
        return None;
    }
    let sep = *bytes.get(10)?;
    if sep != b'T' && sep != b' ' {
        return None;
    }
    let date = &base[..10];
    let time = &base[11..];
    let year = date[0..4].parse::<i32>().ok()?;
    let month = date[5..7].parse::<u32>().ok()?;
    let day = date[8..10].parse::<u32>().ok()?;

    let (hms, frac) = match time.split_once('.') {
        Some((h, f)) => (h, Some(f)),
        None => (time, None),
    };
    if hms.len() != 8 || &hms[2..3] != ":" || &hms[5..6] != ":" {
        return None;
    }
    let hour = hms[0..2].parse::<u32>().ok()?;
    let minute = hms[3..5].parse::<u32>().ok()?;
    let second = hms[6..8].parse::<u32>().ok()?;
    let nanos = if let Some(f) = frac {
        let digits: String = f.chars().take_while(|ch| ch.is_ascii_digit()).collect();
        if digits.is_empty() {
            0u32
        } else {
            let truncated = if digits.len() > 9 { &digits[..9] } else { &digits };
            let mut padded = truncated.to_string();
            while padded.len() < 9 {
                padded.push('0');
            }
            padded.parse::<u32>().ok()?
        }
    } else {
        0u32
    };
    let date = NaiveDate::from_ymd_opt(year, month, day)?;
    let dt = date.and_hms_nano_opt(hour, minute, second, nanos)?;
    Some(Utc.from_utc_datetime(&dt).timestamp())
}

fn parse_structured_data_prefix_v1(s: &str) -> Option<(Option<String>, String)> {
    let s = trim_ascii_start_v1(s);
    if let Some(rest) = s.strip_prefix('-') {
        return Some((None, trim_ascii_start_v1(rest).to_string()));
    }
    if !s.starts_with('[') {
        return None;
    }

    let mut depth = 0i32;
    let mut in_quote = false;
    let mut escaped = false;
    let mut end_idx: Option<usize> = None;
    for (idx, ch) in s.char_indices() {
        if escaped {
            escaped = false;
            continue;
        }
        if ch == '\\' && in_quote {
            escaped = true;
            continue;
        }
        if ch == '"' {
            in_quote = !in_quote;
            continue;
        }
        if in_quote {
            continue;
        }
        if ch == '[' {
            depth += 1;
        } else if ch == ']' {
            depth -= 1;
            if depth == 0 {
                let next = idx + ch.len_utf8();
                let rest = &s[next..];
                if rest.is_empty() || rest.starts_with(' ') || rest.starts_with('[') {
                    end_idx = Some(next);
                    if !rest.starts_with('[') {
                        break;
                    }
                }
            }
        }
    }
    let end_idx = end_idx?;
    let value = s[..end_idx].to_string();
    let msg = trim_ascii_start_v1(&s[end_idx..]).to_string();
    Some((Some(value), msg))
}

fn parse_bsd_month_v1(token: &str) -> Option<u32> {
    Some(match token {
        "Jan" => 1,
        "Feb" => 2,
        "Mar" => 3,
        "Apr" => 4,
        "May" => 5,
        "Jun" => 6,
        "Jul" => 7,
        "Aug" => 8,
        "Sep" => 9,
        "Oct" => 10,
        "Nov" => 11,
        "Dec" => 12,
        _ => return None,
    })
}

fn infer_bsd_timestamp_v1(ingest_ts: UnixSec, month: u32, day: u32, time: NaiveTime) -> Option<i64> {
    let ingest = Utc.timestamp_opt(ingest_ts, 0).single()?;
    let mut year = ingest.year();
    let mut candidate = Utc
        .with_ymd_and_hms(year, month, day, time.hour(), time.minute(), time.second())
        .single()?;
    if candidate.timestamp() - ingest_ts > 86_400 {
        year -= 1;
        candidate = Utc
            .with_ymd_and_hms(year, month, day, time.hour(), time.minute(), time.second())
            .single()?;
    }
    Some(candidate.timestamp())
}

fn parse_bsd_tag_v1(tag: &str) -> (Option<String>, Option<String>) {
    if let Some(open_idx) = tag.rfind('[') {
        if tag.ends_with(']') && open_idx > 0 {
            let app = tag[..open_idx].trim();
            let procid = &tag[open_idx + 1..tag.len() - 1];
            if !app.is_empty() && !procid.is_empty() {
                return (Some(app.to_string()), Some(procid.to_string()));
            }
        }
    }
    (Some(tag.to_string()), None)
}

fn dash_is_none_v1(s: &str) -> Option<String> {
    if s == "-" {
        None
    } else {
        Some(s.to_string())
    }
}

fn take_space_token_v1(s: &str) -> Option<(&str, &str)> {
    let s = trim_ascii_start_v1(s);
    if s.is_empty() {
        return None;
    }
    let mut end = s.len();
    for (idx, ch) in s.char_indices() {
        if ch.is_ascii_whitespace() {
            end = idx;
            break;
        }
    }
    let token = &s[..end];
    let rest = if end >= s.len() { "" } else { &s[end..] };
    Some((token, rest))
}

fn trim_ascii_start_v1(s: &str) -> &str {
    s.trim_start_matches(|ch: char| ch.is_ascii_whitespace())
}

fn is_host_like_v1(s: &str) -> bool {
    !s.is_empty()
        && s.bytes().all(|b| {
            b.is_ascii_alphanumeric() || matches!(b, b'.' | b'-' | b'_' | b':')
        })
}

