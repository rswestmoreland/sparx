// Copyright (c) 2026 Richard S. Westmoreland
// SPDX-License-Identifier: MIT

// Tokenization event types.
// See: contracts/23_tokenizer_details_v0_1.md and contracts/18_syslog_envelope_and_cef_reverse_kv_v0_1.md

mod cef;
mod generic;
mod syslog;

pub use cef::parse_cef_message_v1;
pub use generic::{
    tokenize_message_bytes_v1, tokenize_message_events_v1, tokenize_message_v1, CsvHeaderModeV1, TokenizeResultV1,
    TokenizeStatsV1,
};
pub use syslog::{parse_syslog_envelope_v1, peel_syslog_envelope_v1};

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct SyslogEnvelopeV1 {
    pub pri: Option<u32>,
    pub version: Option<u32>,
    pub ts_guess: Option<i64>,
    pub host: Option<String>,
    pub app: Option<String>,
    pub procid: Option<String>,
    pub msgid: Option<String>,
    pub structured_data: Option<String>,
    pub embedded_ts_guess: Option<i64>,
    pub peeled_prefixes: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ParsedLineV1 {
    pub envelope: SyslogEnvelopeV1,
    pub msg: String,
}

// Token events emitted by the tokenizer.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TokenEventV1 {
    Kv {
        key_norm: String,
        value_raw: String,
    },
    JsonKv {
        key_path_norm: String,
        value_raw: String,
    },
    CsvKv {
        key_norm: String,
        value_raw: String,
    },
    Word {
        token_raw: String,
    },
    CefHeader {
        field: String,
        value: String,
    },
    ResidualText {
        text_raw: String,
    },
}
