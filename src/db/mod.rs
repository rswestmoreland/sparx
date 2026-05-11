// Copyright (c) 2026 Richard S. Westmoreland
// SPDX-License-Identifier: MIT

// DB module.
// See: contracts/25_tenant_db_key_prefix_map_v0_1.md
//   and contracts/30_global_db_key_prefix_map_v0_1.md
//
// Key builders and binary encodings live behind this module boundary.
// Canonical filesystem layout helpers live behind this module boundary.
// The real Fjall-backed global DB layer lives behind this module boundary.
// The real Fjall-backed tenant DB layer lives behind this module boundary.
// The deterministic tenant DB handle cache lives behind this module boundary.

use std::error::Error;
use std::fmt;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DbErrorV1 {
    pub msg: String,
}

impl DbErrorV1 {
    pub fn new_v1(msg: impl Into<String>) -> Self {
        Self { msg: msg.into() }
    }
}

impl fmt::Display for DbErrorV1 {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.msg)
    }
}

impl Error for DbErrorV1 {}

pub mod baseline_sketch;
pub mod fjall;
pub mod global;
pub mod keys;
pub mod layout;
pub mod open_window;
pub mod silence;
pub mod source_stream;
pub mod tenant;
pub mod tenant_cache;
pub mod tenant_values;

pub use fjall::{FjallKvDbV1, KvWriteOpV1, PRIMARY_KEYSPACE_NAME_V1};
pub use global::{
    GlobalDbV1, GlobalMigrateJournalEntryV1, GlobalProcessStateV1, GlobalSchemaStateV1,
    GlobalTenantPurgeEntryV1, GlobalTenantRecordV1,
};
pub use silence::{
    ExpectedSourceStateUpdateV1, ExpectedSourceStateV1, OpenDropStateV1, OpenSilenceStateV1,
    SharpDropCandidateV1, SharpDropCurrentWindowV1, SharpDropEvaluationConfigV1,
    SharpDropEvaluationV1, SharpDropExpectedVolumeV1, SharpDropSuppressionReasonV1,
    VDropCandidateV1, VDropEvaluationConfigV1, VDropEvaluationV1, VDropSuppressionReasonV1,
};
pub use source_stream::{
    SourceStreamCatalogV1, SourceStreamCurrentWindowV1, SourceStreamErrorV1,
    SourceStreamIdentityV1, SourceStreamStatsV1, SourceStreamSubjectV1,
    source_stream_open_drop_state_from_candidate_v1,
    source_stream_open_drop_state_suppresses_candidate_v1,
    source_stream_open_silence_state_from_candidate_v1,
    source_stream_open_silence_state_suppresses_candidate_v1,
};
pub use tenant::{
    TenantDbV1, TenantDeviceBaselineStateV1, TenantDfSlotBucketStateV1,
    TenantMigrateJournalEntryV1, TenantOpenWindowStateV1, TenantSchemaStateV1,
};
pub use tenant_cache::{TenantDbCacheConfigV1, TenantDbCacheEntryInfoV1, TenantDbCacheV1};
