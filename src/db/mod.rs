// DB module.
// See: contracts/25_tenant_db_key_prefix_map_v0_1.md
//   and contracts/30_global_db_key_prefix_map_v0_1.md
//
// Phase 2a/2b/2c/2d implemented key builders and binary encodings.
// Phase 10b implemented canonical filesystem layout helpers.
// Phase 10c adds the real Fjall-backed global DB layer.
// Phase 10d adds the real Fjall-backed tenant DB layer.
// Phase 10e adds the deterministic tenant DB handle cache.

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
pub mod tenant;
pub mod tenant_cache;
pub mod tenant_values;

pub use fjall::{FjallKvDbV1, KvWriteOpV1, PRIMARY_KEYSPACE_NAME_V1};
pub use global::{
    GlobalDbV1, GlobalMigrateJournalEntryV1, GlobalProcessStateV1, GlobalSchemaStateV1,
    GlobalTenantPurgeEntryV1, GlobalTenantRecordV1,
};
pub use tenant::{
    TenantDbV1, TenantDeviceBaselineStateV1, TenantDfSlotBucketStateV1,
    TenantMigrateJournalEntryV1, TenantOpenWindowStateV1, TenantSchemaStateV1,
};
pub use tenant_cache::{TenantDbCacheConfigV1, TenantDbCacheEntryInfoV1, TenantDbCacheV1};
