// Tenant DB handle cache and lifecycle helpers.
//
// Phase 10e:
// - max-open cap
// - deterministic LRU eviction
// - idle close
// - explicit close for purge
// - safe reopen

use std::collections::BTreeMap;

use crate::config::ConfigV1;
use crate::db::layout::{filesystem_layout_v1, FilesystemLayoutV1};
use crate::db::{DbErrorV1, TenantDbV1};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TenantDbCacheConfigV1 {
    pub max_open: u32,
    pub idle_close_s: u32,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TenantDbCacheEntryInfoV1 {
    pub tenant_id: String,
    pub path: String,
    pub last_touch_ts: i64,
    pub last_touch_seq: u64,
}

struct TenantDbCacheEntryV1 {
    tenant_id: String,
    db: TenantDbV1,
    last_touch_ts: i64,
    last_touch_seq: u64,
}

pub struct TenantDbCacheV1 {
    layout: FilesystemLayoutV1,
    cfg: TenantDbCacheConfigV1,
    next_touch_seq: u64,
    entries: BTreeMap<String, TenantDbCacheEntryV1>,
}

impl TenantDbCacheV1 {
    pub fn new_v1(layout: FilesystemLayoutV1, cfg: TenantDbCacheConfigV1) -> Self {
        Self {
            layout,
            cfg,
            next_touch_seq: 1,
            entries: BTreeMap::new(),
        }
    }

    pub fn from_config_v1(cfg: &ConfigV1) -> Self {
        Self::new_v1(
            filesystem_layout_v1(cfg),
            TenantDbCacheConfigV1 {
                max_open: cfg.storage.tenant_db_max_open,
                idle_close_s: cfg.storage.tenant_db_idle_close_s,
            },
        )
    }

    pub fn layout_v1(&self) -> &FilesystemLayoutV1 {
        &self.layout
    }

    pub fn config_v1(&self) -> &TenantDbCacheConfigV1 {
        &self.cfg
    }

    pub fn open_count_v1(&self) -> usize {
        self.entries.len()
    }

    pub fn contains_v1(&self, tenant_id: &str) -> bool {
        self.entries.contains_key(tenant_id)
    }

    pub fn list_open_tenant_ids_v1(&self) -> Vec<String> {
        self.entries.keys().cloned().collect()
    }

    pub fn snapshot_v1(&self) -> Vec<TenantDbCacheEntryInfoV1> {
        let mut out = Vec::new();
        for entry in self.entries.values() {
            out.push(TenantDbCacheEntryInfoV1 {
                tenant_id: entry.tenant_id.clone(),
                path: entry.db.path_v1().display().to_string(),
                last_touch_ts: entry.last_touch_ts,
                last_touch_seq: entry.last_touch_seq,
            });
        }
        out
    }

    pub fn with_tenant_db_v1<T, F>(
        &mut self,
        tenant_id: &str,
        now_ts: i64,
        f: F,
    ) -> Result<T, DbErrorV1>
    where
        F: FnOnce(&TenantDbV1) -> Result<T, DbErrorV1>,
    {
        let _ = self.close_idle_v1(now_ts);
        self.ensure_open_v1(tenant_id, now_ts)?;
        let db = &self
            .entries
            .get(tenant_id)
            .ok_or_else(|| DbErrorV1::new_v1("tenant cache invariant failed after open"))?
            .db;
        f(db)
    }

    pub fn close_tenant_v1(&mut self, tenant_id: &str) -> bool {
        self.entries.remove(tenant_id).is_some()
    }

    pub fn close_all_v1(&mut self) -> Vec<String> {
        let ids = self.list_open_tenant_ids_v1();
        self.entries.clear();
        ids
    }

    pub fn close_idle_v1(&mut self, now_ts: i64) -> Vec<String> {
        let idle_close_s = i64::from(self.cfg.idle_close_s);
        let mut victims: Vec<(i64, String)> = Vec::new();
        for (tenant_id, entry) in &self.entries {
            if now_ts >= entry.last_touch_ts && now_ts - entry.last_touch_ts >= idle_close_s {
                victims.push((entry.last_touch_ts, tenant_id.clone()));
            }
        }
        victims.sort_by(|a, b| a.0.cmp(&b.0).then_with(|| a.1.cmp(&b.1)));

        let mut closed = Vec::new();
        for (_, tenant_id) in victims {
            if self.entries.remove(&tenant_id).is_some() {
                closed.push(tenant_id);
            }
        }
        closed
    }

    fn ensure_open_v1(&mut self, tenant_id: &str, now_ts: i64) -> Result<(), DbErrorV1> {
        if self.entries.contains_key(tenant_id) {
            let seq = self.next_touch_seq_v1();
            let entry = self
                .entries
                .get_mut(tenant_id)
                .ok_or_else(|| DbErrorV1::new_v1("tenant cache invariant failed on touch"))?;
            entry.last_touch_ts = now_ts;
            entry.last_touch_seq = seq;
            return Ok(());
        }

        if self.cfg.max_open == 0 {
            return Err(DbErrorV1::new_v1(
                "tenant cache max_open is 0; cannot open tenant database",
            ));
        }

        self.evict_until_capacity_v1(1);

        let db = TenantDbV1::open_from_layout_v1(&self.layout, tenant_id)?;
        let seq = self.next_touch_seq_v1();
        self.entries.insert(
            tenant_id.to_string(),
            TenantDbCacheEntryV1 {
                tenant_id: tenant_id.to_string(),
                db,
                last_touch_ts: now_ts,
                last_touch_seq: seq,
            },
        );
        Ok(())
    }

    fn evict_until_capacity_v1(&mut self, needed_slots: usize) {
        let max_open = self.cfg.max_open as usize;
        while self.entries.len().saturating_add(needed_slots) > max_open {
            let victim = self
                .entries
                .iter()
                .min_by(|a, b| {
                    a.1.last_touch_seq
                        .cmp(&b.1.last_touch_seq)
                        .then_with(|| a.0.cmp(b.0))
                })
                .map(|(tenant_id, _)| tenant_id.clone());
            match victim {
                Some(tenant_id) => {
                    self.entries.remove(&tenant_id);
                }
                None => break,
            }
        }
    }

    fn next_touch_seq_v1(&mut self) -> u64 {
        let seq = self.next_touch_seq;
        self.next_touch_seq = self.next_touch_seq.saturating_add(1);
        seq
    }
}
