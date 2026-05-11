// Copyright (c) 2026 Richard S. Westmoreland
// SPDX-License-Identifier: MIT

// Bounded entity sketch helpers for explainable alerts.
// See: contracts/02_shape_catalog_v0_1.md and contracts/03_alert_object_explanation_v0_1.md

use std::collections::BTreeMap;

use crate::config::CapsSectionV1;
use crate::db::keys::{
    key_tenant_window_row_ent_domain_v1, key_tenant_window_row_ent_dstip_v1,
    key_tenant_window_row_ent_host_v1, key_tenant_window_row_ent_srcip_v1,
    key_tenant_window_row_ent_userid_v1, KeyBytes,
};
use crate::db::open_window::{
    encode_win_row_ent_domain_v1, encode_win_row_ent_dstip_v1, encode_win_row_ent_host_v1,
    encode_win_row_ent_srcip_v1, encode_win_row_ent_userid_v1, OpenWindowErrorV1,
    TopKStringEntryV1,
};
use crate::features::{FeatureEmissionLineV1, MetadataIdentityKindV1, MetadataIdentityV1};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EntitySketchCapsV1 {
    pub max_srcips: u32,
    pub max_dstips: u32,
    pub max_userids: u32,
    pub max_domains: u32,
    pub max_hosts: u32,
}

impl From<&CapsSectionV1> for EntitySketchCapsV1 {
    fn from(value: &CapsSectionV1) -> Self {
        Self {
            max_srcips: value.max_srcips,
            max_dstips: value.max_dstips,
            max_userids: value.max_userids,
            max_domains: value.max_domains,
            max_hosts: value.max_hosts,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EntitySketchKindV1 {
    SrcIp,
    DstIp,
    UserId,
    Domain,
    Host,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EntitySketchKvV1 {
    pub key: KeyBytes,
    pub value: Vec<u8>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct EntitySketchSnapshotV1 {
    pub srcips: Vec<TopKStringEntryV1>,
    pub dstips: Vec<TopKStringEntryV1>,
    pub userids: Vec<TopKStringEntryV1>,
    pub domains: Vec<TopKStringEntryV1>,
    pub hosts: Vec<TopKStringEntryV1>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EntitySketchesV1 {
    caps: EntitySketchCapsV1,
    srcips: BTreeMap<String, u32>,
    dstips: BTreeMap<String, u32>,
    userids: BTreeMap<String, u32>,
    domains: BTreeMap<String, u32>,
    hosts: BTreeMap<String, u32>,
}

impl EntitySketchesV1 {
    pub fn new_v1(caps: EntitySketchCapsV1) -> Self {
        Self {
            caps,
            srcips: BTreeMap::new(),
            dstips: BTreeMap::new(),
            userids: BTreeMap::new(),
            domains: BTreeMap::new(),
            hosts: BTreeMap::new(),
        }
    }

    pub fn caps_v1(&self) -> &EntitySketchCapsV1 {
        &self.caps
    }

    pub fn from_snapshot_v1(caps: EntitySketchCapsV1, snapshot: &EntitySketchSnapshotV1) -> Self {
        let mut this = Self::new_v1(caps);
        for entry in &snapshot.srcips {
            this.srcips.insert(entry.value.clone(), entry.count);
        }
        for entry in &snapshot.dstips {
            this.dstips.insert(entry.value.clone(), entry.count);
        }
        for entry in &snapshot.userids {
            this.userids.insert(entry.value.clone(), entry.count);
        }
        for entry in &snapshot.domains {
            this.domains.insert(entry.value.clone(), entry.count);
        }
        for entry in &snapshot.hosts {
            this.hosts.insert(entry.value.clone(), entry.count);
        }
        this
    }

    pub fn ingest_metadata_v1(&mut self, metadata: &[MetadataIdentityV1]) {
        for item in metadata {
            self.ingest_identity_v1(item.kind, &item.value);
        }
    }

    pub fn ingest_line_v1(&mut self, line: &FeatureEmissionLineV1) {
        self.ingest_metadata_v1(&line.metadata);
    }

    pub fn ingest_identity_v1(&mut self, kind: MetadataIdentityKindV1, value: &str) {
        let trimmed = value.trim();
        if trimmed.is_empty() {
            return;
        }

        let target = match kind {
            MetadataIdentityKindV1::SourceIp => Some(&mut self.srcips),
            MetadataIdentityKindV1::DestIp => Some(&mut self.dstips),
            MetadataIdentityKindV1::UserId => Some(&mut self.userids),
            MetadataIdentityKindV1::Domain => Some(&mut self.domains),
            MetadataIdentityKindV1::Host => Some(&mut self.hosts),
            MetadataIdentityKindV1::UserRaw => None,
        };

        if let Some(map) = target {
            let entry = map.entry(trimmed.to_string()).or_insert(0);
            *entry = entry.saturating_add(1);
        }
    }

    pub fn snapshot_v1(&self) -> EntitySketchSnapshotV1 {
        EntitySketchSnapshotV1 {
            srcips: build_topk_v1(&self.srcips, self.caps.max_srcips),
            dstips: build_topk_v1(&self.dstips, self.caps.max_dstips),
            userids: build_topk_v1(&self.userids, self.caps.max_userids),
            domains: build_topk_v1(&self.domains, self.caps.max_domains),
            hosts: build_topk_v1(&self.hosts, self.caps.max_hosts),
        }
    }

    pub fn checkpoint_writes_v1(
        &self,
        device_key: &str,
        window_id: u64,
    ) -> Result<Vec<EntitySketchKvV1>, OpenWindowErrorV1> {
        let snapshot = self.snapshot_v1();
        Ok(vec![
            EntitySketchKvV1 {
                key: key_tenant_window_row_ent_srcip_v1(device_key, window_id),
                value: encode_win_row_ent_srcip_v1(&snapshot.srcips)?,
            },
            EntitySketchKvV1 {
                key: key_tenant_window_row_ent_dstip_v1(device_key, window_id),
                value: encode_win_row_ent_dstip_v1(&snapshot.dstips)?,
            },
            EntitySketchKvV1 {
                key: key_tenant_window_row_ent_userid_v1(device_key, window_id),
                value: encode_win_row_ent_userid_v1(&snapshot.userids)?,
            },
            EntitySketchKvV1 {
                key: key_tenant_window_row_ent_domain_v1(device_key, window_id),
                value: encode_win_row_ent_domain_v1(&snapshot.domains)?,
            },
            EntitySketchKvV1 {
                key: key_tenant_window_row_ent_host_v1(device_key, window_id),
                value: encode_win_row_ent_host_v1(&snapshot.hosts)?,
            },
        ])
    }

    pub fn counts_for_kind_v1(&self, kind: EntitySketchKindV1) -> Vec<(String, u32)> {
        let map = match kind {
            EntitySketchKindV1::SrcIp => &self.srcips,
            EntitySketchKindV1::DstIp => &self.dstips,
            EntitySketchKindV1::UserId => &self.userids,
            EntitySketchKindV1::Domain => &self.domains,
            EntitySketchKindV1::Host => &self.hosts,
        };
        map.iter().map(|(k, v)| (k.clone(), *v)).collect()
    }
}

fn build_topk_v1(counts: &BTreeMap<String, u32>, cap: u32) -> Vec<TopKStringEntryV1> {
    let mut entries: Vec<TopKStringEntryV1> = counts
        .iter()
        .filter_map(|(value, count)| {
            if *count == 0 {
                None
            } else {
                Some(TopKStringEntryV1 {
                    value: value.clone(),
                    count: *count,
                })
            }
        })
        .collect();

    entries.sort_by(|a, b| b.count.cmp(&a.count).then_with(|| a.value.as_bytes().cmp(b.value.as_bytes())));

    let cap = usize::try_from(cap).unwrap_or(usize::MAX);
    if entries.len() > cap {
        entries.truncate(cap);
    }
    entries
}
