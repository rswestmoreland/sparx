// Copyright (c) 2026 Richard S. Westmoreland
// SPDX-License-Identifier: MIT

// Thin Fjall-backed raw KV wrapper.
//
// This module keeps Fjall-specific types inside src/db/ and exposes only the
// small operations Sparx needs for v0.1.

use std::path::{Path, PathBuf};

use fjall::{Database, Keyspace, KeyspaceCreateOptions, PersistMode};

use crate::db::DbErrorV1;

pub const PRIMARY_KEYSPACE_NAME_V1: &str = "kv";

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum KvWriteOpV1 {
    Put { key: Vec<u8>, value: Vec<u8> },
    Delete { key: Vec<u8> },
}

#[derive(Clone)]
pub struct FjallKvDbV1 {
    path: PathBuf,
    db: Database,
    kv: Keyspace,
}

impl FjallKvDbV1 {
    pub fn open_at_v1(path: impl AsRef<Path>) -> Result<Self, DbErrorV1> {
        let path = path.as_ref().to_path_buf();
        std::fs::create_dir_all(&path).map_err(|e| {
            DbErrorV1::new_v1(format!(
                "failed to create Fjall database directory {}: {}",
                path.display(),
                e
            ))
        })?;
        let db = Database::builder(&path).open().map_err(|e| {
            DbErrorV1::new_v1(format!(
                "failed to open Fjall database at {}: {}",
                path.display(),
                e
            ))
        })?;
        let kv = db
            .keyspace(PRIMARY_KEYSPACE_NAME_V1, || {
                KeyspaceCreateOptions::default()
            })
            .map_err(|e| {
                DbErrorV1::new_v1(format!(
                    "failed to open Fjall keyspace '{}' at {}: {}",
                    PRIMARY_KEYSPACE_NAME_V1,
                    path.display(),
                    e
                ))
            })?;
        Ok(Self { path, db, kv })
    }

    pub fn path_v1(&self) -> &Path {
        &self.path
    }

    pub fn get_raw_v1(&self, key: &[u8]) -> Result<Option<Vec<u8>>, DbErrorV1> {
        let value = self.kv.get(key).map_err(|e| {
            DbErrorV1::new_v1(format!(
                "failed to read key from {}: {}",
                self.path.display(),
                e
            ))
        })?;
        Ok(value.map(|bytes| bytes.as_ref().to_vec()))
    }

    pub fn put_raw_v1(&self, key: &[u8], value: &[u8]) -> Result<(), DbErrorV1> {
        self.kv.insert(key, value).map_err(|e| {
            DbErrorV1::new_v1(format!(
                "failed to write key to {}: {}",
                self.path.display(),
                e
            ))
        })
    }

    pub fn delete_raw_v1(&self, key: &[u8]) -> Result<(), DbErrorV1> {
        self.kv.remove(key).map_err(|e| {
            DbErrorV1::new_v1(format!(
                "failed to delete key from {}: {}",
                self.path.display(),
                e
            ))
        })
    }

    pub fn write_batch_raw_v1(&self, ops: &[KvWriteOpV1]) -> Result<(), DbErrorV1> {
        let mut batch = self.db.batch();
        for op in ops {
            match op {
                KvWriteOpV1::Put { key, value } => {
                    batch.insert(&self.kv, key.as_slice(), value.as_slice())
                }
                KvWriteOpV1::Delete { key } => batch.remove(&self.kv, key.as_slice()),
            }
        }
        batch.commit().map_err(|e| {
            DbErrorV1::new_v1(format!(
                "failed to commit write batch to {}: {}",
                self.path.display(),
                e
            ))
        })
    }

    pub fn scan_prefix_raw_v1(&self, prefix: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>, DbErrorV1> {
        let mut out = Vec::new();
        for guard in self.kv.prefix(prefix) {
            let (key, value) = guard.into_inner().map_err(|e| {
                DbErrorV1::new_v1(format!(
                    "failed to scan prefix in {}: {}",
                    self.path.display(),
                    e
                ))
            })?;
            out.push((key.as_ref().to_vec(), value.as_ref().to_vec()));
        }
        Ok(out)
    }

    pub fn scan_range_raw_v1(
        &self,
        start_inclusive: &[u8],
        end_inclusive: &[u8],
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, DbErrorV1> {
        let start = start_inclusive.to_vec();
        let end = end_inclusive.to_vec();
        let mut out = Vec::new();
        for guard in self.kv.range(start..=end) {
            let (key, value) = guard.into_inner().map_err(|e| {
                DbErrorV1::new_v1(format!(
                    "failed to scan range in {}: {}",
                    self.path.display(),
                    e
                ))
            })?;
            out.push((key.as_ref().to_vec(), value.as_ref().to_vec()));
        }
        Ok(out)
    }

    pub fn persist_sync_all_v1(&self) -> Result<(), DbErrorV1> {
        self.db.persist(PersistMode::SyncAll).map_err(|e| {
            DbErrorV1::new_v1(format!("failed to persist {}: {}", self.path.display(), e))
        })
    }
}
