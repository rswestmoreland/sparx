// Copyright (c) 2026 Richard S. Westmoreland
// SPDX-License-Identifier: MIT

// Ingest discovery and file inventory helpers.
// See: contracts/08_directory_discovery_v0_1.md

pub mod cursor;
pub mod reader;

use std::fs;
use std::io;
use std::path::{Path, PathBuf};

use crate::stable_hash::stable_hash_hex128_v1;
use crate::types::{DeviceKey, TenantId, UnixSec};

pub const DEFAULT_FILE_SUFFIXES_V1: [&str; 7] = [
    ".log", ".txt", ".json", ".csv", ".cef", ".gz", ".zlg",
];

pub use cursor::{
    apply_cursor_read_progress_v1, reconcile_cursor_v1, CursorPlanV1, CursorResetReasonV1,
    ObservedFileStateV1,
};

pub use reader::{
    open_file_reader_v1, FileReaderV1, GzipFileReaderV1, PlainFileReaderV1, ReadChunkV1,
    ZlgFileReaderV1,
};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TenantDeviceV1 {
    pub tenant_id: TenantId,
    pub device_dir_name: String,
    pub device_dir_rel: String,
    pub device_key: DeviceKey,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FileCursorV1 {
    pub inode: u64,
    pub mtime: UnixSec,
    pub size: u64,
    pub offset: u64,
    pub is_gzip: bool,
    pub last_read_ts: UnixSec,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DiscoveredFileV1 {
    pub file_rel: String,
    pub file_key: String,
    pub is_gzip: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DeviceInventoryV1 {
    pub device: TenantDeviceV1,
    pub files: Vec<DiscoveredFileV1>,
}

pub fn device_key_v1(tenant_id: &str, device_dir_rel: &str) -> DeviceKey {
    let mut s = String::with_capacity(tenant_id.len() + 1 + device_dir_rel.len());
    s.push_str(tenant_id);
    s.push('/');
    s.push_str(device_dir_rel);
    stable_hash_hex128_v1(&s)
}

pub fn file_key_v1(file_rel: &str) -> String {
    stable_hash_hex128_v1(file_rel)
}

pub fn discover_tenant_devices_v1(
    watch_root: &Path,
    follow_symlinks: bool,
) -> io::Result<Vec<TenantDeviceV1>> {
    let mut out = Vec::new();
    for tenant_entry in sorted_dir_entries(watch_root)? {
        let tenant_name = os_name_to_string(tenant_entry.file_name());
        if !entry_matches_kind(
            &tenant_entry.path(),
            follow_symlinks,
            EntryKindV1::Directory,
        )? {
            continue;
        }

        for device_entry in sorted_dir_entries(&tenant_entry.path())? {
            let device_name = os_name_to_string(device_entry.file_name());
            if !entry_matches_kind(
                &device_entry.path(),
                follow_symlinks,
                EntryKindV1::Directory,
            )? {
                continue;
            }
            let device_rel = device_name.clone();
            out.push(TenantDeviceV1 {
                tenant_id: tenant_name.clone(),
                device_dir_name: device_name,
                device_dir_rel: device_rel.clone(),
                device_key: device_key_v1(&tenant_name, &device_rel),
            });
        }
    }

    out.sort_by(|a, b| {
        a.tenant_id
            .cmp(&b.tenant_id)
            .then(a.device_dir_rel.cmp(&b.device_dir_rel))
            .then(a.device_key.cmp(&b.device_key))
    });
    Ok(out)
}

pub fn discover_device_files_v1(
    watch_root: &Path,
    device: &TenantDeviceV1,
    follow_symlinks: bool,
) -> io::Result<Vec<DiscoveredFileV1>> {
    let device_root = watch_root
        .join(&device.tenant_id)
        .join(&device.device_dir_rel);
    discover_device_files_at_v1(&device_root, follow_symlinks)
}

pub fn discover_device_files_at_v1(
    device_root: &Path,
    follow_symlinks: bool,
) -> io::Result<Vec<DiscoveredFileV1>> {
    let mut out = Vec::new();
    for entry in sorted_dir_entries(device_root)? {
        let name = os_name_to_string(entry.file_name());
        if is_hidden_name(&name) {
            continue;
        }
        if !entry_matches_kind(&entry.path(), follow_symlinks, EntryKindV1::File)? {
            continue;
        }
        if !has_allowed_suffix_v1(&name) {
            continue;
        }
        out.push(DiscoveredFileV1 {
            file_rel: name.clone(),
            file_key: file_key_v1(&name),
            is_gzip: is_gzip_name_v1(&name),
        });
    }
    out.sort_by(|a, b| {
        a.file_rel
            .cmp(&b.file_rel)
            .then(a.file_key.cmp(&b.file_key))
    });
    Ok(out)
}

pub fn discover_device_inventory_v1(
    watch_root: &Path,
    follow_symlinks: bool,
) -> io::Result<Vec<DeviceInventoryV1>> {
    let mut out = Vec::new();
    for device in discover_tenant_devices_v1(watch_root, follow_symlinks)? {
        let files = discover_device_files_v1(watch_root, &device, follow_symlinks)?;
        out.push(DeviceInventoryV1 { device, files });
    }
    out.sort_by(|a, b| {
        a.device
            .tenant_id
            .cmp(&b.device.tenant_id)
            .then(a.device.device_dir_rel.cmp(&b.device.device_dir_rel))
            .then(a.device.device_key.cmp(&b.device.device_key))
    });
    Ok(out)
}

pub fn has_allowed_suffix_v1(file_name: &str) -> bool {
    DEFAULT_FILE_SUFFIXES_V1
        .iter()
        .any(|suffix| file_name.ends_with(suffix))
}

pub fn is_gzip_name_v1(file_name: &str) -> bool {
    file_name.ends_with(".gz")
}

pub fn is_zlg_name_v1(file_name: &str) -> bool {
    file_name.ends_with(".zlg")
}

pub fn uses_compressed_archive_cursor_v1(file_name: &str, is_gzip: bool) -> bool {
    is_gzip || is_zlg_name_v1(file_name)
}

fn sorted_dir_entries(dir: &Path) -> io::Result<Vec<fs::DirEntry>> {
    let mut entries = Vec::new();
    for entry in fs::read_dir(dir)? {
        entries.push(entry?);
    }
    entries.sort_by_key(|entry| os_name_to_string(entry.file_name()));
    Ok(entries)
}

fn os_name_to_string(name: std::ffi::OsString) -> String {
    name.to_string_lossy().into_owned()
}

fn is_hidden_name(name: &str) -> bool {
    name.starts_with('.')
}

enum EntryKindV1 {
    Directory,
    File,
}

fn entry_matches_kind(
    path: &PathBuf,
    follow_symlinks: bool,
    want: EntryKindV1,
) -> io::Result<bool> {
    let meta = if follow_symlinks {
        fs::metadata(path)?
    } else {
        let m = fs::symlink_metadata(path)?;
        if m.file_type().is_symlink() {
            return Ok(false);
        }
        m
    };
    Ok(match want {
        EntryKindV1::Directory => meta.is_dir(),
        EntryKindV1::File => meta.is_file(),
    })
}
