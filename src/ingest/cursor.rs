// Copyright (c) 2026 Richard S. Westmoreland
// SPDX-License-Identifier: MIT

// Cursor state machine helpers.
// See: contracts/04_state_retention_v0_1.md
//   and contracts/31_tenant_db_simple_value_encodings_v0_1.md
// Performs deterministic per-file cursor reconciliation.

use crate::ingest::FileCursorV1;
use crate::types::UnixSec;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ObservedFileStateV1 {
    pub inode: u64,
    pub mtime: UnixSec,
    pub size: u64,
    pub is_gzip: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CursorResetReasonV1 {
    InodeChanged,
    Truncated,
    GzipIdentityChanged,
    TypeChanged,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CursorPlanV1 {
    pub cursor: FileCursorV1,
    pub start_offset: u64,
    pub should_read: bool,
    pub reset_reason: Option<CursorResetReasonV1>,
    pub cursor_resets_total_delta: u64,
}

pub fn reconcile_cursor_v1(
    previous: Option<&FileCursorV1>,
    observed: &ObservedFileStateV1,
) -> CursorPlanV1 {
    match previous {
        None => plan_from_offset_v1(observed, 0, 0, None, 0),
        Some(prev) => reconcile_existing_cursor_v1(prev, observed),
    }
}

pub fn apply_cursor_read_progress_v1(
    cursor: &FileCursorV1,
    new_offset: u64,
    read_ts: UnixSec,
) -> FileCursorV1 {
    let next_offset = new_offset.min(cursor.size);
    FileCursorV1 {
        inode: cursor.inode,
        mtime: cursor.mtime,
        size: cursor.size,
        offset: next_offset,
        is_gzip: cursor.is_gzip,
        last_read_ts: read_ts,
    }
}

fn reconcile_existing_cursor_v1(
    previous: &FileCursorV1,
    observed: &ObservedFileStateV1,
) -> CursorPlanV1 {
    if previous.is_gzip != observed.is_gzip {
        return plan_from_offset_v1(
            observed,
            0,
            previous.last_read_ts,
            Some(CursorResetReasonV1::TypeChanged),
            0,
        );
    }

    if observed.is_gzip {
        return reconcile_gzip_cursor_v1(previous, observed);
    }

    reconcile_plain_cursor_v1(previous, observed)
}

fn reconcile_plain_cursor_v1(
    previous: &FileCursorV1,
    observed: &ObservedFileStateV1,
) -> CursorPlanV1 {
    if previous.inode != observed.inode {
        return plan_from_offset_v1(
            observed,
            0,
            previous.last_read_ts,
            Some(CursorResetReasonV1::InodeChanged),
            1,
        );
    }

    if observed.size < previous.offset {
        return plan_from_offset_v1(
            observed,
            0,
            previous.last_read_ts,
            Some(CursorResetReasonV1::Truncated),
            0,
        );
    }

    plan_from_offset_v1(observed, previous.offset, previous.last_read_ts, None, 0)
}

fn reconcile_gzip_cursor_v1(
    previous: &FileCursorV1,
    observed: &ObservedFileStateV1,
) -> CursorPlanV1 {
    if previous.inode != observed.inode {
        return plan_from_offset_v1(
            observed,
            0,
            previous.last_read_ts,
            Some(CursorResetReasonV1::InodeChanged),
            1,
        );
    }

    if previous.size != observed.size {
        return plan_from_offset_v1(
            observed,
            0,
            previous.last_read_ts,
            Some(CursorResetReasonV1::GzipIdentityChanged),
            0,
        );
    }

    let start_offset = previous.offset.min(observed.size);
    plan_from_offset_v1(observed, start_offset, previous.last_read_ts, None, 0)
}

fn plan_from_offset_v1(
    observed: &ObservedFileStateV1,
    start_offset: u64,
    last_read_ts: UnixSec,
    reset_reason: Option<CursorResetReasonV1>,
    cursor_resets_total_delta: u64,
) -> CursorPlanV1 {
    let clamped_offset = start_offset.min(observed.size);
    CursorPlanV1 {
        cursor: FileCursorV1 {
            inode: observed.inode,
            mtime: observed.mtime,
            size: observed.size,
            offset: clamped_offset,
            is_gzip: observed.is_gzip,
            last_read_ts,
        },
        start_offset: clamped_offset,
        should_read: observed.size > clamped_offset,
        reset_reason,
        cursor_resets_total_delta,
    }
}
