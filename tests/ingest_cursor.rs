// Copyright (c) 2026 Richard S. Westmoreland
// SPDX-License-Identifier: MIT

use sparx::ingest::{
    apply_cursor_read_progress_v1, reconcile_cursor_v1, CursorResetReasonV1, FileCursorV1,
    ObservedFileStateV1,
};

fn observed_plain(inode: u64, mtime: i64, size: u64) -> ObservedFileStateV1 {
    ObservedFileStateV1 {
        inode,
        mtime,
        size,
        is_gzip: false,
    }
}

fn observed_gzip(inode: u64, mtime: i64, size: u64) -> ObservedFileStateV1 {
    ObservedFileStateV1 {
        inode,
        mtime,
        size,
        is_gzip: true,
    }
}

fn cursor_plain(inode: u64, mtime: i64, size: u64, offset: u64, last_read_ts: i64) -> FileCursorV1 {
    FileCursorV1 {
        inode,
        mtime,
        size,
        offset,
        is_gzip: false,
        last_read_ts,
    }
}

fn cursor_gzip(inode: u64, mtime: i64, size: u64, offset: u64, last_read_ts: i64) -> FileCursorV1 {
    FileCursorV1 {
        inode,
        mtime,
        size,
        offset,
        is_gzip: true,
        last_read_ts,
    }
}

#[test]
fn new_plain_file_starts_at_zero_and_reads_available_bytes() {
    let plan = reconcile_cursor_v1(None, &observed_plain(11, 1700001000, 123));
    assert_eq!(plan.start_offset, 0);
    assert!(plan.should_read);
    assert_eq!(plan.reset_reason, None);
    assert_eq!(plan.cursor_resets_total_delta, 0);
    assert_eq!(plan.cursor, cursor_plain(11, 1700001000, 123, 0, 0));
}

#[test]
fn plain_file_resumes_from_existing_offset() {
    let prev = cursor_plain(11, 1700001000, 200, 80, 1700001005);
    let plan = reconcile_cursor_v1(Some(&prev), &observed_plain(11, 1700001010, 240));
    assert_eq!(plan.start_offset, 80);
    assert!(plan.should_read);
    assert_eq!(plan.reset_reason, None);
    assert_eq!(plan.cursor_resets_total_delta, 0);
    assert_eq!(
        plan.cursor,
        cursor_plain(11, 1700001010, 240, 80, 1700001005)
    );
}

#[test]
fn plain_file_with_same_size_and_offset_is_already_caught_up() {
    let prev = cursor_plain(11, 1700001000, 200, 200, 1700001005);
    let plan = reconcile_cursor_v1(Some(&prev), &observed_plain(11, 1700001010, 200));
    assert_eq!(plan.start_offset, 200);
    assert!(!plan.should_read);
    assert_eq!(plan.reset_reason, None);
}

#[test]
fn plain_file_inode_change_resets_to_zero_and_increments_counter() {
    let prev = cursor_plain(11, 1700001000, 200, 150, 1700001005);
    let plan = reconcile_cursor_v1(Some(&prev), &observed_plain(12, 1700001010, 30));
    assert_eq!(plan.start_offset, 0);
    assert!(plan.should_read);
    assert_eq!(plan.reset_reason, Some(CursorResetReasonV1::InodeChanged));
    assert_eq!(plan.cursor_resets_total_delta, 1);
    assert_eq!(plan.cursor, cursor_plain(12, 1700001010, 30, 0, 1700001005));
}

#[test]
fn plain_file_truncation_resets_to_zero_without_counter_increment() {
    let prev = cursor_plain(11, 1700001000, 200, 150, 1700001005);
    let plan = reconcile_cursor_v1(Some(&prev), &observed_plain(11, 1700001010, 80));
    assert_eq!(plan.start_offset, 0);
    assert!(plan.should_read);
    assert_eq!(plan.reset_reason, Some(CursorResetReasonV1::Truncated));
    assert_eq!(plan.cursor_resets_total_delta, 0);
}

#[test]
fn gzip_file_same_identity_and_fully_processed_is_skipped() {
    let prev = cursor_gzip(21, 1700001000, 90, 90, 1700001005);
    let plan = reconcile_cursor_v1(Some(&prev), &observed_gzip(21, 1700001010, 90));
    assert_eq!(plan.start_offset, 90);
    assert!(!plan.should_read);
    assert_eq!(plan.reset_reason, None);
    assert_eq!(plan.cursor_resets_total_delta, 0);
}

#[test]
fn gzip_file_same_identity_and_partial_progress_resumes() {
    let prev = cursor_gzip(21, 1700001000, 90, 25, 1700001005);
    let plan = reconcile_cursor_v1(Some(&prev), &observed_gzip(21, 1700001010, 90));
    assert_eq!(plan.start_offset, 25);
    assert!(plan.should_read);
    assert_eq!(plan.reset_reason, None);
}

#[test]
fn gzip_file_size_change_reprocesses_from_zero() {
    let prev = cursor_gzip(21, 1700001000, 90, 90, 1700001005);
    let plan = reconcile_cursor_v1(Some(&prev), &observed_gzip(21, 1700001010, 120));
    assert_eq!(plan.start_offset, 0);
    assert!(plan.should_read);
    assert_eq!(
        plan.reset_reason,
        Some(CursorResetReasonV1::GzipIdentityChanged)
    );
    assert_eq!(plan.cursor_resets_total_delta, 0);
}

#[test]
fn type_change_reprocesses_from_zero() {
    let prev = cursor_plain(11, 1700001000, 10, 10, 1700001005);
    let plan = reconcile_cursor_v1(Some(&prev), &observed_gzip(11, 1700001010, 10));
    assert_eq!(plan.start_offset, 0);
    assert!(plan.should_read);
    assert_eq!(plan.reset_reason, Some(CursorResetReasonV1::TypeChanged));
}

#[test]
fn apply_read_progress_clamps_to_observed_size_and_updates_last_read_ts() {
    let base = cursor_plain(11, 1700001010, 80, 0, 1700001005);
    let next = apply_cursor_read_progress_v1(&base, 999, 1700001020);
    assert_eq!(next.offset, 80);
    assert_eq!(next.last_read_ts, 1700001020);
    assert_eq!(next.inode, 11);
    assert_eq!(next.mtime, 1700001010);
    assert_eq!(next.size, 80);
}
