// Library root for sparx.
//
// Phase 0b: types-only skeleton modules.
// Keep ASCII-only text in this repo.

pub mod alert;
pub mod baseline;
pub mod cli;
pub mod config;
pub mod db;
pub mod drilldown;
pub mod features;
pub mod fixture_validate;
pub mod ingest;
pub mod observability;
pub mod policy;
pub mod runtime;
pub mod sink;
pub mod stable_hash;
pub mod tokenize;
pub mod window;

// Shared primitives used across modules.
pub mod types;
