//! Integration tests for the namespace handler.
//!
//! Tests are organized by entity type for better maintainability.
//! These tests require a Docker-compatible runtime (Docker, Colima, etc).

#[path = "../common/mod.rs"]
mod common;

mod groups;
mod labels;
mod merge_request_diffs;
mod merge_requests;
mod milestones;
mod notes;
mod projects;
mod watermarking;
