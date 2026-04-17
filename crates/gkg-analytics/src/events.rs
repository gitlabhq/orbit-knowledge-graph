//! Typed event catalog. Surface-area MRs add events here.
//!
//! Each event is a `#[derive(bon::Builder, Serialize)]` struct with a
//! `sealed::Sealed + AnalyticsEvent` impl pair.

use bon::Builder;
use serde::Serialize;

use crate::event::{AnalyticsEvent, sealed::Sealed};

/// Stub event for the query pipeline. Proof-of-wiring only — real fields
/// (result_status, ch_read_*, durations, error_reason, ...) land in the
/// `gkg_query_executed` surface-area MR per epic &21189.
#[derive(Builder, Clone, Debug, Serialize)]
pub struct QueryExecuted {
    pub query_type: String,
    pub duration_ms: u64,
}

impl Sealed for QueryExecuted {}
impl AnalyticsEvent for QueryExecuted {
    fn schema_uri() -> &'static str {
        "iglu:com.gitlab/gkg_query_executed/jsonschema/1-0-0"
    }
    fn event_name() -> &'static str {
        "gkg_query_executed"
    }
}
