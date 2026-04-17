//! Typed event catalog. Surface-area MRs add events here via
//! [`crate::event::declare_event`].

use bon::Builder;
use serde::Serialize;

use crate::context::QueryContext;
use crate::event::declare_event;

/// Stub event for the query pipeline. Proof-of-wiring only — the real
/// `gkg_query_executed` with full field set lands in the surface-area
/// MR under epic &21189.
#[derive(Builder, Clone, Debug, Serialize)]
pub struct QueryExecuted {
    pub query_type: String,
    pub duration_ms: u64,
}

declare_event!(
    QueryExecuted
        => "gkg_query_executed"
        @  "iglu:com.gitlab/gkg_query_executed/jsonschema/1-0-0"
        with QueryContext
);
