//! Consumer-owned Snowplow context types for Orbit analytics.
//!
//! Data types are codegen'd from `config/schemas/iglu/<name>/<version>.json`
//! by `build.rs` via [`typify`] and included here at compile time. Each
//! generated module ([`orbit_common`], [`orbit_query`]) exposes the
//! typed struct (e.g. `OrbitCommon`) plus three `&'static str` consts:
//! `VERSION`, `SCHEMA_URI`, `SCHEMA_JSON`.
//!
//! Wrap a generated type with [`OrbitCommonContext`] or
//! [`OrbitQueryContext`] to attach it to a `StructuredEvent` — these
//! wrappers `impl labkit_events::SnowplowContext`.

use labkit_events::SnowplowContext;

include!(concat!(env!("OUT_DIR"), "/iglu_schemas.rs"));

// ─────────────────────────────────────────────────────────────────────────────
// orbit_common
// ─────────────────────────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct OrbitCommonContext {
    pub data: orbit_common::OrbitCommon,
}

impl OrbitCommonContext {
    pub fn new(data: orbit_common::OrbitCommon) -> Self {
        Self { data }
    }
}

impl SnowplowContext for OrbitCommonContext {
    fn schema(&self) -> &str {
        orbit_common::SCHEMA_URI
    }

    fn data(&self) -> serde_json::Value {
        serde_json::to_value(&self.data).expect("generated OrbitCommon is always serializable")
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// orbit_query
// ─────────────────────────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct OrbitQueryContext {
    pub data: orbit_query::OrbitQuery,
}

impl OrbitQueryContext {
    pub fn new(data: orbit_query::OrbitQuery) -> Self {
        Self { data }
    }
}

impl SnowplowContext for OrbitQueryContext {
    fn schema(&self) -> &str {
        orbit_query::SCHEMA_URI
    }

    fn data(&self) -> serde_json::Value {
        serde_json::to_value(&self.data).expect("generated OrbitQuery is always serializable")
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Schema URIs (re-exported for callers that need the bare URI string,
// e.g. assertions in observer and integration tests).
// ─────────────────────────────────────────────────────────────────────────────

pub const ORBIT_COMMON_SCHEMA: &str = orbit_common::SCHEMA_URI;
pub const ORBIT_QUERY_SCHEMA: &str = orbit_query::SCHEMA_URI;

/// Return the inlined schema JSON for `name` at its pinned version.
///
/// Used by tests in this crate's consumers to compile an Iglu validator
/// against the same JSON the runtime emits contexts for.
#[cfg(any(test, feature = "testkit"))]
pub fn load_schema_json(name: &str) -> serde_json::Value {
    let raw = match name {
        "orbit_common" => orbit_common::SCHEMA_JSON,
        "orbit_query" => orbit_query::SCHEMA_JSON,
        other => panic!("unknown iglu schema {other:?}"),
    };
    serde_json::from_str(raw).expect("vendored Iglu schema is valid JSON")
}
