use serde::Serialize;

use crate::context::AnalyticsContext;

pub(crate) mod sealed {
    pub trait Sealed {}
}

/// A product analytics event. Sealed — only this crate declares events.
/// New events are one line via [`declare_event!`].
///
/// Each event declares which path-specific `AnalyticsContext` accompanies
/// it on the wire. `OrbitCommon` is attached to every event regardless.
pub trait AnalyticsEvent: sealed::Sealed + Serialize + Send + 'static {
    /// Path-specific context attached alongside `OrbitCommon`.
    type PathContext: AnalyticsContext;

    /// Snowplow event name, e.g. `gkg_query_executed`.
    const EVENT_NAME: &'static str;
    /// Iglu schema URI, e.g. `iglu:com.gitlab/gkg_query_executed/jsonschema/1-0-0`.
    const SCHEMA_URI: &'static str;
}

/// Declare a sealed [`AnalyticsEvent`] impl in one line.
///
/// ```ignore
/// declare_event!(
///     QueryExecuted
///         => "gkg_query_executed"
///         @  "iglu:com.gitlab/gkg_query_executed/jsonschema/1-0-0"
///         with QueryContext
/// );
/// ```
macro_rules! declare_event {
    ($ty:ty => $name:literal @ $schema:literal with $ctx:ty) => {
        impl $crate::event::sealed::Sealed for $ty {}
        impl $crate::event::AnalyticsEvent for $ty {
            type PathContext = $ctx;
            const EVENT_NAME: &'static str = $name;
            const SCHEMA_URI: &'static str = $schema;
        }
    };
}
pub(crate) use declare_event;
