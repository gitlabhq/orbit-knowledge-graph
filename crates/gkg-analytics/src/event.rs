use serde::Serialize;

pub(crate) mod sealed {
    pub trait Sealed {}
}

/// A product analytics event. Sealed — only this crate declares events.
/// New events are one line via [`declare_event!`].
pub trait AnalyticsEvent: sealed::Sealed + Serialize + Send + 'static {
    /// Snowplow event name, e.g. `gkg_query_executed`.
    const EVENT_NAME: &'static str;
    /// Iglu schema URI, e.g. `iglu:com.gitlab/gkg_query_executed/jsonschema/1-0-0`.
    const SCHEMA_URI: &'static str;
}

/// Declare a sealed [`AnalyticsEvent`] impl in one line.
///
/// ```ignore
/// declare_event!(QueryExecuted => "gkg_query_executed" @ "iglu:com.gitlab/gkg_query_executed/jsonschema/1-0-0");
/// ```
macro_rules! declare_event {
    ($ty:ty => $name:literal @ $schema:literal) => {
        impl $crate::event::sealed::Sealed for $ty {}
        impl $crate::event::AnalyticsEvent for $ty {
            const EVENT_NAME: &'static str = $name;
            const SCHEMA_URI: &'static str = $schema;
        }
    };
}
pub(crate) use declare_event;
