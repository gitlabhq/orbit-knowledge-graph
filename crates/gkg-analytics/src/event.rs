use serde::Serialize;

pub(crate) mod sealed {
    pub trait Sealed {}
}

/// A product analytics event. Sealed — only this crate declares events.
/// Add a new event by defining a struct in this crate, implementing
/// `sealed::Sealed` and `AnalyticsEvent` for it.
pub trait AnalyticsEvent: sealed::Sealed + Serialize + Send + 'static {
    /// Iglu schema URI, e.g. `iglu:com.gitlab/gkg_query_executed/jsonschema/1-0-0`.
    fn schema_uri() -> &'static str
    where
        Self: Sized;

    /// Snowplow event name, e.g. `gkg_query_executed`.
    fn event_name() -> &'static str
    where
        Self: Sized;
}
