pub(crate) mod context;
mod observer;

pub use gkg_analytics::{AnalyticsTracker, SnowplowAnalyticsTracker};
pub(crate) use observer::AnalyticsObserver;

#[cfg(any(test, feature = "testkit"))]
pub use gkg_analytics::InMemoryAnalyticsTracker;
