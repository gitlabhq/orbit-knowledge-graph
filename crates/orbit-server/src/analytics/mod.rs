pub(crate) mod context;
mod observer;

pub(crate) use observer::AnalyticsObserver;
pub use orbit_analytics::{
    AnalyticsTracker, GitlabInstanceWatch, SnowplowAnalyticsTracker, consenting_tracker,
};

#[cfg(any(test, feature = "testkit"))]
pub use orbit_analytics::InMemoryAnalyticsTracker;
