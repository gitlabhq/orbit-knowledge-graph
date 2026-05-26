mod tracker;

pub use tracker::{AnalyticsTracker, SnowplowAnalyticsTracker};

#[cfg(feature = "testkit")]
pub use tracker::InMemoryAnalyticsTracker;
