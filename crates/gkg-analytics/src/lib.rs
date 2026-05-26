mod context;
mod tracker;

pub use context::{deployment_env, deployment_type};
pub use tracker::{AnalyticsTracker, SnowplowAnalyticsTracker};

#[cfg(feature = "testkit")]
pub use tracker::InMemoryAnalyticsTracker;
