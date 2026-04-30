//! Cross-crate Snowplow analytics primitives for GKG (Orbit).
//!
//! Owns the [`AnalyticsTracker`] trait and its production
//! ([`SnowplowAnalyticsTracker`]) and test ([`InMemoryAnalyticsTracker`])
//! implementations. Crates that emit events (`gkg-server`, `indexer`, ...)
//! depend on this crate for the trait and ship their own observers and
//! call sites that build the right [`labkit_events::gkg::GkgEvent`] for
//! their domain.
//!
//! Domain-specific context construction (Claims → `OrbitQueryContext` for
//! the server, indexer state → `OrbitSdlcIndexingContext` etc.) stays in
//! the consuming crate. Only the deployment-level [`OrbitCommonContext`]
//! prefix is shared, via [`common_builder`].
//!
//! [`OrbitCommonContext`]: labkit_events::orbit::OrbitCommonContext

mod context;
mod tracker;

pub use context::common_builder;
pub use tracker::{AnalyticsTracker, SnowplowAnalyticsTracker};

#[cfg(feature = "testkit")]
pub use tracker::InMemoryAnalyticsTracker;
