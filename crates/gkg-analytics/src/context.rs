//! Shared helpers for building the [`OrbitCommonContext`] entity that every
//! GKG event carries. Domain-specific contexts ([`OrbitQueryContext`] etc.)
//! are built in the consuming crate, since they read from data structures
//! that crate owns.
//!
//! [`OrbitCommonContext`]: labkit_events::orbit::OrbitCommonContext
//! [`OrbitQueryContext`]: labkit_events::orbit::OrbitQueryContext

use gkg_server_config::{AnalyticsConfig, DeploymentKind};
use labkit_events::orbit::{DeploymentType, OrbitCommonContext, OrbitCommonContextBuilder};

/// Start an [`OrbitCommonContext`] builder pre-populated with the
/// deployment-level fields from [`AnalyticsConfig`]. Callers chain on
/// request-scoped fields (`correlation_id`, `instance_id`, …) and call
/// `build()` themselves.
pub fn common_builder(config: &AnalyticsConfig) -> OrbitCommonContextBuilder {
    OrbitCommonContext::builder(
        map_deployment(config.deployment.kind),
        config.deployment.environment.as_str(),
    )
}

fn map_deployment(kind: DeploymentKind) -> DeploymentType {
    match kind {
        DeploymentKind::Com => DeploymentType::Com,
        DeploymentKind::Dedicated => DeploymentType::Dedicated,
        DeploymentKind::SelfManaged => DeploymentType::SelfManaged,
    }
}
