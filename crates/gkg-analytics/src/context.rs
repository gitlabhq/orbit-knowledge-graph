use gkg_server_config::{AnalyticsConfig, DeploymentKind};
use labkit_events::orbit::{DeploymentType, OrbitCommonContext, OrbitCommonContextBuilder};

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
