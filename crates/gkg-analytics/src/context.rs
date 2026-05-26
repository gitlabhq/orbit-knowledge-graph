use gkg_server_config::{AnalyticsConfig, DeploymentKind};

pub fn deployment_type(kind: DeploymentKind) -> &'static str {
    match kind {
        DeploymentKind::Com => ".com",
        DeploymentKind::Dedicated => "dedicated",
        DeploymentKind::SelfManaged => "self-managed",
    }
}

pub fn deployment_env(config: &AnalyticsConfig) -> &str {
    config.deployment.environment.as_str()
}
