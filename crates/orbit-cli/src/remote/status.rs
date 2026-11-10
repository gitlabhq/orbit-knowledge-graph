use cynic::QueryBuilder;
use orbit_cli_graphql::queries::service_status::{OrbitServiceStatus, ServiceStatus};

use super::client::OrbitClient;
use super::error::{EXIT_UNAVAILABLE, RemoteError, build_graphql_error};
use super::{legacy, pretty_value, write_stdout};

pub(crate) async fn run_status() -> Result<(), RemoteError> {
    let client = OrbitClient::from_env()?;
    match read_service_status(&client).await? {
        Some(status) => write_stdout(&pretty_value(&status)),
        None => legacy::run_status(&client).await,
    }
}

impl OrbitClient {
    pub(crate) async fn check_status(&self) -> Result<(), RemoteError> {
        match read_service_status(self).await? {
            Some(_) => Ok(()),
            None => legacy::check_status(self).await,
        }
    }
}

pub(crate) async fn read_service_status(
    client: &OrbitClient,
) -> Result<Option<OrbitServiceStatus>, RemoteError> {
    let Some(response) = client.graphql(&ServiceStatus::build(())).await? else {
        return Ok(None);
    };
    if let Some(errors) = response.errors.filter(|errors| !errors.is_empty()) {
        return Err(build_graphql_error("Orbit status query failed", &errors));
    }
    let Some(orbit) = response.data.and_then(|data| data.orbit) else {
        return Ok(None);
    };
    orbit
        .service_status
        .map(Some)
        .ok_or_else(build_unavailable_error)
}

pub(crate) fn build_unavailable_error() -> RemoteError {
    RemoteError::new(
        EXIT_UNAVAILABLE,
        "Orbit is not available for your user\n\n\
         The most common causes are:\n\
         \x20 - The `knowledge_graph` feature flag is disabled for your user.\n\
         \x20 - The instance does not include the `:orbit` license add-on.\n\
         Contact an instance administrator to enable it.",
    )
}
