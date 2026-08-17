use super::client::OrbitClient;
use super::error::RemoteError;

const DSL_PATH: &str = "/api/v4/orbit/schema/dsl";

pub(crate) async fn run_dsl() -> Result<(), RemoteError> {
    let client = OrbitClient::from_env()?;
    client.get_stream(DSL_PATH, true).await
}
