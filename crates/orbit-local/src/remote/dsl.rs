use super::client::OrbitClient;
use super::error::RemoteError;
use super::write_stdout;

pub(crate) async fn run_dsl() -> Result<(), RemoteError> {
    let client = OrbitClient::from_env()?;
    let dsl = client.get_dsl().await?;
    write_stdout(&dsl)
}
