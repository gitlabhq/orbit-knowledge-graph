use anyhow::{Result, bail};
use clickhouse_client::ArrowClickHouseClient;

/// Check that ClickHouse is running and healthy.
pub async fn check_clickhouse_health(client: &ArrowClickHouseClient) -> Result<()> {
    // Try a simple query to verify connectivity
    let result: Result<String, _> = client.inner().query("SELECT version()").fetch_one().await;

    match result {
        Ok(version) => {
            tracing::debug!("ClickHouse version: {}", version);
            Ok(())
        }
        Err(e) => {
            let error_msg = e.to_string();

            if error_msg.contains("Connect") || error_msg.contains("connection") {
                bail!(
                    "Cannot connect to ClickHouse.\n\n\
                    Make sure ClickHouse is running:\n\
                    - Docker: docker run -d -p 8123:8123 clickhouse/clickhouse-server\n\
                    - Local: clickhouse-server\n\n\
                    Error: {}",
                    error_msg
                );
            } else {
                bail!("ClickHouse health check failed: {}", error_msg)
            }
        }
    }
}
