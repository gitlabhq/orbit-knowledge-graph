//! `--mode clickhouse-setup`: applies `config/clickhouse-setup.sql` to the graph
//! ClickHouse as an administrator.

use std::collections::HashMap;

use anyhow::{Context, anyhow};
use clickhouse_client::ClickHouseConfigurationExt;
use orbit_server_config::{AppConfig, ClickHouseConfiguration};
use orbit_utils::clickhouse::quote_sql_literal;
use tracing::info;

const CONTRACT: &str = include_str!(concat!(env!("CONFIG_DIR"), "/clickhouse-setup.sql"));

pub async fn run(config: &AppConfig) -> anyhow::Result<()> {
    let setup = &config.clickhouse_setup;
    let sql = render(
        &config.graph.database,
        &config.datalake.database,
        required(&setup.writer_password, "writer_password")?,
        required(&setup.reader_password, "reader_password")?,
        required(&setup.siphon_reader_password, "siphon_reader_password")?,
    );

    // The graph database does not exist until the contract creates it.
    let admin = ClickHouseConfiguration {
        database: "default".to_string(),
        username: setup.admin_username.clone(),
        password: Some(required(&setup.admin_password, "admin_password")?.to_string()),
        session_settings: HashMap::new(),
        insert_settings: HashMap::new(),
        replicated: false,
        ..config.graph.clone()
    }
    .build_client();

    let statements: Vec<&str> = sql
        .lines()
        .filter(|line| !line.is_empty() && !line.starts_with("--"))
        .collect();
    for (index, statement) in statements.iter().enumerate() {
        // The client reads every `?` as a bind placeholder, quoted or not.
        admin
            .execute(&statement.replace('?', "??"))
            .await
            .with_context(|| format!("statement {} of {} failed", index + 1, statements.len()))?;
    }

    info!(statements = statements.len(), url = %config.graph.url, "clickhouse setup applied");
    Ok(())
}

fn required<'a>(value: &'a Option<String>, key: &str) -> anyhow::Result<&'a str> {
    value
        .as_deref()
        .filter(|text| !text.is_empty())
        .ok_or_else(|| {
            anyhow!(
                "clickhouse_setup.{key} is required: mount it at /etc/secrets/clickhouse_setup/{key}"
            )
        })
}

fn render(
    graph_db: &str,
    datalake_db: &str,
    writer: &str,
    reader: &str,
    siphon_reader: &str,
) -> String {
    CONTRACT
        .replace("${GRAPH_DB}", graph_db)
        .replace("${DATALAKE_DB}", datalake_db)
        .replace("'${GKG_WRITER_PASSWORD}'", &quote_sql_literal(writer))
        .replace("'${GKG_READER_PASSWORD}'", &quote_sql_literal(reader))
        .replace(
            "'${GKG_SIPHON_READER_PASSWORD}'",
            &quote_sql_literal(siphon_reader),
        )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn shipped_contract_has_only_known_placeholders() {
        let sql = render("orbit", "datalake", "w", "r", "s");

        assert!(!sql.contains("${"), "{sql}");
    }

    #[tokio::test]
    async fn missing_password_is_named_before_connecting() {
        let mut config = AppConfig::embedded_defaults();
        config.clickhouse_setup.admin_password = Some("admin".to_string());
        config.clickhouse_setup.writer_password = Some("writer".to_string());
        config.clickhouse_setup.siphon_reader_password = Some("siphon".to_string());

        let error = run(&config).await.unwrap_err();

        assert!(error.to_string().contains("reader_password"), "{error}");
    }
}
