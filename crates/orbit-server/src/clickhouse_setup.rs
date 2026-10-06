//! `--mode clickhouse-setup`: applies `config/clickhouse-setup.sql` to the graph
//! ClickHouse as an administrator.

use anyhow::{Context, anyhow};
use clickhouse_client::{ArrowClickHouseClient, ClickHouseConfigurationExt};
use orbit_server_config::{AppConfig, ClickHouseConfiguration};
use orbit_utils::clickhouse::quote_sql_literal;
use tracing::info;

const CONTRACT: &str = include_str!(concat!(env!("CONFIG_DIR"), "/clickhouse-setup.sql"));

pub async fn run(config: &AppConfig) -> anyhow::Result<()> {
    let setup = &config.clickhouse_setup;
    let admin_password = required(&setup.admin_password, "admin_password")?;
    let writer_password = required(&setup.writer_password, "writer_password")?;
    let reader_password = required(&setup.reader_password, "reader_password")?;
    let siphon_reader_password = required(&setup.siphon_reader_password, "siphon_reader_password")?;

    // The graph database does not exist until the contract creates it.
    let admin = ClickHouseConfiguration {
        database: "default".to_string(),
        username: setup.admin_username.clone(),
        password: Some(admin_password.to_string()),
        ..config.graph.clone()
    }
    .build_client();

    if config.graph.replicated {
        check_replicated_prerequisites(&admin, &config.graph.database).await?;
    }

    let statements = statements(
        &config.graph.database,
        &config.datalake.database,
        writer_password,
        reader_password,
        siphon_reader_password,
    );
    for (index, statement) in statements.iter().enumerate() {
        admin
            .inner()
            .query_raw(statement)
            .execute()
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
                "clickhouse_setup.{key} is required (set it in a config overlay or mount it at /etc/secrets/clickhouse_setup/{key})"
            )
        })
}

// The contract has no ON CLUSTER: users reach every replica only through replicated
// user storage, and replicated tables need a database the operator made Replicated.
async fn check_replicated_prerequisites(
    admin: &ArrowClickHouseClient,
    graph_db: &str,
) -> anyhow::Result<()> {
    let checks = [
        "SELECT throwIf((SELECT count() FROM system.user_directories WHERE type = 'replicated') = 0, \
         'a replicated graph needs a replicated user directory, or the users exist on one replica only')"
            .to_string(),
        format!(
            "SELECT throwIf((SELECT count() FROM system.databases WHERE name = {} AND engine = 'Replicated') = 0, \
             'a replicated graph needs the graph database created with the Replicated engine ON CLUSTER before setup')",
            quote_sql_literal(graph_db)
        ),
    ];
    for check in &checks {
        admin.inner().query_raw(check).execute().await?;
    }
    Ok(())
}

fn statements(
    graph_db: &str,
    datalake_db: &str,
    writer: &str,
    reader: &str,
    siphon_reader: &str,
) -> Vec<String> {
    CONTRACT
        .lines()
        .filter(|line| !line.is_empty() && !line.starts_with("--"))
        .map(|line| {
            line.replace("${GRAPH_DB}", graph_db)
                .replace("${DATALAKE_DB}", datalake_db)
                .replace("'${GKG_WRITER_PASSWORD}'", &quote_sql_literal(writer))
                .replace("'${GKG_READER_PASSWORD}'", &quote_sql_literal(reader))
                .replace(
                    "'${GKG_SIPHON_READER_PASSWORD}'",
                    &quote_sql_literal(siphon_reader),
                )
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn shipped_contract_has_only_known_placeholders() {
        let statements = statements("orbit", "datalake", "w", "r", "s");

        assert!(
            statements.iter().all(|sql| !sql.contains("${")),
            "{statements:?}"
        );
    }

    #[test]
    fn password_text_does_not_change_the_statement_count() {
        let plain = statements("orbit", "datalake", "w", "r", "s");
        let multiline = statements("orbit", "datalake", "w\n-- x", "r\n\ny", "s");

        assert_eq!(plain.len(), multiline.len());
    }

    #[tokio::test]
    async fn missing_password_is_named_before_connecting() {
        let mut config = AppConfig::embedded_defaults();
        config.clickhouse_setup.admin_password = Some("admin".to_string());
        config.clickhouse_setup.writer_password = Some("writer".to_string());
        config.clickhouse_setup.siphon_reader_password = Some("siphon".to_string());

        let error = run(&config).await.unwrap_err();

        assert!(
            error
                .to_string()
                .contains("clickhouse_setup.reader_password"),
            "{error}"
        );
    }
}
