//! `--mode clickhouse-setup`: applies `config/clickhouse-setup.sql` to the graph
//! ClickHouse as an administrator. Values are bound, never pasted, so password text
//! cannot change the SQL.

use std::collections::HashMap;

use anyhow::{Context, anyhow, ensure};
use clickhouse::sql::Identifier;
use clickhouse_client::ClickHouseConfigurationExt;
use orbit_server_config::{AppConfig, ClickHouseConfiguration};
use tracing::info;

const CONTRACT: &str = include_str!(concat!(env!("CONFIG_DIR"), "/clickhouse-setup.sql"));

#[derive(Clone, Copy, Debug, PartialEq)]
enum Value<'a> {
    Literal(&'a str),
    Identifier(&'a str),
}

struct Statement<'a> {
    sql: String,
    values: Vec<Value<'a>>,
}

pub async fn run(config: &AppConfig) -> anyhow::Result<()> {
    let setup = &config.clickhouse_setup;
    let tokens = [
        ("${GRAPH_DB}", Value::Identifier(&config.graph.database)),
        (
            "${DATALAKE_DB}",
            Value::Identifier(&config.datalake.database),
        ),
        (
            "'${GKG_WRITER_PASSWORD}'",
            Value::Literal(required(&setup.writer_password, "writer_password")?),
        ),
        (
            "'${GKG_READER_PASSWORD}'",
            Value::Literal(required(&setup.reader_password, "reader_password")?),
        ),
        (
            "'${GKG_SIPHON_READER_PASSWORD}'",
            Value::Literal(required(
                &setup.siphon_reader_password,
                "siphon_reader_password",
            )?),
        ),
    ];
    let statements = render(CONTRACT, &tokens)?;

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

    let total = statements.len();
    for (index, statement) in statements.iter().enumerate() {
        let mut query = admin.inner().query(&statement.sql);
        for value in &statement.values {
            query = match *value {
                Value::Literal(text) => query.bind(text),
                Value::Identifier(name) => query.bind(Identifier(name)),
            };
        }
        query.execute().await.with_context(|| {
            format!(
                "statement {} of {total} failed: {}",
                index + 1,
                statement.sql
            )
        })?;
    }

    info!(statements = total, url = %config.graph.url, "clickhouse setup applied");
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

fn render<'a>(contract: &str, tokens: &[(&str, Value<'a>)]) -> anyhow::Result<Vec<Statement<'a>>> {
    let body = contract
        .lines()
        .filter(|line| !line.trim_start().starts_with("--"))
        .collect::<Vec<_>>()
        .join("\n");

    body.split(';')
        .map(str::trim)
        .filter(|statement| !statement.is_empty())
        .map(|statement| bind_tokens(statement, tokens))
        .collect()
}

fn bind_tokens<'a>(statement: &str, tokens: &[(&str, Value<'a>)]) -> anyhow::Result<Statement<'a>> {
    let mut sql = String::new();
    let mut values = Vec::new();
    let mut rest = statement;
    while let Some((at, token, value)) = next_token(rest, tokens) {
        // The client reads every `?` as a bind placeholder, quoted or not.
        sql.push_str(&rest[..at].replace('?', "??"));
        sql.push('?');
        values.push(value);
        rest = &rest[at + token.len()..];
    }
    sql.push_str(&rest.replace('?', "??"));

    ensure!(
        !sql.contains("${"),
        "unknown placeholder in the setup contract: {statement}"
    );
    Ok(Statement { sql, values })
}

fn next_token<'a, 't>(
    text: &str,
    tokens: &[(&'t str, Value<'a>)],
) -> Option<(usize, &'t str, Value<'a>)> {
    tokens
        .iter()
        .filter_map(|(token, value)| text.find(token).map(|at| (at, *token, *value)))
        .min_by_key(|(at, _, _)| *at)
}

#[cfg(test)]
mod tests {
    use super::*;

    const TOKENS: [(&str, Value<'static>); 5] = [
        ("${GRAPH_DB}", Value::Identifier("orbit")),
        ("${DATALAKE_DB}", Value::Identifier("datalake")),
        ("'${GKG_WRITER_PASSWORD}'", Value::Literal("writer-secret")),
        ("'${GKG_READER_PASSWORD}'", Value::Literal("reader-secret")),
        (
            "'${GKG_SIPHON_READER_PASSWORD}'",
            Value::Literal("siphon-secret"),
        ),
    ];

    #[test]
    fn shipped_contract_binds_every_placeholder() {
        let statements = render(CONTRACT, &TOKENS).unwrap();

        assert!(!statements.is_empty());
        for statement in &statements {
            assert!(!statement.sql.contains("secret"), "{}", statement.sql);
            assert_eq!(
                statement.sql.matches('?').count(),
                statement.values.len(),
                "{}",
                statement.sql
            );
        }
        for (_, password) in &TOKENS[2..] {
            let binds = statements
                .iter()
                .flat_map(|statement| &statement.values)
                .filter(|value| *value == password)
                .count();
            assert_eq!(binds, 2, "CREATE and ALTER each bind {password:?}");
        }
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

    #[test]
    fn unknown_placeholder_is_rejected() {
        let error = render("CREATE USER x IDENTIFIED BY '${NEW_PASSWORD}';", &TOKENS)
            .err()
            .unwrap();

        assert!(error.to_string().contains("unknown placeholder"), "{error}");
    }
}
