use std::sync::Arc;

use clickhouse_client::{ArrowClickHouseClient, ClickHouseConfiguration};
use ontology::Ontology;
use query_engine::{SecurityContext, compile};
use serde_json::{Value, json};
use thiserror::Error;

use crate::auth::Claims;
use crate::redaction::{
    QueryResult as RedactionQueryResult, QueryResultRow, RedactionExtractor, ResourceCheck,
};

#[derive(Debug, Error)]
pub enum QueryError {
    #[error("Invalid query: {0}")]
    InvalidQuery(String),

    #[error("Parse error: {0}")]
    ParseError(String),

    #[error("Execution failed: {0}")]
    ExecutionFailed(String),

    #[error("Security context error: {0}")]
    SecurityError(String),
}

impl QueryError {
    pub fn code(&self) -> String {
        match self {
            Self::InvalidQuery(_) => "invalid_query".to_string(),
            Self::ParseError(_) => "parse_error".to_string(),
            Self::ExecutionFailed(_) => "execution_error".to_string(),
            Self::SecurityError(_) => "security_error".to_string(),
        }
    }
}

#[derive(Debug)]
pub struct QueryResult {
    pub result: Value,
    pub generated_sql: String,
    pub resources_to_check: Vec<ResourceCheck>,
}

#[derive(Clone)]
pub struct QueryExecutor {
    client: Arc<ArrowClickHouseClient>,
    ontology: Arc<Ontology>,
}

impl std::fmt::Debug for QueryExecutor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("QueryExecutor")
            .field("ontology", &"<Ontology>")
            .finish()
    }
}

impl QueryExecutor {
    pub fn new(config: &ClickHouseConfiguration, ontology: Arc<Ontology>) -> Self {
        let client = Arc::new(config.build_client());
        Self { client, ontology }
    }

    pub async fn execute(
        &self,
        query_json: &str,
        claims: &Claims,
    ) -> Result<QueryResult, QueryError> {
        let ctx = security_context_from_claims(claims)?;

        let compiled = compile(query_json, &self.ontology, &ctx)
            .map_err(|e| QueryError::InvalidQuery(e.to_string()))?;

        let mut query = self.client.query(&compiled.sql);
        for (key, value) in &compiled.params {
            query = bind_param(query, key, value);
        }

        let batches = query
            .fetch_arrow()
            .await
            .map_err(|e| QueryError::ExecutionFailed(e.to_string()))?;

        let redaction_result =
            RedactionQueryResult::from_batches(&batches, &compiled.result_context);
        let extractor = RedactionExtractor::new(&self.ontology);
        let (_, resources_to_check) = extractor.extract(&redaction_result);

        let rows: Vec<Value> = redaction_result
            .rows()
            .iter()
            .map(|row| row_to_json(row, &compiled.result_context))
            .collect();

        Ok(QueryResult {
            result: json!({ "rows": rows, "count": rows.len() }),
            generated_sql: compiled.sql.clone(),
            resources_to_check,
        })
    }
}

fn security_context_from_claims(claims: &Claims) -> Result<SecurityContext, QueryError> {
    let org_id = claims.organization_id.unwrap_or(1) as i64;
    let traversal_paths = if claims.group_traversal_ids.is_empty() {
        vec![format!("{}/", org_id)]
    } else {
        claims.group_traversal_ids.clone()
    };
    SecurityContext::new(org_id, traversal_paths)
        .map_err(|e| QueryError::SecurityError(e.to_string()))
}

fn bind_param(
    query: clickhouse_client::ArrowQuery,
    key: &str,
    value: &Value,
) -> clickhouse_client::ArrowQuery {
    match value {
        Value::String(s) => query.param(key, s.as_str()),
        Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                query.param(key, i)
            } else if let Some(f) = n.as_f64() {
                query.param(key, f)
            } else {
                query.param(key, n.to_string())
            }
        }
        Value::Bool(b) => query.param(key, *b),
        Value::Array(arr) => {
            let strings: Vec<String> = arr
                .iter()
                .map(|v| match v {
                    Value::String(s) => s.clone(),
                    other => other.to_string(),
                })
                .collect();
            query.param(key, strings)
        }
        _ => query.param(key, value.to_string()),
    }
}

fn row_to_json(row: &QueryResultRow, ctx: &query_engine::ResultContext) -> Value {
    let mut obj = serde_json::Map::new();
    for node in ctx.nodes() {
        if let Some(id) = row.get_id(&node.alias) {
            obj.insert(format!("{}_id", node.alias), json!(id));
        }
        if let Some(entity_type) = row.get_type(&node.alias) {
            obj.insert(format!("{}_type", node.alias), json!(entity_type));
        }
    }
    Value::Object(obj)
}
