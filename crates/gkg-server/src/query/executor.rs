use std::sync::Arc;

use clickhouse_client::{ArrowClickHouseClient, ClickHouseConfiguration};
use ontology::Ontology;
use query_engine::{SecurityContext, compile};
use serde_json::Value;
use thiserror::Error;

use crate::auth::Claims;
use crate::redaction::{
    QueryResult as RedactionQueryResult, RedactionExtractor, ResourceCheck,
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
    pub redaction_result: RedactionQueryResult,
    pub result_context: query_engine::ResultContext,
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

        Ok(QueryResult {
            redaction_result,
            result_context: compiled.result_context,
            generated_sql: compiled.sql.clone(),
            resources_to_check,
        })
    }
}

fn security_context_from_claims(claims: &Claims) -> Result<SecurityContext, QueryError> {
    let org_id = claims.organization_id.unwrap_or(1) as i64;
    let traversal_paths = if claims.admin {
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

#[cfg(test)]
mod tests {
    use super::*;

    fn make_claims(
        admin: bool,
        group_traversal_ids: Vec<String>,
        organization_id: Option<u64>,
    ) -> Claims {
        Claims {
            sub: "user:1".to_string(),
            iss: "gitlab".to_string(),
            aud: "gitlab-knowledge-graph".to_string(),
            iat: 0,
            exp: i64::MAX,
            user_id: 1,
            username: "test_user".to_string(),
            admin,
            organization_id,
            min_access_level: Some(20),
            group_traversal_ids,
        }
    }

    #[test]
    fn admin_gets_org_wide_access() {
        let claims = make_claims(true, vec![], Some(42));
        let ctx = security_context_from_claims(&claims).unwrap();

        assert_eq!(ctx.org_id, 42);
        assert_eq!(ctx.traversal_paths, vec!["42/"]);
    }

    #[test]
    fn admin_with_default_org_gets_org_1() {
        let claims = make_claims(true, vec![], None);
        let ctx = security_context_from_claims(&claims).unwrap();

        assert_eq!(ctx.org_id, 1);
        assert_eq!(ctx.traversal_paths, vec!["1/"]);
    }

    #[test]
    fn admin_ignores_group_traversal_ids() {
        let claims = make_claims(
            true,
            vec!["1/22/".to_string(), "1/33/".to_string()],
            Some(1),
        );
        let ctx = security_context_from_claims(&claims).unwrap();

        assert_eq!(ctx.traversal_paths, vec!["1/"]);
    }

    #[test]
    fn non_admin_gets_their_group_paths() {
        let claims = make_claims(
            false,
            vec!["1/22/".to_string(), "1/33/".to_string()],
            Some(1),
        );
        let ctx = security_context_from_claims(&claims).unwrap();

        assert_eq!(ctx.traversal_paths, vec!["1/22/", "1/33/"]);
    }

    #[test]
    fn non_admin_with_empty_groups_gets_no_access() {
        let claims = make_claims(false, vec![], Some(1));
        let ctx = security_context_from_claims(&claims).unwrap();

        assert!(ctx.traversal_paths.is_empty());
    }

    #[test]
    fn non_admin_with_single_group_path() {
        let claims = make_claims(false, vec!["1/24/111/".to_string()], Some(1));
        let ctx = security_context_from_claims(&claims).unwrap();

        assert_eq!(ctx.traversal_paths, vec!["1/24/111/"]);
    }
}
