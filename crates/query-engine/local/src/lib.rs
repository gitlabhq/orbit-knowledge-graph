use std::sync::{Mutex, MutexGuard, PoisonError};

use arrow::record_batch::RecordBatch;
use compiler::{ParamValue, ParameterizedQuery};
use duckdb_client::DuckDbClient;
use pipeline::{PipelineError, PipelineObserver, QueryPipelineContext};
use shared::QueryExecutor;

pub struct DuckDbExecutor {
    client: Mutex<DuckDbClient>,
}

impl DuckDbExecutor {
    pub fn new(client: DuckDbClient) -> Self {
        Self {
            client: Mutex::new(client),
        }
    }

    pub fn client(&self) -> MutexGuard<'_, DuckDbClient> {
        self.client.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

fn positional(query: &ParameterizedQuery) -> Result<Vec<ParamValue>, PipelineError> {
    let mut bound = query
        .params
        .iter()
        .map(|(name, param)| {
            name.strip_prefix('p')
                .and_then(|position| position.parse::<usize>().ok())
                .map(|position| (position, param.clone()))
                .ok_or_else(|| {
                    PipelineError::Execution(format!("parameter {name} is not positional"))
                })
        })
        .collect::<Result<Vec<_>, _>>()?;
    bound.sort_by_key(|(position, _)| *position);
    if bound
        .iter()
        .enumerate()
        .any(|(index, (position, _))| *position != index + 1)
    {
        return Err(PipelineError::Execution(
            "parameters are not numbered from 1 without gaps".into(),
        ));
    }
    Ok(bound.into_iter().map(|(_, param)| param).collect())
}

impl QueryExecutor for DuckDbExecutor {
    async fn run(
        &self,
        query: &ParameterizedQuery,
        _ctx: &mut QueryPipelineContext,
        _obs: &mut dyn PipelineObserver,
    ) -> Result<Vec<RecordBatch>, PipelineError> {
        let params = positional(query)?;
        self.client()
            .query_arrow_bound(&query.sql, &params)
            .map_err(|error| PipelineError::Execution(format!("{error}\n{}", query.render())))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use compiler::{Frontend, SqlDialect};
    use orbit_server_config::QueryConfig;
    use orbit_utils::query_types::{ScalarType, SqlType, TimeZone};
    use pipeline::{NoOpObserver, TypeMap};
    use serde_json::{Value, json};

    use super::*;

    fn context() -> QueryPipelineContext {
        QueryPipelineContext {
            frontend: Frontend::Gql,
            query_json: String::new(),
            compiled: None,
            ontology: Arc::new(ontology::Ontology::new()),
            security_context: None,
            server_extensions: TypeMap::default(),
            phases: TypeMap::default(),
        }
    }

    fn query(sql: &str, params: Vec<(SqlType, Value)>) -> ParameterizedQuery {
        ParameterizedQuery {
            sql: sql.into(),
            params: params
                .into_iter()
                .enumerate()
                .map(|(index, (data_type, value))| {
                    (format!("p{}", index + 1), ParamValue { data_type, value })
                })
                .collect::<HashMap<_, _>>(),
            result_context: compiler::ResultContext::new(),
            query_config: QueryConfig::default(),
            dialect: SqlDialect::DuckDb,
        }
    }

    fn executor() -> DuckDbExecutor {
        let client = DuckDbClient::open(duckdb_client::OpenMode::InMemory).unwrap();
        client
            .initialize_schema(
                "CREATE TABLE t (id BIGINT, name VARCHAR, flag BOOLEAN, d DATE, ts TIMESTAMP, score DOUBLE, small UINTEGER);\
                 INSERT INTO t VALUES \
                 (1, 'a', true, DATE '2024-01-02', TIMESTAMP '2024-01-02 03:04:05', 1.5, 7), \
                 (2, 'b', false, DATE '2024-02-03', TIMESTAMP '2024-02-03 04:05:06', 2.5, 8), \
                 (9223372036854775807, 'c', true, DATE '2024-03-04', TIMESTAMP '2024-03-04 05:06:07', 3.5, 9);",
            )
            .unwrap();
        DuckDbExecutor::new(client)
    }

    async fn bound(executor: &DuckDbExecutor, query: &ParameterizedQuery) -> Vec<RecordBatch> {
        executor
            .run(query, &mut context(), &mut NoOpObserver)
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn bound_parameters_return_the_rows_the_rendered_sql_returns() {
        let executor = executor();
        let timestamp = SqlType::Timestamp {
            precision: 6,
            timezone: Some(TimeZone::Utc),
        };
        let cases = [
            query(
                "SELECT id FROM t WHERE id = $1",
                vec![(SqlType::Int64, json!(i64::MAX))],
            ),
            query(
                "SELECT id FROM t WHERE name = $1",
                vec![(SqlType::String, json!("b"))],
            ),
            query(
                "SELECT id FROM t WHERE name IN ($1, $2) ORDER BY id",
                vec![(SqlType::String, json!("a")), (SqlType::String, json!("c"))],
            ),
            query(
                "SELECT id FROM t WHERE id IN ($1, $2) ORDER BY id",
                vec![(SqlType::Int64, json!(1)), (SqlType::Int64, json!(2))],
            ),
            query(
                "SELECT id FROM t WHERE flag = $1 ORDER BY id",
                vec![(SqlType::Bool, json!(true))],
            ),
            query(
                "SELECT id FROM t WHERE d = CAST($1 AS DATE)",
                vec![(SqlType::Date, json!("2024-02-03"))],
            ),
            query(
                "SELECT id FROM t WHERE ts >= CAST($1 AS TIMESTAMP)",
                vec![(timestamp, json!("2024-02-03 04:05:06"))],
            ),
            query(
                "SELECT id FROM t WHERE score > $1 ORDER BY id",
                vec![(SqlType::Float64, json!(2.0))],
            ),
            query(
                "SELECT id FROM t WHERE small = $1",
                vec![(SqlType::UInt32, json!(8))],
            ),
            query(
                "SELECT id FROM t WHERE name = $1 AND flag = $2 AND id > $3",
                vec![
                    (SqlType::String, json!("c")),
                    (SqlType::Bool, json!(true)),
                    (SqlType::Int64, json!(1)),
                ],
            ),
        ];
        for case in cases {
            let rendered = executor.client().query_arrow(&case.render()).unwrap();
            let rows = bound(&executor, &case).await;
            assert!(!rows.is_empty(), "{}", case.sql);
            assert_eq!(rows, rendered, "{}", case.sql);
        }
    }

    #[tokio::test]
    async fn parameters_that_do_not_fit_their_type_fail() {
        let executor = executor();
        for (data_type, value) in [
            (SqlType::Int64, json!(u64::MAX)),
            (SqlType::Int64, json!("1")),
            (SqlType::UInt32, json!(u64::from(u32::MAX) + 1)),
            (SqlType::Bool, json!(1)),
            (SqlType::Array(ScalarType::Int64), json!([1])),
        ] {
            let case = query("SELECT id FROM t WHERE id = $1", vec![(data_type, value)]);
            let error = executor
                .run(&case, &mut context(), &mut NoOpObserver)
                .await
                .unwrap_err();
            assert!(error.to_string().contains("does not fit"), "{error}");
        }
    }

    #[tokio::test]
    async fn parameters_must_be_positional() {
        let executor = executor();
        let mut case = query(
            "SELECT id FROM t WHERE id = $1",
            vec![(SqlType::Int64, json!(1))],
        );
        case.params.insert(
            "named".into(),
            ParamValue {
                data_type: SqlType::Int64,
                value: json!(2),
            },
        );
        let error = executor
            .run(&case, &mut context(), &mut NoOpObserver)
            .await
            .unwrap_err();
        assert!(error.to_string().contains("not positional"), "{error}");
    }
}
