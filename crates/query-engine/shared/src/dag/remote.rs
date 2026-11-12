//! The security a remote stage needs around its request: the scope keys its reads' `via`
//! chains resolve on the relational connection, and the row checks its rows go through.

use arrow::record_batch::RecordBatch;
use compiler::dag::Request;
use compiler::dag::RowCheck;
use compiler::remote::{Admitted, Namespaces, Target, lookup};
use query_data_model::catalog::Row;
use query_data_model::surface::Statement;
use serde_json::Value;

use compiler::orbit_authorization::{AuthError, Caller, CheckRequest, Checks};

use super::executor::{Executed, ExecutorError};
use super::rows;
use super::runner::{DagError, Runner};

/// Runs the scope lookups of a remote stage on the relational connection, and what its reads'
/// policies then admit; none when they admit nothing, so the stage reads nothing.
pub(super) async fn admitted(
    runner: &mut Runner<'_>,
    stage: compiler::dag::StageId,
    target: &Target,
    statement: &Statement,
) -> Result<Option<Admitted>, DagError> {
    let grants = runner.env.grants;
    let mut namespaces = Namespaces::default();
    let lookups = match runner.env.primary {
        compiler::dag::Primary::ClickHouse(graph) => {
            lookup(target, &statement.plan, graph, grants)?
        }
        compiler::dag::Primary::DuckDb(_) => Vec::new(),
    };
    for lookup in lookups {
        let columns = [compiler::remote::KEY.to_string(), lookup.scope.clone()];
        let request = Request::Lookup(lookup.clone());
        runner.emit(|| super::observer::Event::Rendered {
            stage,
            requests: request.rendered(),
        });
        let executor = runner.executors.get(runner.env.primary.connection())?;
        let executed = executor.execute(&request).await?;
        let rows = json(&executed.batches, &columns)?;
        compiler::remote::namespaces(&lookup, &rows, &mut namespaces)?;
    }
    let admitted = compiler::remote::admitted(target, &statement.plan, grants, &namespaces)?;
    Ok((!admitted.is_empty()).then_some(admitted))
}

/// Lookup rows as JSON rows of their key and scope columns.
fn json(batches: &[RecordBatch], columns: &[String]) -> Result<Vec<Row>, ExecutorError> {
    let mut out = Vec::new();
    for batch in batches {
        let mut values = Vec::new();
        for column in columns {
            let keys = rows::column_keys(batch, column)?;
            values.push(keys);
        }
        for row in 0..batch.num_rows() {
            let mut json = Row::new();
            for (column, keys) in columns.iter().zip(&values) {
                let value = match &keys[row] {
                    Some(compiler::orbit_authorization::Key::Int(key)) => Value::from(*key),
                    Some(key) => Value::from(key.to_string()),
                    None => Value::Null,
                };
                json.insert(column.clone(), value);
            }
            out.push(json);
        }
    }
    Ok(out)
}

/// Keeps the rows of a stage that every row check of its entities allows, asked of the providers
/// the deployment binds.
pub(super) async fn checked(
    checks: &Checks,
    caller: &Caller,
    row_checks: &[RowCheck],
    mut executed: Executed,
) -> Result<Executed, DagError> {
    for check in row_checks {
        let keys = rows::checked_keys(&executed.batches, &check.column, check.kind)?;
        let request = CheckRequest {
            permission: check.permission.clone(),
            resource: check.resource.clone(),
            ids: keys.into_iter().collect(),
        };
        let allowed = checks
            .check(caller, &check.provider, vec![request])
            .await
            .map_err(|error: AuthError| ExecutorError(error.to_string()))?;
        let allowed = allowed.into_iter().next().unwrap_or_default();
        executed.batches = rows::retain(executed.batches, &check.column, check.kind, &allowed)?;
    }
    Ok(executed)
}
