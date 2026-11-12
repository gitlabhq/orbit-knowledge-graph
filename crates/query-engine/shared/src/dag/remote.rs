//! The security a remote stage needs around its request: the scope keys its reads' `via`
//! chains resolve on the relational connection, and the row checks its rows go through.

use arrow::record_batch::RecordBatch;
use compiler::dag::Request;
use compiler::remote::{Namespaces, RowCheck, Target, lookups};
use query_data_model::catalog::Row;
use query_data_model::surface::Statement;
use serde_json::Value;

use super::executor::{Executed, ExecutorError, RowChecks};
use super::rows;
use super::runner::{DagError, Runner};

/// Runs the scope lookups of a remote stage on the relational connection.
pub(super) async fn namespaces(
    runner: &mut Runner<'_>,
    stage: compiler::dag::StageId,
    target: &Target,
    statement: &Statement,
) -> Result<Namespaces, DagError> {
    let compiler::dag::Primary::ClickHouse(graph) = runner.env.primary else {
        return Ok(Namespaces::default());
    };
    let lookups = lookups(target, &statement.plan, graph, runner.env.grants)?;
    if lookups.is_empty() {
        return Ok(Namespaces::default());
    }
    let request = Request::Lookup(lookups.clone());
    runner.emit(|| super::observer::Event::Rendered {
        stage,
        requests: request.rendered(),
    });
    let executor = runner.executors.get(runner.env.primary.connection())?;
    let executed = executor.execute(&request).await?;
    let mut results = Vec::new();
    for (lookup, batches) in lookups.iter().zip(split(executed.batches, lookups.len())?) {
        let columns = [compiler::remote::KEY.to_string(), lookup.scope.clone()];
        results.push((lookup, json(&batches, &columns)?));
    }
    Ok(compiler::remote::namespaces(&results)?)
}

/// An executor returns one batch per lookup, in lookup order.
fn split(batches: Vec<RecordBatch>, count: usize) -> Result<Vec<Vec<RecordBatch>>, DagError> {
    match batches.len() == count {
        true => Ok(batches.into_iter().map(|batch| vec![batch]).collect()),
        false => Err(ExecutorError(format!(
            "{count} scope lookups returned {} result sets",
            batches.len()
        ))
        .into()),
    }
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

/// Keeps the rows of a remote stage that every row check of its entity allows.
pub(super) async fn checked(
    checks: &dyn RowChecks,
    row_checks: &[RowCheck],
    mut executed: Executed,
) -> Result<Executed, DagError> {
    for check in row_checks {
        let keys = rows::checked_keys(&executed.batches, &check.field, check.kind)?;
        let allowed = checks.allowed(check, keys.into_iter().collect()).await?;
        executed.batches = rows::retain(executed.batches, &check.field, check.kind, &allowed)?;
    }
    Ok(executed)
}
