//! The row checks a stage's rows go through before a later stage reads them.

use compiler::dag::RowCheck;
use compiler::orbit_authorization::{AuthError, Caller, CheckRequest, Checks};

use super::executor::{Executed, ExecutorError};
use super::rows;
use super::runner::DagError;

/// Keeps the rows of a stage that every row check of its entities allows, asked of the providers
/// the deployment binds.
pub(super) async fn checked(
    checks: &Checks,
    caller: &Caller,
    row_checks: &[RowCheck],
    mut executed: Executed,
) -> Result<Executed, DagError> {
    for check in row_checks {
        let keys = rows::checked_keys(&executed.batches, check.position, check.kind)?;
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
        executed.batches = rows::retain(executed.batches, check.position, check.kind, &allowed)?;
    }
    Ok(executed)
}
