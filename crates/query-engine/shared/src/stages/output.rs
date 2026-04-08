use std::sync::Arc;

use serde_json::json;

use crate::types::{HydrationOutput, PaginationMeta, PipelineOutput, QueryExecutionLog};
use pipeline::{PipelineError, PipelineObserver, PipelineStage, QueryPipelineContext};

/// Traversal path prefix for the GitLab org namespace. Users whose
/// security context includes a path starting with this prefix get
/// compiled SQL in the response for debugging.
const GITLAB_ORG_PATH_PREFIX: &str = "1/9970/";

#[derive(Clone)]
pub struct OutputStage;

impl PipelineStage for OutputStage {
    type Input = HydrationOutput;
    type Output = PipelineOutput;

    async fn execute(
        &self,
        ctx: &mut QueryPipelineContext,
        _obs: &mut dyn PipelineObserver,
    ) -> Result<Self::Output, PipelineError> {
        let input = ctx.phases.get::<HydrationOutput>().ok_or_else(|| {
            PipelineError::Execution("HydrationOutput not found in phases".into())
        })?;

        let compiled = ctx.compiled()?;

        let raw_query_strings = if can_see_debug_sql(ctx) {
            let debug_json = json!({
                "base": compiled.base.sql,
                "base_rendered": compiled.base.render(),
                "hydration": input.hydration_queries,
            });
            vec![debug_json.to_string()]
        } else {
            vec![]
        };

        let execution_log = ctx
            .phases
            .get::<QueryExecutionLog>()
            .map(|log| log.0.clone())
            .unwrap_or_default();

        let mut query_result = input.query_result.clone();

        let pagination = compiled.input.cursor.map(|cursor| {
            let total_rows = query_result.authorized_count();
            let has_more = query_result.apply_cursor(cursor.offset, cursor.page_size);
            PaginationMeta {
                has_more,
                total_rows,
            }
        });

        Ok(PipelineOutput {
            row_count: query_result.authorized_count(),
            redacted_count: input.redacted_count,
            query_type: compiled.query_type.to_string(),
            raw_query_strings,
            compiled: Arc::clone(compiled),
            query_result,
            result_context: input.result_context.clone(),
            execution_log,
            pagination,
        })
    }
}

/// Debug SQL output is available to GitLab org members (Developer+) and admins.
fn can_see_debug_sql(ctx: &QueryPipelineContext) -> bool {
    ctx.security_context.as_ref().is_some_and(|sc| {
        let in_gitlab_org = sc
            .traversal_paths
            .iter()
            .any(|p| p.starts_with(GITLAB_ORG_PATH_PREFIX));
        let developer_or_above = sc.min_access_level.is_some_and(|level| level >= 30);
        sc.admin || (in_gitlab_org && developer_or_above)
    })
}
