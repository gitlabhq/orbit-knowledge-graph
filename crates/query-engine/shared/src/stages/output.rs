use std::sync::Arc;

use serde_json::json;

use crate::types::{HydrationOutput, PaginationMeta, PipelineOutput, QueryExecutionLog};
use pipeline::{PipelineError, PipelineObserver, PipelineStage, QueryPipelineContext};

/// Traversal path for the top-level GitLab org group. Only direct
/// members of this group with Reporter+ access get compiled SQL.
const GITLAB_ORG_PATH_PREFIX: &str = "1/9970/";

#[derive(Clone)]
pub struct OutputStage;

impl PipelineStage for OutputStage {
    type Input = HydrationOutput;
    type Output = PipelineOutput;

    async fn execute(
        &self,
        ctx: &mut QueryPipelineContext,
        obs: &mut dyn PipelineObserver,
    ) -> Result<Self::Output, PipelineError> {
        let input = ctx
            .phases
            .get::<HydrationOutput>()
            .ok_or_else(|| PipelineError::Execution("HydrationOutput not found in phases".into()))
            .inspect_err(|e| obs.record_error(e))?;

        let compiled = ctx.compiled().inspect_err(|e| obs.record_error(e))?;

        let requested = compiled.input.options.include_debug_sql;
        let raw_query_strings = if requested && can_see_debug_sql(ctx) {
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

/// Debug SQL output requires direct membership in the top-level GitLab org
/// group (traversal path exactly `1/9970/`) with Reporter+ access on that
/// path. Sub-group or project-only members don't qualify -- this prevents
/// external contributors invited to a single project from seeing compiled SQL.
///
/// Access level is derived from the per-path `access_levels` populated by
/// Rails (via `TraversalPathClaim`), not the top-level `min_access_level`
/// JWT field which may be absent. The minimum across all access levels on
/// the GitLab org path must be at least Reporter.
///
/// Instance admins do not bypass this check -- debug SQL requires explicit
/// GitLab org membership.
fn can_see_debug_sql(ctx: &QueryPipelineContext) -> bool {
    ctx.security_context.as_ref().is_some_and(|sc| {
        sc.traversal_paths
            .iter()
            .find(|tp| tp.path == GITLAB_ORG_PATH_PREFIX)
            .is_some_and(|tp| {
                tp.access_levels
                    .iter()
                    .copied()
                    .min()
                    .is_some_and(|min_level| min_level >= compiler::AccessLevel::Reporter as u32)
            })
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use compiler::SecurityContext;
    use ontology::Ontology;
    use pipeline::QueryPipelineContext;
    use std::sync::Arc;

    fn make_ctx(security_context: Option<SecurityContext>) -> QueryPipelineContext {
        QueryPipelineContext {
            query_json: String::new(),
            compiled: None,
            ontology: Arc::new(Ontology::default()),
            security_context,
            server_extensions: Default::default(),
            phases: Default::default(),
        }
    }

    #[test]
    fn no_security_context_denies_debug_sql() {
        let ctx = make_ctx(None);
        assert!(!can_see_debug_sql(&ctx));
    }

    #[test]
    fn external_user_denied() {
        let sc =
            SecurityContext::new_with_roles(1, vec![compiler::TraversalPath::new("1/12345/", 20)])
                .unwrap();
        let ctx = make_ctx(Some(sc));
        assert!(!can_see_debug_sql(&ctx));
    }

    #[test]
    fn gitlab_org_subgroup_member_denied() {
        let sc = SecurityContext::new_with_roles(
            1,
            vec![compiler::TraversalPath::new("1/9970/555/", 20)],
        )
        .unwrap();
        let ctx = make_ctx(Some(sc));
        assert!(!can_see_debug_sql(&ctx));
    }

    #[test]
    fn gitlab_org_direct_member_guest_denied() {
        let sc =
            SecurityContext::new_with_roles(1, vec![compiler::TraversalPath::new("1/9970/", 10)])
                .unwrap();
        let ctx = make_ctx(Some(sc));
        assert!(!can_see_debug_sql(&ctx));
    }

    #[test]
    fn gitlab_org_direct_member_reporter_allowed() {
        let sc =
            SecurityContext::new_with_roles(1, vec![compiler::TraversalPath::new("1/9970/", 20)])
                .unwrap();
        let ctx = make_ctx(Some(sc));
        assert!(can_see_debug_sql(&ctx));
    }

    #[test]
    fn gitlab_org_direct_member_developer_allowed() {
        let sc =
            SecurityContext::new_with_roles(1, vec![compiler::TraversalPath::new("1/9970/", 30)])
                .unwrap();
        let ctx = make_ctx(Some(sc));
        assert!(can_see_debug_sql(&ctx));
    }

    #[test]
    fn admin_without_gitlab_org_membership_denied() {
        let sc = SecurityContext::new(1, vec!["1/".into()])
            .unwrap()
            .with_role(true, None);
        let ctx = make_ctx(Some(sc));
        assert!(!can_see_debug_sql(&ctx));
    }

    #[test]
    fn admin_with_gitlab_org_reporter_allowed() {
        let sc =
            SecurityContext::new_with_roles(1, vec![compiler::TraversalPath::new("1/9970/", 20)])
                .unwrap()
                .with_role(true, None);
        let ctx = make_ctx(Some(sc));
        assert!(can_see_debug_sql(&ctx));
    }

    #[test]
    fn multiple_paths_uses_gitlab_org_path_access_level() {
        let sc = SecurityContext::new_with_roles(
            1,
            vec![
                compiler::TraversalPath::new("1/12345/", 30),
                compiler::TraversalPath::new("1/9970/", 10),
            ],
        )
        .unwrap();
        let ctx = make_ctx(Some(sc));
        assert!(
            !can_see_debug_sql(&ctx),
            "Guest on gitlab-org path should deny even with Developer on other paths"
        );
    }
}
