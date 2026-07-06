use std::sync::Arc;

use serde_json::json;

use crate::types::{HydrationOutput, PipelineOutput, QueryExecutionLog};
use pipeline::{PipelineError, PipelineObserver, PipelineStage, QueryPipelineContext};

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
        let pagination = Some(crate::types::paginate(&mut query_result, &compiled.input));

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

/// Debug SQL is gated by the JWT `is_gitlab_team_member` claim and realm:
///
/// - **SaaS** (`realm == "SaaS"`): requires `is_gitlab_team_member == true`.
///   Rails sets this via `Gitlab::Com.gitlab_com_group_member?(user)`.
/// - **Self-managed / Dedicated**: instance admins only.
///   `is_gitlab_team_member` is always false off `.com`.
/// - **Unknown realm or missing context**: denied.
fn can_see_debug_sql(ctx: &QueryPipelineContext) -> bool {
    ctx.security_context
        .as_ref()
        .is_some_and(|sc| match sc.realm {
            Some(compiler::Realm::SaaS) => sc.is_gitlab_team_member,
            Some(compiler::Realm::SelfManaged) | Some(compiler::Realm::Dedicated) => sc.admin,
            None => false,
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

    fn saas_ctx(team_member: bool, admin: bool) -> QueryPipelineContext {
        let sc = SecurityContext::new(1, vec!["1/9970/".into()])
            .unwrap()
            .with_role(admin, Some(20))
            .with_realm(Some(compiler::Realm::SaaS))
            .with_team_member(team_member);
        make_ctx(Some(sc))
    }

    fn self_managed_ctx(admin: bool) -> QueryPipelineContext {
        let sc = SecurityContext::new(1, vec!["1/".into()])
            .unwrap()
            .with_role(admin, None)
            .with_realm(Some(compiler::Realm::SelfManaged));
        make_ctx(Some(sc))
    }

    #[test]
    fn no_security_context_denies() {
        assert!(!can_see_debug_sql(&make_ctx(None)));
    }

    #[test]
    fn saas_team_member_allowed() {
        assert!(can_see_debug_sql(&saas_ctx(true, false)));
    }

    #[test]
    fn saas_non_team_member_denied() {
        assert!(!can_see_debug_sql(&saas_ctx(false, false)));
    }

    #[test]
    fn saas_admin_non_team_member_denied() {
        assert!(!can_see_debug_sql(&saas_ctx(false, true)));
    }

    #[test]
    fn saas_admin_team_member_allowed() {
        assert!(can_see_debug_sql(&saas_ctx(true, true)));
    }

    #[test]
    fn self_managed_admin_allowed() {
        assert!(can_see_debug_sql(&self_managed_ctx(true)));
    }

    #[test]
    fn self_managed_non_admin_denied() {
        assert!(!can_see_debug_sql(&self_managed_ctx(false)));
    }

    #[test]
    fn dedicated_admin_allowed() {
        let sc = SecurityContext::new(1, vec!["1/".into()])
            .unwrap()
            .with_role(true, None)
            .with_realm(Some(compiler::Realm::Dedicated));
        assert!(can_see_debug_sql(&make_ctx(Some(sc))));
    }

    #[test]
    fn dedicated_non_admin_denied() {
        let sc = SecurityContext::new(1, vec!["1/".into()])
            .unwrap()
            .with_realm(Some(compiler::Realm::Dedicated));
        assert!(!can_see_debug_sql(&make_ctx(Some(sc))));
    }

    #[test]
    fn missing_realm_denied() {
        let sc = SecurityContext::new(1, vec!["1/".into()])
            .unwrap()
            .with_role(true, Some(50))
            .with_team_member(true);
        assert!(!can_see_debug_sql(&make_ctx(Some(sc))));
    }
}
