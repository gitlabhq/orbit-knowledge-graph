use std::sync::Arc;
use std::time::Instant;

use pipeline::{PipelineError, PipelineObserver, PipelineStage, QueryPipelineContext};

#[derive(Clone)]
pub struct CompilationStage;

impl PipelineStage for CompilationStage {
    type Input = ();
    type Output = ();

    async fn execute(
        &self,
        ctx: &mut QueryPipelineContext,
        obs: &mut dyn PipelineObserver,
    ) -> Result<Self::Output, PipelineError> {
        let t = Instant::now();
        let ontology = &ctx.ontology;
        let security_context = ctx
            .security_context()
            .inspect_err(|e| obs.record_error(e))?;

        let compiled = compiler::compile(&ctx.query_json, ctx.frontend, ontology, security_context)
            .map_err(|e| PipelineError::Compile {
                client_safe: e.is_client_safe(),
                message: e.to_string(),
            })
            .inspect_err(|e| obs.record_error(e))?;

        let query_type: &str = compiled.query_type.into();
        obs.set_query_type(query_type);
        obs.set_compiled(&compiled);
        obs.compiled(t.elapsed());

        ctx.compiled = Some(Arc::new(compiled));
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use compiler::{Frontend, SecurityContext};
    use ontology::Ontology;
    use pipeline::{NoOpObserver, TypeMap};

    fn context(raw: &str, frontend: Frontend) -> QueryPipelineContext {
        QueryPipelineContext {
            query_json: raw.into(),
            frontend,
            compiled: None,
            ontology: Arc::new(Ontology::load_embedded().unwrap()),
            security_context: Some(SecurityContext::new(1, vec!["1/100/".into()]).unwrap()),
            server_extensions: TypeMap::default(),
            phases: TypeMap::default(),
        }
    }

    #[tokio::test]
    async fn compilation_stage_uses_selected_frontend() {
        let mut ctx = context("MATCH (p:Project {id: 42}) RETURN p LIMIT 1", Frontend::Gql);
        CompilationStage
            .execute(&mut ctx, &mut NoOpObserver)
            .await
            .unwrap();
        let sql = ctx.compiled().unwrap().base.render();
        assert!(
            sql.contains("startsWith(p.traversal_path, '1/100/')"),
            "{sql}"
        );

        let mut ctx = context(
            "MATCH (p:Project {id: 42}) RETURN p LIMIT 1",
            Frontend::JsonDsl,
        );
        let error = CompilationStage
            .execute(&mut ctx, &mut NoOpObserver)
            .await
            .unwrap_err();
        assert!(matches!(error, PipelineError::Compile { .. }), "{error}");
    }
}
