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

        let data_model = ctx
            .server_extensions
            .get::<Arc<query_data_model::ClickHouseDataModel>>();
        let compiled = match ctx.phases.get::<compiler::gql::ParsedQuery>() {
            Some(query) => match data_model {
                Some(data_model) => {
                    compiler::gql::compile_query_model(query.clone(), data_model, security_context)
                }
                None => compiler::gql::compile_query(query.clone(), ontology, security_context),
            },
            None => match data_model {
                Some(data_model) => compiler::compile_model(
                    &ctx.query_json,
                    ctx.frontend,
                    data_model,
                    security_context,
                ),
                None => {
                    compiler::compile(&ctx.query_json, ctx.frontend, ontology, security_context)
                }
            },
        }
        .inspect_err(|e| obs.compiler_rejected(e))
        .map_err(PipelineError::from)
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
    use std::sync::Mutex;
    use std::time::Duration;

    use compiler::{Frontend, QueryError, RejectionReason, SecurityContext};
    use ontology::Ontology;

    use super::*;

    #[derive(Default)]
    struct RejectionLog(Mutex<Vec<RejectionReason>>);

    impl PipelineObserver for RejectionLog {
        fn set_query_type(&mut self, _query_type: &'static str) {}
        fn compiled(&mut self, _elapsed: Duration) {}
        fn executed(&mut self, _elapsed: Duration, _batch_count: usize) {}
        fn authorized(&mut self, _elapsed: Duration) {}
        fn hydrated(&mut self, _elapsed: Duration) {}
        fn record_error(&self, _error: &PipelineError) {}
        fn compiler_rejected(&self, error: &QueryError) {
            self.0.lock().unwrap().push(error.rejection_reason());
        }
        fn finish(&self, _row_count: usize, _redacted_count: usize) {}
    }

    fn context(query: &str, security_context: Option<SecurityContext>) -> QueryPipelineContext {
        QueryPipelineContext {
            frontend: Frontend::JsonDsl,
            query_json: query.to_string(),
            compiled: None,
            ontology: Arc::new(Ontology::load_embedded().unwrap()),
            security_context,
            server_extensions: Default::default(),
            phases: Default::default(),
        }
    }

    #[tokio::test]
    async fn compiler_rejections_reach_the_observer_with_their_reason() {
        let unknown_entity = r#"{"query_type":"traversal","nodes":[{"id":"x","entity":"NotARealEntity","columns":["id"]}],"limit":1}"#;
        for (query, expected) in [
            ("not a query", RejectionReason::Parse),
            (unknown_entity, RejectionReason::Ontology),
        ] {
            let security_context = SecurityContext::new(1, vec!["1/".into()]).unwrap();
            let mut ctx = context(query, Some(security_context));
            let obs = &mut RejectionLog::default();

            let error = CompilationStage.execute(&mut ctx, obs).await.unwrap_err();

            let PipelineError::Compile { reason, .. } = error else {
                panic!("expected a compile error, got {error:?}");
            };
            assert_eq!(reason, expected, "{query}");
            assert_eq!(*obs.0.lock().unwrap(), [expected], "{query}");
        }
    }

    #[tokio::test]
    async fn missing_security_context_is_not_a_compiler_rejection() {
        let mut ctx = context("not a query", None);
        let obs = &mut RejectionLog::default();

        let error = CompilationStage.execute(&mut ctx, obs).await.unwrap_err();

        assert!(matches!(error, PipelineError::Security(_)), "{error:?}");
        assert!(obs.0.lock().unwrap().is_empty());
    }
}
