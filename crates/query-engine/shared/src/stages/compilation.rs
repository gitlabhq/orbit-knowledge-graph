use std::marker::PhantomData;
use std::sync::Arc;
use std::time::Instant;

use compiler::{CompileSource, CompileTarget};
use pipeline::{PipelineError, PipelineObserver, PipelineStage, QueryPipelineContext};

pub struct CompilationStage<M>(PhantomData<fn() -> M>);

impl<M> CompilationStage<M> {
    pub const fn new() -> Self {
        Self(PhantomData)
    }
}

impl<M> Default for CompilationStage<M> {
    fn default() -> Self {
        Self::new()
    }
}

impl<M: CompileTarget + Send + Sync + 'static> PipelineStage for CompilationStage<M> {
    type Input = ();
    type Output = ();

    async fn execute(
        &self,
        ctx: &mut QueryPipelineContext,
        obs: &mut dyn PipelineObserver,
    ) -> Result<Self::Output, PipelineError> {
        let t = Instant::now();
        let data_model = ctx
            .server_extensions
            .get::<Arc<M>>()
            .ok_or_else(|| PipelineError::custom("query data model missing"))
            .inspect_err(|e| obs.record_error(e))?;
        let source = match ctx.phases.get::<compiler::gql::ParsedQuery>() {
            Some(query) => CompileSource::Parsed(Box::new(query.clone())),
            None => CompileSource::Raw(&ctx.query_json),
        };
        let security = if M::REQUIRES_SECURITY {
            Some(
                ctx.security_context()
                    .inspect_err(|e| obs.record_error(e))?,
            )
        } else {
            ctx.security_context.as_ref()
        };
        let compiled = M::compile(source, ctx.frontend, data_model, security)
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
    use super::*;
    use compiler::{Frontend, SqlDialect};
    use ontology::Ontology;
    use pipeline::NoOpObserver;
    use query_data_model::{ClickHouseDataModel, DuckDbDataModel};

    fn context(query: &str) -> QueryPipelineContext {
        QueryPipelineContext {
            frontend: Frontend::JsonDsl,
            query_json: query.to_string(),
            compiled: None,
            ontology: Arc::new(Ontology::load_embedded().unwrap()),
            security_context: None,
            server_extensions: Default::default(),
            phases: Default::default(),
        }
    }

    const QUERY: &str = r#"{"query_type":"traversal","nodes":[{"id":"d","entity":"Definition","node_ids":[1],"columns":["name"]}]}"#;

    #[tokio::test]
    async fn the_data_model_in_extensions_selects_the_compile_target() {
        let mut ctx = context(QUERY);
        let duckdb = compiler::data_model::duckdb(Arc::clone(&ctx.ontology)).unwrap();
        ctx.server_extensions.insert(duckdb);

        CompilationStage::<DuckDbDataModel>::new()
            .execute(&mut ctx, &mut NoOpObserver)
            .await
            .unwrap();

        assert_eq!(ctx.compiled().unwrap().base.dialect, SqlDialect::DuckDb);
    }

    #[tokio::test]
    async fn a_stage_without_its_data_model_fails() {
        let mut ctx = context(QUERY);

        let error = CompilationStage::<ClickHouseDataModel>::new()
            .execute(&mut ctx, &mut NoOpObserver)
            .await
            .unwrap_err();

        assert!(error.to_string().contains("query data model missing"));
    }

    #[tokio::test]
    async fn a_remote_stage_without_a_security_context_reports_a_security_error() {
        let mut ctx = context(QUERY);
        let clickhouse = compiler::data_model::clickhouse(Arc::clone(&ctx.ontology)).unwrap();
        ctx.server_extensions.insert(clickhouse);

        let error = CompilationStage::<ClickHouseDataModel>::new()
            .execute(&mut ctx, &mut NoOpObserver)
            .await
            .unwrap_err();

        assert!(matches!(error, PipelineError::Security(_)), "{error}");
    }
}
