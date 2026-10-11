use ontology::introspection::SchemaResponse;
use query_engine::compiler::{Frontend, gql::RoutedStatement};
use query_engine::pipeline::{
    PipelineError, PipelineObserver, PipelineStage, QueryPipelineContext,
};

pub enum RoutingOutput {
    Query,
    Schema(SchemaResponse),
}

#[derive(Clone)]
pub struct RoutingStage;

impl PipelineStage for RoutingStage {
    type Input = ();
    type Output = RoutingOutput;

    async fn execute(
        &self,
        ctx: &mut QueryPipelineContext,
        obs: &mut dyn PipelineObserver,
    ) -> Result<Self::Output, PipelineError> {
        match ctx.frontend {
            Frontend::JsonDsl => Ok(RoutingOutput::Query),
            Frontend::Gql => match query_engine::compiler::gql::route(
                &ctx.query_json,
                &ctx.ontology,
                ontology::introspection::IntrospectionScope::All,
            )
            .map_err(PipelineError::from)
            .inspect_err(|error| obs.record_error(error))?
            {
                RoutedStatement::Query(query) => {
                    ctx.phases.insert(*query);
                    Ok(RoutingOutput::Query)
                }
                RoutedStatement::Schema(response) => Ok(RoutingOutput::Schema(response)),
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use ontology::Ontology;
    use query_engine::compiler::{CompiledQueryContext, SecurityContext, passes::cursor};
    use query_engine::pipeline::{NoOpObserver, TypeMap};
    use query_engine::shared::CompilationStage;

    const QUERY: &str =
        "MATCH (u:User) WHERE u.id >= 1 AND u.id <= 10000 RETURN u.username ORDER BY u.id";

    async fn route_and_compile(query: String) -> Result<Arc<CompiledQueryContext>, PipelineError> {
        let mut ctx = QueryPipelineContext {
            frontend: Frontend::Gql,
            query_json: query,
            compiled: None,
            ontology: Arc::new(Ontology::load_embedded().unwrap()),
            security_context: Some(SecurityContext::new(1, vec!["1/".into()]).unwrap()),
            server_extensions: TypeMap::default(),
            phases: TypeMap::default(),
        };
        let mut obs = NoOpObserver;
        RoutingStage.execute(&mut ctx, &mut obs).await?;
        CompilationStage.execute(&mut ctx, &mut obs).await?;
        Ok(ctx.compiled.expect("compiled query"))
    }

    #[tokio::test]
    async fn gql_page_after_uses_the_routed_query_hash() {
        let first = route_and_compile(format!("{QUERY} PAGE 2")).await.unwrap();
        let hash = first.pagination.query_hash;
        assert_ne!(hash, 0);

        let keys = vec![Some("2".to_owned()); first.pagination.key_count];
        let token = cursor::encode(hash, &keys);
        let second = route_and_compile(format!("{QUERY} PAGE 2 AFTER '{token}'"))
            .await
            .unwrap();

        assert_eq!(second.pagination.query_hash, hash);
        assert!(second.base.render().contains("u.id >"));
    }
}
