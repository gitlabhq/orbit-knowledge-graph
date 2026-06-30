use async_trait::async_trait;
use indexer::orchestrator::scheduled::ScheduledTask;
use integration_testkit::TestContext;
use integration_testkit::scenario::{DispatchedMessage, HandlerInput, ScenarioHandlers, Scope};

use super::handlers::{
    global_envelope, global_handler, handler_context, namespace_envelope,
    namespace_envelope_with_targets, namespace_handler, stale_edge_task, system_notes_handler,
};

pub struct SdlcScenarioHandlers;

#[async_trait]
impl ScenarioHandlers for SdlcScenarioHandlers {
    async fn run(
        &self,
        ctx: &TestContext,
        handler: &str,
        input: HandlerInput<'_>,
    ) -> Vec<DispatchedMessage> {
        match handler {
            "namespace" => {
                let scope = require_scope(handler, input.scope);
                namespace_handler(ctx)
                    .await
                    .handle(
                        handler_context(),
                        namespace_envelope_with_targets(
                            scope.organization,
                            scope.namespace,
                            input.targets,
                        ),
                    )
                    .await
                    .unwrap();
            }
            "global" => {
                global_handler(ctx)
                    .await
                    .handle(handler_context(), global_envelope())
                    .await
                    .unwrap();
            }
            "system_notes" => {
                let scope = require_scope(handler, input.scope);
                system_notes_handler(ctx)
                    .await
                    .handle(
                        handler_context(),
                        namespace_envelope(scope.organization, scope.namespace),
                    )
                    .await
                    .unwrap();
            }
            "stale_edge_reconciliation" => stale_edge_task(ctx).run().await.unwrap(),
            other => panic!("unknown scenario handler '{other}'"),
        }
        Vec::new()
    }
}

fn require_scope(handler: &str, scope: Option<Scope>) -> Scope {
    scope.unwrap_or_else(|| panic!("the '{handler}' handler requires a scenario scope:"))
}
