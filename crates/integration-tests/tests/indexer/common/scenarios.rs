use async_trait::async_trait;
use integration_testkit::TestContext;
use integration_testkit::scenario::{CdcEvent, DispatchedMessage, ScenarioHandlers, Scope};

use super::handlers::{
    global_envelope, global_handler, handler_context, namespace_envelope, namespace_handler,
    system_notes_handler,
};

pub struct SdlcScenarioHandlers;

#[async_trait]
impl ScenarioHandlers for SdlcScenarioHandlers {
    async fn run(
        &self,
        ctx: &TestContext,
        handler: &str,
        scope: Option<Scope>,
        _cdc: &[CdcEvent],
    ) -> Vec<DispatchedMessage> {
        match handler {
            "namespace" => {
                let scope = require_scope(handler, scope);
                namespace_handler(ctx)
                    .await
                    .handle(
                        handler_context(),
                        namespace_envelope(scope.organization, scope.namespace),
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
                let scope = require_scope(handler, scope);
                system_notes_handler(ctx)
                    .await
                    .handle(
                        handler_context(),
                        namespace_envelope(scope.organization, scope.namespace),
                    )
                    .await
                    .unwrap();
            }
            other => panic!("unknown scenario handler '{other}'"),
        }
        Vec::new()
    }
}

fn require_scope(handler: &str, scope: Option<Scope>) -> Scope {
    scope.unwrap_or_else(|| panic!("the '{handler}' handler requires a scenario scope:"))
}
