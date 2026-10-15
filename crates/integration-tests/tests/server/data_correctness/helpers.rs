use integration_testkit::mock_redaction::MockRedactionService;
use integration_testkit::visitor::ResponseView;
use integration_testkit::{TestContext, query_scenario};
use query_engine::compiler::Frontend;

use crate::common::test_security_context;

pub(super) async fn run_query(ctx: &TestContext, query: &str) -> ResponseView {
    let mut redaction = MockRedactionService::new();
    redaction.allow("user", &[1, 2, 3, 4, 5, 6, 7]);
    redaction.allow("note", &[3000, 3001, 3002, 3003]);
    query_scenario::run_query(
        ctx,
        Frontend::JsonDsl,
        query,
        &test_security_context(),
        &redaction,
    )
    .await
}
