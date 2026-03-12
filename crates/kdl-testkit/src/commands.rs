use std::collections::HashSet;

use query_engine::compile;

use crate::error::{Result, RunnerError, assert_eq_result};
use crate::register_command;
use integration_testkit::mock_redaction::MockRedactionService;
use crate::runner::{
    TestState, collect_trailing_ids, execute_sql_statements, require_int_arg, require_string_arg,
    resolve_alias,
};

// ─────────────────────────────────────────────────────────────────────────────
// Registry
// ─────────────────────────────────────────────────────────────────────────────

register_command!("extra-sql", cmd_extra_sql);
register_command!("compile", cmd_compile);
register_command!("execute", cmd_execute);
register_command!("allow", cmd_allow);
register_command!("deny", cmd_deny);
register_command!("redact", cmd_redact);
register_command!("reset-redaction", cmd_reset_redaction);
register_command!("authorized-ids", cmd_authorized_ids);
register_command!("raw-ids", cmd_raw_ids);
register_command!("denied-ids", cmd_denied_ids);
register_command!("authorized-count", cmd_authorized_count);
register_command!("raw-count", cmd_raw_count);
register_command!("sql-contains", cmd_sql_contains);
register_command!("sql-not-contains", cmd_sql_not_contains);

// ─────────────────────────────────────────────────────────────────────────────
// Commands
// ─────────────────────────────────────────────────────────────────────────────

async fn cmd_extra_sql(state: &mut TestState, node: &kdl::KdlNode) -> Result {
    let sql = require_string_arg(node, 0)?;
    execute_sql_statements(&state.ctx, sql).await;
    Ok(())
}

async fn cmd_compile(state: &mut TestState, node: &kdl::KdlNode) -> Result {
    let json = require_string_arg(node, 0)?;

    let expect_error = node
        .get("expect")
        .and_then(|v| v.as_string())
        .is_some_and(|s| s == "error");

    let result = compile(json, &state.ontology, &state.security_ctx);

    if expect_error {
        return match result {
            Err(_) => Ok(()),
            Ok(_) => Err(RunnerError::ExpectedCompileError),
        };
    }

    state.compiled = Some(result.map_err(|e| RunnerError::CompileError(e.to_string()))?);
    Ok(())
}

async fn cmd_execute(state: &mut TestState, node: &kdl::KdlNode) -> Result {
    let compiled = state.compiled()?;
    let batches = state.ctx.query_parameterized(&compiled.base).await;
    let result =
        gkg_server::redaction::QueryResult::from_batches(&batches, &compiled.base.result_context);

    if let Some(expected) = node.get("expect").and_then(|v| v.as_integer()) {
        assert_eq_result(result.len(), expected as usize, "execute row count")?;
    }

    state.result = Some(result);
    Ok(())
}

async fn cmd_allow(state: &mut TestState, node: &kdl::KdlNode) -> Result {
    let (entity, ids) = parse_entity_ids(node)?;
    state.mock_service.allow(&entity, &ids);
    Ok(())
}

async fn cmd_deny(state: &mut TestState, node: &kdl::KdlNode) -> Result {
    let (entity, ids) = parse_entity_ids(node)?;
    state.mock_service.deny(&entity, &ids);
    Ok(())
}

async fn cmd_redact(state: &mut TestState, node: &kdl::KdlNode) -> Result {
    let result = state
        .result
        .as_mut()
        .ok_or_else(|| RunnerError::StateError("`execute` must run first".into()))?;

    let checks = result.resource_checks();
    let authorizations = state.mock_service.check(&checks);
    let redacted = result.apply_authorizations(&authorizations);

    if let Some(expected) = node.get("expect").and_then(|v| v.as_integer()) {
        assert_eq_result(redacted, expected as usize, "redacted count")?;
    }
    Ok(())
}

async fn cmd_reset_redaction(state: &mut TestState, _node: &kdl::KdlNode) -> Result {
    state.mock_service = MockRedactionService::new();
    Ok(())
}

// ─────────────────────────────────────────────────────────────────────────────
// Assertion commands
// ─────────────────────────────────────────────────────────────────────────────

async fn cmd_authorized_ids(state: &mut TestState, node: &kdl::KdlNode) -> Result {
    assert_ids(state, node, true)
}

async fn cmd_raw_ids(state: &mut TestState, node: &kdl::KdlNode) -> Result {
    assert_ids(state, node, false)
}

async fn cmd_denied_ids(state: &mut TestState, node: &kdl::KdlNode) -> Result {
    let cmd = node.name().value();
    let (alias, denied_ids) = parse_alias_ids(node)?;
    let result = state.result()?;
    let node_ref = resolve_alias(result, &alias)?;

    let actual: HashSet<i64> = result
        .authorized_rows()
        .filter_map(|r| r.get_id(&node_ref))
        .collect();

    for id in denied_ids {
        if actual.contains(&id) {
            return Err(RunnerError::AssertionFailed(format!(
                "{cmd} `{alias}`: id {id} should NOT be in authorized rows"
            )));
        }
    }
    Ok(())
}

async fn cmd_authorized_count(state: &mut TestState, node: &kdl::KdlNode) -> Result {
    let expected = require_int_arg(node, 0)?;
    assert_eq_result(
        state.result()?.authorized_count(),
        expected as usize,
        "authorized-count",
    )
}

async fn cmd_raw_count(state: &mut TestState, node: &kdl::KdlNode) -> Result {
    let expected = require_int_arg(node, 0)?;
    assert_eq_result(state.result()?.len(), expected as usize, "raw-count")
}

async fn cmd_sql_contains(state: &mut TestState, node: &kdl::KdlNode) -> Result {
    assert_sql(state, node, true)
}

async fn cmd_sql_not_contains(state: &mut TestState, node: &kdl::KdlNode) -> Result {
    assert_sql(state, node, false)
}

// ─────────────────────────────────────────────────────────────────────────────
// Shared assertion logic
// ─────────────────────────────────────────────────────────────────────────────

fn assert_ids(state: &TestState, node: &kdl::KdlNode, authorized_only: bool) -> Result {
    let cmd = node.name().value();
    let (alias, expected_ids) = parse_alias_ids(node)?;
    let result = state.result()?;
    let node_ref = resolve_alias(result, &alias)?;

    let actual: HashSet<i64> = if authorized_only {
        result
            .authorized_rows()
            .filter_map(|r| r.get_id(&node_ref))
            .collect()
    } else {
        result.iter().filter_map(|r| r.get_id(&node_ref)).collect()
    };
    let expected: HashSet<i64> = expected_ids.into_iter().collect();

    if actual != expected {
        return Err(RunnerError::AssertionFailed(format!(
            "{cmd} `{alias}`: expected {expected:?}, got {actual:?}"
        )));
    }
    Ok(())
}

fn assert_sql(state: &TestState, node: &kdl::KdlNode, expect_present: bool) -> Result {
    let cmd = node.name().value();
    let fragment = require_string_arg(node, 0)?;
    let sql = &state.compiled()?.base.sql;
    let found = sql.contains(fragment);

    if found != expect_present {
        let verb = if expect_present {
            "not found in"
        } else {
            "unexpectedly found in"
        };
        return Err(RunnerError::AssertionFailed(format!(
            "{cmd}: `{fragment}` {verb} SQL:\n{sql}"
        )));
    }
    Ok(())
}

// ─────────────────────────────────────────────────────────────────────────────
// Parsing helpers
// ─────────────────────────────────────────────────────────────────────────────

fn parse_entity_ids(node: &kdl::KdlNode) -> Result<(String, Vec<i64>)> {
    let cmd = node.name().value();
    let entity = require_string_arg(node, 0)?.to_string();
    let ids = collect_trailing_ids(node, 1);

    if ids.is_empty() {
        return Err(RunnerError::MissingArg(format!(
            "`{cmd}` requires at least one ID after the entity type"
        )));
    }

    Ok((entity, ids))
}

fn parse_alias_ids(node: &kdl::KdlNode) -> Result<(String, Vec<i64>)> {
    let alias = require_string_arg(node, 0)?.to_string();
    Ok((alias, collect_trailing_ids(node, 1)))
}
