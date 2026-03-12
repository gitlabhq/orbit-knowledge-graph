use std::collections::HashSet;

use query_engine::compile;

use crate::error::{Result, RunnerError, assert_eq_result};
use crate::register_command;
use crate::runner::{
    TestState, collect_trailing_ids, execute_sql_statements, require_string_arg, resolve_alias,
};
use integration_testkit::mock_redaction::MockRedactionService;

// ─────────────────────────────────────────────────────────────────────────────
// Registry
// ─────────────────────────────────────────────────────────────────────────────

register_command!("query", cmd_query);
register_command!("extra-sql", cmd_extra_sql);
register_command!("redact", cmd_redact);
register_command!("count", cmd_count);
register_command!("assert-ids", cmd_assert_ids);
register_command!("sql-contains", cmd_sql_contains);
register_command!("sql-not-contains", cmd_sql_not_contains);

// ─────────────────────────────────────────────────────────────────────────────
// Action commands
// ─────────────────────────────────────────────────────────────────────────────

async fn cmd_query(state: &mut TestState, node: &kdl::KdlNode) -> Result {
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

    let compiled = result.map_err(|e| RunnerError::CompileError(e.to_string()))?;
    let batches = state.ctx.query_parameterized(&compiled.base).await;
    let query_result =
        gkg_server::redaction::QueryResult::from_batches(&batches, &compiled.base.result_context);

    state.compiled = Some(compiled);
    state.result = Some(query_result);
    Ok(())
}

async fn cmd_extra_sql(state: &mut TestState, node: &kdl::KdlNode) -> Result {
    let sql = require_string_arg(node, 0)?;
    execute_sql_statements(&state.ctx, sql).await;
    Ok(())
}

/// Handles all redaction subcommands:
///   redact allow "entity" id1 id2 ...   — authorize IDs
///   redact deny "entity" id1 id2 ...    — deny IDs
///   redact reset                         — clear mock service
///   redact                               — execute redaction
async fn cmd_redact(state: &mut TestState, node: &kdl::KdlNode) -> Result {
    match node.get(0).and_then(|v| v.as_string()) {
        Some("allow") => {
            let (entity, ids) = parse_entity_ids(node, 1)?;
            state.mock_service.allow(&entity, &ids);
        }
        Some("deny") => {
            let (entity, ids) = parse_entity_ids(node, 1)?;
            state.mock_service.deny(&entity, &ids);
        }
        Some("reset") => {
            state.mock_service = MockRedactionService::new();
        }
        Some("run") => {
            let result = state
                .result
                .as_mut()
                .ok_or_else(|| RunnerError::StateError("`query` must run first".into()))?;

            let checks = result.resource_checks();
            let authorizations = state.mock_service.check(&checks);
            let redacted = result.apply_authorizations(&authorizations);
            state.last_redacted_count = Some(redacted);
        }
        Some(other) => {
            return Err(RunnerError::MissingArg(format!(
                "`redact` subcommand must be `allow`, `deny`, `reset`, or `run`, got `{other}`"
            )));
        }
        None => {
            return Err(RunnerError::MissingArg(
                "`redact` requires a subcommand: `allow`, `deny`, `reset`, or `run`".into(),
            ));
        }
    }
    Ok(())
}

// ─────────────────────────────────────────────────────────────────────────────
// Assertion commands
// ─────────────────────────────────────────────────────────────────────────────

async fn cmd_count(state: &mut TestState, node: &kdl::KdlNode) -> Result {
    if let Some(expected) = node.get("raw").and_then(|v| v.as_integer()) {
        assert_eq_result(state.result()?.len(), expected as usize, "count raw")?;
    }
    if let Some(expected) = node.get("authorized").and_then(|v| v.as_integer()) {
        assert_eq_result(
            state.result()?.authorized_count(),
            expected as usize,
            "count authorized",
        )?;
    }
    if let Some(expected) = node.get("redacted").and_then(|v| v.as_integer()) {
        let actual = state.last_redacted_count.ok_or_else(|| {
            RunnerError::StateError("`redact` must run before checking redacted count".into())
        })?;
        assert_eq_result(actual, expected as usize, "count redacted")?;
    }
    Ok(())
}

async fn cmd_assert_ids(state: &mut TestState, node: &kdl::KdlNode) -> Result {
    let mode = require_string_arg(node, 0)?;
    let alias = require_string_arg(node, 1)?.to_string();
    let expected_ids: HashSet<i64> = collect_trailing_ids(node, 2).into_iter().collect();
    let result = state.result()?;
    let node_ref = resolve_alias(result, &alias)?;

    match mode {
        "raw" => {
            let actual: HashSet<i64> =
                result.iter().filter_map(|r| r.get_id(&node_ref)).collect();
            if actual != expected_ids {
                return Err(RunnerError::AssertionFailed(format!(
                    "assert-ids raw `{alias}`: expected {expected_ids:?}, got {actual:?}"
                )));
            }
        }
        "authorized" => {
            let actual: HashSet<i64> = result
                .authorized_rows()
                .filter_map(|r| r.get_id(&node_ref))
                .collect();
            if actual != expected_ids {
                return Err(RunnerError::AssertionFailed(format!(
                    "assert-ids authorized `{alias}`: expected {expected_ids:?}, got {actual:?}"
                )));
            }
        }
        "denied" => {
            let authorized: HashSet<i64> = result
                .authorized_rows()
                .filter_map(|r| r.get_id(&node_ref))
                .collect();
            for id in &expected_ids {
                if authorized.contains(id) {
                    return Err(RunnerError::AssertionFailed(format!(
                        "assert-ids denied `{alias}`: id {id} should NOT be in authorized rows"
                    )));
                }
            }
        }
        other => {
            return Err(RunnerError::MissingArg(format!(
                "`assert-ids` mode must be `raw`, `authorized`, or `denied`, got `{other}`"
            )));
        }
    }
    Ok(())
}

async fn cmd_sql_contains(state: &mut TestState, node: &kdl::KdlNode) -> Result {
    assert_sql(state, node, true)
}

async fn cmd_sql_not_contains(state: &mut TestState, node: &kdl::KdlNode) -> Result {
    assert_sql(state, node, false)
}

// ─────────────────────────────────────────────────────────────────────────────
// Shared helpers
// ─────────────────────────────────────────────────────────────────────────────

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

/// Parse entity type and IDs starting at the given offset.
/// e.g. for `redact allow "user" 1 2 3`, offset=1 reads "user" at pos 1 and IDs from pos 2.
fn parse_entity_ids(node: &kdl::KdlNode, offset: usize) -> Result<(String, Vec<i64>)> {
    let cmd = node.name().value();
    let entity = node
        .get(offset)
        .and_then(|v| v.as_string())
        .ok_or_else(|| {
            RunnerError::MissingArg(format!(
                "`{cmd}` requires an entity type at position {offset}"
            ))
        })?
        .to_string();
    let ids = collect_trailing_ids(node, offset + 1);

    if ids.is_empty() {
        return Err(RunnerError::MissingArg(format!(
            "`{cmd}` requires at least one ID after the entity type"
        )));
    }

    Ok((entity, ids))
}
