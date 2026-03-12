use std::path::Path;

use gkg_server::redaction::QueryResult;
use integration_testkit::TestContext;
use ontology::Ontology;
use query_engine::{CompiledQueryContext, RedactionNode, SecurityContext};

use crate::error::{LocatedError, Result, RunnerError, located};
use crate::registry::lookup;
use integration_testkit::mock_redaction::MockRedactionService;

pub use integration_testkit::{GRAPH_SCHEMA_SQL, SIPHON_SCHEMA_SQL};

pub const SEED_DIR: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/../../config/seeds");

// ─────────────────────────────────────────────────────────────────────────────
// Test state
// ─────────────────────────────────────────────────────────────────────────────

pub struct TestState {
    pub ctx: TestContext,
    pub ontology: Ontology,
    pub security_ctx: SecurityContext,
    pub compiled: Option<CompiledQueryContext>,
    pub result: Option<QueryResult>,
    pub mock_service: MockRedactionService,
}

impl TestState {
    fn new(ctx: TestContext) -> Self {
        Self {
            ctx,
            ontology: Ontology::load_embedded().expect("embedded ontology should load"),
            security_ctx: SecurityContext::new(1, vec!["1/".into()])
                .expect("valid security context"),
            compiled: None,
            result: None,
            mock_service: MockRedactionService::new(),
        }
    }

    pub fn compiled(&self) -> Result<&CompiledQueryContext> {
        self.compiled
            .as_ref()
            .ok_or_else(|| RunnerError::StateError("`compile` must run first".into()))
    }

    pub fn result(&self) -> Result<&QueryResult> {
        self.result
            .as_ref()
            .ok_or_else(|| RunnerError::StateError("`execute` must run first".into()))
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Public entry points
// ─────────────────────────────────────────────────────────────────────────────

/// Run a single `.kdl` test file against the given parent context.
/// Panics with a `LocatedError` on any failure so test runners get a clear message.
pub async fn run_kdl_test(parent_ctx: &TestContext, path: &Path) {
    if let Err(e) = run_kdl_test_inner(parent_ctx, path).await {
        panic!("{e}");
    }
}

async fn run_kdl_test_inner(
    parent_ctx: &TestContext,
    path: &Path,
) -> std::result::Result<(), LocatedError> {
    let display = path.display().to_string();

    let content = std::fs::read_to_string(path).map_err(|e| {
        located(
            &display,
            None,
            RunnerError::Io {
                path: display.clone(),
                source: e,
            },
        )
    })?;

    let doc: kdl::KdlDocument = content
        .parse()
        .map_err(|e: kdl::KdlError| located(&display, None, RunnerError::Parse(e.to_string())))?;

    let nodes = doc.nodes();
    if nodes.is_empty() {
        return Err(located(
            &display,
            None,
            RunnerError::Parse("empty KDL document".into()),
        ));
    }

    let first = &nodes[0];
    if first.name().value() != "test" {
        return Err(located(
            &display,
            None,
            RunnerError::Parse(format!(
                "first node must be `test`, got `{}`",
                first.name().value()
            )),
        ));
    }

    let test_name = require_string_arg(first, 0).map_err(|e| located(&display, Some("test"), e))?;

    let db_name = sanitize_db_name(test_name);
    let ctx = parent_ctx.fork(&db_name).await;
    let mut state = TestState::new(ctx);

    if let Some(seed) = first.get("seed").and_then(|v| v.as_string()) {
        load_seed(&state.ctx, seed)
            .await
            .map_err(|e| located(&display, Some("test"), e))?;
    }

    eprintln!("--- kdl: {test_name}");

    for node in &nodes[1..] {
        let cmd = node.name().value();
        let command = lookup(cmd).ok_or_else(|| {
            located(
                &display,
                Some(cmd),
                RunnerError::UnknownCommand(cmd.to_string()),
            )
        })?;
        (command.handler)(&mut state, node)
            .await
            .map_err(|e| located(&display, Some(cmd), e))?;
    }

    Ok(())
}

/// Discover and run all `.kdl` files in a directory against a shared testcontainer.
/// Each file gets a forked database. Panics on any failure.
pub async fn run_kdl_fixtures(fixtures_dir: &str) {
    let ctx = create_query_test_context().await;

    let pattern = format!("{fixtures_dir}/**/*.kdl");
    let paths: Vec<_> = glob::glob(&pattern)
        .unwrap_or_else(|e| panic!("invalid glob pattern: {e}"))
        .collect();

    assert!(
        !paths.is_empty(),
        "no .kdl fixture files found in {fixtures_dir}",
    );

    for entry in paths {
        let path = entry.unwrap_or_else(|e| panic!("glob error: {e}"));
        run_kdl_test(&ctx, &path).await;
    }
}

/// Parse all `.kdl` files in a directory and panic on any syntax error.
pub fn validate_kdl_fixtures(fixtures_dir: &str) {
    let pattern = format!("{fixtures_dir}/**/*.kdl");
    let paths: Vec<_> = glob::glob(&pattern)
        .unwrap_or_else(|e| panic!("invalid glob pattern `{pattern}`: {e}"))
        .collect();

    assert!(
        !paths.is_empty(),
        "no .kdl files found matching `{pattern}`"
    );

    for entry in paths {
        let path = entry.unwrap_or_else(|e| panic!("glob error: {e}"));
        let content = std::fs::read_to_string(&path)
            .unwrap_or_else(|e| panic!("{}: read failed: {e}", path.display()));
        let _doc: kdl::KdlDocument = content
            .parse()
            .unwrap_or_else(|e: kdl::KdlError| panic!("{}: {e}", path.display()));
    }
}

/// Create a `TestContext` with the schemas needed by the query pipeline.
pub async fn create_query_test_context() -> TestContext {
    TestContext::new(&[SIPHON_SCHEMA_SQL, GRAPH_SCHEMA_SQL]).await
}

// ─────────────────────────────────────────────────────────────────────────────
// Seed helpers
// ─────────────────────────────────────────────────────────────────────────────

pub async fn load_seed(ctx: &TestContext, name: &str) -> Result {
    let path = format!("{SEED_DIR}/{name}.sql");
    let sql = std::fs::read_to_string(&path).map_err(|e| RunnerError::SeedNotFound {
        name: name.to_string(),
        path: path.clone(),
        source: e,
    })?;
    execute_sql_statements(ctx, &sql).await;
    Ok(())
}

pub async fn execute_sql_statements(ctx: &TestContext, sql: &str) {
    for stmt in sql.split(';') {
        let stmt = stmt.trim();
        if stmt.is_empty() || stmt.starts_with("--") {
            continue;
        }
        ctx.execute(stmt).await;
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Argument parsing helpers — read command name from the node itself
// ─────────────────────────────────────────────────────────────────────────────

pub fn require_string_arg(node: &kdl::KdlNode, index: usize) -> Result<&str> {
    let cmd = node.name().value();
    node.get(index).and_then(|v| v.as_string()).ok_or_else(|| {
        RunnerError::MissingArg(format!("`{cmd}` requires a string at position {index}"))
    })
}

pub fn require_int_arg(node: &kdl::KdlNode, index: usize) -> Result<i128> {
    let cmd = node.name().value();
    node.get(index).and_then(|v| v.as_integer()).ok_or_else(|| {
        RunnerError::MissingArg(format!("`{cmd}` requires an integer at position {index}"))
    })
}

/// Collects trailing integer arguments starting at `from` (e.g. positions 1..N).
pub fn collect_trailing_ids(node: &kdl::KdlNode, from: usize) -> Vec<i64> {
    (from..64)
        .map_while(|i| node.get(i).and_then(|v| v.as_integer()).map(|n| n as i64))
        .collect()
}

pub fn resolve_alias(result: &QueryResult, alias: &str) -> Result<RedactionNode> {
    result.ctx().get(alias).cloned().ok_or_else(|| {
        RunnerError::StateError(format!("alias `{alias}` not found in result context"))
    })
}

fn sanitize_db_name(name: &str) -> String {
    let sanitized: String = name
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '_' {
                c
            } else {
                '_'
            }
        })
        .collect();
    format!("kdl_{sanitized}")
}
