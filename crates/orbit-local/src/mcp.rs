use anyhow::{Context, Result};
use arrow::array::RecordBatch;
use rmcp::{
    ErrorData, ServerHandler, ServiceExt,
    handler::server::{router::tool::ToolRouter, wrapper::Parameters},
    model::{
        CallToolResult, Content, Implementation, ProtocolVersion, ServerCapabilities, ServerInfo,
    },
    schemars, tool, tool_handler, tool_router,
    transport::stdio,
};
use serde::Deserialize;
use std::path::PathBuf;

use crate::{descriptions, index_collect, sql, sql_format};

const MAX_RESULT_ARROW_BYTES: usize = 1_000_000;

#[derive(Deserialize, schemars::JsonSchema)]
pub struct RunSqlArgs {
    /// One or more read-only SQL statements, executed in order. Each element
    /// produces one result set in the returned JSON array, at the same index.
    pub sql: Vec<String>,
    /// Optional override for the DuckDB file path. Defaults to the workspace
    /// database (`~/.orbit/graph.duckdb`).
    #[serde(default)]
    pub db: Option<PathBuf>,
}

#[derive(Deserialize, schemars::JsonSchema)]
pub struct GetGraphSchemaArgs {
    #[serde(default)]
    pub db: Option<PathBuf>,
}

#[derive(Deserialize, schemars::JsonSchema)]
pub struct IndexArgs {
    /// Filesystem path to a repository, or a directory containing one or more
    /// repositories. Each repo found is indexed into the workspace DuckDB.
    pub path: PathBuf,
    /// Worker threads. 0 (default) auto-detects from CPU cores.
    #[serde(default)]
    pub threads: usize,
    /// If true, include per-file skip/error breakdowns in the result.
    #[serde(default)]
    pub stats: bool,
    /// Optional override for the DuckDB file path. Defaults to the workspace
    /// database (`~/.orbit/graph.duckdb`).
    #[serde(default)]
    pub db: Option<PathBuf>,
}

#[derive(Clone)]
pub struct OrbitLocalServer {
    tool_router: ToolRouter<Self>,
}

fn mcp_description(name: &str) -> &'static str {
    match name {
        "run_sql" => descriptions::RUN_SQL_MCP,
        "get_graph_schema" => descriptions::GET_SCHEMA_MCP,
        "index" => descriptions::INDEX_MCP,
        other => unreachable!("tool `{other}` has no entry in `descriptions`"),
    }
}

async fn blocking_tool<F>(f: F) -> Result<CallToolResult, ErrorData>
where
    F: FnOnce() -> Result<String> + Send + 'static,
{
    let res = tokio::task::spawn_blocking(f)
        .await
        .map_err(|e| ErrorData::internal_error(format!("join error: {e}"), None))?;
    Ok(match res {
        Ok(json) => CallToolResult::success(vec![Content::text(json)]),
        Err(e) => CallToolResult::error(vec![Content::text(format!("{e:#}"))]),
    })
}

#[tool_router]
impl OrbitLocalServer {
    pub fn new() -> Self {
        let mut tool_router = ToolRouter::new();
        for mut route in Self::tool_router() {
            route.attr.description = Some(mcp_description(route.name()).into());
            tool_router.add_route(route);
        }
        Self { tool_router }
    }

    #[tool]
    async fn run_sql(
        &self,
        Parameters(args): Parameters<RunSqlArgs>,
    ) -> Result<CallToolResult, ErrorData> {
        blocking_tool(move || run_sql_impl(args)).await
    }

    #[tool]
    async fn get_graph_schema(
        &self,
        Parameters(args): Parameters<GetGraphSchemaArgs>,
    ) -> Result<CallToolResult, ErrorData> {
        blocking_tool(move || {
            batches_to_json(&sql::query(
                &sql::open_graph(args.db)?,
                sql::SCHEMA_INTROSPECTION_SQL,
            )?)
        })
        .await
    }

    #[tool]
    async fn index(
        &self,
        Parameters(args): Parameters<IndexArgs>,
    ) -> Result<CallToolResult, ErrorData> {
        blocking_tool(move || {
            let outputs = index_collect(args.path, args.threads, args.stats, args.db)?;
            serde_json::to_string_pretty(&outputs).context("failed to serialise index output")
        })
        .await
    }
}

impl Default for OrbitLocalServer {
    fn default() -> Self {
        Self::new()
    }
}

#[tool_handler(router = self.tool_router)]
impl ServerHandler for OrbitLocalServer {
    fn get_info(&self) -> ServerInfo {
        let mut info = ServerInfo::new(ServerCapabilities::builder().enable_tools().build())
            .with_server_info(Implementation::new("orbit-local", env!("ORBIT_VERSION")))
            .with_instructions(
                "Local code graph backed by DuckDB. Use `get_graph_schema` to learn \
                 the tables, then `run_sql` for read-only SQL (DuckDB dialect). \
                 `SELECT * FROM _orbit_manifest` lists the indexed repositories; \
                 use the `index` tool to add one."
                    .to_string(),
            );
        info.protocol_version = ProtocolVersion::V_2024_11_05;
        info
    }
}

fn batches_to_json(batches: &[RecordBatch]) -> Result<String> {
    let mut buf = Vec::new();
    sql_format::write_json(&mut buf, batches)?;
    let mut json = String::from_utf8(buf).context("query result was not valid UTF-8")?;
    json.truncate(json.trim_end().len());
    Ok(json)
}

fn run_sql_impl(args: RunSqlArgs) -> Result<String> {
    if args.sql.is_empty() {
        anyhow::bail!("`sql` must contain at least one statement");
    }
    let client = sql::open_graph(args.db)?;

    let mut arrow_bytes = 0usize;
    let mut results = Vec::with_capacity(args.sql.len());
    for (i, stmt) in args.sql.iter().enumerate() {
        let batches = sql::query(&client, stmt.trim()).with_context(|| format!("statement {i}"))?;
        arrow_bytes += batches
            .iter()
            .map(RecordBatch::get_array_memory_size)
            .sum::<usize>();
        if arrow_bytes > MAX_RESULT_ARROW_BYTES {
            anyhow::bail!(
                "combined result size exceeds {MAX_RESULT_ARROW_BYTES} bytes (Arrow \
                 in-memory estimate: {arrow_bytes}). Add LIMIT or narrow the projection."
            );
        }
        results.push(batches_to_json(&batches)?);
    }
    Ok(format!("[{}]", results.join(",")))
}

pub async fn serve() -> Result<()> {
    let service = OrbitLocalServer::new()
        .serve(stdio())
        .await
        .context("failed to start MCP stdio server")?;
    service
        .waiting()
        .await
        .context("MCP service exited with error")?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tools_carry_shared_descriptions() {
        let tools = OrbitLocalServer::new().tool_router.list_all();
        assert_eq!(tools.len(), 3);
        for tool in &tools {
            assert_eq!(
                tool.description.as_deref(),
                Some(mcp_description(&tool.name))
            );
        }
    }
}
