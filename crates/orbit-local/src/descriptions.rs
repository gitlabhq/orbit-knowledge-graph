//! Shared subcommand and MCP tool descriptions. `*_SHORT` feeds the CLI
//! `--help`; `*_MCP` is `concat!`-extended with an agent hint. The test below
//! holds the prefix relationship so the two cannot diverge.

pub(crate) const INDEX_SHORT: &str = "Index a code repository and output graph statistics as JSON";
pub(crate) const INDEX_MCP: &str = concat!(
    "Index a code repository and output graph statistics as JSON",
    "\n\n",
    "Pass a repository path, or a directory containing one or more repos; \
     each repo is indexed into the workspace DuckDB. Indexing is slow \
     (seconds to minutes) — call sparingly. `SELECT * FROM _orbit_manifest` \
     via `run_sql` shows what is already indexed."
);

pub(crate) const RUN_SQL_SHORT: &str = "Run a read-only SQL query against the local DuckDB graph";
pub(crate) const RUN_SQL_MCP: &str = concat!(
    "Run a read-only SQL query against the local DuckDB graph",
    "\n\n",
    "Accepts an array of DuckDB SQL statements; each runs sequentially and \
     its rows are returned as the element of the result array at the same \
     index. Call `get_graph_schema` first if you do not know the tables. \
     Large result sets are rejected — add `LIMIT` or narrow the projection."
);

pub(crate) const GET_SCHEMA_SHORT: &str = "Describe the schema of the local DuckDB graph";
pub(crate) const GET_SCHEMA_MCP: &str = concat!(
    "Describe the schema of the local DuckDB graph",
    "\n\n",
    "Returns JSON rows of (table_name, column_name, data_type) for every \
     user table in the workspace database."
);

pub(crate) const MCP_SERVE_SHORT: &str = "Serve the local graph to MCP-compatible AI agents";

pub(crate) const SKILL_SHORT: &str =
    "Print the bundled orbit-local skill content (SKILL.md or a file path)";

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mcp_descriptions_extend_their_short_form() {
        for (short, mcp) in [
            (INDEX_SHORT, INDEX_MCP),
            (RUN_SQL_SHORT, RUN_SQL_MCP),
            (GET_SCHEMA_SHORT, GET_SCHEMA_MCP),
        ] {
            assert!(
                mcp.starts_with(short),
                "MCP description must start with the shared short form.\n\
                 short: {short:?}\n\
                 mcp:   {mcp:?}"
            );
        }
    }
}
