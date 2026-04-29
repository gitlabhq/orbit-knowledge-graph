use crate::TABLE_PREFIX;
use crate::context::TestContext;
use sqlparser::ast::Statement;
use sqlparser::dialect::ClickHouseDialect;
use sqlparser::parser::Parser;

const SEED_DIR: &str = env!("SEEDS_DIR");

pub async fn load_seed(ctx: &TestContext, name: &str) {
    let path = format!("{SEED_DIR}/{name}.sql");
    let sql = std::fs::read_to_string(&path)
        .unwrap_or_else(|e| panic!("seed '{name}' not found at {path}: {e}"));
    for stmt in split_sql_statements(&sql).unwrap() {
        let prefixed = prefix_graph_tables(&stmt, &TABLE_PREFIX);
        ctx.execute(&prefixed).await;
    }
}

/// Applies the schema version table prefix to graph table names in SQL.
/// Prefixes tables starting with `gl_`, plus `checkpoint`,
/// `namespace_deletion_schedule`, and `code_indexing_checkpoint`.
/// Siphon tables (`siphon_*`) and system tables are never prefixed.
fn prefix_graph_tables(sql: &str, prefix: &str) -> String {
    if prefix.is_empty() {
        return sql.to_string();
    }
    let mut result = sql.to_string();
    for aux in [
        "checkpoint",
        "namespace_deletion_schedule",
        "code_indexing_checkpoint",
    ] {
        result = result.replace(&format!(" {aux}"), &format!(" {prefix}{aux}"));
    }
    result = result.replace(" gl_", &format!(" {prefix}gl_"));
    result
}

/// Split a SQL seed file into individual statement strings, validated
/// by a real SQL parser. Fails on any parse error, empty input, or
/// non-INSERT statement.
pub fn split_sql_statements(sql: &str) -> Result<Vec<String>, SplitError> {
    let dialect = ClickHouseDialect {};
    let stmts = Parser::parse_sql(&dialect, sql).map_err(|e| SplitError::Parse(e.to_string()))?;

    if stmts.is_empty() {
        return Err(SplitError::EmptyInput);
    }

    stmts
        .into_iter()
        .enumerate()
        .map(|(i, stmt)| match &stmt {
            Statement::Insert(_) => Ok(stmt.to_string()),
            other => Err(SplitError::UnexpectedStatement {
                index: i,
                kind: format!("{other:?}")
                    .split('(')
                    .next()
                    .unwrap_or("Unknown")
                    .to_string(),
            }),
        })
        .collect()
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SplitError {
    Parse(String),
    EmptyInput,
    UnexpectedStatement { index: usize, kind: String },
}

impl std::fmt::Display for SplitError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Parse(e) => write!(f, "SQL parse error: {e}"),
            Self::EmptyInput => write!(f, "input contains no SQL statements"),
            Self::UnexpectedStatement { index, kind } => {
                write!(f, "statement {index} is {kind}, expected INSERT")
            }
        }
    }
}

impl std::error::Error for SplitError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn splits_basic_inserts() {
        let sql = "INSERT INTO t VALUES (1); INSERT INTO t VALUES (2);";
        let stmts = split_sql_statements(sql).unwrap();
        assert_eq!(stmts.len(), 2);
    }

    #[test]
    fn handles_escaped_quotes_and_injection() {
        let sql = "INSERT INTO t VALUES ('Robert'' DROP TABLE t --');";
        let stmts = split_sql_statements(sql).unwrap();
        assert_eq!(stmts.len(), 1);
    }

    #[test]
    fn rejects_non_insert() {
        let sql = "INSERT INTO t VALUES (1); SELECT 1;";
        let err = split_sql_statements(sql).unwrap_err();
        assert!(matches!(
            err,
            SplitError::UnexpectedStatement { index: 1, .. }
        ));
    }

    #[test]
    fn rejects_syntax_errors() {
        let sql = "INSERT INTO t VALUES ('unterminated;";
        assert!(matches!(
            split_sql_statements(sql).unwrap_err(),
            SplitError::Parse(_)
        ));
    }

    #[test]
    fn rejects_empty() {
        assert_eq!(
            split_sql_statements("").unwrap_err(),
            SplitError::EmptyInput
        );
    }

    #[test]
    fn splits_data_correctness_seed() {
        let path = format!("{}/data_correctness.sql", env!("SEEDS_DIR"));
        let seed = std::fs::read_to_string(&path).unwrap();
        let stmts = split_sql_statements(&seed).unwrap();
        assert_eq!(stmts.len(), 20);
    }
}
