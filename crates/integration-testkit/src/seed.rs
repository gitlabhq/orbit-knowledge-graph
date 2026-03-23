use crate::context::TestContext;
use regex::Regex;

const SEED_DIR: &str = env!("SEEDS_DIR");

pub async fn load_seed(ctx: &TestContext, name: &str) {
    let path = format!("{SEED_DIR}/{name}.sql");
    let sql = std::fs::read_to_string(&path)
        .unwrap_or_else(|e| panic!("seed '{name}' not found at {path}: {e}"));
    for statement in split_statements(&sql) {
        ctx.execute(&statement).await;
    }
}

/// Split SQL into statements on `;`, skipping quoted strings.
/// Comments (`-- ...`) are stripped first, then the comment-free SQL
/// is split on unquoted `;` boundaries.
fn split_statements(sql: &str) -> Vec<String> {
    // Pass 1: strip `-- ...` comments while preserving quoted strings.
    let strip_re = Regex::new(r"(?s)'(?:''|[^'])*'|--[^\n]*").unwrap();
    let stripped = strip_re.replace_all(sql, |caps: &regex::Captures| {
        let m = caps.get(0).unwrap().as_str();
        if m.starts_with('\'') {
            m.to_string()
        } else {
            String::new()
        }
    });

    // Pass 2: split on unquoted `;`.
    let split_re = Regex::new(r"(?s)'(?:''|[^'])*'|;").unwrap();
    let mut statements = Vec::new();
    let mut last = 0;

    for m in split_re.find_iter(&stripped) {
        if m.as_str() == ";" {
            let chunk = stripped[last..m.start()].trim();
            if !chunk.is_empty() {
                statements.push(chunk.to_string());
            }
            last = m.end();
        }
    }

    let tail = stripped[last..].trim();
    if !tail.is_empty() {
        statements.push(tail.to_string());
    }

    statements
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn splits_simple_statements() {
        let sql = "INSERT INTO t VALUES (1); INSERT INTO t VALUES (2);";
        let stmts = split_statements(sql);
        assert_eq!(stmts.len(), 2);
        assert!(stmts[0].starts_with("INSERT"));
        assert!(stmts[1].starts_with("INSERT"));
    }

    #[test]
    fn handles_semicolons_inside_quotes() {
        let sql = "INSERT INTO t VALUES ('a;b'); INSERT INTO t VALUES ('c')";
        let stmts = split_statements(sql);
        assert_eq!(stmts.len(), 2);
        assert!(stmts[0].contains("'a;b'"));
    }

    #[test]
    fn handles_escaped_quotes_with_semicolons() {
        let sql = "INSERT INTO t VALUES ('Robert''); DROP TABLE t;--'); SELECT 1";
        let stmts = split_statements(sql);
        assert_eq!(stmts.len(), 2);
        assert!(stmts[0].contains("DROP TABLE"));
        assert_eq!(stmts[1], "SELECT 1");
    }

    #[test]
    fn skips_empty_and_comment_only() {
        let sql = "-- comment\n\n; ; INSERT INTO t VALUES (1);";
        let stmts = split_statements(sql);
        assert_eq!(stmts.len(), 1);
    }

    #[test]
    fn strips_leading_comment_block() {
        let sql = "-- header\n-- line 2\n\nINSERT INTO t VALUES (1);\nINSERT INTO t VALUES (2);";
        let stmts = split_statements(sql);
        assert_eq!(stmts.len(), 2);
        assert!(stmts[0].starts_with("INSERT"));
    }
}
