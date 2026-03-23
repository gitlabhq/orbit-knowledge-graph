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

/// Split SQL text into individual statements on `;` boundaries,
/// ignoring semicolons inside single-quoted string literals.
/// ClickHouse escapes quotes by doubling: `''`.
/// Leading `-- ...` comment lines are stripped before parsing.
fn split_statements(sql: &str) -> Vec<String> {
    let comment_re = Regex::new(r"(?m)^--.*$").unwrap();
    let stripped = comment_re.replace_all(sql, "");

    let mut statements = Vec::new();
    let mut current = String::new();
    let mut in_quote = false;
    let mut chars = stripped.chars().peekable();

    while let Some(ch) = chars.next() {
        match ch {
            '\'' => {
                current.push(ch);
                if in_quote && chars.peek() == Some(&'\'') {
                    current.push(chars.next().unwrap());
                } else {
                    in_quote = !in_quote;
                }
            }
            ';' if !in_quote => {
                let trimmed = current.trim().to_string();
                if !trimmed.is_empty() {
                    statements.push(trimmed);
                }
                current.clear();
            }
            _ => current.push(ch),
        }
    }

    let trimmed = current.trim().to_string();
    if !trimmed.is_empty() {
        statements.push(trimmed);
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
