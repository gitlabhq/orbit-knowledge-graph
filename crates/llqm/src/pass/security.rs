//! Security context injection pass.
//!
//! Walks the `Rel` tree and injects `startsWith(alias.traversal_path, path)`
//! predicates for namespace-scoped access control.
//!
//! Unlike the old pass (which uses `Expr::Raw` because Substrait's positional
//! resolution can't accept late-injected column references), this pass uses
//! `Expr::Column` directly — the plan stores expressions symbolically.
//!
//! Path filtering strategy:
//! - 1 path:  `startsWith(alias.traversal_path, path)`
//! - 2+ paths: `startsWith(LCP) AND (startsWith(p1) OR startsWith(p2) OR ...)`

use crate::ir::expr::{self, Expr};
use crate::ir::plan::Plan;

pub const GL_TABLE_PREFIX: &str = "gl_";
pub const TRAVERSAL_PATH_COLUMN: &str = "traversal_path";
pub const SKIP_TABLES: &[&str] = &["gl_user"];

#[derive(Debug, Clone)]
pub struct SecurityContext {
    pub org_id: i64,
    pub traversal_paths: Vec<String>,
}

impl SecurityContext {
    pub fn new(org_id: i64, traversal_paths: Vec<String>) -> Result<Self, SecurityError> {
        for path in &traversal_paths {
            validate_traversal_path(path, org_id)?;
        }
        Ok(Self {
            org_id,
            traversal_paths,
        })
    }
}

#[derive(Debug, thiserror::Error)]
pub enum SecurityError {
    #[error("invalid traversal_path format: '{0}' (expected pattern like '1/2/3/')")]
    InvalidPathFormat(String),

    #[error("traversal_path segment '{0}' exceeds i64 range")]
    SegmentOverflow(String),

    #[error("traversal_path '{path}' does not start with org_id {org_id}")]
    OrgMismatch { path: String, org_id: i64 },
}

pub struct SecurityPass {
    pub context: SecurityContext,
}

impl SecurityPass {
    pub fn transform(&self, mut plan: Plan) -> Result<Plan, SecurityError> {
        inject_filters(&mut plan, &self.context.traversal_paths);

        for cte in &mut plan.ctes {
            inject_filters(&mut cte.plan, &self.context.traversal_paths);
        }

        Ok(plan)
    }
}

fn inject_filters(plan: &mut Plan, paths: &[String]) {
    let aliases = plan.filterable_aliases(should_filter);
    if aliases.is_empty() {
        return;
    }
    let conds: Vec<Expr> = aliases
        .iter()
        .map(|(_, alias)| build_path_filter(alias, paths))
        .collect();
    plan.inject_filter(expr::and(conds));
}

fn should_filter(table: &str) -> bool {
    table.starts_with(GL_TABLE_PREFIX) && !SKIP_TABLES.contains(&table)
}

fn build_path_filter(alias: &str, paths: &[String]) -> Expr {
    match paths.len() {
        0 => expr::boolean(false),
        1 => starts_with_expr(alias, &paths[0]),
        _ => {
            let prefix = lowest_common_prefix(paths);
            let prefix_filter = starts_with_expr(alias, &prefix);
            let or_filters = expr::or(paths.iter().map(|p| starts_with_expr(alias, p)));
            prefix_filter.and(or_filters)
        }
    }
}

fn lowest_common_prefix(paths: &[String]) -> String {
    if paths.is_empty() {
        return String::new();
    }
    let segments: Vec<Vec<&str>> = paths
        .iter()
        .map(|p| p.trim_end_matches('/').split('/').collect())
        .collect();
    let first = &segments[0];
    let common_len = (0..first.len())
        .take_while(|&i| segments.iter().all(|s| s.get(i) == first.get(i)))
        .count();
    if common_len == 0 {
        String::new()
    } else {
        format!("{}/", first[..common_len].join("/"))
    }
}

/// Build `startsWith(alias.traversal_path, path)` using a proper `Expr::Column`.
fn starts_with_expr(alias: &str, path: &str) -> Expr {
    expr::col(alias, TRAVERSAL_PATH_COLUMN).starts_with(expr::string(path))
}

fn validate_traversal_path(path: &str, org_id: i64) -> Result<(), SecurityError> {
    let is_valid_format = !path.is_empty()
        && path.ends_with('/')
        && path[..path.len() - 1]
            .split('/')
            .all(|s| !s.is_empty() && s.chars().all(|c| c.is_ascii_digit()));

    if !is_valid_format {
        return Err(SecurityError::InvalidPathFormat(path.into()));
    }

    let segments: Vec<&str> = path.trim_end_matches('/').split('/').collect();
    for segment in &segments {
        segment
            .parse::<i64>()
            .map_err(|_| SecurityError::SegmentOverflow((*segment).into()))?;
    }

    let first: i64 = segments[0].parse().expect("validated above");
    if first != org_id {
        return Err(SecurityError::OrgMismatch {
            path: path.into(),
            org_id,
        });
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::clickhouse::emit_clickhouse_sql;
    use crate::ir::expr::*;
    use crate::ir::plan::Rel;

    fn make_plan(table: &str, alias: &str) -> Plan {
        Rel::read(
            table,
            alias,
            &[
                ("id", DataType::Int64),
                (TRAVERSAL_PATH_COLUMN, DataType::String),
            ],
        )
        .project(&[(col(alias, "id"), "id")])
        .into_plan()
    }

    fn emit_sql(plan: &Plan) -> String {
        emit_clickhouse_sql(plan).unwrap().sql
    }

    // -- Context validation --

    #[test]
    fn valid_paths() {
        assert!(SecurityContext::new(1, vec!["1/".into()]).is_ok());
        assert!(SecurityContext::new(1, vec!["1/2/3/".into()]).is_ok());
        assert!(SecurityContext::new(42, vec!["42/100/".into()]).is_ok());
    }

    #[test]
    fn org_mismatch() {
        assert!(SecurityContext::new(1, vec!["42/".into()]).is_err());
        assert!(SecurityContext::new(99, vec!["1/2/3/".into()]).is_err());
    }

    #[test]
    fn bad_format() {
        assert!(SecurityContext::new(1, vec!["1/2/3".into()]).is_err());
        assert!(SecurityContext::new(1, vec!["".into()]).is_err());
        assert!(SecurityContext::new(1, vec!["abc/".into()]).is_err());
        assert!(SecurityContext::new(1, vec!["1/abc/2/".into()]).is_err());
        assert!(SecurityContext::new(1, vec!["99999999999999999999999999999/".into()]).is_err());
        assert!(SecurityContext::new(1, vec!["-1/".into()]).is_err());
    }

    // -- Path filter expressions --

    #[test]
    fn single_path_uses_starts_with() {
        let e = build_path_filter("u", &["42/43/".into()]);
        assert!(
            matches!(e, Expr::FuncCall { ref name, .. } if name == "startsWith"),
            "expected startsWith, got: {e:?}"
        );
    }

    #[test]
    fn multiple_paths_uses_lcp_and_or() {
        let e = build_path_filter("u", &["1/2/4/".into(), "1/2/5/".into()]);
        assert!(
            matches!(
                e,
                Expr::BinaryOp {
                    op: BinaryOp::And,
                    ..
                }
            ),
            "expected AND, got: {e:?}"
        );
    }

    #[test]
    fn lowest_common_prefix_cases() {
        assert_eq!(
            lowest_common_prefix(&["1/2/4/".into(), "1/2/5/".into()]),
            "1/2/"
        );
        assert_eq!(lowest_common_prefix(&["1/2/".into(), "1/3/".into()]), "1/");
        assert_eq!(lowest_common_prefix(&["1/".into(), "2/".into()]), "");
        assert_eq!(lowest_common_prefix(&["42/".into()]), "42/");
    }

    // -- Uses Expr::Column, not Expr::Raw --

    #[test]
    fn filter_uses_column_not_raw() {
        let e = starts_with_expr("p", "42/");
        if let Expr::FuncCall { args, .. } = &e {
            assert!(
                matches!(&args[0], Expr::Column { table, name }
                    if table == "p" && name == TRAVERSAL_PATH_COLUMN),
                "expected Expr::Column, got: {:?}",
                args[0]
            );
        } else {
            panic!("expected FuncCall, got: {e:?}");
        }
    }

    // -- Plan injection --

    #[test]
    fn injects_filter_on_gl_table() {
        let ctx = SecurityContext::new(42, vec!["42/43/".into()]).unwrap();
        let pass = SecurityPass { context: ctx };
        let plan = pass.transform(make_plan("gl_project", "p")).unwrap();

        let sql = emit_sql(&plan);
        assert!(sql.contains("startsWith"), "sql: {sql}");
        assert!(sql.contains("p.traversal_path"), "sql: {sql}");
    }

    #[test]
    fn skips_user_table() {
        let ctx = SecurityContext::new(42, vec!["42/43/".into()]).unwrap();
        let pass = SecurityPass { context: ctx };
        let plan = pass.transform(make_plan("gl_user", "u")).unwrap();

        let sql = emit_sql(&plan);
        assert!(!sql.contains("startsWith"), "should skip gl_user: {sql}");
    }

    #[test]
    fn skips_non_gl_tables() {
        let ctx = SecurityContext::new(1, vec!["1/".into()]).unwrap();
        let pass = SecurityPass { context: ctx };
        let plan = pass.transform(make_plan("path_cte", "c")).unwrap();

        let sql = emit_sql(&plan);
        assert!(!sql.contains("startsWith"), "should skip non-gl: {sql}");
    }

    #[test]
    fn join_filters_both_gl_tables() {
        let ctx = SecurityContext::new(42, vec!["42/43/".into()]).unwrap();
        let pass = SecurityPass { context: ctx };

        let plan = Rel::read(
            "gl_project",
            "p",
            &[
                ("id", DataType::Int64),
                (TRAVERSAL_PATH_COLUMN, DataType::String),
            ],
        )
        .join(
            JoinType::Inner,
            Rel::read(
                "gl_merge_request",
                "mr",
                &[
                    ("id", DataType::Int64),
                    (TRAVERSAL_PATH_COLUMN, DataType::String),
                ],
            ),
            col("p", "id").eq(col("mr", "id")),
        )
        .project(&[(col("p", "id"), "id")])
        .into_plan();

        let plan = pass.transform(plan).unwrap();
        let sql = emit_sql(&plan);

        assert!(sql.contains("p.traversal_path"), "sql: {sql}");
        assert!(sql.contains("mr.traversal_path"), "sql: {sql}");
    }

    #[test]
    fn join_skips_user_in_mixed() {
        let plan = Rel::read(
            "gl_user",
            "u",
            &[
                ("id", DataType::Int64),
                (TRAVERSAL_PATH_COLUMN, DataType::String),
            ],
        )
        .join(
            JoinType::Inner,
            Rel::read(
                "gl_merge_request",
                "mr",
                &[
                    ("id", DataType::Int64),
                    (TRAVERSAL_PATH_COLUMN, DataType::String),
                ],
            ),
            col("u", "id").eq(col("mr", "id")),
        )
        .project(&[(col("u", "id"), "id")])
        .into_plan();

        let aliases = plan.filterable_aliases(should_filter);
        assert_eq!(aliases.len(), 1);
        assert_eq!(aliases[0].1, "mr");
    }

    #[test]
    fn cte_gets_filter() {
        let ctx = SecurityContext::new(42, vec!["42/43/".into()]).unwrap();
        let pass = SecurityPass { context: ctx };

        let cte_plan = Rel::read(
            "gl_project",
            "p",
            &[
                ("id", DataType::Int64),
                (TRAVERSAL_PATH_COLUMN, DataType::String),
            ],
        )
        .project(&[(col("p", "id"), "node_id")])
        .into_plan();

        let plan = Rel::read("base", "b", &[("node_id", DataType::Int64)])
            .project(&[(col("b", "node_id"), "node_id")])
            .into_plan_with_ctes(vec![crate::ir::plan::CteDef {
                name: "base".into(),
                plan: cte_plan,
                recursive: false,
            }]);

        let plan = pass.transform(plan).unwrap();
        let sql = emit_sql(&plan);
        assert!(
            sql.contains("startsWith"),
            "CTE should have startsWith: {sql}"
        );
        assert!(
            sql.contains("p.traversal_path"),
            "CTE should reference p.traversal_path: {sql}"
        );
    }

    #[test]
    fn multiple_paths_emits_lcp_optimization() {
        let ctx = SecurityContext::new(42, vec!["42/10/".into(), "42/20/".into()]).unwrap();
        let pass = SecurityPass { context: ctx };
        let plan = pass.transform(make_plan("gl_project", "p")).unwrap();

        let sql = emit_sql(&plan);
        // Should have both LCP check and individual path checks
        let starts_with_count = sql.matches("startsWith").count();
        assert!(
            starts_with_count >= 3,
            "expected 3+ startsWith (LCP + 2 paths), got {starts_with_count}: {sql}"
        );
    }
}
