//! Security context injection pass.
//!
//! Walks the Substrait plan tree and injects traversal-path predicates
//! to enforce namespace-scoped access control.
//!
//! Path filtering strategy:
//! - 1 path: `startsWith(alias.traversal_path, path)`
//! - 2+ paths: `startsWith(LCP) AND (startsWith(p1) OR startsWith(p2) OR ...)`

use crate::ir::expr::{self, Expr};
use crate::ir::plan::Plan;
use crate::pipeline::IrPass;

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

pub struct SecurityPass {
    pub context: SecurityContext,
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

impl IrPass for SecurityPass {
    type Error = SecurityError;

    fn transform(&self, mut plan: Plan) -> Result<Plan, Self::Error> {
        let aliases = plan.filterable_aliases(should_filter);
        if !aliases.is_empty() {
            let conds: Vec<Expr> = aliases
                .iter()
                .map(|(_, alias)| build_path_filter(alias, &self.context.traversal_paths))
                .collect();
            plan.inject_filter(expr::and(conds));
        }

        for cte in &mut plan.ctes {
            let cte_aliases = cte.plan.filterable_aliases(should_filter);
            if !cte_aliases.is_empty() {
                let conds: Vec<Expr> = cte_aliases
                    .iter()
                    .map(|(_, alias)| build_path_filter(alias, &self.context.traversal_paths))
                    .collect();
                cte.plan.inject_filter(expr::and(conds));
            }
        }

        Ok(plan)
    }
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
            expr::and([prefix_filter, or_filters])
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

fn starts_with_expr(alias: &str, path: &str) -> Expr {
    // Use raw() for the column reference since we're injecting into a
    // finalized plan — positional schema resolution doesn't apply here.
    expr::starts_with(
        expr::raw(&format!("{alias}.{TRAVERSAL_PATH_COLUMN}")),
        expr::string(path),
    )
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
    use crate::backend::clickhouse::ClickHouseBackend;
    use crate::ir::expr::*;
    use crate::ir::plan::PlanBuilder;
    use crate::pipeline::{Backend, IrPass};

    fn make_plan(table: &str, alias: &str) -> Plan {
        let mut b = PlanBuilder::new();
        let rel = b
            .read(
                table,
                alias,
                &[
                    ("id", DataType::Int64),
                    (TRAVERSAL_PATH_COLUMN, DataType::String),
                ],
            )
            .project(&mut b, &[(col(alias, "id"), "id")]);
        b.build(rel)
    }

    fn emit_sql(plan: &Plan) -> String {
        ClickHouseBackend.emit(plan).unwrap().sql
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
    fn single_path() {
        let e = build_path_filter("u", &["42/43/".into()]);
        assert!(matches!(e, Expr::FuncCall { ref name, .. } if name == "startsWith"));
    }

    #[test]
    fn multiple_paths() {
        let e = build_path_filter("u", &["1/2/4/".into(), "1/2/5/".into()]);
        assert!(matches!(
            e,
            Expr::BinaryOp {
                op: BinaryOp::And,
                ..
            }
        ));
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

    // -- Plan injection --

    #[test]
    fn injects_filter_on_gl_table() {
        let ctx = SecurityContext::new(42, vec!["42/43/".into()]).unwrap();
        let pass = SecurityPass { context: ctx };
        let plan = pass.transform(make_plan("gl_project", "p")).unwrap();

        let sql = emit_sql(&plan);
        assert!(sql.contains("startsWith"), "sql: {sql}");
        assert!(sql.contains("traversal_path"), "sql: {sql}");
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

        let mut b = PlanBuilder::new();
        let p = b.read(
            "gl_project",
            "p",
            &[
                ("id", DataType::Int64),
                (TRAVERSAL_PATH_COLUMN, DataType::String),
            ],
        );
        let mr = b.read(
            "gl_merge_request",
            "mr",
            &[
                ("id", DataType::Int64),
                (TRAVERSAL_PATH_COLUMN, DataType::String),
            ],
        );
        let joined = p
            .join(
                &mut b,
                JoinType::Inner,
                mr,
                eq(col("p", "id"), col("mr", "id")),
            )
            .project(&mut b, &[(col("p", "id"), "id")]);
        let plan = b.build(joined);

        let plan = pass.transform(plan).unwrap();
        let sql = emit_sql(&plan);

        // Both p and mr should get startsWith filters
        assert!(sql.contains("p.traversal_path"), "sql: {sql}");
        assert!(sql.contains("mr.traversal_path"), "sql: {sql}");
    }

    #[test]
    fn join_skips_user_in_mixed() {
        let mut b = PlanBuilder::new();
        let u = b.read(
            "gl_user",
            "u",
            &[
                ("id", DataType::Int64),
                (TRAVERSAL_PATH_COLUMN, DataType::String),
            ],
        );
        let mr = b.read(
            "gl_merge_request",
            "mr",
            &[
                ("id", DataType::Int64),
                (TRAVERSAL_PATH_COLUMN, DataType::String),
            ],
        );
        let joined = u
            .join(
                &mut b,
                JoinType::Inner,
                mr,
                eq(col("u", "id"), col("mr", "id")),
            )
            .project(&mut b, &[(col("u", "id"), "id")]);
        let plan = b.build(joined);

        let aliases = plan.filterable_aliases(should_filter);
        assert_eq!(aliases.len(), 1);
        assert_eq!(aliases[0].1, "mr");
    }

    #[test]
    fn end_to_end_via_pipeline() {
        use crate::pipeline::{Frontend, Pipeline};

        struct TestFrontend;
        impl Frontend for TestFrontend {
            type Input = ();
            type Error = SecurityError;
            fn lower(&self, _: ()) -> Result<Plan, Self::Error> {
                Ok(make_plan("gl_project", "p"))
            }
        }

        let ctx = SecurityContext::new(42, vec!["42/43/".into()]).unwrap();
        let pass = SecurityPass { context: ctx };

        let pq = Pipeline::new()
            .input(TestFrontend, ())
            .lower()
            .unwrap()
            .pass(&pass)
            .unwrap()
            .emit(&ClickHouseBackend)
            .unwrap()
            .finish();

        assert!(pq.sql.contains("startsWith"), "sql: {}", pq.sql);
    }
}
