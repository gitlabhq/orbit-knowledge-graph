//! Post-compilation safety check.
//!
//! Walks the `Rel`/`Expr` tree to verify that every `gl_*` table alias
//! (except skip tables) has a `startsWith(alias.traversal_path, path)` predicate
//! whose path literal is consistent with the security context.
//!
//! Defense-in-depth — if the security pass worked, this always passes.
//! Unlike the old check pass that digs through Substrait protobuf function
//! anchors and `__raw_sql` wrappers, this one directly pattern-matches on
//! `Expr::Column` and `Expr::FuncCall`.

use super::security::{GL_TABLE_PREFIX, SKIP_TABLES, SecurityContext, TRAVERSAL_PATH_COLUMN};
use llqm::ir::expr::Expr;
use llqm::ir::plan::{Plan, Rel};

#[derive(Debug, thiserror::Error)]
pub enum CheckError {
    #[error("post-check failed: alias '{alias}' missing valid traversal_path filter")]
    MissingFilter { alias: String },
}

pub struct CheckPass {
    pub context: SecurityContext,
}

impl CheckPass {
    pub fn transform(&self, plan: &Plan) -> Result<(), CheckError> {
        check_plan(plan, &self.context)
    }
}

pub fn check_plan(plan: &Plan, ctx: &SecurityContext) -> Result<(), CheckError> {
    let aliases = plan.filterable_aliases(should_check);

    for (_, alias) in &aliases {
        if !rel_has_valid_filter(&plan.root, alias, ctx) {
            return Err(CheckError::MissingFilter {
                alias: alias.clone(),
            });
        }
    }

    for cte in &plan.ctes {
        check_plan(&cte.plan, ctx)?;
    }

    Ok(())
}

fn should_check(table: &str) -> bool {
    table.starts_with(GL_TABLE_PREFIX) && !SKIP_TABLES.contains(&table)
}

// ---------------------------------------------------------------------------
// Rel-tree walking
// ---------------------------------------------------------------------------

fn rel_has_valid_filter(rel: &Rel, alias: &str, ctx: &SecurityContext) -> bool {
    match rel {
        Rel::Filter(f) => {
            if expr_has_valid_starts_with(&f.condition, alias, ctx) {
                return true;
            }
            rel_has_valid_filter(&f.input, alias, ctx)
        }
        Rel::Project(p) => rel_has_valid_filter(&p.input, alias, ctx),
        Rel::Fetch(f) => rel_has_valid_filter(&f.input, alias, ctx),
        Rel::Sort(s) => rel_has_valid_filter(&s.input, alias, ctx),
        Rel::Aggregate(a) => rel_has_valid_filter(&a.input, alias, ctx),
        Rel::Join(j) => {
            rel_has_valid_filter(&j.left, alias, ctx) || rel_has_valid_filter(&j.right, alias, ctx)
        }
        Rel::Subquery(s) => rel_has_valid_filter(&s.input, alias, ctx),
        Rel::Distinct(d) => rel_has_valid_filter(&d.input, alias, ctx),
        Rel::Read(_) | Rel::UnionAll(_) => false,
    }
}

// ---------------------------------------------------------------------------
// Expression walking
// ---------------------------------------------------------------------------

fn expr_has_valid_starts_with(expr: &Expr, alias: &str, ctx: &SecurityContext) -> bool {
    match expr {
        Expr::FuncCall { name, args } if name == "startsWith" => {
            is_valid_starts_with_args(args, alias, ctx)
        }
        // Recurse through AND / OR
        Expr::BinaryOp { left, right, .. } => {
            expr_has_valid_starts_with(left, alias, ctx)
                || expr_has_valid_starts_with(right, alias, ctx)
        }
        _ => false,
    }
}

/// Check if `startsWith` args match `(alias.traversal_path, valid_path_literal)`.
fn is_valid_starts_with_args(args: &[Expr], alias: &str, ctx: &SecurityContext) -> bool {
    let mut has_column = false;
    let mut valid_path = false;

    for arg in args {
        match arg {
            Expr::Column { table, name } if table == alias && name == TRAVERSAL_PATH_COLUMN => {
                has_column = true;
            }
            Expr::Literal(lit) => {
                if let Some(path_str) = lit.as_string() {
                    // The filter path must be a prefix of (or equal to) at least
                    // one authorized traversal path. This covers both exact paths
                    // and LCP prefixes.
                    if ctx
                        .traversal_paths
                        .iter()
                        .any(|tp| tp.starts_with(path_str))
                    {
                        valid_path = true;
                    }
                }
            }
            _ => {}
        }
    }

    has_column && valid_path
}

#[cfg(test)]
mod tests {
    use super::super::security::SecurityPass;
    use super::*;
    use llqm::backend::clickhouse::emit_clickhouse_sql;
    use llqm::ir::expr::*;
    use llqm::ir::plan::{CteDef, Rel};

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

    fn secured_plan(table: &str, alias: &str, ctx: &SecurityContext) -> Plan {
        let pass = SecurityPass {
            context: ctx.clone(),
        };
        pass.transform(make_plan(table, alias)).unwrap()
    }

    #[test]
    fn passes_after_security_injection() {
        let ctx = SecurityContext::new(42, vec!["42/43/".into()]).unwrap();
        let plan = secured_plan("gl_project", "p", &ctx);
        assert!(check_plan(&plan, &ctx).is_ok());
    }

    #[test]
    fn fails_without_any_filter() {
        let ctx = SecurityContext::new(1, vec!["1/".into()]).unwrap();
        let plan = make_plan("gl_project", "p");
        let err = check_plan(&plan, &ctx).unwrap_err();
        assert!(
            err.to_string()
                .contains("missing valid traversal_path filter")
        );
    }

    #[test]
    fn fails_with_wrong_path() {
        let ctx = SecurityContext::new(42, vec!["42/43/".into()]).unwrap();
        let mut plan = make_plan("gl_project", "p");
        plan.inject_filter(col("p", TRAVERSAL_PATH_COLUMN).starts_with(string("99/")));
        let err = check_plan(&plan, &ctx).unwrap_err();
        assert!(
            err.to_string()
                .contains("missing valid traversal_path filter")
        );
    }

    #[test]
    fn accepts_lcp_with_multiple_paths() {
        let ctx = SecurityContext::new(42, vec!["42/10/".into(), "42/20/".into()]).unwrap();
        let plan = secured_plan("gl_project", "p", &ctx);
        assert!(check_plan(&plan, &ctx).is_ok());
    }

    #[test]
    fn skips_non_gl_tables() {
        let ctx = SecurityContext::new(1, vec!["1/".into()]).unwrap();
        let plan = make_plan("custom_table", "c");
        assert!(check_plan(&plan, &ctx).is_ok());
    }

    #[test]
    fn skips_user_table() {
        let ctx = SecurityContext::new(42, vec!["42/43/".into()]).unwrap();
        let plan = make_plan("gl_user", "u");
        assert!(check_plan(&plan, &ctx).is_ok());
    }

    #[test]
    fn join_both_tables_checked() {
        let ctx = SecurityContext::new(42, vec!["42/43/".into()]).unwrap();

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

        // Without security: should fail
        let err = check_plan(&plan, &ctx).unwrap_err();
        assert!(err.to_string().contains("missing valid traversal_path"));

        // With security: should pass
        let pass = SecurityPass {
            context: ctx.clone(),
        };
        let plan = pass.transform(plan).unwrap();
        assert!(check_plan(&plan, &ctx).is_ok());
    }

    #[test]
    fn cte_checked() {
        let ctx = SecurityContext::new(42, vec!["42/43/".into()]).unwrap();

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
            .into_plan_with_ctes(vec![CteDef {
                name: "base".into(),
                plan: cte_plan,
                recursive: false,
            }]);

        // CTE has gl_project without filter — should fail
        let err = check_plan(&plan, &ctx).unwrap_err();
        assert!(err.to_string().contains("missing valid traversal_path"));
    }

    #[test]
    fn end_to_end_security_then_check() {
        let ctx = SecurityContext::new(42, vec!["42/43/".into()]).unwrap();

        let plan = make_plan("gl_project", "p");

        // Security pass → check pass
        let security = SecurityPass {
            context: ctx.clone(),
        };
        let check = CheckPass {
            context: ctx.clone(),
        };

        let plan = security.transform(plan).unwrap();
        check.transform(&plan).unwrap();

        // Verify SQL is valid
        let pq = emit_clickhouse_sql(&plan).unwrap();
        assert!(pq.sql.contains("startsWith"), "sql: {}", pq.sql);
    }

    #[test]
    fn check_catches_missing_filter() {
        let ctx = SecurityContext::new(42, vec!["42/43/".into()]).unwrap();
        let check = CheckPass {
            context: ctx.clone(),
        };

        let plan = make_plan("gl_project", "p");
        assert!(check.transform(&plan).is_err());
    }
}
