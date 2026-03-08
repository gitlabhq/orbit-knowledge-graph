//! Post-compilation safety checks.
//!
//! Walks the Substrait plan tree after security filter injection to verify
//! that every `gl_*` table alias (except skip tables) has a
//! `startsWith(alias.traversal_path, path)` predicate whose path literal
//! is derivable from the [`SecurityContext`].
//!
//! This is defense-in-depth — if [`SecurityPass`](super::security::SecurityPass)
//! worked correctly, this should always pass.

use std::collections::HashMap;

use substrait::proto::expression::literal::LiteralType;
use substrait::proto::expression::{self, ScalarFunction};
use substrait::proto::extensions::simple_extension_declaration::MappingType;
use substrait::proto::{rel, Expression, Rel};

use crate::ir::plan::Plan;
use crate::pass::security::{SecurityContext, TRAVERSAL_PATH_COLUMN};
use crate::pipeline::IrPass;

use super::security::{GL_TABLE_PREFIX, SKIP_TABLES};

const STARTS_WITH_FNAME: &str = "startsWith";
const RAW_SQL_FNAME: &str = "__raw_sql";

#[derive(Debug, thiserror::Error)]
pub enum CheckError {
    #[error("post-check failed: alias '{alias}' missing valid traversal_path filter")]
    MissingFilter { alias: String },

    #[error("post-check failed: plan has no root relation")]
    NoRoot,
}

pub struct CheckPass {
    pub context: SecurityContext,
}

impl IrPass for CheckPass {
    type Error = CheckError;

    fn transform(&self, plan: Plan) -> Result<Plan, Self::Error> {
        check_plan(&plan, &self.context)?;
        Ok(plan)
    }
}

/// Verify that every filterable alias in the plan has a valid security filter.
pub fn check_plan(plan: &Plan, ctx: &SecurityContext) -> Result<(), CheckError> {
    let root = plan.root_rel().ok_or(CheckError::NoRoot)?;
    let fn_map = build_function_map(&plan.inner);
    let aliases = plan.filterable_aliases(should_check);

    for (_, alias) in &aliases {
        if !rel_has_valid_filter(root, alias, &fn_map, ctx) {
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
// Function anchor → name map
// ---------------------------------------------------------------------------

fn build_function_map(plan: &substrait::proto::Plan) -> HashMap<u32, String> {
    let mut map = HashMap::new();
    for ext in &plan.extensions {
        if let Some(MappingType::ExtensionFunction(f)) = &ext.mapping_type {
            map.insert(f.function_anchor, f.name.clone());
        }
    }
    map
}

// ---------------------------------------------------------------------------
// Rel-tree walking
// ---------------------------------------------------------------------------

/// Walk the relation tree looking for FilterRels whose condition validates
/// `alias.traversal_path` with a startsWith predicate.
fn rel_has_valid_filter(
    rel: &Rel,
    alias: &str,
    fn_map: &HashMap<u32, String>,
    ctx: &SecurityContext,
) -> bool {
    match &rel.rel_type {
        Some(rel::RelType::Filter(f)) => {
            if let Some(cond) = &f.condition
                && expr_has_valid_starts_with(cond, alias, fn_map, ctx)
            {
                return true;
            }
            // Keep searching deeper — the filter might be on a child rel
            f.input
                .as_ref()
                .is_some_and(|input| rel_has_valid_filter(input, alias, fn_map, ctx))
        }
        Some(rel::RelType::Project(p)) => p
            .input
            .as_ref()
            .is_some_and(|input| rel_has_valid_filter(input, alias, fn_map, ctx)),
        Some(rel::RelType::Fetch(f)) => f
            .input
            .as_ref()
            .is_some_and(|input| rel_has_valid_filter(input, alias, fn_map, ctx)),
        Some(rel::RelType::Sort(s)) => s
            .input
            .as_ref()
            .is_some_and(|input| rel_has_valid_filter(input, alias, fn_map, ctx)),
        Some(rel::RelType::Aggregate(a)) => a
            .input
            .as_ref()
            .is_some_and(|input| rel_has_valid_filter(input, alias, fn_map, ctx)),
        Some(rel::RelType::Join(j)) => {
            let in_left = j
                .left
                .as_ref()
                .is_some_and(|l| rel_has_valid_filter(l, alias, fn_map, ctx));
            let in_right = j
                .right
                .as_ref()
                .is_some_and(|r| rel_has_valid_filter(r, alias, fn_map, ctx));
            in_left || in_right
        }
        // Don't recurse into SetRel — arms are derived tables secured
        // transitively through join conditions.
        _ => false,
    }
}

// ---------------------------------------------------------------------------
// Expression walking
// ---------------------------------------------------------------------------

/// Recursively check whether an expression contains a valid
/// `startsWith(alias.traversal_path, path)` call.
fn expr_has_valid_starts_with(
    expr: &Expression,
    alias: &str,
    fn_map: &HashMap<u32, String>,
    ctx: &SecurityContext,
) -> bool {
    let Some(rex) = &expr.rex_type else {
        return false;
    };

    match rex {
        expression::RexType::ScalarFunction(sf) => {
            let fname = fn_map.get(&sf.function_reference).map(String::as_str);

            if fname == Some(STARTS_WITH_FNAME) {
                return is_valid_starts_with_call(sf, alias, fn_map, ctx);
            }

            // Recurse into AND / OR operands
            sf.arguments.iter().any(|arg| {
                if let Some(substrait::proto::function_argument::ArgType::Value(inner)) =
                    &arg.arg_type
                {
                    expr_has_valid_starts_with(inner, alias, fn_map, ctx)
                } else {
                    false
                }
            })
        }
        _ => false,
    }
}

/// Check if a `startsWith` ScalarFunction call references the right
/// `alias.traversal_path` column and has a path literal consistent with
/// the security context.
fn is_valid_starts_with_call(
    sf: &ScalarFunction,
    alias: &str,
    fn_map: &HashMap<u32, String>,
    ctx: &SecurityContext,
) -> bool {
    let expected_raw = format!("{alias}.{TRAVERSAL_PATH_COLUMN}");
    let mut has_column = false;
    let mut valid_path = false;

    for arg in &sf.arguments {
        let Some(substrait::proto::function_argument::ArgType::Value(expr)) = &arg.arg_type else {
            continue;
        };
        let Some(rex) = &expr.rex_type else { continue };

        match rex {
            // __raw_sql("alias.traversal_path")
            expression::RexType::ScalarFunction(inner_sf) => {
                if fn_map.get(&inner_sf.function_reference).map(String::as_str)
                    == Some(RAW_SQL_FNAME)
                    && let Some(raw_str) = extract_string_literal_from_args(&inner_sf.arguments)
                    && raw_str == expected_raw
                {
                    has_column = true;
                }
            }
            // String literal path value
            expression::RexType::Literal(lit) => {
                if let Some(LiteralType::String(path)) = &lit.literal_type
                    && ctx
                        .traversal_paths
                        .iter()
                        .any(|tp| tp.starts_with(path.as_str()))
                {
                    valid_path = true;
                }
            }
            _ => {}
        }
    }

    has_column && valid_path
}

/// Extract the string literal from a `__raw_sql` function's arguments.
fn extract_string_literal_from_args(args: &[substrait::proto::FunctionArgument]) -> Option<String> {
    for arg in args {
        if let Some(substrait::proto::function_argument::ArgType::Value(expr)) = &arg.arg_type
            && let Some(expression::RexType::Literal(lit)) = &expr.rex_type
            && let Some(LiteralType::String(s)) = &lit.literal_type
        {
            return Some(s.clone());
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::clickhouse::ClickHouseBackend;
    use crate::ir::expr::*;
    use crate::ir::plan::PlanBuilder;
    use crate::pass::security::{SecurityPass, TRAVERSAL_PATH_COLUMN};
    use crate::pipeline::{Frontend, IrPass, Pipeline};

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
        assert!(err
            .to_string()
            .contains("missing valid traversal_path filter"));
    }

    #[test]
    fn fails_with_wrong_path() {
        let ctx = SecurityContext::new(42, vec!["42/43/".into()]).unwrap();
        let mut plan = make_plan("gl_project", "p");
        // Manually inject a filter with the wrong path
        plan.inject_filter(starts_with(raw("p.traversal_path"), string("99/")));
        let err = check_plan(&plan, &ctx).unwrap_err();
        assert!(err
            .to_string()
            .contains("missing valid traversal_path filter"));
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

        // Build a CTE plan with a gl_project read
        let mut b1 = PlanBuilder::new();
        let cte_rel = b1
            .read(
                "gl_project",
                "p",
                &[
                    ("id", DataType::Int64),
                    (TRAVERSAL_PATH_COLUMN, DataType::String),
                ],
            )
            .project(&mut b1, &[(col("p", "id"), "node_id")]);
        let cte_plan = b1.build(cte_rel);

        // Main query reads from the CTE name (non-gl)
        let mut b2 = PlanBuilder::new();
        let main_rel = b2
            .read("base", "b", &[("node_id", DataType::Int64)])
            .project(&mut b2, &[(col("b", "node_id"), "node_id")]);
        let plan = b2.build_with_ctes(
            main_rel,
            vec![crate::ir::plan::CteDef {
                name: "base".into(),
                plan: cte_plan,
                recursive: false,
            }],
        );

        // CTE has gl_project without filter — should fail
        let err = check_plan(&plan, &ctx).unwrap_err();
        assert!(err.to_string().contains("missing valid traversal_path"));
    }

    #[test]
    fn end_to_end_pipeline() {
        struct TestFrontend;
        impl Frontend for TestFrontend {
            type Input = ();
            type Error = Box<dyn std::error::Error>;
            fn lower(&self, _: ()) -> Result<Plan, Self::Error> {
                Ok(make_plan("gl_project", "p"))
            }
        }

        let ctx = SecurityContext::new(42, vec!["42/43/".into()]).unwrap();
        let security = SecurityPass {
            context: ctx.clone(),
        };
        let check = CheckPass {
            context: ctx.clone(),
        };

        let pq = Pipeline::new()
            .input(TestFrontend, ())
            .lower()
            .unwrap()
            .pass(&security)
            .unwrap()
            .pass(&check)
            .unwrap()
            .emit(&ClickHouseBackend)
            .unwrap()
            .finish();

        assert!(pq.sql.contains("startsWith"), "sql: {}", pq.sql);
    }

    #[test]
    fn pipeline_catches_missing_filter() {
        struct UnsecuredFrontend;
        impl Frontend for UnsecuredFrontend {
            type Input = ();
            type Error = Box<dyn std::error::Error>;
            fn lower(&self, _: ()) -> Result<Plan, Self::Error> {
                Ok(make_plan("gl_project", "p"))
            }
        }

        let ctx = SecurityContext::new(42, vec!["42/43/".into()]).unwrap();
        let check = CheckPass {
            context: ctx.clone(),
        };

        // No SecurityPass — check should catch it
        let result = Pipeline::new()
            .input(UnsecuredFrontend, ())
            .lower()
            .unwrap()
            .pass(&check);

        assert!(result.is_err());
    }
}
