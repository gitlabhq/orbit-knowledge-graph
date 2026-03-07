//! Post-compilation safety checks.
//!
//! Runs after security filter injection to verify invariants that must hold
//! before the plan is handed to codegen. Checks that every node table alias
//! has a `startsWith(alias.traversal_path, path)` predicate whose path literal
//! is derivable from the [`SecurityContext`] — catching both injection bugs
//! and path value mismatches.

use llqm::codegen::emit_clickhouse_sql;
use llqm::plan::Plan;

use crate::constants::TRAVERSAL_PATH_COLUMN;
use crate::error::{QueryError, Result};
use crate::security::{should_apply_security_filter, SecurityContext};

/// Verify post-compilation invariants on the final plan.
///
/// Generates SQL and checks that every `gl_*` table alias (except skip tables)
/// has a `startsWith(alias.traversal_path, path)` in the SQL where `path`
/// is a prefix of at least one SecurityContext traversal path.
///
/// This is a defense-in-depth check — if security injection worked correctly,
/// this should always pass.
pub fn check_plan(plan: &Plan, ctx: &SecurityContext) -> Result<()> {
    // Generate SQL to inspect it
    let pq = emit_clickhouse_sql(plan)
        .map_err(|e| QueryError::Security(format!("codegen failed during check: {e}")))?;

    // Extract all table aliases from the SQL.
    // Convention: "table_name AS alias" in FROM/JOIN clauses.
    let aliases = extract_filtered_aliases(&pq.sql);

    for (table, alias) in &aliases {
        if !should_apply_security_filter(table) {
            continue;
        }

        // Verify there is a startsWith(alias.traversal_path, path) in the SQL
        if !has_valid_starts_with(&pq.sql, alias, &pq.params, ctx) {
            return Err(QueryError::Security(format!(
                "post-check failed: alias '{alias}' missing valid traversal_path filter"
            )));
        }
    }

    // Also check CTE queries — each CTE plan may contain sensitive tables
    for cte in &plan.ctes {
        check_plan_inner(&cte.plan, ctx)?;
    }

    Ok(())
}

fn check_plan_inner(plan: &Plan, ctx: &SecurityContext) -> Result<()> {
    let pq = emit_clickhouse_sql(plan)
        .map_err(|e| QueryError::Security(format!("codegen failed during CTE check: {e}")))?;

    let aliases = extract_filtered_aliases(&pq.sql);
    for (table, alias) in &aliases {
        if !should_apply_security_filter(table) {
            continue;
        }
        if !has_valid_starts_with(&pq.sql, alias, &pq.params, ctx) {
            return Err(QueryError::Security(format!(
                "post-check failed: alias '{alias}' missing valid traversal_path filter in CTE"
            )));
        }
    }
    Ok(())
}

/// Extract (table_name, alias) pairs from SQL using the "table AS alias" pattern.
///
/// Only extracts aliases at the top-level (paren depth 0). Aliases inside
/// parenthesized subqueries (e.g. `(SELECT ... FROM gl_edge AS e1 ...) AS hop_e0`)
/// are scoped to the subquery and transitively secured through join conditions.
fn extract_filtered_aliases(sql: &str) -> Vec<(String, String)> {
    let mut result = Vec::new();
    let re = regex::Regex::new(r"\b(\w+)\s+AS\s+(\w+)\b").expect("valid regex");

    // Build a paren-depth map: for each byte position, track paren depth
    let mut depths: Vec<u32> = Vec::with_capacity(sql.len());
    let mut depth: u32 = 0;
    for ch in sql.chars() {
        if ch == '(' {
            depth += 1;
        }
        for _ in 0..ch.len_utf8() {
            depths.push(depth);
        }
        if ch == ')' {
            depth = depth.saturating_sub(1);
        }
    }

    for cap in re.captures_iter(sql) {
        let m = cap.get(0).unwrap();
        let pos_depth = depths.get(m.start()).copied().unwrap_or(0);
        if pos_depth > 0 {
            continue; // inside a subquery
        }
        let table = cap[1].to_string();
        let alias = cap[2].to_string();
        if table.starts_with("gl_") {
            result.push((table, alias));
        }
    }
    result
}

/// Check if the SQL contains a valid `startsWith(alias.traversal_path, path)` where
/// path is consistent with the SecurityContext.
fn has_valid_starts_with(
    sql: &str,
    alias: &str,
    params: &std::collections::HashMap<String, llqm::codegen::ParamValue>,
    ctx: &SecurityContext,
) -> bool {
    // Look for `startsWith(alias.traversal_path, {pN:String})` or
    // `startsWith(alias.traversal_path, 'literal')`
    let pattern = format!("startsWith({alias}.{TRAVERSAL_PATH_COLUMN}, ");
    for (pos, _) in sql.match_indices(&pattern) {
        let after = &sql[pos + pattern.len()..];
        // Extract the path argument
        if let Some(path) = extract_path_arg(after, params)
            && ctx.traversal_paths.iter().any(|tp| tp.starts_with(&path))
        {
            return true;
        }
    }
    false
}

/// Extract the path argument from after "startsWith(alias.traversal_path, "
fn extract_path_arg(
    s: &str,
    params: &std::collections::HashMap<String, llqm::codegen::ParamValue>,
) -> Option<String> {
    // Case 1: parameter reference like {p0:String}
    if s.starts_with('{') {
        let end = s.find('}')?;
        let placeholder = &s[1..end];
        let name = placeholder.split(':').next()?;
        let param = params.get(name)?;
        if let serde_json::Value::String(path) = &param.value {
            return Some(path.clone());
        }
    }
    // Case 2: string literal like 'path'
    if let Some(rest) = s.strip_prefix('\'') {
        let end = rest.find('\'')?;
        return Some(rest[..end].to_string());
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use llqm::expr::{self, DataType};
    use llqm::plan::PlanBuilder;

    fn make_project_plan(with_security: bool, ctx: &SecurityContext) -> Plan {
        let mut b = PlanBuilder::new();
        let rel = b.read(
            "gl_project",
            "p",
            &[
                ("id", DataType::Int64),
                ("traversal_path", DataType::String),
            ],
        );
        let rel = if with_security {
            let cond = expr::starts_with(
                expr::col("p", "traversal_path"),
                expr::string(&ctx.traversal_paths[0]),
            );
            b.filter(rel, cond)
        } else {
            rel
        };
        let rel = b.project(rel, &[(expr::col("p", "id"), "p_id")]);
        let rel = b.fetch(rel, 10, None);
        b.build(rel)
    }

    #[test]
    fn passes_with_security_filter() {
        let ctx = SecurityContext::new(42, vec!["42/43/".into()]).unwrap();
        let plan = make_project_plan(true, &ctx);
        assert!(check_plan(&plan, &ctx).is_ok());
    }

    #[test]
    fn fails_without_any_filter() {
        let ctx = SecurityContext::new(1, vec!["1/".into()]).unwrap();
        let plan = make_project_plan(false, &ctx);
        let err = check_plan(&plan, &ctx).unwrap_err();
        assert!(err
            .to_string()
            .contains("missing valid traversal_path filter"));
    }

    #[test]
    fn fails_with_wrong_path_literal() {
        let ctx = SecurityContext::new(42, vec!["42/43/".into()]).unwrap();
        let mut b = PlanBuilder::new();
        let rel = b.read(
            "gl_project",
            "p",
            &[
                ("id", DataType::Int64),
                ("traversal_path", DataType::String),
            ],
        );
        let wrong_filter = expr::starts_with(expr::col("p", "traversal_path"), expr::string("99/"));
        let rel = b.filter(rel, wrong_filter);
        let rel = b.project(rel, &[(expr::col("p", "id"), "p_id")]);
        let plan = b.build(rel);
        let err = check_plan(&plan, &ctx).unwrap_err();
        assert!(err
            .to_string()
            .contains("missing valid traversal_path filter"));
    }

    #[test]
    fn accepts_lowest_common_prefix() {
        let ctx = SecurityContext::new(42, vec!["42/10/".into(), "42/20/".into()]).unwrap();
        let mut b = PlanBuilder::new();
        let rel = b.read(
            "gl_project",
            "p",
            &[
                ("id", DataType::Int64),
                ("traversal_path", DataType::String),
            ],
        );
        // LCP filter: startsWith(p.traversal_path, '42/')
        let lcp_filter = expr::starts_with(expr::col("p", "traversal_path"), expr::string("42/"));
        let or_filter = expr::or([
            expr::starts_with(expr::col("p", "traversal_path"), expr::string("42/10/")),
            expr::starts_with(expr::col("p", "traversal_path"), expr::string("42/20/")),
        ]);
        let combined = Expr::BinaryOp {
            op: llqm::expr::BinaryOp::And,
            left: Box::new(lcp_filter),
            right: Box::new(or_filter),
        };
        let rel = b.filter(rel, combined);
        let rel = b.project(rel, &[(expr::col("p", "id"), "p_id")]);
        let plan = b.build(rel);
        assert!(check_plan(&plan, &ctx).is_ok());
    }

    #[test]
    fn skips_non_gl_tables() {
        let ctx = SecurityContext::new(1, vec!["1/".into()]).unwrap();
        let mut b = PlanBuilder::new();
        let rel = b.read(
            "path_cte",
            "c",
            &[("id", DataType::Int64), ("value", DataType::String)],
        );
        let rel = b.project(rel, &[(expr::col("c", "id"), "c_id")]);
        let plan = b.build(rel);
        assert!(check_plan(&plan, &ctx).is_ok());
    }

    use llqm::expr::Expr;
}
