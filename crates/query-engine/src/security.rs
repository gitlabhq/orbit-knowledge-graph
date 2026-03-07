//! Security filter injection for multi-tenant isolation.
//!
//! Injects traversal_path filters on all node table scans.
//! The org_id is encoded as the first segment of each path, validated at construction.
//!
//! Path filtering strategy:
//! - 1 path: `startsWith(path)`
//! - 2+ paths: `startsWith(LCP) AND (startsWith(p1) OR startsWith(p2) OR ...)`

use llqm::expr::{self, Expr};
use substrait::proto::{self, Rel, read_rel, rel};

use crate::constants::{GL_TABLE_PREFIX, SKIP_SECURITY_FILTER_TABLES, TRAVERSAL_PATH_COLUMN};
use crate::error::{QueryError, Result};
use crate::lower::LoweredQuery;
use once_cell::sync::Lazy;
use regex::Regex;

/// Matches paths like "1/", "1/2/", "123/456/789/"
static TRAVERSAL_PATH_REGEX: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"^(\d+/)+$").expect("valid regex"));

/// Security context for request-level isolation.
#[derive(Debug, Clone)]
pub struct SecurityContext {
    pub org_id: i64,
    pub traversal_paths: Vec<String>,
}

impl SecurityContext {
    /// Create a new security context with validation.
    ///
    /// Validates that:
    /// - Each path matches the format `int/int/.../`
    /// - Each segment fits in i64
    /// - The first segment of each path equals org_id
    pub fn new(org_id: i64, traversal_paths: Vec<String>) -> Result<Self> {
        for path in &traversal_paths {
            validate_traversal_path(path, org_id)?;
        }
        Ok(Self {
            org_id,
            traversal_paths,
        })
    }
}

fn validate_traversal_path(path: &str, org_id: i64) -> Result<()> {
    if !TRAVERSAL_PATH_REGEX.is_match(path) {
        return Err(QueryError::Security(format!(
            "invalid traversal_path format: '{path}' (expected pattern like '1/2/3/')"
        )));
    }

    let segments: Vec<&str> = path.trim_end_matches('/').split('/').collect();

    for segment in &segments {
        segment.parse::<i64>().map_err(|_| {
            QueryError::Security(format!(
                "traversal_path segment '{segment}' exceeds i64 range"
            ))
        })?;
    }

    let first_segment: i64 = segments[0].parse().expect("validated above");
    if first_segment != org_id {
        return Err(QueryError::Security(format!(
            "traversal_path '{path}' does not start with org_id {org_id}"
        )));
    }

    Ok(())
}

/// Inject security filters into a `LoweredQuery`.
///
/// Walks the Substrait relation tree in `lq.base_rel` to find all ReadRels
/// that scan `gl_*` tables (except skip tables like `gl_user`), then wraps
/// `lq.base_rel` with a FilterRel containing `startsWith(alias.traversal_path, path)`
/// for each such table. Also injects filters into CTE plans.
pub fn apply_security_context(lq: &mut LoweredQuery, ctx: &SecurityContext) -> Result<()> {
    // Apply to main base_rel
    let aliases = collect_filterable_aliases_from_rel(&lq.base_rel.rel);
    if !aliases.is_empty() {
        let security_conds: Vec<Expr> = aliases
            .iter()
            .map(|a| build_path_filter(a, &ctx.traversal_paths))
            .collect();

        let combined = expr::and(security_conds);
        let base = std::mem::replace(
            &mut lq.base_rel,
            // Placeholder — immediately replaced below
            lq.builder.read("__placeholder", "__ph", &[]),
        );
        lq.base_rel = lq.builder.filter(base, combined);
    }

    // Apply to CTE base rels (unbuilt — security can filter before finalize builds them).
    for cte in &mut lq.ctes {
        let cte_aliases = collect_filterable_aliases_from_rel(&cte.root_rel.rel);
        if !cte_aliases.is_empty() {
            let cte_conds: Vec<Expr> = cte_aliases
                .iter()
                .map(|a| build_path_filter(a, &ctx.traversal_paths))
                .collect();
            let combined = expr::and(cte_conds);
            let placeholder = cte.builder.read("__placeholder", "__ph", &[]);
            let base = std::mem::replace(&mut cte.root_rel, placeholder);
            cte.root_rel = cte.builder.filter(base, combined);
        }
    }

    Ok(())
}

/// Walk the Substrait Rel tree to find all table aliases that need security filters.
///
/// Examines ReadRels for `gl_*` tables (except skip tables), extracts the alias
/// from the AdvancedExtension metadata, and returns the list of aliases.
fn collect_filterable_aliases_from_rel(rel: &Rel) -> Vec<String> {
    let mut aliases = Vec::new();
    walk_rel_for_filterable_aliases(rel, &mut aliases);
    aliases
}

fn walk_rel_for_filterable_aliases(rel: &Rel, aliases: &mut Vec<String>) {
    match &rel.rel_type {
        Some(rel::RelType::Read(read)) => {
            if let Some((table, alias)) = extract_table_and_alias(read)
                && should_apply_security_filter(&table)
            {
                aliases.push(alias);
            }
        }
        Some(rel::RelType::Filter(f)) => {
            if let Some(input) = &f.input {
                walk_rel_for_filterable_aliases(input, aliases);
            }
        }
        Some(rel::RelType::Project(p)) => {
            if let Some(input) = &p.input {
                walk_rel_for_filterable_aliases(input, aliases);
            }
        }
        Some(rel::RelType::Join(j)) => {
            if let Some(left) = &j.left {
                walk_rel_for_filterable_aliases(left, aliases);
            }
            if let Some(right) = &j.right {
                walk_rel_for_filterable_aliases(right, aliases);
            }
        }
        Some(rel::RelType::Fetch(f)) => {
            if let Some(input) = &f.input {
                walk_rel_for_filterable_aliases(input, aliases);
            }
        }
        Some(rel::RelType::Sort(s)) => {
            if let Some(input) = &s.input {
                walk_rel_for_filterable_aliases(input, aliases);
            }
        }
        Some(rel::RelType::Aggregate(a)) => {
            if let Some(input) = &a.input {
                walk_rel_for_filterable_aliases(input, aliases);
            }
        }
        Some(rel::RelType::Set(_)) => {
            // Don't recurse into SetRel (UNION ALL) arms — they are derived tables.
            // Edge reads inside hop unions are transitively secured through join
            // conditions like startsWith(e.traversal_path, node.traversal_path)
            // where the node already has a direct security filter.
        }
        _ => {}
    }
}

/// Extract (table_name, alias) from a ReadRel's AdvancedExtension metadata and NamedTable.
fn extract_table_and_alias(read: &proto::ReadRel) -> Option<(String, String)> {
    let alias = read
        .advanced_extension
        .as_ref()
        .and_then(|adv| adv.optimization.first())
        .and_then(|opt| serde_json::from_slice::<serde_json::Value>(&opt.value).ok())
        .and_then(|meta| meta.get("alias").and_then(|v| v.as_str()).map(String::from))?;

    let table = match &read.read_type {
        Some(read_rel::ReadType::NamedTable(nt)) => nt.names.first().cloned(),
        _ => None,
    }?;

    Some((table, alias))
}

fn build_path_filter(alias: &str, paths: &[String]) -> Expr {
    match paths.len() {
        0 => expr::boolean(false),
        1 => starts_with_expr(alias, &paths[0]),
        _ => {
            let prefix = lowest_common_prefix(paths);
            let prefix_filter = starts_with_expr(alias, &prefix);
            let or_filters = expr::or_opt(paths.iter().map(|p| Some(starts_with_expr(alias, p))));
            match or_filters {
                Some(or_expr) => Expr::BinaryOp {
                    op: llqm::expr::BinaryOp::And,
                    left: Box::new(prefix_filter),
                    right: Box::new(or_expr),
                },
                None => prefix_filter,
            }
        }
    }
}

/// Find the lowest common path prefix.
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
    expr::starts_with(expr::col(alias, TRAVERSAL_PATH_COLUMN), expr::string(path))
}

/// Determines if a table should have traversal path security filters applied.
pub(crate) fn should_apply_security_filter(table: &str) -> bool {
    if !table.starts_with(GL_TABLE_PREFIX) {
        return false;
    }
    if SKIP_SECURITY_FILTER_TABLES.contains(&table) {
        return false;
    }
    true
}

#[cfg(test)]
mod tests {
    use super::*;
    use llqm::expr::DataType;
    use llqm::plan::PlanBuilder;
    use ontology::constants::EDGE_TABLE;

    fn make_simple_lq(table: &str, alias: &str) -> LoweredQuery {
        let mut b = PlanBuilder::new();
        let rel = b.read(
            table,
            alias,
            &[
                ("id", DataType::Int64),
                (TRAVERSAL_PATH_COLUMN, DataType::String),
            ],
        );
        LoweredQuery {
            builder: b,
            base_rel: rel,
            projections: vec![(expr::col(alias, "id"), format!("{alias}_id"))],
            group_by: vec![],
            agg_measures: vec![],
            sort_keys: vec![],
            limit: Some(10),
            offset: None,
            ctes: vec![],
        }
    }

    #[test]
    fn traversal_path_validation() {
        assert!(SecurityContext::new(1, vec!["1/".into()]).is_ok());
        assert!(SecurityContext::new(1, vec!["1/2/3/".into()]).is_ok());
        assert!(SecurityContext::new(42, vec!["42/100/".into()]).is_ok());

        assert!(SecurityContext::new(1, vec!["42/".into()]).is_err());
        assert!(SecurityContext::new(99, vec!["1/2/3/".into()]).is_err());

        assert!(SecurityContext::new(1, vec!["1/2/3".into()]).is_err());
        assert!(SecurityContext::new(1, vec!["".into()]).is_err());
        assert!(SecurityContext::new(1, vec!["abc/".into()]).is_err());
        assert!(SecurityContext::new(1, vec!["1/abc/2/".into()]).is_err());
        assert!(SecurityContext::new(1, vec!["99999999999999999999999999999/".into()]).is_err());
        assert!(SecurityContext::new(1, vec!["-1/".into()]).is_err());
    }

    #[test]
    fn single_path_uses_starts_with() {
        let e = build_path_filter("u", &["42/43/".into()]);
        assert!(
            matches!(e, Expr::FuncCall { ref name, .. } if name == "startsWith"),
            "expected startsWith, got: {:?}",
            e
        );
    }

    #[test]
    fn multiple_paths_uses_prefix_and_or_starts_with() {
        let e = build_path_filter("u", &["1/2/4/".into(), "1/2/5/".into()]);
        assert!(matches!(
            e,
            Expr::BinaryOp {
                op: llqm::expr::BinaryOp::And,
                ..
            }
        ));
    }

    #[test]
    fn lowest_common_prefix_finds_shared_path() {
        assert_eq!(
            lowest_common_prefix(&["1/2/4/".into(), "1/2/5/".into()]),
            "1/2/"
        );
        assert_eq!(lowest_common_prefix(&["1/2/".into(), "1/3/".into()]), "1/");
        assert_eq!(lowest_common_prefix(&["1/".into(), "2/".into()]), "");
        assert_eq!(lowest_common_prefix(&["42/".into()]), "42/");
    }

    #[test]
    fn inject_adds_security_to_simple_query() {
        let ctx = SecurityContext::new(42, vec!["42/43/".into()]).unwrap();
        let mut lq = make_simple_lq("gl_project", "p");
        apply_security_context(&mut lq, &ctx).unwrap();

        assert!(matches!(
            lq.base_rel.rel.rel_type,
            Some(rel::RelType::Filter(_))
        ));
    }

    #[test]
    fn inject_filters_edge_table() {
        let ctx = SecurityContext::new(42, vec!["42/43/".into()]).unwrap();
        let mut lq = make_simple_lq(EDGE_TABLE, "e");
        apply_security_context(&mut lq, &ctx).unwrap();

        assert!(matches!(
            lq.base_rel.rel.rel_type,
            Some(rel::RelType::Filter(_))
        ));
    }

    #[test]
    fn inject_skips_user_table() {
        let ctx = SecurityContext::new(42, vec!["42/43/".into()]).unwrap();
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
        let joined = b.join(
            llqm::expr::JoinType::Inner,
            u,
            mr,
            expr::eq(expr::col("u", "id"), expr::col("mr", "id")),
        );
        let mut lq = LoweredQuery {
            builder: b,
            base_rel: joined,
            projections: vec![],
            group_by: vec![],
            agg_measures: vec![],
            sort_keys: vec![],
            limit: None,
            offset: None,
            ctes: vec![],
        };

        let aliases = collect_filterable_aliases_from_rel(&lq.base_rel.rel);
        assert_eq!(aliases, vec!["mr"]);

        apply_security_context(&mut lq, &ctx).unwrap();

        assert!(matches!(
            lq.base_rel.rel.rel_type,
            Some(rel::RelType::Filter(_))
        ));
    }

    #[test]
    fn should_apply_security_filter_skips_user() {
        assert!(!should_apply_security_filter("gl_user"));
        assert!(should_apply_security_filter(EDGE_TABLE));
        assert!(should_apply_security_filter("gl_project"));
        assert!(should_apply_security_filter("gl_merge_request"));
    }

    #[test]
    fn should_apply_security_filter_skips_ctes() {
        assert!(!should_apply_security_filter("path_cte"));
        assert!(!should_apply_security_filter("some_cte"));
        assert!(!should_apply_security_filter("nodes"));
    }

    #[test]
    fn inject_generates_valid_sql() {
        let ctx = SecurityContext::new(42, vec!["42/43/".into()]).unwrap();
        let mut lq = make_simple_lq("gl_project", "p");
        apply_security_context(&mut lq, &ctx).unwrap();

        let plan = crate::lower::finalize(lq);
        let pq = llqm::codegen::emit_clickhouse_sql(&plan).unwrap();
        assert!(
            pq.sql.contains("startsWith"),
            "expected startsWith: {}",
            pq.sql
        );
        assert!(
            pq.sql.contains("traversal_path"),
            "expected traversal_path: {}",
            pq.sql
        );
    }
}
