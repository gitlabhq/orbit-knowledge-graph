//! Security filter injection for multi-tenant isolation.
//!
//! Injects traversal_path filters on all node table scans.
//! The org_id is encoded as the first segment of each path, validated at construction.
//!
//! Path filtering strategy:
//! - 1 path: `startsWith(path)`
//! - 2+ paths: `startsWith(LCP) AND (startsWith(p1) OR startsWith(p2) OR ...)`
//!
//! # Per-entity role scoping
//!
//! Each entity's ontology can declare a `required_role`. Before injecting
//! the `startsWith` predicate for an alias we look up the entity attached
//! to that alias's physical table and drop any traversal path where the
//! user's access level is below the entity's `required_access_level`.
//!
//! This closes the aggregation-query oracle where a Reporter-only user
//! could count or binary-search properties on a higher-privilege entity
//! (e.g. Vulnerability) by pairing a Project `group_by` with a Vulnerability
//! target. Now the target entity's scan is filtered down to zero paths
//! (producing a Bool(false) predicate) and the aggregation counts nothing.

use std::sync::OnceLock;

use regex::Regex;

use serde_json::Value;

use crate::ast::{Expr, Node, Query, TableRef};
use crate::constants::{GL_TABLE_PREFIX, TRAVERSAL_PATH_COLUMN, global_tables};
use crate::error::Result;
pub use crate::types::SecurityContext;
use ontology::Ontology;

/// Matches `gl_*` or `v{N}_gl_*`, captures the unprefixed name.
static GL_TABLE_RE: OnceLock<Regex> = OnceLock::new();

/// Per-alias role floors come from `ontology.min_access_level_for_table`;
/// tables without a `redaction` block keep the historical Reporter floor.
pub fn apply_security_context(
    node: &mut Node,
    ctx: &SecurityContext,
    ontology: &Ontology,
) -> Result<()> {
    // An entirely empty security context is treated as a fail-closed bug:
    // the caller forgot to populate traversal paths. Emitting `Bool(false)`
    // here would silently return empty results, which is indistinguishable
    // from "user has no namespaces" and obscures the root cause. Note that
    // this differs from a role-mismatch empty-path set for a specific
    // alias — in that case the user has paths, just none at the required
    // role, and returning zero rows for the protected entity is the
    // intended behavior.
    if ctx.traversal_paths.is_empty() {
        return Err(crate::error::QueryError::Security(
            "security context has no traversal_path entries; refusing to compile \
             because every gl_* alias would fall back to Bool(false) and hide \
             the underlying auth misconfiguration"
                .into(),
        ));
    }
    match node {
        Node::Query(q) => {
            for cte in &mut q.ctes {
                apply_to_query(&mut cte.query, ctx, ontology)?;
            }
            apply_to_query(q, ctx, ontology)
        }
        Node::Insert(_) => Ok(()),
    }
}

fn apply_to_query(q: &mut Query, ctx: &SecurityContext, ontology: &Ontology) -> Result<()> {
    let aliased_tables = collect_aliased_tables(&q.from);
    if !aliased_tables.is_empty() {
        let security_conds = aliased_tables.iter().map(|(alias, table)| {
            let min_role = ontology
                .min_access_level_for_table(table)
                .unwrap_or(crate::types::DEFAULT_PATH_ACCESS_LEVEL);
            let eligible = ctx.paths_at_least(min_role);
            // Inject the resolved scope prefix as the alias's authorization filter
            // when it sits within an eligible path; otherwise the broad path set.
            let scope_prefix = ctx
                .scope_prefixes
                .get(alias)
                .filter(|_| ontology.is_table_path_scopable(table));
            let base = match scope_prefix {
                Some(prefix) if eligible.iter().any(|p| prefix.starts_with(p)) => {
                    starts_with_expr(alias, prefix)
                }
                Some(prefix) => Expr::and(
                    build_path_filter(alias, &eligible),
                    starts_with_expr(alias, prefix),
                ),
                None => build_path_filter(alias, &eligible),
            };
            // A scoped prefix that pins a top-level namespace lets ClickHouse
            // prune to one partition; the broad/global cases span buckets.
            match scope_prefix.and_then(|prefix| partition_predicate(ontology, alias, prefix)) {
                Some(pred) => Expr::and(base, pred),
                None => base,
            }
        });
        q.where_clause = Expr::and_all(
            security_conds
                .map(Some)
                .chain(std::iter::once(q.where_clause.take())),
        );
    }

    apply_security_to_from(&mut q.from, ctx, ontology)?;

    for arm in &mut q.union_all {
        apply_to_query(arm, ctx, ontology)?;
    }

    Ok(())
}

fn build_path_filter(alias: &str, paths: &[&str]) -> Expr {
    match paths.len() {
        0 => Expr::Literal(Value::Bool(false)),
        1 => starts_with_expr(alias, paths[0]),
        _ => {
            let collapsed = PathTrie::from_paths(paths).to_minimal_prefixes();
            if collapsed.len() == 1 {
                return starts_with_expr(alias, &collapsed[0]);
            }
            let lcp = lowest_common_prefix(&collapsed);
            let lcp_filter = starts_with_expr(alias, &lcp);
            Expr::and(lcp_filter, path_or_filter(alias, &collapsed))
        }
    }
}

/// A trie keyed on path segments (`"1"`, `"100"`, …). Each node tracks
/// whether it was explicitly inserted (i.e., the user has access to that
/// exact namespace prefix). Inserting `"1/100/"` marks the `1 → 100` node
/// as terminal.
#[derive(Default)]
struct PathTrie {
    children: std::collections::BTreeMap<String, PathTrie>,
    terminal: bool,
}

impl PathTrie {
    fn from_paths(paths: &[&str]) -> Self {
        let mut root = Self::default();
        for path in paths {
            root.insert(path);
        }
        root
    }

    fn insert(&mut self, path: &str) {
        let segments: Vec<&str> = path
            .trim_end_matches('/')
            .split('/')
            .filter(|s| !s.is_empty())
            .collect();
        // Empty paths are impossible: SecurityContext::validate_traversal_path
        // enforces ^(\d+/)+$. Guard here to prevent the root node from being
        // marked terminal, which would emit "" and match everything.
        debug_assert!(
            !segments.is_empty(),
            "PathTrie::insert called with empty path"
        );
        if segments.is_empty() {
            return;
        }
        let mut node = self;
        for seg in segments {
            node = node.children.entry(seg.to_string()).or_default();
        }
        node.terminal = true;
    }

    /// Walk the trie and emit the minimal set of prefixes. A terminal
    /// node emits its path and prunes all descendants (subsumption).
    /// A non-terminal node with exactly one child merges into that
    /// child (prefix compression).
    fn to_minimal_prefixes(&self) -> Vec<String> {
        let mut result = Vec::new();
        self.collect(&mut String::new(), &mut result);
        result
    }

    fn collect(&self, prefix: &mut String, out: &mut Vec<String>) {
        if self.terminal {
            let mut p = prefix.clone();
            if !p.is_empty() {
                p.push('/');
            }
            out.push(p);
            return;
        }

        for (seg, child) in &self.children {
            let restore_len = prefix.len();
            if !prefix.is_empty() {
                prefix.push('/');
            }
            prefix.push_str(seg);
            child.collect(prefix, out);
            prefix.truncate(restore_len);
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
    starts_with_value_expr(alias, Expr::string(path))
}

/// Equates the table's partition expression to the same expression over the
/// scope prefix, so ClickHouse folds the constant side to a bucket and prunes
/// partitions. Returns `None` when partitioning is disabled or the prefix does
/// not pin a top-level namespace (the partition input), in which case the rows
/// span buckets and no single partition can be selected.
fn partition_predicate(ontology: &Ontology, alias: &str, prefix: &str) -> Option<Expr> {
    let strategy = &ontology.partition()?.strategy;
    if !strategy_input_is_pinned(strategy, prefix) {
        return None;
    }
    let column = Expr::col(alias, strategy.column());
    Some(Expr::eq(
        partition_expr(strategy, column),
        partition_expr(strategy, Expr::string(prefix)),
    ))
}

/// Whether `prefix` pins the value the strategy hashes. For hash-bucket over
/// the second path segment, that needs at least `org/top_level/`.
fn strategy_input_is_pinned(strategy: &ontology::PartitionStrategy, prefix: &str) -> bool {
    match strategy {
        ontology::PartitionStrategy::HashBucket { .. } => {
            prefix.split('/').filter(|s| !s.is_empty()).count() >= 2
        }
    }
}

/// Builds the strategy's partition expression as an `Expr` over `input`,
/// mirroring `PartitionStrategy::to_sql` (the DDL renderer) so the predicate
/// stays byte-identical to the table's `PARTITION BY`.
fn partition_expr(strategy: &ontology::PartitionStrategy, input: Expr) -> Expr {
    match strategy {
        ontology::PartitionStrategy::HashBucket { buckets, .. } => Expr::func(
            "modulo",
            vec![
                Expr::func(
                    "sipHash64",
                    vec![Expr::func(
                        "toUInt64OrZero",
                        vec![Expr::func(
                            "arrayElement",
                            vec![
                                Expr::func("splitByChar", vec![Expr::string("/"), input]),
                                Expr::int(2),
                            ],
                        )],
                    )],
                ),
                Expr::int(i64::from(*buckets)),
            ],
        ),
    }
}

fn starts_with_value_expr(alias: &str, path: Expr) -> Expr {
    Expr::func(
        "startsWith",
        vec![Expr::col(alias, TRAVERSAL_PATH_COLUMN), path],
    )
}

/// OR chain of `startsWith(alias.traversal_path, path)` for each path.
///
/// Each `startsWith` is visible to ClickHouse's PK index analyser, enabling
/// granule pruning per path prefix. This matters inside `dedup_edge_scan`
/// FINAL subqueries: PK range pruning reduces the scan from the entire LCP
/// namespace to only the user's authorized paths.
fn path_or_filter(alias: &str, paths: &[String]) -> Expr {
    let mut iter = paths.iter().map(|p| starts_with_expr(alias, p));
    let first = iter.next().expect("paths is non-empty (caller checks)");
    iter.fold(first, |a, b| Expr::binary(crate::ast::Op::Or, a, b))
}

pub(crate) fn collect_node_aliases(table_ref: &TableRef) -> Vec<String> {
    collect_aliased_tables(table_ref)
        .into_iter()
        .map(|(a, _)| a)
        .collect()
}

/// Collect `(alias, table)` pairs for every scan that should receive a
/// security filter. Returning the table lets the caller pick a per-entity
/// minimum role before building the `startsWith(...)` predicate.
pub(crate) fn collect_aliased_tables(table_ref: &TableRef) -> Vec<(String, String)> {
    match table_ref {
        TableRef::Scan { table, alias, .. } if should_apply_security_filter(table) => {
            vec![(alias.clone(), table.clone())]
        }
        TableRef::Scan { .. } => vec![],
        TableRef::Join { left, right, .. } => {
            let mut aliases = collect_aliased_tables(left);
            aliases.extend(collect_aliased_tables(right));
            aliases
        }
        // Derived tables don't have traversal_path columns themselves.
        // Their arms get security filters via apply_security_to_from.
        TableRef::Union { .. } | TableRef::Subquery { .. } => vec![],
    }
}

fn apply_security_to_from(
    table_ref: &mut TableRef,
    ctx: &SecurityContext,
    ontology: &Ontology,
) -> Result<()> {
    match table_ref {
        TableRef::Union { queries, .. } => {
            for arm in queries {
                apply_to_query(arm, ctx, ontology)?;
            }
        }
        TableRef::Subquery { query, .. } => {
            apply_to_query(query, ctx, ontology)?;
        }
        TableRef::Join { left, right, .. } => {
            apply_security_to_from(left, ctx, ontology)?;
            apply_security_to_from(right, ctx, ontology)?;
        }
        TableRef::Scan { .. } => {}
    }
    Ok(())
}

/// Handles both unprefixed (`gl_user`) and schema-version-prefixed
/// (`v1_gl_user`) table names. CTEs like `path_cte` are excluded.
fn should_apply_security_filter(table: &str) -> bool {
    let re = GL_TABLE_RE.get_or_init(|| {
        Regex::new(&format!(
            r"^(?:v\d+_)?({}.+)$",
            regex::escape(GL_TABLE_PREFIX)
        ))
        .expect("valid regex")
    });

    let unprefixed = match re.captures(table).and_then(|c| c.get(1)) {
        Some(m) => m.as_str(),
        None => return false,
    };

    // Global hubs (User, Runner) are non-namespaced; names are unprefixed.
    !global_tables().iter().any(|t| t == unprefixed)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::TraversalPath;
    use crate::ast::{JoinType, Op, SelectExpr};
    use ontology::constants::EDGE_TABLE;
    use serde_json::Value;

    fn simple_query() -> Node {
        Node::Query(Box::new(Query {
            select: vec![SelectExpr {
                expr: Expr::col("p", "id"),
                alias: None,
            }],
            from: TableRef::scan("gl_project", "p"),
            where_clause: None,
            limit: Some(10),
            ..Default::default()
        }))
    }

    #[test]
    fn traversal_path_validation() {
        assert!(SecurityContext::new(1, vec!["1/".into()]).is_ok());
        assert!(SecurityContext::new(1, vec!["1/2/3/".into()]).is_ok());
        assert!(SecurityContext::new(42, vec!["42/100/".into()]).is_ok());

        // Cross-org paths are allowed (user's home org != path org)
        assert!(SecurityContext::new(1, vec!["42/".into()]).is_ok());
        assert!(SecurityContext::new(99, vec!["1/2/3/".into()]).is_ok());

        assert!(SecurityContext::new(1, vec!["1/2/3".into()]).is_err());
        assert!(SecurityContext::new(1, vec!["".into()]).is_err());
        assert!(SecurityContext::new(1, vec!["abc/".into()]).is_err());
        assert!(SecurityContext::new(1, vec!["1/abc/2/".into()]).is_err());
        assert!(SecurityContext::new(1, vec!["99999999999999999999999999999/".into()]).is_err());
        assert!(SecurityContext::new(1, vec!["-1/".into()]).is_err());
    }

    #[test]
    fn single_path_uses_starts_with() {
        let expr = build_path_filter("u", &["42/43/"]);
        assert!(matches!(expr, Expr::FuncCall { name, .. } if name == "startsWith"));
    }

    #[test]
    fn multiple_paths_uses_prefix_and_or_starts_with() {
        let expr = build_path_filter("u", &["1/2/4/", "1/2/5/"]);
        assert!(matches!(expr, Expr::BinaryOp { op: Op::And, .. }));
    }

    #[test]
    fn many_paths_uses_or_chain() {
        let paths: Vec<String> = (0..200u64).map(|i| format!("1/{i}/")).collect();
        let refs: Vec<&str> = paths.iter().map(|s| s.as_str()).collect();
        let expr = build_path_filter("e", &refs);
        let dbg = format!("{expr:?}");
        assert!(
            !dbg.contains("arrayExists"),
            "large path sets should use OR chain, not arrayExists: {dbg}"
        );
        assert!(
            dbg.contains("startsWith"),
            "should produce startsWith predicates: {dbg}"
        );
    }

    #[test]
    fn empty_paths_produces_false_literal() {
        let expr = build_path_filter("v", &[]);
        // Literal false guarantees zero rows for this alias. Using a literal
        // (not a parameterized Bool) lets ClickHouse constant-fold it at plan
        // time, avoiding full edge scans on denied entities.
        assert!(matches!(expr, Expr::Literal(Value::Bool(false))));
    }

    #[test]
    fn paths_at_least_keeps_matching_roles() {
        let sc = SecurityContext::new_with_roles(
            1,
            vec![
                TraversalPath::new("1/100/", 20),
                TraversalPath::new("1/101/", 30),
            ],
        )
        .unwrap();
        assert_eq!(sc.paths_at_least(20), vec!["1/100/", "1/101/"]);
        assert_eq!(sc.paths_at_least(30), vec!["1/101/"]);
        assert!(sc.paths_at_least(50).is_empty());
    }

    #[test]
    fn empty_access_levels_are_invalid() {
        assert!(
            SecurityContext::new_with_roles(
                1,
                vec![TraversalPath::with_access_levels("1/100/", vec![])]
            )
            .is_err()
        );
    }

    fn ontology_with_sm_vulnerability() -> Ontology {
        Ontology::new()
            .with_nodes(["Project", "Vulnerability"])
            .with_redaction("Vulnerability", "vulnerabilities", "id")
            .with_redaction_role("Vulnerability", ontology::RequiredRole::SecurityManager)
    }

    fn starts_with_paths_for_alias(expr: &Expr, alias: &str) -> Vec<String> {
        let mut paths = Vec::new();
        collect_starts_with_paths(expr, alias, &mut paths);
        paths
    }

    fn collect_starts_with_paths(expr: &Expr, alias: &str, paths: &mut Vec<String>) {
        match expr {
            Expr::FuncCall { name, args } if name == "startsWith" && args.len() == 2 => {
                if let (
                    Expr::Column { table, column },
                    Expr::Param {
                        value: Value::String(path),
                        ..
                    },
                ) = (&args[0], &args[1])
                    && table == alias
                    && column == TRAVERSAL_PATH_COLUMN
                {
                    paths.push(path.clone());
                }

                for arg in args {
                    collect_starts_with_paths(arg, alias, paths);
                }
            }
            Expr::FuncCall { args, .. } => {
                for arg in args {
                    collect_starts_with_paths(arg, alias, paths);
                }
            }
            Expr::BinaryOp { left, right, .. } => {
                collect_starts_with_paths(left, alias, paths);
                collect_starts_with_paths(right, alias, paths);
            }
            Expr::UnaryOp { expr, .. } => collect_starts_with_paths(expr, alias, paths),
            Expr::InSubquery { expr, .. } | Expr::InSelect { expr, .. } => {
                collect_starts_with_paths(expr, alias, paths)
            }
            Expr::Lambda { body, .. } => collect_starts_with_paths(body, alias, paths),
            Expr::Identifier(_)
            | Expr::Column { .. }
            | Expr::Literal(_)
            | Expr::Param { .. }
            | Expr::Star => {}
        }
    }

    fn has_partition_predicate_for(expr: &Expr, alias: &str) -> bool {
        fn refs_alias_column(e: &Expr, alias: &str) -> bool {
            match e {
                Expr::Column { table, .. } => table == alias,
                Expr::FuncCall { args, .. } => args.iter().any(|a| refs_alias_column(a, alias)),
                Expr::BinaryOp { left, right, .. } => {
                    refs_alias_column(left, alias) || refs_alias_column(right, alias)
                }
                _ => false,
            }
        }
        match expr {
            Expr::FuncCall { name, args } if name == "modulo" => {
                args.iter().any(|a| refs_alias_column(a, alias))
                    || args.iter().any(|a| has_partition_predicate_for(a, alias))
            }
            Expr::FuncCall { args, .. } => {
                args.iter().any(|a| has_partition_predicate_for(a, alias))
            }
            Expr::BinaryOp { left, right, .. } => {
                has_partition_predicate_for(left, alias)
                    || has_partition_predicate_for(right, alias)
            }
            Expr::UnaryOp { expr, .. }
            | Expr::InSubquery { expr, .. }
            | Expr::InSelect { expr, .. } => has_partition_predicate_for(expr, alias),
            Expr::Lambda { body, .. } => has_partition_predicate_for(body, alias),
            _ => false,
        }
    }

    // Paths tagged at Developer (30) still qualify because 30 >= the
    // Security Manager floor (25).
    #[test]
    fn per_entity_role_scoping_filters_vulnerability_alias() {
        let ctx = SecurityContext::new_with_roles(
            1,
            vec![
                TraversalPath::new("1/100/", 20), // Reporter
                TraversalPath::new("1/101/", 30), // Developer (covers SM)
            ],
        )
        .unwrap();

        let ontology = ontology_with_sm_vulnerability();

        let mut node = Node::Query(Box::new(Query {
            select: vec![SelectExpr {
                expr: Expr::col("v", "id"),
                alias: None,
            }],
            from: TableRef::join(
                JoinType::Inner,
                TableRef::scan("gl_project", "p"),
                TableRef::scan("gl_vulnerability", "v"),
                Expr::eq(Expr::col("p", "id"), Expr::col("v", "project_id")),
            ),
            limit: Some(10),
            ..Default::default()
        }));

        apply_security_context(&mut node, &ctx, &ontology).unwrap();

        let Node::Query(q) = &node else {
            unreachable!()
        };
        let where_sql = format!("{:?}", q.where_clause);
        assert!(
            where_sql.contains("1/100/"),
            "Project alias must retain Reporter path '1/100/', got: {where_sql}"
        );
        assert!(
            where_sql.contains("1/101/"),
            "Project alias must retain Developer path '1/101/', got: {where_sql}"
        );
        assert_eq!(
            starts_with_paths_for_alias(q.where_clause.as_ref().unwrap(), "v"),
            vec!["1/101/".to_string()],
            "Vulnerability alias 'v' must only keep the higher-role path, got: {where_sql}"
        );
    }

    // Bool(false) for the protected alias is the predicate that closes the
    // aggregation-query oracle (see module docs).
    #[test]
    fn no_eligible_paths_compile_to_bool_false() {
        let ctx = SecurityContext::new_with_roles(
            1,
            vec![TraversalPath::new("1/100/", 20)], // Reporter only
        )
        .unwrap();

        let ontology = ontology_with_sm_vulnerability();

        let mut node = Node::Query(Box::new(Query {
            select: vec![SelectExpr {
                expr: Expr::col("v", "id"),
                alias: None,
            }],
            from: TableRef::scan("gl_vulnerability", "v"),
            limit: Some(10),
            ..Default::default()
        }));

        apply_security_context(&mut node, &ctx, &ontology).unwrap();

        let Node::Query(q) = &node else {
            unreachable!()
        };
        let where_sql = format!("{:?}", q.where_clause);
        assert!(
            !where_sql.contains("1/100/"),
            "no traversal path should be bound for Vulnerability, got: {where_sql}"
        );
        assert!(
            where_sql.contains("Bool") && where_sql.contains("false"),
            "where clause should compile to Bool(false) for empty path set, got: {where_sql}"
        );
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
    fn path_trie_subsumes_children() {
        let t = PathTrie::from_paths(&["1/100/", "1/100/200/", "1/100/201/"]);
        assert_eq!(t.to_minimal_prefixes(), vec!["1/100/"]);
    }

    #[test]
    fn path_trie_keeps_siblings() {
        let t = PathTrie::from_paths(&["1/100/", "1/200/"]);
        assert_eq!(t.to_minimal_prefixes(), vec!["1/100/", "1/200/"]);
    }

    #[test]
    fn path_trie_siblings_under_shared_parent() {
        let t = PathTrie::from_paths(&["1/100/200/", "1/100/201/", "1/100/202/", "1/200/300/"]);
        let result = t.to_minimal_prefixes();
        assert_eq!(result.len(), 4);
        assert!(result.contains(&"1/200/300/".to_string()));
    }

    #[test]
    fn path_trie_single_path() {
        let t = PathTrie::from_paths(&["1/100/"]);
        assert_eq!(t.to_minimal_prefixes(), vec!["1/100/"]);
    }

    #[test]
    fn path_trie_deduplicates() {
        let t = PathTrie::from_paths(&["1/100/", "1/100/", "1/200/"]);
        assert_eq!(t.to_minimal_prefixes(), vec!["1/100/", "1/200/"]);
    }

    #[test]
    fn path_trie_deep_subsumption() {
        let t = PathTrie::from_paths(&["1/", "1/100/", "1/100/200/", "1/100/200/300/"]);
        assert_eq!(t.to_minimal_prefixes(), vec!["1/"]);
    }

    #[test]
    fn path_trie_mixed_orgs() {
        let t = PathTrie::from_paths(&["1/100/", "2/100/"]);
        assert_eq!(t.to_minimal_prefixes(), vec!["1/100/", "2/100/"]);
    }

    #[test]
    fn path_trie_realistic_38_paths() {
        let mut paths: Vec<String> = (100..130).map(|i| format!("1/10/{i}/")).collect();
        paths.extend((200..208).map(|i| format!("1/{i}/")));
        let refs: Vec<&str> = paths.iter().map(|s| s.as_str()).collect();
        let t = PathTrie::from_paths(&refs);
        let result = t.to_minimal_prefixes();
        assert_eq!(result.len(), 38);
    }

    #[test]
    fn path_trie_parent_collapses_many_children() {
        let mut paths = vec!["1/10/"];
        let children: Vec<String> = (100..130).map(|i| format!("1/10/{i}/")).collect();
        let refs: Vec<&str> = children.iter().map(|s| s.as_str()).collect();
        paths.extend(refs);
        let t = PathTrie::from_paths(&paths);
        assert_eq!(t.to_minimal_prefixes(), vec!["1/10/"]);
    }

    #[test]
    #[should_panic(expected = "empty path")]
    fn path_trie_empty_path_panics_in_debug() {
        PathTrie::from_paths(&[""]);
    }

    #[test]
    fn trie_collapse_after_role_filtering() {
        use crate::types::TraversalPath;
        let ctx = SecurityContext::new_with_roles(
            1,
            vec![
                TraversalPath::new(String::from("1/100/"), 20),
                TraversalPath::new(String::from("1/100/200/"), 20),
                TraversalPath::new(String::from("1/100/200/"), 30),
                TraversalPath::new(String::from("1/300/"), 30),
            ],
        )
        .unwrap();

        let eligible = ctx.paths_at_least(20);
        assert_eq!(eligible.len(), 4);

        let collapsed = PathTrie::from_paths(&eligible).to_minimal_prefixes();
        assert_eq!(collapsed, vec!["1/100/", "1/300/"]);

        let filter = build_path_filter("t", &eligible);
        let sql = format!("{filter:?}");
        assert!(
            sql.contains("startsWith"),
            "should produce startsWith predicates: {sql}"
        );
    }

    #[test]
    fn inject_adds_security_to_simple_query() {
        let ctx = SecurityContext::new(42, vec!["42/43/".into()]).unwrap();
        let mut node = simple_query();
        apply_security_context(&mut node, &ctx, &Ontology::new()).unwrap();
        assert!(matches!(node, Node::Query(q) if q.where_clause.is_some()));
    }

    #[test]
    fn inject_filters_edge_table() {
        let ctx = SecurityContext::new(42, vec!["42/43/".into()]).unwrap();
        let mut node = Node::Query(Box::new(Query {
            select: vec![SelectExpr {
                expr: Expr::col("e", "id"),
                alias: None,
            }],
            from: TableRef::scan(EDGE_TABLE, "e"),
            ..Default::default()
        }));

        apply_security_context(&mut node, &ctx, &Ontology::new()).unwrap();
        assert!(matches!(node, Node::Query(q) if q.where_clause.is_some()));
    }

    #[test]
    fn inject_includes_edge_table() {
        let from = TableRef::join(
            JoinType::Inner,
            TableRef::scan("gl_project", "p"),
            TableRef::scan(EDGE_TABLE, "e"),
            Expr::eq(Expr::col("p", "id"), Expr::col("e", "source")),
        );

        let aliases = collect_node_aliases(&from);
        assert_eq!(aliases, vec!["p", "e"]);
    }

    #[test]
    fn inject_skips_user_table() {
        // User visibility is determined through MEMBER_OF, not traversal path
        let from = TableRef::join(
            JoinType::Inner,
            TableRef::scan("gl_user", "u"),
            TableRef::scan("gl_merge_request", "mr"),
            Expr::lit(true),
        );

        let aliases = collect_node_aliases(&from);
        assert_eq!(aliases, vec!["mr"]);
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
    fn union_aliases_are_not_collected() {
        let from = TableRef::union_all(
            vec![Query {
                select: vec![SelectExpr {
                    expr: Expr::col("e", "source_id"),
                    alias: None,
                }],
                from: TableRef::scan(EDGE_TABLE, "e"),
                ..Default::default()
            }],
            "hop_e0",
        );
        let aliases = collect_node_aliases(&from);
        assert!(aliases.is_empty());
    }

    #[test]
    fn inject_recurses_into_union_from_arms() {
        let ctx = SecurityContext::new(42, vec!["42/43/".into()]).unwrap();
        let mut node = Node::Query(Box::new(Query {
            select: vec![SelectExpr {
                expr: Expr::col("outer_e", "source_id"),
                alias: None,
            }],
            from: TableRef::join(
                JoinType::Inner,
                TableRef::scan(EDGE_TABLE, "outer_e"),
                TableRef::union_all(
                    vec![Query {
                        select: vec![SelectExpr {
                            expr: Expr::col("e1", "source_id"),
                            alias: None,
                        }],
                        from: TableRef::scan(EDGE_TABLE, "e1"),
                        where_clause: None,
                        ..Default::default()
                    }],
                    "hop_e0",
                ),
                Expr::lit(true),
            ),
            where_clause: None,
            ..Default::default()
        }));

        apply_security_context(&mut node, &ctx, &Ontology::new()).unwrap();

        let Node::Query(q) = &node else {
            unreachable!()
        };
        assert!(
            q.where_clause.is_some(),
            "outer query should have security filter on outer_e"
        );

        if let TableRef::Join { right, .. } = &q.from {
            if let TableRef::Union { queries, .. } = right.as_ref() {
                assert!(
                    queries[0].where_clause.is_some(),
                    "UNION ALL arm should have security filter applied"
                );
            } else {
                panic!("expected Union");
            }
        } else {
            panic!("expected Join");
        }
    }

    #[test]
    fn scope_prefix_replaces_broad_on_scoped_alias() {
        let mut prefixes = std::collections::HashMap::new();
        prefixes.insert("p".to_string(), "1/24/23/".to_string());
        let ctx = SecurityContext::new(1, vec!["1/".into()])
            .unwrap()
            .with_scope_prefixes(prefixes);

        let mut node = Node::Query(Box::new(Query {
            select: vec![SelectExpr {
                expr: Expr::col("p", "id"),
                alias: None,
            }],
            from: TableRef::join(
                JoinType::Inner,
                TableRef::scan("gl_project", "p"),
                TableRef::scan("gl_work_item", "wi"),
                Expr::eq(Expr::col("p", "id"), Expr::col("wi", "project_id")),
            ),
            limit: Some(10),
            ..Default::default()
        }));

        let ontology = Ontology::new().with_path_scopable_nodes(["Project", "WorkItem"]);
        apply_security_context(&mut node, &ctx, &ontology).unwrap();

        let Node::Query(q) = &node else {
            unreachable!()
        };
        let where_clause = q.where_clause.as_ref().unwrap();
        assert_eq!(
            starts_with_paths_for_alias(where_clause, "p"),
            vec!["1/24/23/".to_string()],
            "scoped alias is injected with the tight prefix as its only auth filter"
        );
        assert_eq!(
            starts_with_paths_for_alias(where_clause, "wi"),
            vec!["1/".to_string()],
            "unscoped alias gets the broad authz set"
        );
    }

    fn project_scan() -> Node {
        Node::Query(Box::new(Query {
            select: vec![SelectExpr {
                expr: Expr::col("p", "id"),
                alias: None,
            }],
            from: TableRef::scan("gl_project", "p"),
            limit: Some(10),
            ..Default::default()
        }))
    }

    fn scoped_ctx(prefix: Option<&str>) -> SecurityContext {
        let ctx = SecurityContext::new(1, vec!["1/".into()]).unwrap();
        match prefix {
            Some(p) => {
                let mut prefixes = std::collections::HashMap::new();
                prefixes.insert("p".to_string(), p.to_string());
                ctx.with_scope_prefixes(prefixes)
            }
            None => ctx,
        }
    }

    fn project_where_clause(prefix: Option<&str>) -> Expr {
        let mut node = project_scan();
        apply_security_context(
            &mut node,
            &scoped_ctx(prefix),
            &Ontology::load_embedded().unwrap(),
        )
        .unwrap();
        let Node::Query(q) = node else { unreachable!() };
        q.where_clause.unwrap()
    }

    #[test]
    fn partition_predicate_emitted_for_pinned_namespace_prefix() {
        assert!(has_partition_predicate_for(
            &project_where_clause(Some("1/100/1000/")),
            "p"
        ));
    }

    #[test]
    fn partition_predicate_absent_for_org_only_prefix() {
        assert!(!has_partition_predicate_for(
            &project_where_clause(Some("1/")),
            "p"
        ));
    }

    #[test]
    fn partition_predicate_absent_without_scope_prefix() {
        assert!(!has_partition_predicate_for(
            &project_where_clause(None),
            "p"
        ));
    }

    #[test]
    fn scope_prefix_below_role_floor_keeps_broad() {
        let ontology = Ontology::load_embedded().unwrap();
        let mut prefixes = std::collections::HashMap::new();
        prefixes.insert("v".to_string(), "1/100/200/".to_string());
        let ctx = SecurityContext::new_with_roles(1, vec![TraversalPath::new("1/100/", 20)])
            .unwrap()
            .with_scope_prefixes(prefixes);

        let mut node = Node::Query(Box::new(Query {
            select: vec![SelectExpr {
                expr: Expr::col("v", "id"),
                alias: None,
            }],
            from: TableRef::scan("gl_vulnerability", "v"),
            limit: Some(10),
            ..Default::default()
        }));
        apply_security_context(&mut node, &ctx, &ontology).unwrap();

        let Node::Query(q) = &node else {
            unreachable!()
        };
        let where_sql = format!("{:?}", q.where_clause);
        assert!(
            where_sql.contains("Bool") && where_sql.contains("false"),
            "a prefix below the entity role floor must keep the role-filtered (dead) broad filter: {where_sql}"
        );
    }

    #[test]
    fn scope_prefix_dropped_on_non_path_scopable_alias() {
        let mut prefixes = std::collections::HashMap::new();
        prefixes.insert("g".to_string(), "1/24/23/".to_string());
        let ctx = SecurityContext::new(1, vec!["1/".into()])
            .unwrap()
            .with_scope_prefixes(prefixes);

        let ontology = Ontology::new().with_nodes(["Global"]);

        let mut node = Node::Query(Box::new(Query {
            select: vec![SelectExpr {
                expr: Expr::col("g", "id"),
                alias: None,
            }],
            from: TableRef::scan("gl_global", "g"),
            limit: Some(10),
            ..Default::default()
        }));

        apply_security_context(&mut node, &ctx, &ontology).unwrap();

        let Node::Query(q) = &node else {
            unreachable!()
        };
        let where_clause = q.where_clause.as_ref().unwrap();
        assert_eq!(
            starts_with_paths_for_alias(where_clause, "g"),
            vec!["1/".to_string()],
            "non-path-scopable alias must drop scope_prefix and keep broad authz only"
        );
    }

    #[test]
    fn inject_recurses_into_union_all_arms() {
        let ctx = SecurityContext::new(42, vec!["42/43/".into()]).unwrap();
        let mut node = Node::Query(Box::new(Query {
            select: vec![SelectExpr {
                expr: Expr::col("u", "id"),
                alias: None,
            }],
            from: TableRef::scan("gl_project", "u"),
            where_clause: None,
            union_all: vec![Query {
                select: vec![SelectExpr {
                    expr: Expr::col("p", "id"),
                    alias: None,
                }],
                from: TableRef::scan("gl_project", "p"),
                where_clause: None,
                ..Default::default()
            }],
            ..Default::default()
        }));

        apply_security_context(&mut node, &ctx, &Ontology::new()).unwrap();

        let Node::Query(q) = &node else {
            unreachable!()
        };
        assert!(
            q.where_clause.is_some(),
            "base query should have security filter"
        );
        assert_eq!(q.union_all.len(), 1);
        assert!(
            q.union_all[0].where_clause.is_some(),
            "UNION ALL arm should have security filter"
        );
    }
}
