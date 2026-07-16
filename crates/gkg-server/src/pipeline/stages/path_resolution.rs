use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use query_engine::compiler::{
    DEFAULT_PATH_ACCESS_LEVEL, PathResolutionKey, QueryType, scope_edges, scope_keys,
    validate_normalize,
};
use query_engine::pipeline::{
    PipelineError, PipelineObserver, PipelineStage, QueryPipelineContext,
};

use crate::pipeline::path_resolver::PathResolver;

#[derive(Clone)]
pub struct PathResolutionStage;

impl PipelineStage for PathResolutionStage {
    type Input = ();
    type Output = ();

    async fn execute(
        &self,
        ctx: &mut QueryPipelineContext,
        obs: &mut dyn PipelineObserver,
    ) -> Result<Self::Output, PipelineError> {
        let Some(resolver) = ctx.server_extensions.get::<Arc<PathResolver>>() else {
            return Ok(());
        };
        let resolver = Arc::clone(resolver);

        let security_context = ctx
            .security_context()
            .inspect_err(|e| obs.record_error(e))?;
        let authorized = security_context.paths_at_least(DEFAULT_PATH_ACCESS_LEVEL);
        if authorized.is_empty() {
            return Ok(());
        }

        let input = validate_normalize(&ctx.query_json, &ctx.ontology).map_err(|e| {
            PipelineError::Compile {
                client_safe: e.is_client_safe(),
                message: e.to_string(),
            }
        })?;

        if !scopes_query_type(input.query_type) {
            return Ok(());
        }

        let anchor_fks = ctx.ontology.anchor_fk_mappings();
        let mut wanted = Vec::new();
        let mut seen = HashSet::new();
        let mut keys_by_alias: HashMap<String, Vec<PathResolutionKey>> = HashMap::new();
        for node in &input.nodes {
            for key in scope_keys(node, &anchor_fks) {
                if ctx
                    .ontology
                    .traversal_path_lookup(&key.entity, key.kind)
                    .is_none()
                {
                    continue;
                }
                keys_by_alias
                    .entry(node.id.clone())
                    .or_default()
                    .push(key.clone());
                if seen.insert(key.clone()) {
                    wanted.push(key);
                }
            }
        }

        // resolve_batch returns a cached prefix that can lag a namespace transfer's
        // re-stamp; gating on is_descendant keeps a stale or wrong prefix from widening
        // past authorized scope, leaving only a benign, self-healing under-prune within it.
        let resolved = resolver.resolve_batch(&wanted).await;
        let mut scope_prefixes = HashMap::new();
        for (alias, keys) in keys_by_alias {
            let paths: Vec<String> = keys
                .iter()
                .filter_map(|k| resolved.get(k).cloned().flatten())
                .collect();
            if paths.is_empty() || paths.len() < keys.len() {
                continue;
            }
            let Some(prefix) = longest_common_path_prefix(&paths) else {
                continue;
            };
            if is_descendant(&prefix, &authorized) {
                scope_prefixes.insert(alias, prefix);
            }
        }

        // Flood each resolved prefix to scope-preserving-reachable nodes (e.g. a
        // MergeRequest's diffs and diff files, or a Project's work items) so
        // their node-table scans inherit the anchor PK prefix too, not just the
        // directly-pinned node. The ontology taint walk skips any alias reachable
        // through a cross-namespace edge. Edge scans are scoped separately in the
        // compiler's restrict pass.
        if !scope_prefixes.is_empty() {
            let edges = scope_edges(&input);
            scope_prefixes = ctx
                .ontology
                .propagate_scope_prefixes(&edges, &scope_prefixes);
        }

        if !scope_prefixes.is_empty()
            && let Some(sc) = ctx.security_context.take()
        {
            ctx.security_context = Some(sc.with_scope_prefixes(scope_prefixes));
        }
        Ok(())
    }
}

fn is_descendant(resolved: &str, authorized: &[&str]) -> bool {
    authorized.iter().any(|auth| resolved.starts_with(auth))
}

fn longest_common_path_prefix(paths: &[String]) -> Option<String> {
    let (first, rest) = paths.split_first()?;
    let mut common: &str = first;
    for p in rest {
        let n = common
            .bytes()
            .zip(p.bytes())
            .take_while(|(a, b)| a == b)
            .count();
        common = &common[..n];
    }
    common
        .rfind('/')
        .map(|i| common[..=i].to_string())
        .filter(|s| !s.is_empty())
}

/// Only Traversal/Aggregation scope a node to a tight prefix. Neighbors and
/// path_finding keep cross-namespace reach through the broad authz filter alone.
fn scopes_query_type(query_type: QueryType) -> bool {
    matches!(query_type, QueryType::Traversal | QueryType::Aggregation)
}

#[cfg(test)]
mod tests {
    use super::*;
    use ontology::{Ontology, TraversalPathKind};
    use query_engine::compiler::{InputNode, PathResolutionKey, PathScopeId, parse_input};

    fn ontology() -> Ontology {
        Ontology::load_embedded()
            .unwrap()
            .with_schema_version_prefix("v51_")
    }

    fn node(entity: &str, json: &str) -> InputNode {
        let input = parse_input(json).unwrap();
        input
            .nodes
            .into_iter()
            .find(|n| n.entity.as_deref() == Some(entity))
            .unwrap()
    }

    #[test]
    fn flood_reaches_diff_and_file_with_embedded_ontology() {
        use std::collections::HashMap;
        let json = r#"{
            "query_type": "traversal",
            "nodes": [
                {"id": "mr", "entity": "MergeRequest", "filters": {"project_id": {"op": "eq", "value": 278964}}},
                {"id": "diff", "entity": "MergeRequestDiff"},
                {"id": "df", "entity": "MergeRequestDiffFile"}
            ],
            "relationships": [
                {"type": "HAS_DIFF", "from": "mr", "to": "diff"},
                {"type": "HAS_FILE", "from": "diff", "to": "df"}
            ],
            "limit": 50
        }"#;
        let input = parse_input(json).unwrap();
        let seed = HashMap::from([("mr".to_string(), "1/9970/15846663/".to_string())]);
        let got = ontology().propagate_scope_prefixes(&scope_edges(&input), &seed);
        assert_eq!(
            got.get("diff").map(String::as_str),
            Some("1/9970/15846663/")
        );
        assert_eq!(got.get("df").map(String::as_str), Some("1/9970/15846663/"));
    }

    #[test]
    fn flood_reaches_diff_and_file_via_has_latest_diff() {
        use std::collections::HashMap;
        let json = r#"{
            "query_type": "traversal",
            "nodes": [
                {"id": "mr", "entity": "MergeRequest", "node_ids": [492857469]},
                {"id": "diff", "entity": "MergeRequestDiff"},
                {"id": "df", "entity": "MergeRequestDiffFile"}
            ],
            "relationships": [
                {"type": "HAS_LATEST_DIFF", "from": "mr", "to": "diff"},
                {"type": "HAS_FILE", "from": "diff", "to": "df"}
            ],
            "limit": 80
        }"#;
        let input = parse_input(json).unwrap();
        let seed = HashMap::from([("mr".to_string(), "1/9970/120946322/122873006/".to_string())]);
        let got = ontology().propagate_scope_prefixes(&scope_edges(&input), &seed);
        assert_eq!(
            got.get("diff").map(String::as_str),
            Some("1/9970/120946322/122873006/")
        );
        assert_eq!(
            got.get("df").map(String::as_str),
            Some("1/9970/120946322/122873006/")
        );
    }

    #[test]
    fn merge_request_node_ids_yield_mr_scope() {
        let n = node(
            "MergeRequest",
            r#"{"query_type": "traversal", "nodes": [{"id": "mr", "entity": "MergeRequest", "node_ids": [492857469, 492764321]}], "limit": 1}"#,
        );
        assert_eq!(
            ontology_keys(&n, &ontology()),
            vec![
                PathResolutionKey::id("MergeRequest", 492857469),
                PathResolutionKey::id("MergeRequest", 492764321),
            ]
        );
    }

    #[test]
    fn flood_keeps_distinct_anchor_prefixes_separate() {
        use std::collections::HashMap;
        let json = r#"{
            "query_type": "traversal",
            "nodes": [
                {"id": "mr_a", "entity": "MergeRequest", "filters": {"project_id": {"op": "eq", "value": 1000}}},
                {"id": "diff_a", "entity": "MergeRequestDiff"},
                {"id": "mr_b", "entity": "MergeRequest", "filters": {"project_id": {"op": "eq", "value": 1001}}},
                {"id": "diff_b", "entity": "MergeRequestDiff"}
            ],
            "relationships": [
                {"type": "HAS_DIFF", "from": "mr_a", "to": "diff_a"},
                {"type": "HAS_DIFF", "from": "mr_b", "to": "diff_b"}
            ],
            "limit": 50
        }"#;
        let input = parse_input(json).unwrap();
        let seed = HashMap::from([
            ("mr_a".to_string(), "1/100/1000/".to_string()),
            ("mr_b".to_string(), "1/101/1001/".to_string()),
        ]);
        let got = ontology().propagate_scope_prefixes(&scope_edges(&input), &seed);
        assert_eq!(got.get("mr_a").map(String::as_str), Some("1/100/1000/"));
        assert_eq!(got.get("diff_a").map(String::as_str), Some("1/100/1000/"));
        assert_eq!(got.get("mr_b").map(String::as_str), Some("1/101/1001/"));
        assert_eq!(got.get("diff_b").map(String::as_str), Some("1/101/1001/"));
    }

    fn ontology_keys(node: &InputNode, ontology: &Ontology) -> Vec<PathResolutionKey> {
        scope_keys(node, &ontology.anchor_fk_mappings())
            .into_iter()
            .filter(|k| ontology.traversal_path_lookup(&k.entity, k.kind).is_some())
            .collect()
    }

    #[test]
    fn project_node_ids_yields_project_scope() {
        let n = node(
            "Project",
            r#"{"query_type": "traversal", "nodes": [{"id": "p", "entity": "Project", "node_ids": [42]}], "limit": 1}"#,
        );
        let keys = ontology_keys(&n, &ontology());
        assert_eq!(keys, vec![PathResolutionKey::id("Project", 42)]);
    }

    #[test]
    fn project_eq_id_filter_yields_project_scope() {
        let n = node(
            "Project",
            r#"{"query_type": "traversal", "nodes": [{"id": "p", "entity": "Project", "filters": {"id": {"op": "eq", "value": 7}}}], "limit": 1}"#,
        );
        let keys = ontology_keys(&n, &ontology());
        assert_eq!(keys[0], PathResolutionKey::id("Project", 7));
        assert_eq!(keys[0].value, PathScopeId::Numeric(7));
    }

    #[test]
    fn group_node_ids_yields_group_scope() {
        let n = node(
            "Group",
            r#"{"query_type": "traversal", "nodes": [{"id": "g", "entity": "Group", "node_ids": [9]}], "limit": 1}"#,
        );
        let keys = ontology_keys(&n, &ontology());
        assert_eq!(keys[0], PathResolutionKey::id("Group", 9));
    }

    #[test]
    fn full_path_filter_resolves_against_prefixed_source_table() {
        let o = ontology();
        let n = node(
            "Project",
            r#"{"query_type": "traversal", "nodes": [{"id": "p", "entity": "Project", "filters": {"full_path": {"op": "eq", "value": "group/project"}}}], "limit": 1}"#,
        );
        let keys = ontology_keys(&n, &o);
        assert_eq!(
            keys[0],
            PathResolutionKey::full_path("Project", "group/project")
        );
        assert_eq!(
            o.traversal_path_lookup("Project", TraversalPathKind::FullPath)
                .unwrap()
                .source_table,
            "v51_gl_project"
        );
    }

    #[test]
    fn multi_id_yields_one_key_per_id() {
        let n = node(
            "Project",
            r#"{"query_type": "traversal", "nodes": [{"id": "p", "entity": "Project", "node_ids": [1, 2, 3]}], "limit": 1}"#,
        );
        assert_eq!(
            ontology_keys(&n, &ontology()),
            vec![
                PathResolutionKey::id("Project", 1),
                PathResolutionKey::id("Project", 2),
                PathResolutionKey::id("Project", 3),
            ]
        );
    }

    #[test]
    fn lcp_collapses_sibling_projects_to_shared_group() {
        let paths = vec!["1/9970/15846663/".to_string(), "1/9970/18/".to_string()];
        assert_eq!(
            longest_common_path_prefix(&paths),
            Some("1/9970/".to_string())
        );
    }

    #[test]
    fn lcp_is_segment_aligned_not_byte_aligned() {
        let paths = vec!["1/9970/15846663/".to_string(), "1/9971/".to_string()];
        assert_eq!(longest_common_path_prefix(&paths), Some("1/".to_string()));
    }

    #[test]
    fn lcp_returns_none_when_paths_share_no_segment() {
        let paths = vec!["1/9970/".to_string(), "2/100/".to_string()];
        assert_eq!(longest_common_path_prefix(&paths), None);
    }

    #[test]
    fn lcp_identity_for_single_path() {
        let paths = vec!["1/9970/15846663/".to_string()];
        assert_eq!(
            longest_common_path_prefix(&paths),
            Some("1/9970/15846663/".to_string())
        );
    }

    #[test]
    fn non_scoping_entity_is_skipped() {
        let n = node(
            "User",
            r#"{"query_type": "traversal", "nodes": [{"id": "u", "entity": "User", "node_ids": [1]}], "limit": 1}"#,
        );
        assert!(ontology_keys(&n, &ontology()).is_empty());
    }

    #[test]
    fn injection_only_when_resolved_within_authorized_and_never_widens() {
        assert!(is_descendant("1/22/", &["1/22/"]));
        assert!(is_descendant("1/22/333/", &["1/22/"]));
        assert!(!is_descendant("1/99/", &["1/22/"]));
        assert!(!is_descendant("9/9/", &["1/22/"]));
        assert!(!is_descendant("1/22/", &["1/22/333/"]));
    }

    #[test]
    fn stale_prefix_after_move_cannot_widen_past_authorized() {
        // A transfer re-stamps rows while the cache may still resolve the pre-move
        // prefix. is_descendant confines that to a benign under-prune within authorized
        // scope and rejects any stale prefix the caller is not already authorized over.
        let authorized = ["1/255/"];
        assert!(is_descendant("1/255/273/292/", &authorized));
        assert!(!is_descendant("1/700/701/292/", &authorized));
    }

    #[test]
    fn batch_collects_one_key_per_distinct_scope() {
        let o = ontology();
        let project = node(
            "Project",
            r#"{"query_type": "traversal", "nodes": [{"id": "p", "entity": "Project", "node_ids": [42]}], "limit": 1}"#,
        );
        let group = node(
            "Group",
            r#"{"query_type": "traversal", "nodes": [{"id": "g", "entity": "Group", "node_ids": [9]}], "limit": 1}"#,
        );

        let anchor_fks = o.anchor_fk_mappings();
        let mut wanted = Vec::new();
        let mut seen = HashSet::new();
        for n in [&project, &group, &project] {
            for key in scope_keys(n, &anchor_fks) {
                if o.traversal_path_lookup(&key.entity, key.kind).is_none()
                    || !seen.insert(key.clone())
                {
                    continue;
                }
                wanted.push(key);
            }
        }

        assert_eq!(
            wanted,
            vec![
                PathResolutionKey::id("Project", 42),
                PathResolutionKey::id("Group", 9),
            ]
        );
    }

    #[test]
    fn only_traversal_and_aggregation_scope_to_tight_prefix() {
        let qt = |json: &str| validate_normalize(json, &ontology()).unwrap().query_type;
        assert!(scopes_query_type(qt(
            r#"{"query_type": "traversal", "nodes": [{"id": "p", "entity": "Project", "node_ids": [1]}], "limit": 1}"#
        )));
        assert!(scopes_query_type(qt(
            r#"{"query_type": "aggregation", "nodes": [{"id": "p", "entity": "Project", "node_ids": [1]}], "group_by": [{"kind": "node", "node": "p"}], "aggregations": [{"function": "count", "target": "p"}], "limit": 1}"#
        )));
        assert!(!scopes_query_type(qt(
            r#"{"query_type": "neighbors", "nodes": [{"id": "p", "entity": "Project", "node_ids": [1]}], "neighbors": {"node": "p", "direction": "both"}}"#
        )));
        assert!(!scopes_query_type(qt(
            r#"{"query_type": "path_finding", "nodes": [{"id": "p", "entity": "Project", "node_ids": [1]}, {"id": "wi", "entity": "WorkItem", "node_ids": [9]}], "path": {"type": "shortest", "from": "p", "to": "wi", "max_depth": 3, "rel_types": ["CONTAINS"]}}"#
        )));
    }

    #[tokio::test]
    async fn missing_resolver_is_noop() {
        use query_engine::compiler::SecurityContext;
        use query_engine::pipeline::{NoOpObserver, TypeMap};

        let mut ctx = QueryPipelineContext {
            query_json: r#"{"query_type": "traversal", "nodes": [{"id": "p", "entity": "Project", "node_ids": [42]}], "limit": 1}"#.to_string(),
            compiled: None,
            ontology: Arc::new(Ontology::load_embedded().unwrap()),
            security_context: Some(SecurityContext::new(1, vec!["1/".into()]).unwrap()),
            server_extensions: TypeMap::default(),
            phases: TypeMap::default(),
        };
        let mut obs = NoOpObserver;

        PathResolutionStage
            .execute(&mut ctx, &mut obs)
            .await
            .unwrap();

        assert!(ctx.security_context.unwrap().scope_prefixes.is_empty());
    }
}
