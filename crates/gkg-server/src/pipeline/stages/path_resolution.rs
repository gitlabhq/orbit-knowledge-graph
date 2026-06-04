use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use query_engine::compiler::{
    DEFAULT_PATH_ACCESS_LEVEL, PathResolutionKey, QueryType, scope_keys, validate_normalize,
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

        let mut wanted = Vec::new();
        let mut seen = HashSet::new();
        let mut aliases_by_key: HashMap<PathResolutionKey, Vec<String>> = HashMap::new();
        for node in &input.nodes {
            for key in scope_keys(node) {
                if ctx
                    .ontology
                    .traversal_path_lookup(&key.entity, key.kind)
                    .is_none()
                {
                    continue;
                }
                aliases_by_key
                    .entry(key.clone())
                    .or_default()
                    .push(node.id.clone());
                if seen.insert(key.clone()) {
                    wanted.push(key);
                }
            }
        }

        // resolve_batch returns a cached prefix that can lag a namespace transfer's
        // re-stamp; gating on is_descendant keeps a stale or wrong prefix from widening
        // past authorized scope, leaving only a benign, self-healing under-prune within it.
        let mut scope_prefixes = HashMap::new();
        for (key, path) in resolver.resolve_batch(&wanted).await {
            if let Some(path) = path
                && is_descendant(&path, &authorized)
            {
                for alias in aliases_by_key.get(&key).into_iter().flatten() {
                    scope_prefixes.insert(alias.clone(), path.clone());
                }
            }
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

    fn ontology_keys(node: &InputNode, ontology: &Ontology) -> Vec<PathResolutionKey> {
        scope_keys(node)
            .into_iter()
            .filter(|k| ontology.traversal_path_lookup(&k.entity, k.kind).is_some())
            .collect()
    }

    #[test]
    fn project_node_ids_yields_project_scope() {
        let n = node(
            "Project",
            r#"{"query_type": "traversal", "node": {"id": "p", "entity": "Project", "node_ids": [42]}, "limit": 1}"#,
        );
        let keys = ontology_keys(&n, &ontology());
        assert_eq!(keys, vec![PathResolutionKey::id("Project", 42)]);
    }

    #[test]
    fn project_eq_id_filter_yields_project_scope() {
        let n = node(
            "Project",
            r#"{"query_type": "traversal", "node": {"id": "p", "entity": "Project", "filters": {"id": {"op": "eq", "value": 7}}}, "limit": 1}"#,
        );
        let keys = ontology_keys(&n, &ontology());
        assert_eq!(keys[0], PathResolutionKey::id("Project", 7));
        assert_eq!(keys[0].value, PathScopeId::Numeric(7));
    }

    #[test]
    fn group_node_ids_yields_group_scope() {
        let n = node(
            "Group",
            r#"{"query_type": "traversal", "node": {"id": "g", "entity": "Group", "node_ids": [9]}, "limit": 1}"#,
        );
        let keys = ontology_keys(&n, &ontology());
        assert_eq!(keys[0], PathResolutionKey::id("Group", 9));
    }

    #[test]
    fn full_path_filter_resolves_against_prefixed_source_table() {
        let o = ontology();
        let n = node(
            "Project",
            r#"{"query_type": "traversal", "node": {"id": "p", "entity": "Project", "filters": {"full_path": {"op": "eq", "value": "group/project"}}}, "limit": 1}"#,
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
    fn multi_id_is_skipped() {
        let n = node(
            "Project",
            r#"{"query_type": "traversal", "node": {"id": "p", "entity": "Project", "node_ids": [1, 2, 3]}, "limit": 1}"#,
        );
        assert!(ontology_keys(&n, &ontology()).is_empty());
    }

    #[test]
    fn non_scoping_entity_is_skipped() {
        let n = node(
            "User",
            r#"{"query_type": "traversal", "node": {"id": "u", "entity": "User", "node_ids": [1]}, "limit": 1}"#,
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
        // intra-subtree move (caller authorized over the shared parent): stale prefix
        // still injects, so the only fallout is a transient under-prune, never a widen.
        assert!(is_descendant("1/255/273/292/", &authorized));
        // cross-namespace move where the caller only gained access to the new tree:
        // the stale old-tree prefix is rejected, leaving a clean fallback to the id filter.
        assert!(!is_descendant("1/700/701/292/", &authorized));
    }

    #[test]
    fn batch_collects_one_key_per_distinct_scope() {
        let o = ontology();
        let project = node(
            "Project",
            r#"{"query_type": "traversal", "node": {"id": "p", "entity": "Project", "node_ids": [42]}, "limit": 1}"#,
        );
        let group = node(
            "Group",
            r#"{"query_type": "traversal", "node": {"id": "g", "entity": "Group", "node_ids": [9]}, "limit": 1}"#,
        );

        let mut wanted = Vec::new();
        let mut seen = HashSet::new();
        for n in [&project, &group, &project] {
            for key in scope_keys(n) {
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
            r#"{"query_type": "traversal", "node": {"id": "p", "entity": "Project", "node_ids": [1]}, "limit": 1}"#
        )));
        assert!(scopes_query_type(qt(
            r#"{"query_type": "aggregation", "nodes": [{"id": "p", "entity": "Project", "node_ids": [1]}], "group_by": [{"kind": "node", "node": "p"}], "aggregations": [{"function": "count", "target": "p"}], "limit": 1}"#
        )));
        assert!(!scopes_query_type(qt(
            r#"{"query_type": "neighbors", "node": {"id": "p", "entity": "Project", "node_ids": [1]}, "neighbors": {"node": "p", "direction": "both"}}"#
        )));
        assert!(!scopes_query_type(qt(
            r#"{"query_type": "path_finding", "nodes": [{"id": "p", "entity": "Project", "node_ids": [1]}, {"id": "wi", "entity": "WorkItem", "node_ids": [9]}], "path": {"type": "shortest", "from": "p", "to": "wi", "max_depth": 3}}"#
        )));
    }

    #[tokio::test]
    async fn missing_resolver_is_noop() {
        use query_engine::compiler::SecurityContext;
        use query_engine::pipeline::{NoOpObserver, TypeMap};

        let mut ctx = QueryPipelineContext {
            query_json: r#"{"query_type": "traversal", "node": {"id": "p", "entity": "Project", "node_ids": [42]}, "limit": 1}"#.to_string(),
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
