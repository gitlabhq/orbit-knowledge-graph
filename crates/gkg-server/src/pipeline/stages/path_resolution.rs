use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;

use ontology::Ontology;
use query_engine::compiler::input::Input;
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

        let scope_prefixes = if gkg_server_config::features::enabled(
            gkg_server_config::features::Feature::TraversalPathPayloadScoping,
        ) && !scope_prefixes.is_empty()
        {
            propagate_scope_prefixes(&input, &scope_prefixes, &authorized, &ctx.ontology)
        } else {
            scope_prefixes
        };

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

#[derive(Clone, Copy, PartialEq, Eq)]
enum EdgeClass {
    ScopePreserving,
    NeverPropagate,
}

/// True when traversing a relationship `kind` between entities `a` and `b`
/// cannot leave the namespace subtree, so the anchor prefix can ride across it.
/// The pair test is unordered: the query author may write either endpoint as
/// `from`, and the ontology's containment variants are direction-agnostic for
/// prefix purposes. `triples` is the ontology-derived preserving set
/// (`Ontology::scope_preserving_edge_triples`); membership is the only signal.
fn is_scope_preserving(
    triples: &HashSet<(String, String, String)>,
    kind: &str,
    a: &str,
    b: &str,
) -> bool {
    triples.contains(&(kind.to_string(), a.to_string(), b.to_string()))
        || triples.contains(&(kind.to_string(), b.to_string(), a.to_string()))
}

/// Copies each anchor's resolved prefix `P` onto payload aliases connected to
/// the anchor by an all-scope-preserving, taint-free join path. Edge scans are
/// structurally absent from `input.nodes`, so they are never candidates.
fn propagate_scope_prefixes(
    input: &Input,
    anchors: &HashMap<String, String>,
    authorized: &[&str],
    ontology: &Ontology,
) -> HashMap<String, String> {
    if anchors.is_empty() {
        return anchors.clone();
    }

    let entity_by_alias: HashMap<&str, &str> = input
        .nodes
        .iter()
        .filter_map(|n| n.entity.as_deref().map(|e| (n.id.as_str(), e)))
        .collect();

    let preserving = ontology.scope_preserving_edge_triples();

    let mut adjacency: HashMap<&str, Vec<(&str, EdgeClass)>> = HashMap::new();
    for rel in &input.relationships {
        if rel.min_hops != 1 || rel.max_hops != 1 {
            // A multi-hop edge is dropped, never modeled as NeverPropagate: its far
            // endpoint stays broad-only (safe). The min/max==1 guard keeps a variable-hop
            // CONTAINS from being treated as a single scope-preserving hop between its
            // endpoints, which could skip an intervening cross-namespace link.
            continue;
        }
        let (Some(&from_entity), Some(&to_entity)) = (
            entity_by_alias.get(rel.from.as_str()),
            entity_by_alias.get(rel.to.as_str()),
        ) else {
            continue;
        };
        // AND over a multi-kind relationship: scope-preserving only if every listed
        // kind is, preserving the AND-over-paths guarantee within one hop.
        let class = if rel
            .types
            .iter()
            .all(|kind| is_scope_preserving(&preserving, kind, from_entity, to_entity))
        {
            EdgeClass::ScopePreserving
        } else {
            EdgeClass::NeverPropagate
        };
        adjacency
            .entry(rel.from.as_str())
            .or_default()
            .push((rel.to.as_str(), class));
        adjacency
            .entry(rel.to.as_str())
            .or_default()
            .push((rel.from.as_str(), class));
    }

    let anchor_aliases: HashSet<&str> = anchors.keys().map(String::as_str).collect();

    // Pass A: flood taint across all edges from every NeverPropagate endpoint, never
    // seeding or entering an anchor. Flooding only across bad edges would let a clean
    // edge launder taint and reopen the diamond hole.
    let mut tainted: HashSet<&str> = HashSet::new();
    let mut queue: VecDeque<&str> = VecDeque::new();
    for (&alias, neighbors) in &adjacency {
        for &(neighbor, class) in neighbors {
            if class == EdgeClass::NeverPropagate {
                for endpoint in [alias, neighbor] {
                    if !anchor_aliases.contains(endpoint) && tainted.insert(endpoint) {
                        queue.push_back(endpoint);
                    }
                }
            }
        }
    }
    while let Some(alias) = queue.pop_front() {
        for &(neighbor, _) in adjacency.get(alias).into_iter().flatten() {
            if !anchor_aliases.contains(neighbor) && tainted.insert(neighbor) {
                queue.push_back(neighbor);
            }
        }
    }

    // Pass B: BFS over ScopePreserving edges only, never entering a tainted alias.
    // `reached` maps an alias to the anchor whose P it carries; on contention we keep
    // the shallowest anchor prefix (never a re-derived intermediate).
    let mut result = anchors.clone();
    let mut reached: HashMap<&str, &str> = HashMap::new();
    let mut bfs: VecDeque<&str> = VecDeque::new();
    for alias in anchors.keys() {
        bfs.push_back(alias.as_str());
        reached.insert(alias.as_str(), alias.as_str());
    }
    while let Some(alias) = bfs.pop_front() {
        let carried_anchor = reached[alias];
        let candidate = anchors[carried_anchor].as_str();
        for &(neighbor, class) in adjacency.get(alias).into_iter().flatten() {
            if class != EdgeClass::ScopePreserving
                || tainted.contains(neighbor)
                || anchor_aliases.contains(neighbor)
            {
                continue;
            }
            match reached.get(neighbor) {
                Some(&existing) if !shallower(candidate, anchors[existing].as_str()) => {}
                _ => {
                    reached.insert(neighbor, carried_anchor);
                    bfs.push_back(neighbor);
                }
            }
        }
    }

    for (&alias, &anchor_alias) in &reached {
        if anchor_aliases.contains(alias) {
            continue;
        }
        let Some(&entity) = entity_by_alias.get(alias) else {
            continue;
        };
        // An anchor-type node's scope is its own resolved path, never a containment
        // neighbor's. An unresolved Project/Group reached over CONTAINS would otherwise
        // inherit a sibling/child anchor's deeper prefix and over-prune itself to zero rows.
        if !ontology.is_path_scopable(entity) || ontology.is_anchor(entity) {
            continue;
        }
        let prefix = &anchors[anchor_alias];
        // Defense-in-depth re-assert against the Reporter-floor authorized set. P is
        // gated at the looser role, the broad filter at the per-table floor, and the
        // emitted predicate ANDs them, so this can never re-admit an excluded row.
        if is_descendant(prefix, authorized) {
            result.insert(alias.to_string(), prefix.clone());
        }
    }

    result
}

/// True when `a` has strictly fewer path segments than `b`, tie-broken
/// lexicographically. Used to keep the shallowest anchor prefix on contention.
fn shallower(a: &str, b: &str) -> bool {
    let depth = |p: &str| p.matches('/').count();
    match depth(a).cmp(&depth(b)) {
        std::cmp::Ordering::Less => true,
        std::cmp::Ordering::Greater => false,
        std::cmp::Ordering::Equal => a < b,
    }
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

    fn parsed(json: &str) -> Input {
        validate_normalize(json, &ontology()).unwrap()
    }

    fn anchors(pairs: &[(&str, &str)]) -> HashMap<String, String> {
        pairs
            .iter()
            .map(|(a, p)| (a.to_string(), p.to_string()))
            .collect()
    }

    fn propagate(input: &Input, anchors: HashMap<String, String>) -> HashMap<String, String> {
        propagate_scope_prefixes(input, &anchors, &["1/22/"], &ontology())
    }

    #[test]
    fn anchor_node_never_inherits_a_containment_neighbors_prefix() {
        // CONTAINS is scope-preserving and unordered, so an unresolved parent Group, a
        // parent Group of a resolved Project, and a sibling Project under a shared parent
        // are all reachable from a resolved anchor. None may inherit that anchor's deeper
        // prefix: a Project/Group's traversal_path is its own identity, and a child/sibling
        // prefix would AND to zero rows on it. They must stay broad-only.
        let parent_group = parsed(
            r#"{"query_type": "traversal", "nodes": [
                {"id": "child", "entity": "Group", "node_ids": [55]},
                {"id": "parent", "entity": "Group"}],
                "relationships": [{"type": "CONTAINS", "from": "parent", "to": "child"}], "limit": 1}"#,
        );
        assert!(
            !propagate(&parent_group, anchors(&[("child", "1/22/55/")])).contains_key("parent")
        );

        let parent_of_project = parsed(
            r#"{"query_type": "traversal", "nodes": [
                {"id": "proj", "entity": "Project", "node_ids": [55]},
                {"id": "parent", "entity": "Group"}],
                "relationships": [{"type": "CONTAINS", "from": "parent", "to": "proj"}], "limit": 1}"#,
        );
        assert!(
            !propagate(&parent_of_project, anchors(&[("proj", "1/22/55/")])).contains_key("parent")
        );

        let sibling = parsed(
            r#"{"query_type": "traversal", "nodes": [
                {"id": "projA", "entity": "Project", "node_ids": [55]},
                {"id": "parent", "entity": "Group"},
                {"id": "projB", "entity": "Project"}],
                "relationships": [
                    {"type": "CONTAINS", "from": "parent", "to": "projA"},
                    {"type": "CONTAINS", "from": "parent", "to": "projB"}], "limit": 1}"#,
        );
        let out = propagate(&sibling, anchors(&[("projA", "1/22/55/")]));
        assert!(!out.contains_key("parent"));
        assert!(!out.contains_key("projB"));
    }

    #[test]
    fn cross_namespace_neighbors_stay_broad_only() {
        let epic = parsed(
            r#"{"query_type": "traversal", "nodes": [
                {"id": "epic", "entity": "WorkItem", "node_ids": [55]},
                {"id": "child", "entity": "WorkItem"}],
                "relationships": [{"type": "CONTAINS", "from": "epic", "to": "child"}], "limit": 1}"#,
        );
        assert!(!propagate(&epic, anchors(&[("epic", "1/22/55/")])).contains_key("child"));

        let wi = parsed(
            r#"{"query_type": "traversal", "nodes": [
                {"id": "wi", "entity": "WorkItem", "node_ids": [55]},
                {"id": "rel", "entity": "WorkItem"},
                {"id": "note", "entity": "Note"}],
                "relationships": [
                    {"type": "RELATED_TO", "from": "wi", "to": "rel"},
                    {"type": "HAS_NOTE", "from": "wi", "to": "note"}], "limit": 1}"#,
        );
        let out = propagate(&wi, anchors(&[("wi", "1/22/55/")]));
        assert!(!out.contains_key("rel"));
        assert!(!out.contains_key("note"));

        let mr = parsed(
            r#"{"query_type": "traversal", "nodes": [
                {"id": "mr", "entity": "MergeRequest", "node_ids": [55]},
                {"id": "iss", "entity": "WorkItem"},
                {"id": "src", "entity": "Project"}],
                "relationships": [
                    {"type": "CLOSES", "from": "mr", "to": "iss"},
                    {"type": "SOURCE_PROJECT", "from": "mr", "to": "src"}], "limit": 1}"#,
        );
        let out = propagate(&mr, anchors(&[("mr", "1/22/55/")]));
        assert!(!out.contains_key("iss"));
        assert!(!out.contains_key("src"));
    }

    #[test]
    fn in_project_and_in_group_are_scope_preserving() {
        let project = parsed(
            r#"{"query_type": "traversal", "nodes": [
                {"id": "p", "entity": "Project", "node_ids": [42]},
                {"id": "wi", "entity": "WorkItem"},
                {"id": "v", "entity": "Vulnerability"}],
                "relationships": [
                    {"type": "IN_PROJECT", "from": "wi", "to": "p"},
                    {"type": "IN_PROJECT", "from": "v", "to": "p"}], "limit": 1}"#,
        );
        let out = propagate(&project, anchors(&[("p", "1/22/55/")]));
        assert_eq!(out.get("wi").map(String::as_str), Some("1/22/55/"));
        assert_eq!(out.get("v").map(String::as_str), Some("1/22/55/"));

        let group = parsed(
            r#"{"query_type": "traversal", "nodes": [
                {"id": "g", "entity": "Group", "node_ids": [9]},
                {"id": "wi", "entity": "WorkItem"}],
                "relationships": [{"type": "IN_GROUP", "from": "wi", "to": "g"}], "limit": 1}"#,
        );
        let out = propagate(&group, anchors(&[("g", "1/22/")]));
        assert_eq!(out.get("wi").map(String::as_str), Some("1/22/"));
    }

    #[test]
    fn sibling_payload_inherits_via_shared_anchor() {
        let input = parsed(
            r#"{"query_type": "traversal", "nodes": [
                {"id": "p", "entity": "Project", "node_ids": [42]},
                {"id": "s", "entity": "SecurityScan"},
                {"id": "f", "entity": "Finding"}],
                "relationships": [
                    {"type": "IN_PROJECT", "from": "s", "to": "p"},
                    {"type": "IN_PROJECT", "from": "f", "to": "p"}], "limit": 1}"#,
        );
        let out = propagate(&input, anchors(&[("p", "1/22/55/")]));
        assert_eq!(out.get("s").map(String::as_str), Some("1/22/55/"));
        assert_eq!(out.get("f").map(String::as_str), Some("1/22/55/"));
    }

    #[test]
    fn payload_sibling_fk_alone_does_not_preserve() {
        // HAS_FINDING/OCCURRENCE_OF join two payload entities, neither a namespace
        // container, so they carry no prefix without an anchor edge. Strictly safe:
        // the reached payload keeps the broad authz filter.
        let input = parsed(
            r#"{"query_type": "traversal", "nodes": [
                {"id": "s", "entity": "SecurityScan", "node_ids": [55]},
                {"id": "f", "entity": "Finding"}],
                "relationships": [{"type": "HAS_FINDING", "from": "s", "to": "f"}], "limit": 1}"#,
        );
        let out = propagate(&input, anchors(&[("s", "1/22/55/")]));
        assert!(!out.contains_key("f"));
    }

    #[test]
    fn definition_inherits_via_defines_and_contains() {
        let input = parsed(
            r#"{"query_type": "traversal", "nodes": [
                {"id": "dir", "entity": "Directory", "node_ids": [55]},
                {"id": "fl", "entity": "File"},
                {"id": "d", "entity": "Definition"}],
                "relationships": [
                    {"type": "CONTAINS", "from": "dir", "to": "fl"},
                    {"type": "DEFINES", "from": "fl", "to": "d"}], "limit": 1}"#,
        );
        let out = propagate(&input, anchors(&[("dir", "1/22/55/")]));
        assert_eq!(out.get("fl").map(String::as_str), Some("1/22/55/"));
        assert_eq!(out.get("d").map(String::as_str), Some("1/22/55/"));
    }

    #[test]
    fn source_code_chain_from_project_anchor_scopes_every_hop() {
        // The e2e-proven shape: a Project anchor reaches source code through the
        // all-scope-preserving CONTAINS chain Project->Branch->Directory->File plus
        // DEFINES->Definition. Every code-graph entity carries the project's
        // traversal_path, so each hop inherits P.
        let input = parsed(
            r#"{"query_type": "traversal", "nodes": [
                {"id": "p", "entity": "Project", "node_ids": [42]},
                {"id": "b", "entity": "Branch"},
                {"id": "dir", "entity": "Directory"},
                {"id": "fl", "entity": "File"},
                {"id": "d", "entity": "Definition"}],
                "relationships": [
                    {"type": "CONTAINS", "from": "p", "to": "b"},
                    {"type": "CONTAINS", "from": "b", "to": "dir"},
                    {"type": "CONTAINS", "from": "dir", "to": "fl"},
                    {"type": "DEFINES", "from": "fl", "to": "d"}], "limit": 1}"#,
        );
        let out = propagate(&input, anchors(&[("p", "1/22/55/")]));
        assert_eq!(out.get("b").map(String::as_str), Some("1/22/55/"));
        assert_eq!(out.get("dir").map(String::as_str), Some("1/22/55/"));
        assert_eq!(out.get("fl").map(String::as_str), Some("1/22/55/"));
        assert_eq!(out.get("d").map(String::as_str), Some("1/22/55/"));
    }

    #[test]
    fn on_branch_taints_source_code_into_broad_only() {
        // ON_BRANCH (File/Directory->Branch) climbs the storage hierarchy, so it is not
        // derived as scope-preserving: scoping source code to a branch with ON_BRANCH
        // taints the File and floods to the clean Definition. Pure under-prune, but pinned
        // so the conservatism is explicit rather than incidental.
        let input = parsed(
            r#"{"query_type": "traversal", "nodes": [
                {"id": "dir", "entity": "Directory", "node_ids": [55]},
                {"id": "fl", "entity": "File"},
                {"id": "b", "entity": "Branch"},
                {"id": "d", "entity": "Definition"}],
                "relationships": [
                    {"type": "CONTAINS", "from": "dir", "to": "fl"},
                    {"type": "ON_BRANCH", "from": "fl", "to": "b"},
                    {"type": "DEFINES", "from": "fl", "to": "d"}], "limit": 1}"#,
        );
        let out = propagate(&input, anchors(&[("dir", "1/22/55/")]));
        assert!(!out.contains_key("fl"));
        assert!(!out.contains_key("d"));
    }

    #[test]
    fn workitem_contains_never_propagates() {
        for kind in ["CONTAINS", "RELATED_TO"] {
            let input = parsed(&format!(
                r#"{{"query_type": "traversal", "nodes": [
                    {{"id": "epic", "entity": "WorkItem", "node_ids": [55]}},
                    {{"id": "child", "entity": "WorkItem"}}],
                    "relationships": [{{"type": "{kind}", "from": "epic", "to": "child"}}], "limit": 1}}"#,
            ));
            let out = propagate(&input, anchors(&[("epic", "1/22/55/")]));
            assert!(!out.contains_key("child"), "{kind} must not propagate");
        }
    }

    #[test]
    fn imports_resolution_hop_does_not_propagate() {
        // ImportedSymbol is the cross-repo resolution boundary: a resolved Definition may
        // live in a different namespace, so neither IMPORTS variant preserves scope. Both
        // sym and the resolved d2 stay broad-only (documented conservative under-prune).
        let input = parsed(
            r#"{"query_type": "traversal", "nodes": [
                {"id": "fl", "entity": "File", "node_ids": [55]},
                {"id": "sym", "entity": "ImportedSymbol"},
                {"id": "d2", "entity": "Definition"}],
                "relationships": [
                    {"type": "IMPORTS", "from": "fl", "to": "sym"},
                    {"type": "IMPORTS", "from": "sym", "to": "d2"}], "limit": 1}"#,
        );
        let out = propagate(&input, anchors(&[("fl", "1/22/55/")]));
        assert!(!out.contains_key("sym"));
        assert!(!out.contains_key("d2"));
    }

    #[test]
    fn transitive_taint_floods_across_clean_edge() {
        let input = parsed(
            r#"{"query_type": "traversal", "nodes": [
                {"id": "dir", "entity": "Directory", "node_ids": [55]},
                {"id": "fl", "entity": "File"},
                {"id": "b", "entity": "Branch"},
                {"id": "d", "entity": "Definition"}],
                "relationships": [
                    {"type": "CONTAINS", "from": "dir", "to": "fl"},
                    {"type": "ON_BRANCH", "from": "fl", "to": "b"},
                    {"type": "DEFINES", "from": "fl", "to": "d"}], "limit": 1}"#,
        );
        // fl reaches the anchor dir over a clean CONTAINS edge, but the dirty ON_BRANCH
        // edge taints fl; Pass A then floods that taint across the clean DEFINES edge onto
        // d. Both stay broad-only, proving taint is transitive across clean edges and a
        // clean edge cannot launder it.
        let out = propagate(&input, anchors(&[("dir", "1/22/55/")]));
        assert!(!out.contains_key("fl"));
        assert!(!out.contains_key("d"));
    }

    #[test]
    fn clean_only_neighbor_scopes_while_dirty_touching_neighbor_does_not() {
        // Edge tables (gl_edge/gl_ci_edge/gl_code_edge) are relationships, never
        // input.nodes, so they are structurally absent from the walk. v touches only the
        // clean IN_PROJECT edge and scopes; o reaches p over a clean IN_PROJECT edge but
        // also touches the dirty HAS_NOTE edge, so Pass A taints it and it stays broad-only.
        let input = parsed(
            r#"{"query_type": "traversal", "nodes": [
                {"id": "p", "entity": "Project", "node_ids": [42]},
                {"id": "v", "entity": "Vulnerability"},
                {"id": "o", "entity": "WorkItem"},
                {"id": "n", "entity": "Note"}],
                "relationships": [
                    {"type": "IN_PROJECT", "from": "v", "to": "p"},
                    {"type": "IN_PROJECT", "from": "o", "to": "p"},
                    {"type": "HAS_NOTE", "from": "o", "to": "n"}], "limit": 1}"#,
        );
        let out = propagate(&input, anchors(&[("p", "1/22/55/")]));
        assert_eq!(
            out.get("v").map(String::as_str),
            Some("1/22/55/"),
            "the clean-only neighbor scopes"
        );
        assert!(!out.contains_key("o"), "o touches the dirty HAS_NOTE edge");
        assert!(!out.contains_key("n"));
    }

    #[test]
    fn empty_anchors_is_noop() {
        let input = parsed(
            r#"{"query_type": "traversal", "node": {"id": "s", "entity": "SecurityScan", "node_ids": [55]}, "limit": 1}"#,
        );
        assert!(propagate(&input, HashMap::new()).is_empty());
    }

    #[test]
    fn propagated_prefix_rejected_when_not_descendant() {
        let input = parsed(
            r#"{"query_type": "traversal", "nodes": [
                {"id": "p", "entity": "Project", "node_ids": [42]},
                {"id": "f", "entity": "Finding"}],
                "relationships": [{"type": "IN_PROJECT", "from": "f", "to": "p"}], "limit": 1}"#,
        );
        let out =
            propagate_scope_prefixes(&input, &anchors(&[("p", "1/99/")]), &["1/22/"], &ontology());
        assert!(!out.contains_key("f"));
    }

    #[test]
    fn security_payload_prefix_gated_at_reporter_not_security_manager() {
        let input = parsed(
            r#"{"query_type": "traversal", "nodes": [
                {"id": "p", "entity": "Project", "node_ids": [42]},
                {"id": "v", "entity": "Vulnerability"}],
                "relationships": [{"type": "IN_PROJECT", "from": "v", "to": "p"}], "limit": 1}"#,
        );
        let out = propagate_scope_prefixes(
            &input,
            &anchors(&[("p", "1/22/55/")]),
            &["1/22/"],
            &ontology(),
        );
        assert_eq!(out.get("v").map(String::as_str), Some("1/22/55/"));
    }

    #[test]
    fn project_anchor_scopes_in_project_payload() {
        // The dominant query shape: a Project anchor reaches its payload through the
        // namespace-of-record IN_PROJECT edge, which is derived as scope-preserving, so
        // the payload inherits P. HAS_FINDING is a payload-sibling join (not preserving),
        // so o scopes only because it sits on its own clean edge from v.
        let input = parsed(
            r#"{"query_type": "traversal", "nodes": [
                {"id": "p", "entity": "Project", "node_ids": [42]},
                {"id": "v", "entity": "Vulnerability"},
                {"id": "o", "entity": "VulnerabilityOccurrence"}],
                "relationships": [
                    {"type": "IN_PROJECT", "from": "v", "to": "p"},
                    {"type": "IN_PROJECT", "from": "o", "to": "p"}], "limit": 1}"#,
        );
        let out = propagate(&input, anchors(&[("p", "1/22/55/")]));
        assert_eq!(out.get("p").map(String::as_str), Some("1/22/55/"));
        assert_eq!(out.get("v").map(String::as_str), Some("1/22/55/"));
        assert_eq!(out.get("o").map(String::as_str), Some("1/22/55/"));
    }

    #[test]
    fn workitem_multihop_contains_absent() {
        let input = parsed(
            r#"{"query_type": "traversal", "nodes": [
                {"id": "p", "entity": "Project", "node_ids": [42]},
                {"id": "child", "entity": "WorkItem"}],
                "relationships": [{"type": "CONTAINS", "from": "p", "to": "child", "min_hops": 1, "max_hops": 3}], "limit": 1}"#,
        );
        let out = propagate(&input, anchors(&[("p", "1/22/55/")]));
        assert!(!out.contains_key("child"));
    }

    #[test]
    fn flag_defaults_off_so_execute_skips_propagation() {
        // features::init never runs in unit tests, so the OnceLock is empty and the flag
        // reads false. That is the exact condition under which execute() takes the else
        // branch and the anchor-only scope_prefixes flow through byte-identically to the
        // !1521 baseline. The propagation walk itself is exercised directly above.
        assert!(!gkg_server_config::features::enabled(
            gkg_server_config::features::Feature::TraversalPathPayloadScoping
        ));
    }
}
