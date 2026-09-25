use query_data_model::QueryAuthorizationCatalog;
use std::collections::HashMap;

use ontology::TraversalPathKind;
use ontology::constants::{DELETED_COLUMN, TRAVERSAL_PATH_COLUMN, VERSION_COLUMN};

use crate::ast::{ChType, Expr, Op, Query, SelectExpr, TableRef};
use crate::input::{FilterOp, Input, InputFilter, InputNode, QueryType};

const LOOKUP_ALIAS: &str = "_scope";
const UNRESOLVED_PATH: &str = "0/";
const MAX_LOOKUPS_PER_ALIAS: usize = 8;

#[derive(Debug, Clone, PartialEq)]
pub struct ScopeProof(Vec<ScopeSource>);

#[derive(Debug, Clone, PartialEq, Eq)]
enum ScopeSource {
    Literal(String),
    Lookup {
        source_table: String,
        key_column: String,
        value: PathScopeId,
    },
}

impl ScopeProof {
    pub fn literal(path: &str) -> Self {
        Self(vec![ScopeSource::Literal(path.to_string())])
    }
}

pub fn scope_predicate(proof: &ScopeProof, alias: &str) -> Expr {
    let values: Vec<Expr> = proof.0.iter().map(scope_value_expr).collect();
    let matches = values.iter().map(|path| {
        Some(Expr::func(
            "startsWith",
            vec![Expr::col(alias, TRAVERSAL_PATH_COLUMN), path.clone()],
        ))
    });
    let unresolved = values
        .iter()
        .map(|path| Some(Expr::eq(path.clone(), Expr::string(UNRESOLVED_PATH))));
    Expr::or_all(matches.chain(unresolved)).expect("scope proof has at least one source")
}

pub fn resolved_scope_guard(proof: &ScopeProof) -> Expr {
    Expr::and_all(proof.0.iter().map(|source| {
        Some(Expr::binary(
            Op::Ne,
            scope_value_expr(source),
            Expr::string(UNRESOLVED_PATH),
        ))
    }))
    .expect("scope proof has at least one source")
}

fn scope_value_expr(source: &ScopeSource) -> Expr {
    match source {
        ScopeSource::Literal(path) => Expr::string(path),
        ScopeSource::Lookup {
            source_table,
            key_column,
            value,
        } => lookup_expr(source_table, key_column, value),
    }
}

pub fn derive_scope_proofs(
    input: &Input,
    model: &(impl query_data_model::QueryDataModel + ?Sized),
) -> HashMap<String, ScopeProof> {
    if !matches!(
        input.query_type,
        QueryType::Traversal | QueryType::Aggregation
    ) {
        return HashMap::new();
    }
    let anchor_fks: Vec<_> = model
        .query_authorization()
        .anchor_foreign_keys()
        .iter()
        .map(|(column, entity)| (column.as_str(), model.graph().entity(*entity).name.as_str()))
        .collect();
    let seed: HashMap<String, ScopeProof> = input
        .nodes
        .iter()
        .filter_map(|node| {
            let lookups: Vec<ScopeSource> = scope_keys(node, &anchor_fks)
                .into_iter()
                .filter_map(|key| {
                    model.traversal_path_lookup(&key.entity, key.kind).map(
                        |(source_table, key_column)| ScopeSource::Lookup {
                            source_table: source_table.to_string(),
                            key_column: key_column.to_string(),
                            value: key.value,
                        },
                    )
                })
                .collect();
            (1..=MAX_LOOKUPS_PER_ALIAS)
                .contains(&lookups.len())
                .then(|| (node.id.clone(), ScopeProof(lookups)))
        })
        .collect();
    propagate_scope_proofs(input, model, &seed)
}

fn scope_preserving(
    model: &(impl query_data_model::QueryDataModel + ?Sized),
    relationship: &str,
    source: &str,
    target: &str,
) -> bool {
    model
        .variant_scope(relationship, source, target)
        .is_some_and(ontology::EdgeVariantScope::is_scope_preserving)
}

fn propagate_scope_proofs(
    input: &Input,
    model: &(impl query_data_model::QueryDataModel + ?Sized),
    seed: &HashMap<String, ScopeProof>,
) -> HashMap<String, ScopeProof> {
    use std::collections::HashSet;

    if seed.is_empty() {
        return HashMap::new();
    }
    let edges = scope_edges(input);
    let preserving: Vec<bool> = edges
        .iter()
        .map(|edge| {
            edge.types
                .iter()
                .all(|kind| scope_preserving(model, kind, edge.source_kind, edge.target_kind))
        })
        .collect();
    let mut tainted = HashSet::new();
    loop {
        let mut changed = false;
        for (index, edge) in edges.iter().enumerate() {
            if preserving[index] {
                continue;
            }
            for (from, to) in [(edge.from, edge.to), (edge.to, edge.from)] {
                if (seed.contains_key(from) || tainted.contains(from)) && tainted.insert(to) {
                    changed = true;
                }
            }
        }
        if !changed {
            break;
        }
    }
    let mut result = seed.clone();
    loop {
        let mut changed = false;
        for (index, edge) in edges.iter().enumerate() {
            if !preserving[index] {
                continue;
            }
            let next = match (result.get(edge.from).cloned(), result.get(edge.to).cloned()) {
                (Some(proof), None) if !tainted.contains(edge.to) => {
                    Some((edge.to.to_string(), proof))
                }
                (None, Some(proof)) if !tainted.contains(edge.from) => {
                    Some((edge.from.to_string(), proof))
                }
                _ => None,
            };
            if let Some((alias, proof)) = next {
                result.insert(alias, proof);
                changed = true;
            }
        }
        if !changed {
            break;
        }
    }
    result
}

fn lookup_expr(source_table: &str, key_column: &str, value: &PathScopeId) -> Expr {
    let key = match value {
        PathScopeId::Numeric(id) => Expr::param(ChType::Int64, *id),
        PathScopeId::Text(text) => Expr::param(ChType::String, text.clone()),
    };
    let latest = |column: &str| {
        Expr::func(
            "argMaxOrNull",
            vec![
                Expr::col(LOOKUP_ALIAS, column),
                Expr::col(LOOKUP_ALIAS, VERSION_COLUMN),
            ],
        )
    };
    let path = Expr::func(
        "coalesce",
        vec![
            Expr::func(
                "if",
                vec![
                    latest(DELETED_COLUMN),
                    Expr::Literal(serde_json::Value::Null),
                    latest(TRAVERSAL_PATH_COLUMN),
                ],
            ),
            Expr::string(UNRESOLVED_PATH),
        ],
    );
    Expr::Scalar(Box::new(Query {
        select: vec![SelectExpr::new(path, TRAVERSAL_PATH_COLUMN)],
        from: TableRef::scan(source_table, LOOKUP_ALIAS),
        where_clause: Some(Expr::eq(Expr::col(LOOKUP_ALIAS, key_column), key)),
        ..Default::default()
    }))
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum PathScopeId {
    Numeric(i64),
    Text(String),
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct PathResolutionKey {
    pub entity: String,
    pub kind: TraversalPathKind,
    pub value: PathScopeId,
}

impl PathResolutionKey {
    pub fn id(entity: impl Into<String>, id: i64) -> Self {
        Self {
            entity: entity.into(),
            kind: TraversalPathKind::Id,
            value: PathScopeId::Numeric(id),
        }
    }

    pub fn full_path(entity: impl Into<String>, value: impl Into<String>) -> Self {
        Self {
            entity: entity.into(),
            kind: TraversalPathKind::FullPath,
            value: PathScopeId::Text(value.into()),
        }
    }
}

pub fn scope_keys(node: &InputNode, anchor_fks: &[(&str, &str)]) -> Vec<PathResolutionKey> {
    let Some(entity) = node.entity.as_deref() else {
        return Vec::new();
    };

    let mut keys = Vec::new();
    if node.node_ids.len() > 1 {
        for &id in &node.node_ids {
            keys.push(PathResolutionKey::id(entity, id));
        }
    } else if let Some(id) = single_id(node) {
        keys.push(PathResolutionKey::id(entity, id));
    }
    if let Some(value) = single_full_path(node) {
        keys.push(PathResolutionKey::full_path(entity, value));
    }
    for (column, anchor) in anchor_fks {
        if let Some(id) = single_eq_id(node, column) {
            keys.push(PathResolutionKey::id(*anchor, id));
        }
    }
    keys
}
pub fn is_scope_only(node: &InputNode) -> bool {
    if scope_keys(node, &[]).len() != 1 || node.id_range.is_some() || node.node_ids.len() > 1 {
        return false;
    }
    let anchor_filters = single_full_path(node).is_some() as usize
        + (node.node_ids.is_empty() && single_id(node).is_some()) as usize;
    node.filters.len() == anchor_filters
}

fn single_id(node: &InputNode) -> Option<i64> {
    if node.node_ids.len() == 1 {
        return Some(node.node_ids[0]);
    }
    if !node.node_ids.is_empty() {
        return None;
    }
    eq_value(node.filters.get("id")?)?.as_i64()
}

fn single_full_path(node: &InputNode) -> Option<String> {
    eq_value(node.filters.get("full_path")?)?
        .as_str()
        .map(str::to_string)
}

fn single_eq_id(node: &InputNode, column: &str) -> Option<i64> {
    eq_value(node.filters.get(column)?)?.as_i64()
}

fn entity_of<'a>(input: &'a Input, alias: &str) -> &'a str {
    input
        .nodes
        .iter()
        .find(|n| n.id == alias)
        .and_then(|n| n.entity.as_deref())
        .unwrap_or("")
}
struct ScopeEdge<'a> {
    from: &'a str,
    to: &'a str,
    types: &'a [String],
    source_kind: &'a str,
    target_kind: &'a str,
}

fn scope_edges(input: &Input) -> Vec<ScopeEdge<'_>> {
    input
        .relationships
        .iter()
        .map(|r| ScopeEdge {
            from: &r.from,
            to: &r.to,
            types: &r.types,
            source_kind: entity_of(input, &r.from),
            target_kind: entity_of(input, &r.to),
        })
        .collect()
}

fn eq_value(filters: &[InputFilter]) -> Option<&serde_json::Value> {
    let [filter] = filters else { return None };
    match filter.op {
        None | Some(FilterOp::Eq) => filter.value.as_ref(),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    const ANCHOR_FKS: &[(&str, &str)] = &[("project_id", "Project"), ("group_id", "Group")];

    fn project_node(id: &str) -> InputNode {
        InputNode {
            id: id.to_string(),
            entity: Some("Project".to_string()),
            node_ids: vec![42],
            ..Default::default()
        }
    }

    #[test]
    fn node_ids_single_yields_id_key() {
        assert_eq!(
            scope_keys(&project_node("p"), ANCHOR_FKS),
            vec![PathResolutionKey::id("Project", 42)]
        );
    }

    #[test]
    fn multi_id_yields_one_key_per_id_for_lcp_fold() {
        let mut node = project_node("p");
        node.node_ids = vec![1, 2, 3];
        assert_eq!(
            scope_keys(&node, ANCHOR_FKS),
            vec![
                PathResolutionKey::id("Project", 1),
                PathResolutionKey::id("Project", 2),
                PathResolutionKey::id("Project", 3),
            ]
        );
    }

    #[test]
    fn bare_full_path_shorthand_yields_full_path_key() {
        let mut node = InputNode {
            id: "p".to_string(),
            entity: Some("Project".to_string()),
            ..Default::default()
        };
        node.filters.insert(
            "full_path".to_string(),
            vec![InputFilter {
                op: None,
                value: Some(json!("group/project")),
                ..Default::default()
            }],
        );
        assert_eq!(
            scope_keys(&node, ANCHOR_FKS),
            vec![PathResolutionKey::full_path("Project", "group/project")]
        );
    }

    #[test]
    fn eq_full_path_filter_yields_full_path_key() {
        let mut node = InputNode {
            id: "p".to_string(),
            entity: Some("Project".to_string()),
            ..Default::default()
        };
        node.filters.insert(
            "full_path".to_string(),
            vec![InputFilter {
                op: Some(FilterOp::Eq),
                value: Some(json!("group/project")),
                ..Default::default()
            }],
        );
        assert_eq!(
            scope_keys(&node, ANCHOR_FKS),
            vec![PathResolutionKey::full_path("Project", "group/project")]
        );
    }

    #[test]
    fn entityless_node_yields_no_key() {
        let node = InputNode {
            id: "p".to_string(),
            entity: None,
            node_ids: vec![42],
            ..Default::default()
        };
        assert!(scope_keys(&node, ANCHOR_FKS).is_empty());
    }

    fn node_with_filter(entity: &str, column: &str, value: serde_json::Value) -> InputNode {
        let mut node = InputNode {
            id: "n".to_string(),
            entity: Some(entity.to_string()),
            ..Default::default()
        };
        node.filters.insert(
            column.to_string(),
            vec![InputFilter {
                op: Some(FilterOp::Eq),
                value: Some(value),
                ..Default::default()
            }],
        );
        node
    }

    #[test]
    fn project_id_filter_resolves_to_project_anchor() {
        let node = node_with_filter("MergeRequest", "project_id", json!(278964));
        assert_eq!(
            scope_keys(&node, ANCHOR_FKS),
            vec![PathResolutionKey::id("Project", 278964)]
        );
    }

    #[test]
    fn group_id_filter_resolves_to_group_anchor() {
        let node = node_with_filter("MergeRequest", "group_id", json!(9970));
        assert_eq!(
            scope_keys(&node, ANCHOR_FKS),
            vec![PathResolutionKey::id("Group", 9970)]
        );
    }
    #[test]
    fn project_id_anchor_survives_sibling_filters() {
        let mut node = node_with_filter("MergeRequest", "project_id", json!(278964));
        node.filters.insert(
            "state".to_string(),
            vec![InputFilter {
                op: Some(FilterOp::Eq),
                value: Some(json!("merged")),
                ..Default::default()
            }],
        );
        node.filters.insert(
            "merged_at".to_string(),
            vec![InputFilter {
                op: Some(FilterOp::Gte),
                value: Some(json!("2026-03-05T00:00:00Z")),
                ..Default::default()
            }],
        );
        assert_eq!(
            scope_keys(&node, ANCHOR_FKS),
            vec![PathResolutionKey::id("Project", 278964)]
        );
    }

    #[test]
    fn project_id_in_list_yields_no_anchor() {
        let mut node = InputNode {
            id: "n".to_string(),
            entity: Some("MergeRequest".to_string()),
            ..Default::default()
        };
        node.filters.insert(
            "project_id".to_string(),
            vec![InputFilter {
                op: Some(FilterOp::In),
                value: Some(json!([1, 2, 3])),
                ..Default::default()
            }],
        );
        assert!(scope_keys(&node, ANCHOR_FKS).is_empty());
    }

    #[test]
    fn scope_edges_carries_endpoint_entity_kinds() {
        let input = crate::parse_input(
            r#"{"query_type":"traversal","nodes":[{"id":"mr","entity":"MergeRequest"},{"id":"diff","entity":"MergeRequestDiff"}],"relationships":[{"type":"HAS_DIFF","from":"mr","to":"diff"}],"limit":1}"#,
        )
        .unwrap();
        let edges = scope_edges(&input);
        assert_eq!(edges.len(), 1);
        assert_eq!(edges[0].from, "mr");
        assert_eq!(edges[0].to, "diff");
        assert_eq!(edges[0].source_kind, "MergeRequest");
        assert_eq!(edges[0].target_kind, "MergeRequestDiff");
    }
}
