use std::collections::{HashMap, HashSet};

mod application;
mod preparation;

pub use application::apply;
pub use preparation::prepare_bound;

#[derive(Clone, Default)]
pub struct QueryScope {
    nodes: HashMap<String, ScopeProof>,
    relationships: Vec<Option<ScopeProof>>,
    requirements: Vec<ScopeProof>,
    table_scans: HashSet<String>,
}

impl QueryScope {
    pub fn table_scans(&self) -> &HashSet<String> {
        &self.table_scans
    }
}

use ontology::TraversalPathKind;
use ontology::constants::{DELETED_COLUMN, TRAVERSAL_PATH_COLUMN, VERSION_COLUMN};

use crate::ast::{Expr, Function, Op, Query, SelectExpr, SqlType, TableRef};
use crate::query::{Direction, FilterOp, QueryType};

const LOOKUP_ALIAS: &str = "_scope";
const UNRESOLVED_PATH: &str = "0/";
const MAX_LOOKUPS_PER_ALIAS: usize = 8;

#[derive(Debug, Clone, PartialEq)]
pub struct ScopeProof {
    sources: Vec<ScopeSource>,
    depth: Option<(u32, u32)>,
}

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
        Self::from_sources(vec![ScopeSource::Literal(path.to_string())])
    }

    fn from_sources(sources: Vec<ScopeSource>) -> Self {
        Self {
            sources,
            depth: None,
        }
    }

    pub fn is_single_source(&self) -> bool {
        self.sources.len() == 1
    }

    #[must_use]
    pub fn with_depth(mut self, min: u32, max: u32) -> Self {
        self.depth = Some((min, max));
        self
    }
}

pub fn scope_predicate(proof: &ScopeProof, alias: &str) -> Expr {
    let values: Vec<Expr> = proof.sources.iter().map(scope_value_expr).collect();
    let matches = values.iter().map(|path| {
        let column = Expr::col(alias, TRAVERSAL_PATH_COLUMN);
        Some(match proof.depth {
            Some((0, 0)) => Expr::eq(column, path.clone()),
            Some((min, max)) => Expr::and(
                Expr::func(Function::StartsWith, vec![column.clone(), path.clone()]),
                depth_between(column, path, min, max),
            ),
            None => Expr::func(Function::StartsWith, vec![column, path.clone()]),
        })
    });
    let unresolved = values
        .iter()
        .map(|path| Some(Expr::eq(path.clone(), Expr::string(UNRESOLVED_PATH))));
    Expr::or_all(matches.chain(unresolved)).expect("scope proof has at least one source")
}

pub fn resolved_scope_guard(proof: &ScopeProof) -> Expr {
    Expr::and_all(proof.sources.iter().map(|source| {
        Some(Expr::binary(
            Op::Ne,
            scope_value_expr(source),
            Expr::string(UNRESOLVED_PATH),
        ))
    }))
    .expect("scope proof has at least one source")
}

fn depth_between(column: Expr, path: &Expr, min: u32, max: u32) -> Expr {
    let segments =
        |expr: Expr| Expr::func(Function::CountSubstrings, vec![expr, Expr::string("/")]);
    let depth = segments(column);
    let base = segments(path.clone());
    let bound = |hops: u32| Expr::binary(Op::Add, base.clone(), Expr::int(i64::from(hops)));
    Expr::and(
        Expr::binary(Op::Ge, depth.clone(), bound(min)),
        Expr::binary(Op::Le, depth, bound(max)),
    )
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

pub fn derive_bound_scope_proofs(
    query: &crate::query::Query,
    model: &(impl query_data_model::OrbitQueryModel + ?Sized),
) -> HashMap<String, ScopeProof> {
    if !matches!(
        query.query_type,
        QueryType::Traversal | QueryType::Aggregation
    ) {
        return HashMap::new();
    }
    let anchor_fks: Vec<_> = model
        .anchor_foreign_keys()
        .iter()
        .map(|(column, entity)| (column.as_str(), model.graph().entity(*entity).name.as_str()))
        .collect();
    let seed: HashMap<String, ScopeProof> = query
        .nodes
        .iter()
        .filter_map(|node| {
            let lookups: Vec<_> = bound_scope_keys(node, &anchor_fks)
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
                .then(|| (node.alias.clone(), ScopeProof::from_sources(lookups)))
        })
        .collect();
    propagate_bound_scope_proofs(query, model, &seed)
}

fn propagate_bound_scope_proofs(
    query: &crate::query::Query,
    model: &(impl query_data_model::OrbitQueryModel + ?Sized),
    seed: &HashMap<String, ScopeProof>,
) -> HashMap<String, ScopeProof> {
    if seed.is_empty() {
        return HashMap::new();
    }
    let edges: Vec<_> = query
        .relationships
        .iter()
        .map(|relationship| {
            let (from, to) = match relationship.direction {
                Direction::Incoming => (relationship.to, relationship.from),
                Direction::Outgoing | Direction::Both => (relationship.from, relationship.to),
            };
            let source = query.node(from);
            let target = query.node(to);
            let scope_preserving = relationship.names.iter().all(|kind| {
                model
                    .variant_scope(kind, &source.entity_name, &target.entity_name)
                    .is_some_and(ontology::EdgeVariantScope::is_scope_preserving)
            });
            let propagates_to_source = relationship.names.iter().all(|kind| {
                model
                    .variant_scope(kind, &source.entity_name, &target.entity_name)
                    .is_some_and(|scope| scope.propagates_to_source(kind, &source.entity_name))
            });
            (
                source.alias.as_str(),
                target.alias.as_str(),
                scope_preserving,
                propagates_to_source,
            )
        })
        .collect();
    let mut tainted = HashSet::new();
    loop {
        let mut changed = false;
        for (from, to, scope_preserving, _) in &edges {
            if *scope_preserving {
                continue;
            }
            for (from, to) in [(*from, *to), (*to, *from)] {
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
        for (from, to, scope_preserving, propagates_to_source) in &edges {
            if !scope_preserving {
                continue;
            }
            let next = match (result.get(*from).cloned(), result.get(*to).cloned()) {
                (Some(proof), None) if !tainted.contains(*to) => Some(((*to).to_string(), proof)),
                (None, Some(proof)) if *propagates_to_source && !tainted.contains(*from) => {
                    Some(((*from).to_string(), proof))
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

pub(super) fn bound_scope_keys(
    node: &crate::query::Node,
    anchor_fks: &[(&str, &str)],
) -> Vec<PathResolutionKey> {
    let mut keys = Vec::new();
    if !node.node_ids.is_empty() {
        keys.extend(
            node.node_ids
                .iter()
                .map(|id| PathResolutionKey::id(&node.entity_name, *id)),
        );
    } else if let Some(id) = bound_eq_value(node, "id").and_then(serde_json::Value::as_i64) {
        keys.push(PathResolutionKey::id(&node.entity_name, id));
    }
    if let Some(path) = bound_eq_value(node, "full_path").and_then(serde_json::Value::as_str) {
        keys.push(PathResolutionKey::full_path(&node.entity_name, path));
    }
    for (column, entity) in anchor_fks {
        if let Some(id) = bound_eq_value(node, column).and_then(serde_json::Value::as_i64) {
            keys.push(PathResolutionKey::id(*entity, id));
        }
    }
    keys
}

fn bound_eq_value<'a>(node: &'a crate::query::Node, name: &str) -> Option<&'a serde_json::Value> {
    let mut filters = node.filters(name);
    let filter = filters.next()?;
    if filters.next().is_some() || filter.predicate.op != FilterOp::Eq {
        return None;
    }
    filter.predicate.value.as_ref()
}

fn lookup_expr(source_table: &str, key_column: &str, value: &PathScopeId) -> Expr {
    let (key, from) = match value {
        PathScopeId::Numeric(id) => (
            Expr::param(SqlType::Int64, *id),
            TableRef::scan(source_table, LOOKUP_ALIAS),
        ),
        PathScopeId::Text(text) => (
            Expr::param(SqlType::String, text.clone()),
            TableRef::scan_final(source_table, LOOKUP_ALIAS),
        ),
    };
    let latest = |column: &str| {
        Expr::func(
            Function::ArgMaxOrNull,
            vec![
                Expr::col(LOOKUP_ALIAS, column),
                Expr::col(LOOKUP_ALIAS, VERSION_COLUMN),
            ],
        )
    };
    let path = Expr::func(
        Function::Coalesce,
        vec![
            Expr::func(
                Function::If,
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
        from,
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
