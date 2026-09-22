//! Physical plan: a relational operator tree over ClickHouse tables.
//!
//! Every query type plans to a `PhysOp`. Traversal and aggregation go through
//! the optimizer (`optimize.rs`); neighbors, pathfinding, and hydration are
//! single-purpose shapes with nothing to rewrite and skip it. `lower_v2.rs`
//! turns the tree into SQL.

mod chain;
pub mod expr;
mod hydration;
mod neighbors;
mod pathfinding;

use crate::input::*;
use crate::{pe, pn};
use ontology::Ontology;
use ontology::constants::*;
use serde::Serialize;
use std::collections::{HashMap, HashSet};

// ── Join graph ──────────────────────────────────────────────────────────────

pub struct JoinGraph {
    by_kind: HashMap<String, JoinPath>,
}

#[derive(Clone)]
pub struct JoinPath {
    pub scope_preserving: bool,
    pub edge_table: String,
}

impl JoinGraph {
    pub fn build(ontology: &Ontology) -> Self {
        let mut by_kind = HashMap::new();
        for edge in ontology.edges() {
            by_kind
                .entry(edge.relationship_kind.clone())
                .or_insert(JoinPath {
                    scope_preserving: edge.scope.is_some_and(|s| s.is_scope_preserving()),
                    edge_table: edge.destination_table.clone(),
                });
        }
        Self { by_kind }
    }

    /// Every kind's edge carries the containing namespace's traversal path,
    /// so a scope prefix on the edge implies containment.
    pub fn scope_preserving(&self, rel_types: &[String]) -> bool {
        !rel_types.is_empty()
            && rel_types
                .iter()
                .all(|t| self.by_kind.get(t).is_some_and(|jp| jp.scope_preserving))
    }

    pub fn edge_table(&self, rel_types: &[String], default: &str) -> String {
        rel_types
            .iter()
            .find_map(|t| self.by_kind.get(t).map(|jp| jp.edge_table.clone()))
            .unwrap_or_else(|| default.to_string())
    }
}

// ── Expressions ─────────────────────────────────────────────────────────────

/// `(alias, column)`: a fully qualified column reference.
pub type Col = (String, String);

#[derive(Debug, Clone, PartialEq, Serialize)]
pub enum Lit {
    Int(i64),
    Str(String),
    Bool(bool),
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize)]
pub enum CmpOp {
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
}

/// Plan-level scalar expression. Every column reference is qualified, so an
/// expression means the same thing wherever it sits in the tree.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub enum PExpr {
    Col(String, String),
    /// Bare identifier: an output alias in ORDER BY, or a lambda parameter.
    Ident(String),
    Lit(Lit),
    Func(String, Vec<PExpr>),
    Cmp(CmpOp, Box<PExpr>, Box<PExpr>),
    And(Vec<PExpr>),
    Or(Vec<PExpr>),
    In(Box<PExpr>, Vec<Lit>),
    Lambda(String, Box<PExpr>),
    /// A user filter on `alias.property`; lowering owns operator and
    /// parameter typing (`filter_to_expr`).
    NodeFilter {
        alias: String,
        property: String,
        #[serde(skip)]
        filter: InputFilter,
    },
    /// Namespace scope restriction on `alias.traversal_path`.
    Scope(String, #[serde(skip)] crate::scope::ScopePrefix),
    /// The scope's anchor resolved to a real path (guards an elided anchor).
    ScopeResolved(#[serde(skip)] crate::scope::ScopePrefix),
}

pub fn deleted_false(alias: &str) -> PExpr {
    pe!("{alias}._deleted = false")
}

/// `alias.column IN ids`, or `= id` for a single value.
pub fn id_in(alias: &str, column: &str, ids: &[i64]) -> PExpr {
    match ids {
        [id] => pe!("{alias}.{column} = {id}"),
        _ => PExpr::In(
            Box::new(pe!("{alias}.{column}")),
            ids.iter().map(|&i| Lit::Int(i)).collect(),
        ),
    }
}

pub fn id_range(alias: &str, column: &str, r: &InputIdRange) -> PExpr {
    pe!(
        "{alias}.{column} >= {} AND {alias}.{column} <= {}",
        r.start,
        r.end
    )
}

/// Relationship kinds come from the query; they are user data and go
/// through a typed literal, not the expression text.
pub fn rel_kind(alias: &str, types: &[String]) -> Option<PExpr> {
    if crate::passes::normalize::is_wildcard(types) {
        return None;
    }
    let kind = Box::new(pe!("{alias}.relationship_kind"));
    let lits: Vec<Lit> = types.iter().map(|t| Lit::Str(t.clone())).collect();
    Some(match lits.as_slice() {
        [t] => PExpr::Cmp(CmpOp::Eq, kind, Box::new(PExpr::Lit(t.clone()))),
        _ => PExpr::In(kind, lits),
    })
}

/// User filters on a node's properties, in property order for stable output.
pub fn node_filters(alias: &str, filters: &HashMap<String, Vec<InputFilter>>) -> Vec<PExpr> {
    let mut props: Vec<_> = filters.iter().collect();
    props.sort_unstable_by_key(|(k, _)| *k);
    props
        .into_iter()
        .flat_map(|(prop, fs)| {
            fs.iter().map(|f| PExpr::NodeFilter {
                alias: alias.to_string(),
                property: prop.clone(),
                filter: f.clone(),
            })
        })
        .collect()
}

impl PExpr {
    pub fn aliases(&self, out: &mut HashSet<String>) {
        match self {
            PExpr::Col(a, _) | PExpr::Scope(a, _) | PExpr::NodeFilter { alias: a, .. } => {
                out.insert(a.clone());
            }
            PExpr::Ident(_) | PExpr::Lit(_) | PExpr::ScopeResolved(_) => {}
            PExpr::Func(_, xs) | PExpr::And(xs) | PExpr::Or(xs) => {
                xs.iter().for_each(|x| x.aliases(out))
            }
            PExpr::Cmp(_, l, r) => {
                l.aliases(out);
                r.aliases(out);
            }
            PExpr::In(x, _) | PExpr::Lambda(_, x) => x.aliases(out),
        }
    }

    /// Rebuilds the expression bottom-up; `leaf` may replace any node.
    pub fn map(&self, leaf: &dyn Fn(&PExpr) -> Option<PExpr>) -> PExpr {
        if let Some(e) = leaf(self) {
            return e;
        }
        let go = |x: &PExpr| x.map(leaf);
        match self {
            PExpr::Func(n, xs) => PExpr::Func(n.clone(), xs.iter().map(go).collect()),
            PExpr::And(xs) => PExpr::And(xs.iter().map(go).collect()),
            PExpr::Or(xs) => PExpr::Or(xs.iter().map(go).collect()),
            PExpr::Cmp(op, l, r) => PExpr::Cmp(*op, Box::new(go(l)), Box::new(go(r))),
            PExpr::In(x, vs) => PExpr::In(Box::new(go(x)), vs.clone()),
            PExpr::Lambda(p, b) => PExpr::Lambda(p.clone(), Box::new(go(b))),
            other => other.clone(),
        }
    }

    /// Rewrites column references through `s`.
    pub fn subst(&self, s: &HashMap<Col, PExpr>) -> PExpr {
        self.map(&|e| match e {
            PExpr::Col(a, c) => s.get(&(a.clone(), c.clone())).cloned(),
            _ => None,
        })
    }

    /// Renames every reference to alias `from`.
    pub fn realias(&self, from: &str, to: &str) -> PExpr {
        self.map(&|e| match e {
            PExpr::Col(a, c) if a == from => Some(PExpr::Col(to.into(), c.clone())),
            PExpr::Scope(a, p) if a == from => Some(PExpr::Scope(to.into(), p.clone())),
            PExpr::NodeFilter {
                alias,
                property,
                filter,
            } if alias == from => Some(PExpr::NodeFilter {
                alias: to.into(),
                property: property.clone(),
                filter: filter.clone(),
            }),
            _ => None,
        })
    }

    /// The column this predicate constrains, for `col = lit`, `col IN`, and
    /// `col >= .. AND col <= ..` shapes.
    pub fn constrained_col(&self) -> Option<(&str, &str)> {
        match self {
            PExpr::Cmp(_, l, _) | PExpr::In(l, _) => match l.as_ref() {
                PExpr::Col(a, c) => Some((a, c)),
                _ => None,
            },
            PExpr::And(xs) => xs.first().and_then(|x| x.constrained_col()),
            _ => None,
        }
    }
}

// ── Operators ───────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Copy, PartialEq, Serialize)]
pub enum Dedup {
    None,
    /// `FINAL`: ReplacingMergeTree merge-on-read.
    Final,
    /// `ORDER BY <sort_key>, _version DESC LIMIT 1 BY <sort_key>`; keeps
    /// column pruning eligible. `_deleted` must be filtered after the dedup.
    LimitBy,
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize)]
pub enum JoinKind {
    Inner,
    /// `left.col IN (SELECT right.col FROM right)`; uses `on[0]`.
    Semi,
}

/// `expr AS alias`.
pub type Named = (PExpr, String);

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(tag = "op")]
pub enum PhysOp {
    Scan {
        table: String,
        alias: String,
        dedup: Dedup,
    },
    Filter {
        input: Box<PhysOp>,
        predicates: Vec<PExpr>,
    },
    Project {
        input: Box<PhysOp>,
        columns: Vec<Named>,
    },
    Join {
        left: Box<PhysOp>,
        right: Box<PhysOp>,
        on: Vec<(Col, Col)>,
        kind: JoinKind,
    },
    Aggregate {
        input: Box<PhysOp>,
        group_by: Vec<Named>,
        metrics: Vec<Named>,
    },
    Union {
        arms: Vec<PhysOp>,
        alias: String,
    },
    Sort {
        input: Box<PhysOp>,
        keys: Vec<(PExpr, bool)>,
    },
    Limit {
        input: Box<PhysOp>,
        count: u32,
    },
    /// Named subqueries (CTEs) visible to `input`.
    With {
        ctes: Vec<(String, PhysOp)>,
        input: Box<PhysOp>,
    },
}

pub fn scan(table: &str, alias: &str, dedup: Dedup) -> PhysOp {
    PhysOp::Scan {
        table: table.to_string(),
        alias: alias.to_string(),
        dedup,
    }
}

pub fn named(expr: PExpr, alias: impl Into<String>) -> Named {
    (expr, alias.into())
}

impl PhysOp {
    /// The predicate list is a conjunction; top-level ANDs are flattened
    /// into it so rules see one predicate per conjunct.
    pub fn filter(self, predicates: Vec<PExpr>) -> PhysOp {
        let predicates: Vec<PExpr> = predicates
            .into_iter()
            .flat_map(|p| match p {
                PExpr::And(xs) => xs,
                p => vec![p],
            })
            .collect();
        if predicates.is_empty() {
            return self;
        }
        PhysOp::Filter {
            input: Box::new(self),
            predicates,
        }
    }

    pub fn project(self, columns: Vec<Named>) -> PhysOp {
        PhysOp::Project {
            input: Box::new(self),
            columns,
        }
    }

    pub fn join(self, right: PhysOp, on: Vec<(Col, Col)>) -> PhysOp {
        PhysOp::Join {
            left: Box::new(self),
            right: Box::new(right),
            on,
            kind: JoinKind::Inner,
        }
    }

    /// `self.col IN (SELECT right.col FROM right)`.
    pub fn semi(self, right: PhysOp, on: (Col, Col)) -> PhysOp {
        PhysOp::Join {
            left: Box::new(self),
            right: Box::new(right),
            on: vec![on],
            kind: JoinKind::Semi,
        }
    }

    pub fn sort(self, keys: Vec<(PExpr, bool)>) -> PhysOp {
        if keys.is_empty() {
            return self;
        }
        PhysOp::Sort {
            input: Box::new(self),
            keys,
        }
    }

    pub fn limit(self, count: u32) -> PhysOp {
        PhysOp::Limit {
            input: Box::new(self),
            count,
        }
    }

    /// `UNION ALL` of `arms` as a derived table named `alias`; a single arm
    /// is still wrapped so the alias is stable.
    pub fn union(arms: Vec<PhysOp>, alias: &str) -> PhysOp {
        PhysOp::Union {
            arms,
            alias: alias.to_string(),
        }
    }

    /// Alias a relation is visible under: a scan's or union's, through any
    /// filter or projection over it.
    pub fn alias(&self) -> Option<&str> {
        match self {
            PhysOp::Scan { alias, .. } | PhysOp::Union { alias, .. } => Some(alias),
            PhysOp::Filter { input, .. } | PhysOp::Project { input, .. } => input.alias(),
            _ => None,
        }
    }

    pub fn children(&self) -> Vec<&PhysOp> {
        match self {
            PhysOp::Scan { .. } => vec![],
            PhysOp::Filter { input, .. }
            | PhysOp::Project { input, .. }
            | PhysOp::Aggregate { input, .. }
            | PhysOp::Sort { input, .. }
            | PhysOp::Limit { input, .. } => vec![input],
            PhysOp::Join { left, right, .. } => vec![left, right],
            PhysOp::Union { arms, .. } => arms.iter().collect(),
            PhysOp::With { ctes, input } => ctes
                .iter()
                .map(|(_, c)| c)
                .chain([input.as_ref()])
                .collect(),
        }
    }

    pub fn map_children(self, f: &mut dyn FnMut(PhysOp) -> PhysOp) -> PhysOp {
        match self {
            PhysOp::Scan { .. } => self,
            PhysOp::Filter { input, predicates } => PhysOp::Filter {
                input: Box::new(f(*input)),
                predicates,
            },
            PhysOp::Project { input, columns } => PhysOp::Project {
                input: Box::new(f(*input)),
                columns,
            },
            PhysOp::Join {
                left,
                right,
                on,
                kind,
            } => PhysOp::Join {
                left: Box::new(f(*left)),
                right: Box::new(f(*right)),
                on,
                kind,
            },
            PhysOp::Aggregate {
                input,
                group_by,
                metrics,
            } => PhysOp::Aggregate {
                input: Box::new(f(*input)),
                group_by,
                metrics,
            },
            PhysOp::Union { arms, alias } => PhysOp::Union {
                arms: arms.into_iter().map(|a| f(a)).collect(),
                alias,
            },
            PhysOp::Sort { input, keys } => PhysOp::Sort {
                input: Box::new(f(*input)),
                keys,
            },
            PhysOp::Limit { input, count } => PhysOp::Limit {
                input: Box::new(f(*input)),
                count,
            },
            PhysOp::With { ctes, input } => PhysOp::With {
                ctes: ctes.into_iter().map(|(n, c)| (n, f(c))).collect(),
                input: Box::new(f(*input)),
            },
        }
    }

    /// Expressions this operator evaluates itself (not its children's).
    pub fn exprs(&self) -> Vec<&PExpr> {
        match self {
            PhysOp::Filter { predicates, .. } => predicates.iter().collect(),
            PhysOp::Project { columns, .. } => columns.iter().map(|(e, _)| e).collect(),
            PhysOp::Aggregate {
                group_by, metrics, ..
            } => group_by.iter().chain(metrics).map(|(e, _)| e).collect(),
            PhysOp::Sort { keys, .. } => keys.iter().map(|(e, _)| e).collect(),
            _ => vec![],
        }
    }

    pub fn map_exprs(self, f: &dyn Fn(&PExpr) -> PExpr) -> PhysOp {
        let name = |(e, a): Named| (f(&e), a);
        match self {
            PhysOp::Filter { input, predicates } => PhysOp::Filter {
                input,
                predicates: predicates.iter().map(f).collect(),
            },
            PhysOp::Project { input, columns } => PhysOp::Project {
                input,
                columns: columns.into_iter().map(name).collect(),
            },
            PhysOp::Aggregate {
                input,
                group_by,
                metrics,
            } => PhysOp::Aggregate {
                input,
                group_by: group_by.into_iter().map(name).collect(),
                metrics: metrics.into_iter().map(name).collect(),
            },
            PhysOp::Sort { input, keys } => PhysOp::Sort {
                input,
                keys: keys.into_iter().map(|(e, d)| (f(&e), d)).collect(),
            },
            other => other,
        }
    }

    /// Aliases visible to the enclosing query: scans and unions reached
    /// without entering a semi-join's producer or a union arm.
    pub fn visible_aliases(&self) -> HashSet<String> {
        fn go(op: &PhysOp, out: &mut HashSet<String>) {
            match op {
                PhysOp::Scan { alias, .. } | PhysOp::Union { alias, .. } => {
                    out.insert(alias.clone());
                }
                PhysOp::Join {
                    left,
                    kind: JoinKind::Semi,
                    ..
                } => go(left, out),
                _ => op.children().into_iter().for_each(|c| go(c, out)),
            }
        }
        let mut out = HashSet::new();
        go(self, &mut out);
        out
    }

    /// Aliases read by projections, aggregates, and sort keys anywhere in
    /// this scope. Join conditions tie a relation in without reading it;
    /// union arms are their own scope.
    pub fn read_aliases(&self) -> HashSet<String> {
        fn go(op: &PhysOp, out: &mut HashSet<String>) {
            if let PhysOp::Union { .. } = op {
                return;
            }
            if !matches!(op, PhysOp::Filter { .. }) {
                op.exprs().into_iter().for_each(|e| e.aliases(out));
            }
            op.children().into_iter().for_each(|c| go(c, out));
        }
        let mut out = HashSet::new();
        go(self, &mut out);
        out
    }
}

// ── Entry point ─────────────────────────────────────────────────────────────

#[derive(Default)]
pub struct PlanMetadata {
    pub node_edge_mappings: HashMap<String, (String, String)>,
    pub hop_count: usize,
    pub phys_op: Option<PhysOp>,
}

pub(crate) struct PlanCtx<'a> {
    pub input: &'a Input,
    pub graph: &'a JoinGraph,
}

pub fn plan(
    input: &mut Input,
    ontology: &Ontology,
) -> crate::error::Result<(PlanMetadata, PhysOp)> {
    if input.compiler.table_sort_keys.is_empty() {
        for node in ontology.nodes() {
            input
                .compiler
                .table_sort_keys
                .insert(node.destination_table.clone(), node.sort_key.clone());
        }
    }
    let graph = JoinGraph::build(ontology);
    let ctx = PlanCtx {
        input,
        graph: &graph,
    };
    let limit = input.fetch_limit();

    let op = match input.query_type {
        QueryType::Traversal | QueryType::Aggregation => ctx.plan_chain_query(limit),
        QueryType::Neighbors => ctx.plan_neighbors(limit),
        QueryType::PathFinding => ctx.plan_pathfinding(limit),
        QueryType::Hydration => ctx.plan_hydration(limit),
    };

    let meta = PlanMetadata {
        node_edge_mappings: ctx.node_edge_mappings(&op),
        hop_count: input.relationships.len(),
        phys_op: None,
    };
    // A scope anchor the optimizer elided has no representation in the
    // query; later passes must not expect it in the result.
    if input.query_type == QueryType::Aggregation {
        let kept: HashSet<&String> = meta.node_edge_mappings.keys().collect();
        input.nodes.retain(|n| kept.contains(&n.id));
        input
            .relationships
            .retain(|r| kept.contains(&r.from) && kept.contains(&r.to));
    }
    Ok((meta, op))
}

impl<'a> PlanCtx<'a> {
    pub fn node(&self, alias: &str) -> Option<&'a InputNode> {
        self.input.nodes.iter().find(|n| n.id == alias)
    }

    /// Latest-row scan of a node table with its user filters and pins.
    pub fn node_scan(&self, n: &InputNode) -> PhysOp {
        let a = n.id.as_str();
        let mut p = node_filters(a, &n.filters);
        if !n.node_ids.is_empty() {
            p.push(id_in(a, DEFAULT_PRIMARY_KEY, &n.node_ids));
        }
        if let Some(ref r) = n.id_range {
            p.push(id_range(a, DEFAULT_PRIMARY_KEY, r));
        }
        p.push(deleted_false(a));
        scan(n.table.as_deref().unwrap_or(""), a, Dedup::Final).filter(p)
    }

    /// `node.property`, truncated to an excerpt when the ontology marks the
    /// column as long text.
    pub fn property(&self, n: &InputNode, property: &str) -> PExpr {
        let (a, max) = (&n.id, n.excerpt_max_chars);
        if !n.excerpt_columns.contains(property) || max == 0 {
            return pe!("{a}.{property}");
        }
        let excerpt = format!("substringUTF8({a}.{property}, 1, {max})");
        pe!("concat({excerpt}, if(length({a}.{property}) > length({excerpt}), ' [truncated]', ''))")
    }

    /// Scan of one or more physical edge tables under one alias. Several
    /// tables union on the reserved edge columns so extra per-table columns
    /// don't break the union; `arm_where` is pushed into each arm.
    pub fn edge_scan(
        &self,
        tables: &[String],
        alias: &str,
        arm_where: impl Fn(&str) -> Vec<PExpr>,
    ) -> PhysOp {
        if let [table] = tables {
            return scan(table, alias, Dedup::None).filter(arm_where(alias));
        }
        let inner = format!("_{alias}");
        let arms = tables
            .iter()
            .map(|t| {
                let cols = EDGE_RESERVED_COLUMNS
                    .iter()
                    .chain([&DELETED_COLUMN])
                    .map(|c| pn!("{inner}.{c} AS {c}"))
                    .collect();
                scan(t, &inner, Dedup::None)
                    .filter(arm_where(&inner))
                    .project(cols)
            })
            .collect();
        PhysOp::union(arms, alias)
    }

    /// Where `enforce` reads each node's id from: its own scan when joined,
    /// otherwise the edge column that carries it.
    fn node_edge_mappings(&self, op: &PhysOp) -> HashMap<String, (String, String)> {
        if self.input.query_type == QueryType::Neighbors {
            let center = &self.input.nodes[0].id;
            return HashMap::from([(center.clone(), ("e".into(), SOURCE_ID_COLUMN.into()))]);
        }
        let visible = op.visible_aliases();
        let bound = self.bindings();
        self.input
            .nodes
            .iter()
            .filter_map(|n| {
                if visible.contains(&n.id) {
                    return Some((n.id.clone(), (n.id.clone(), DEFAULT_PRIMARY_KEY.into())));
                }
                let b = bound.get(&n.id)?;
                visible.contains(&b.0).then(|| (n.id.clone(), b.clone()))
            })
            .collect()
    }
}

// ── Denormalized tags ───────────────────────────────────────────────────────

/// `has(tags, 'key:value')` / `hasAny(tags, [...])` for an eq or in filter.
pub fn denorm_tag(edge: &str, tag_col: &str, tag_key: &str, f: &InputFilter) -> Option<PExpr> {
    let scalar = |v: &serde_json::Value| match v {
        serde_json::Value::String(s) => Some(s.clone()),
        serde_json::Value::Bool(b) => Some(b.to_string()),
        serde_json::Value::Number(n) => Some(n.to_string()),
        _ => None,
    };
    let tags: Vec<String> = match (&f.op, &f.value) {
        (None | Some(FilterOp::Eq), Some(v)) => vec![scalar(v)?],
        (Some(FilterOp::In), Some(serde_json::Value::Array(vs))) => {
            vs.iter().filter_map(scalar).collect()
        }
        _ => return None,
    };
    // Tag values are user data: typed literals, never expression text.
    let tags: Vec<PExpr> = tags
        .into_iter()
        .map(|t| PExpr::Lit(Lit::Str(format!("{tag_key}:{t}"))))
        .collect();
    let tags_col = pe!("{edge}.{tag_col}");
    match tags.len() {
        0 => None,
        1 => Some(PExpr::Func(
            "has".into(),
            vec![tags_col, tags.into_iter().next().unwrap()],
        )),
        _ => Some(PExpr::Func(
            "hasAny".into(),
            vec![tags_col, PExpr::Func("array".into(), tags)],
        )),
    }
}

impl<'a> PlanCtx<'a> {
    /// Tag predicates on `edge` for every filter of `node` that the edge
    /// denormalizes on its `dir` ("source"/"target") side. With `rel_types`,
    /// only tags those relationship kinds actually write.
    pub fn denorm_tags(
        &self,
        node: &InputNode,
        dir: &str,
        edge: &str,
        rel_types: Option<&[String]>,
    ) -> Vec<PExpr> {
        let meta = &self.input.compiler;
        let entity = node.entity.clone().unwrap_or_default();
        let mut props: Vec<_> = node.filters.iter().collect();
        props.sort_unstable_by_key(|(k, _)| *k);
        let mut out = Vec::new();
        for (prop, fs) in props {
            let key = (entity.clone(), prop.clone(), dir.to_string());
            if let Some(types) = rel_types {
                let writes = meta
                    .denorm_rel_kinds
                    .get(&key)
                    .is_some_and(|ks| types.iter().any(|t| ks.contains(t)));
                if !writes {
                    continue;
                }
            }
            let Some((tag_col, tag_key)) = meta.denormalized_columns.get(&key) else {
                continue;
            };
            out.extend(
                fs.iter()
                    .filter_map(|f| denorm_tag(edge, tag_col, tag_key, f)),
            );
        }
        out
    }

    /// Whether `prop` of `node` is carried as a denorm tag by some edge
    /// adjacent to the node on the given relationship.
    pub fn denorm_covers(&self, node: &InputNode, prop: &str, rel: &InputRelationship) -> bool {
        let (sc, ec) = rel.direction.edge_columns();
        [(&rel.from, sc), (&rel.to, ec)].into_iter().any(|(n, ic)| {
            let dir = if ic == SOURCE_ID_COLUMN {
                "source"
            } else {
                "target"
            };
            let key = (
                node.entity.clone().unwrap_or_default(),
                prop.to_string(),
                dir.to_string(),
            );
            n == &node.id
                && self.input.compiler.denormalized_columns.contains_key(&key)
                && self
                    .input
                    .compiler
                    .denorm_rel_kinds
                    .get(&key)
                    .is_some_and(|ks| rel.types.iter().any(|t| ks.contains(t)))
        })
    }
}
