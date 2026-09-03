use std::collections::{HashMap, HashSet};

use compiler::Input;
use compiler::input::{
    AggExpr, ColumnSelection, Direction, HopRange, InputAggSort, InputAggregation,
    InputAggregationMetric, InputFilter, InputGroupByKey, InputNeighbors, InputNode, InputOrderBy,
    InputPath, InputRelationship, OrderDirection, PathType, QueryType, TruncateUnit,
};

use crate::{Error, OutputColumn, Parsed, Projection};

pub(crate) struct Pattern {
    pub start: InputNode,
    pub chain: Vec<(RelPart, InputNode)>,
}

pub(crate) struct RelPart {
    pub var: Option<String>,
    pub rel: InputRelationship,
    pub quantified: bool,
}

pub(crate) enum Pred {
    Filter(String, String, InputFilter),
    Ids(String, Vec<i64>),
}

pub(crate) enum ReturnItem {
    Var(String),
    Prop(String, String, Option<String>),
    Trunc(TruncateUnit, String, String, Option<String>),
    Agg(AggExpr, Option<String>),
}

#[derive(Default)]
struct Returned {
    vars: Vec<String>,
    props: Vec<(String, String, Option<String>)>,
    truncs: Vec<InputGroupByKey>,
}

pub(crate) enum OrderTarget {
    Prop(String, String),
    Column(String),
}

fn unsupported(msg: impl Into<String>) -> Error {
    Error::Unsupported(msg.into())
}

fn semantic(msg: impl Into<String>) -> Error {
    Error::Semantic(msg.into())
}

#[derive(Default)]
struct Graph {
    nodes: Vec<InputNode>,
    index: HashMap<String, usize>,
    rels: Vec<InputRelationship>,
    rel_index: HashMap<String, usize>,
    named: HashSet<String>,
    anonymous: usize,
}

impl Graph {
    fn new(patterns: &[Pattern]) -> Self {
        let mut named = HashSet::new();
        for pattern in patterns {
            named.insert(pattern.start.id.clone());
            for (rel, node) in &pattern.chain {
                named.insert(node.id.clone());
                named.extend(rel.var.clone());
            }
        }
        Graph {
            named,
            ..Default::default()
        }
    }

    fn add_node(&mut self, mut node: InputNode) -> Result<String, Error> {
        if node.id.is_empty() {
            node.id = loop {
                let candidate = format!("_n{}", self.anonymous);
                self.anonymous += 1;
                if !self.named.contains(&candidate) {
                    break candidate;
                }
            };
        }
        if let Some(&i) = self.index.get(&node.id) {
            let existing = &mut self.nodes[i];
            match (&existing.entity, &node.entity) {
                (Some(a), Some(b)) if a != b => {
                    return Err(semantic(format!(
                        "variable {} is bound to both {a} and {b}",
                        node.id
                    )));
                }
                (None, Some(_)) => existing.entity = node.entity.take(),
                _ => {}
            }
            for (k, fs) in node.filters {
                existing.filters.entry(k).or_default().extend(fs);
            }
            return Ok(existing.id.clone());
        }
        let id = node.id.clone();
        self.index.insert(id.clone(), self.nodes.len());
        self.nodes.push(node);
        Ok(id)
    }

    fn add_rel(&mut self, part: RelPart, from: &str, to: &str) -> Result<(), Error> {
        let mut rel = part.rel;
        rel.from = from.to_string();
        rel.to = to.to_string();
        if let Some(var) = part.var {
            if self.rel_index.contains_key(&var) || self.index.contains_key(&var) {
                return Err(semantic(format!("variable {var} is bound twice")));
            }
            self.rel_index.insert(var, self.rels.len());
        }
        self.rels.push(rel);
        Ok(())
    }

    fn node_mut(&mut self, var: &str) -> Result<&mut InputNode, Error> {
        match self.index.get(var) {
            Some(&i) => Ok(&mut self.nodes[i]),
            None => Err(semantic(format!("variable {var} is not defined in MATCH"))),
        }
    }

    fn apply(&mut self, pred: Pred) -> Result<(), Error> {
        match pred {
            Pred::Ids(var, ids) => self.node_mut(&var)?.node_ids.extend(ids),
            Pred::Filter(var, prop, filter) => {
                if let Some(&i) = self.rel_index.get(&var) {
                    self.rels[i].filters.entry(prop).or_default().push(filter);
                } else {
                    self.node_mut(&var)?
                        .filters
                        .entry(prop)
                        .or_default()
                        .push(filter);
                }
            }
        }
        Ok(())
    }

    fn select_column(&mut self, var: &str, prop: String) -> Result<(), Error> {
        let node = self.node_mut(var)?;
        match &mut node.columns {
            None => node.columns = Some(ColumnSelection::List(vec![prop])),
            Some(ColumnSelection::List(cols)) => {
                if !cols.contains(&prop) {
                    cols.push(prop);
                }
            }
            Some(ColumnSelection::All) => {}
        }
        Ok(())
    }

    fn is_blank(&self, var: &str) -> bool {
        self.index.get(var).is_some_and(|&i| {
            let n = &self.nodes[i];
            n.entity.is_none()
                && n.filters.is_empty()
                && n.node_ids.is_empty()
                && n.columns.is_none()
        })
    }
}

pub(crate) fn assemble(
    shortest: bool,
    patterns: Vec<Pattern>,
    preds: Vec<Pred>,
    ret: Vec<ReturnItem>,
    group: Option<Vec<InputGroupByKey>>,
    order: Vec<(OrderTarget, OrderDirection)>,
    limit: Option<u32>,
) -> Result<Parsed, Error> {
    let mut g = Graph::new(&patterns);
    let mut endpoints = Vec::new();
    for pattern in patterns {
        let mut prev = g.add_node(pattern.start)?;
        for (rel, node) in pattern.chain {
            let next = g.add_node(node)?;
            endpoints.push((
                prev.clone(),
                rel.quantified.then_some(rel.rel.hops),
                rel.rel.types.clone(),
            ));
            g.add_rel(rel, &prev, &next)?;
            prev = next;
        }
    }
    for pred in preds {
        g.apply(pred)?;
    }

    let mut metrics = Vec::new();
    let mut returned = Returned::default();
    let mut edge_columns = Projection::new();
    for item in ret {
        match item {
            ReturnItem::Trunc(unit, node, property, alias) => {
                g.node_mut(&node)?;
                returned.truncs.push(InputGroupByKey::Property {
                    node,
                    property,
                    truncate: Some(unit),
                    alias,
                });
            }
            ReturnItem::Var(v) => {
                g.node_mut(&v)?;
                returned.vars.push(v);
            }
            ReturnItem::Prop(v, p, alias) if g.rel_index.contains_key(&v) => {
                edge_columns.push(OutputColumn::edge_property(&v, &p, alias));
            }
            ReturnItem::Prop(v, p, alias) => {
                g.node_mut(&v)?;
                returned.props.push((v, p, alias));
            }
            ReturnItem::Agg(expr, alias) => {
                g.node_mut(expr.node())?;
                metrics.push(InputAggregationMetric { expr, alias });
            }
        }
    }

    let mut input = Input::default();
    if let Some(limit) = limit {
        input.limit = limit;
    }

    let order = resolve_order_aliases(order, &returned.props);
    if shortest {
        return finish_path(g, endpoints, returned.props, input);
    }
    if !metrics.is_empty() {
        let mut parsed = finish_aggregation(g, metrics, returned, group, order, input)?;
        parsed.projection.extend(edge_columns);
        return Ok(parsed);
    }
    if !returned.truncs.is_empty() {
        return Err(semantic("date_trunc requires an aggregate in RETURN"));
    }
    if group.is_some() {
        return Err(semantic("GROUP BY requires an aggregate in RETURN"));
    }
    let mut projection = Projection::new();
    for v in &returned.vars {
        projection.push(OutputColumn::node(v));
    }
    for (v, p, alias) in returned.props {
        projection.push(OutputColumn::property(&v, &p, alias));
        g.select_column(&v, p)?;
    }
    if let Some(neighbors) = detect_neighbors(&mut g, &returned.vars)? {
        input.query_type = QueryType::Neighbors;
        input.neighbors = Some(neighbors);
    } else {
        input.query_type = QueryType::Traversal;
        input.relationships = std::mem::take(&mut g.rels);
    }
    input.order_by = traversal_order(order)?;
    input.nodes = g.nodes;
    projection.extend(edge_columns);
    Ok(Parsed { input, projection })
}

/// `ORDER BY alias` where `alias` names a returned `node.property` sorts by
/// that property; a bare name that is not such an alias is left for the
/// aggregation path, where it may name an aggregate output column.
fn resolve_order_aliases(
    order: Vec<(OrderTarget, OrderDirection)>,
    returned_props: &[(String, String, Option<String>)],
) -> Vec<(OrderTarget, OrderDirection)> {
    order
        .into_iter()
        .map(|(target, dir)| match target {
            OrderTarget::Column(name) => {
                let hit = returned_props
                    .iter()
                    .find(|(_, _, alias)| alias.as_deref() == Some(name.as_str()));
                match hit {
                    Some((node, prop, _)) => (OrderTarget::Prop(node.clone(), prop.clone()), dir),
                    None => (OrderTarget::Column(name), dir),
                }
            }
            prop => (prop, dir),
        })
        .collect()
}

fn traversal_order(
    order: Vec<(OrderTarget, OrderDirection)>,
) -> Result<Option<InputOrderBy>, Error> {
    let mut order = order.into_iter();
    let Some((target, direction)) = order.next() else {
        return Ok(None);
    };
    if order.next().is_some() {
        return Err(unsupported("more than one ORDER BY key"));
    }
    match target {
        OrderTarget::Prop(node, property) => Ok(Some(InputOrderBy {
            node,
            property,
            direction,
        })),
        OrderTarget::Column(c) => Err(semantic(format!(
            "ORDER BY {c}: expected <variable>.<property>"
        ))),
    }
}

/// `(centre)-[..]-(blank)` with a single hop and an otherwise unreferenced,
/// unlabelled target is the neighbours shape; the blank node is dropped and
/// the hop becomes the `neighbors` configuration relative to the centre.
fn detect_neighbors(
    g: &mut Graph,
    returned_vars: &[String],
) -> Result<Option<InputNeighbors>, Error> {
    if g.rels.len() != 1 || g.nodes.len() != 2 {
        return Ok(None);
    }
    let rel = &g.rels[0];
    let (from, to) = (rel.from.clone(), rel.to.clone());
    let (centre, blank, direction) = match (g.is_blank(&to), g.is_blank(&from)) {
        (true, false) => (from, to, rel.direction),
        (false, true) => (to, from, flip(rel.direction)),
        _ => return Ok(None),
    };
    if returned_vars.iter().any(|v| v != &blank && v != &centre) {
        return Ok(None);
    }
    if rel.hops != HopRange::default() {
        return Err(unsupported("variable-length hop to an unlabelled node"));
    }
    let rel_types = rel.types.clone();
    g.rels.clear();
    g.nodes.retain(|n| n.id != blank);
    g.index.remove(&blank);
    Ok(Some(InputNeighbors {
        direction,
        rel_types,
    }))
}

fn flip(direction: Direction) -> Direction {
    match direction {
        Direction::Outgoing => Direction::Incoming,
        Direction::Incoming => Direction::Outgoing,
        Direction::Both => Direction::Both,
    }
}

fn finish_path(
    mut g: Graph,
    endpoints: Vec<(String, Option<HopRange>, Vec<String>)>,
    returned_props: Vec<(String, String, Option<String>)>,
    mut input: Input,
) -> Result<Parsed, Error> {
    if g.rels.len() != 1 || g.nodes.len() != 2 {
        return Err(unsupported(
            "SHORTEST over anything but a single (a)-[..]-(b) hop",
        ));
    }
    let (from, hops, rel_types) = endpoints.into_iter().next().expect("one hop");
    let rel = g.rels.pop().expect("one hop");
    let Some(hops) = hops else {
        return Err(semantic(
            "SHORTEST needs a bounded quantifier, e.g. -[:KIND]->{1,3}",
        ));
    };
    if !rel.filters.is_empty() {
        return Err(unsupported("relationship filters on a SHORTEST hop"));
    }
    let mut projection = Projection::new();
    for (v, p, alias) in returned_props {
        projection.push(OutputColumn::property(&v, &p, alias));
        g.select_column(&v, p)?;
    }
    input.query_type = QueryType::PathFinding;
    input.path = Some(InputPath {
        path_type: PathType::Shortest,
        from,
        to: rel.to,
        max_depth: hops.max,
        rel_types,
        forward_first_hop_rel_types: Vec::new(),
        backward_first_hop_rel_types: Vec::new(),
    });
    input.nodes = g.nodes;
    Ok(Parsed { input, projection })
}

fn finish_aggregation(
    mut g: Graph,
    metrics: Vec<InputAggregationMetric>,
    returned: Returned,
    group: Option<Vec<InputGroupByKey>>,
    order: Vec<(OrderTarget, OrderDirection)>,
    mut input: Input,
) -> Result<Parsed, Error> {
    let mut projection = Projection::new();
    let Returned {
        vars: returned_vars,
        props: returned_props,
        truncs: returned_truncs,
    } = returned;
    let group_by = match group {
        Some(mut keys) => {
            for trunc in returned_truncs {
                let Some(key) = keys.iter_mut().find(|k| same_truncation(k, &trunc)) else {
                    return Err(semantic(format!(
                        "RETURN date_trunc on {}.{} is not a GROUP BY key",
                        trunc.node(),
                        trunc.property().unwrap_or_default()
                    )));
                };
                if let (
                    InputGroupByKey::Property { alias, .. },
                    InputGroupByKey::Property { alias: wanted, .. },
                ) = (key, &trunc)
                {
                    *alias = wanted.clone();
                }
            }
            for key in &keys {
                g.node_mut(key.node())?;
            }
            let node_keys: Vec<&str> = keys
                .iter()
                .filter(|k| matches!(k, InputGroupByKey::Node { .. }))
                .map(InputGroupByKey::node)
                .collect();
            for v in &returned_vars {
                if !node_keys.contains(&v.as_str()) {
                    return Err(semantic(format!(
                        "RETURN {v} is neither aggregated nor a GROUP BY key"
                    )));
                }
                projection.push(OutputColumn::node(v));
            }
            for (v, p, alias) in returned_props {
                let is_key = keys
                    .iter()
                    .any(|k| k.node() == v && k.property() == Some(p.as_str()));
                projection.push(OutputColumn::property(&v, &p, alias));
                if !is_key {
                    g.select_column(&v, p)?;
                }
            }
            keys
        }
        None => {
            let mut keys = Vec::new();
            for node in returned_vars {
                projection.push(OutputColumn::node(&node));
                keys.push(InputGroupByKey::Node { node, alias: None });
            }
            for (node, property, alias) in returned_props {
                projection.push(OutputColumn::property(&node, &property, alias.clone()));
                keys.push(InputGroupByKey::Property {
                    node,
                    property,
                    truncate: None,
                    alias,
                });
            }
            keys.extend(returned_truncs);
            keys
        }
    };
    for metric in &metrics {
        projection.push(OutputColumn::aggregate(metric.output_name()));
    }

    let mut order = order.into_iter();
    let mut sort = None;
    if let Some((target, direction)) = order.next() {
        if order.next().is_some() {
            return Err(unsupported("more than one ORDER BY key"));
        }
        match target {
            OrderTarget::Column(column) => sort = Some(InputAggSort { column, direction }),
            OrderTarget::Prop(node, property) => {
                input.order_by = Some(InputOrderBy {
                    node,
                    property,
                    direction,
                });
            }
        }
    }

    input.query_type = QueryType::Aggregation;
    input.aggregation = InputAggregation {
        metrics,
        group_by,
        sort,
    };
    input.relationships = g.rels;
    input.nodes = g.nodes;
    Ok(Parsed { input, projection })
}

fn same_truncation(key: &InputGroupByKey, wanted: &InputGroupByKey) -> bool {
    key.node() == wanted.node()
        && key.property() == wanted.property()
        && key.truncate() == wanted.truncate()
}
