use std::collections::{HashMap, HashSet};

use compiler::Input;
use compiler::input::{
    AggExpr, ColumnSelection, Direction, FilterOp, HopRange, InputAggSort, InputAggregation,
    InputAggregationMetric, InputFilter, InputGroupByKey, InputIdRange, InputNeighbors, InputNode,
    InputOrderBy, InputPath, InputRelationship, OrderDirection, PathType, QueryType, TargetRef,
    TruncateUnit,
};
use compiler::schema_limits::{MAX_FILTER_VALUE_LEN, MAX_IN_VALUES, MAX_LIMIT};
use ontology::constants::DEFAULT_PRIMARY_KEY;

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

pub(crate) struct ElementId {
    pub var: String,
    pub property: Option<String>,
}

pub(crate) enum Expr {
    Or,
    Not,
    And(Box<Expr>, Box<Expr>),
    Cmp(Cmp, Box<Expr>, Box<Expr>),
    IsNull(Box<Expr>, bool),
    Between(Box<Expr>, Box<Expr>, Box<Expr>),
    ElementId(ElementId),
    Call(String, Vec<Expr>),
    Prop(String, String),
    Value(serde_json::Value),
}

#[derive(Clone, Copy)]
pub(crate) enum Cmp {
    Eq,
    Ne,
    Lt,
    Lte,
    Gt,
    Gte,
    In,
    StartsWith,
    EndsWith,
    Contains,
}

enum Pred {
    Filter(String, String, InputFilter),
    Ids(ElementId, Vec<i64>),
    IdRange(ElementId, i64, i64),
}

fn filter(op: Option<FilterOp>, value: Option<serde_json::Value>) -> InputFilter {
    InputFilter {
        op,
        value,
        ..Default::default()
    }
}

fn lower_where(expr: Expr, out: &mut Vec<Pred>) -> Result<(), Error> {
    match expr {
        Expr::And(l, r) => {
            lower_where(*l, out)?;
            lower_where(*r, out)
        }
        Expr::Or => Err(unsupported("OR")),
        Expr::Not => Err(unsupported("NOT")),
        Expr::IsNull(target, negated) => {
            let (var, prop) = property_of(*target)?;
            let op = if negated {
                FilterOp::IsNotNull
            } else {
                FilterOp::IsNull
            };
            out.push(Pred::Filter(var, prop, filter(Some(op), None)));
            Ok(())
        }
        Expr::Between(target, lo, hi) => match *target {
            Expr::ElementId(id) => {
                out.push(Pred::IdRange(id, integer(*lo)?, integer(*hi)?));
                Ok(())
            }
            other => {
                let (var, prop) = property_of(other)?;
                out.push(Pred::Filter(
                    var.clone(),
                    prop.clone(),
                    filter(Some(FilterOp::Gte), Some(literal(*lo)?)),
                ));
                out.push(Pred::Filter(
                    var,
                    prop,
                    filter(Some(FilterOp::Lte), Some(literal(*hi)?)),
                ));
                Ok(())
            }
        },
        Expr::Cmp(op, l, r) => lower_comparison(op, *l, *r, out),
        Expr::Call(name, args) => {
            let op = match name.as_str() {
                "token_match" => FilterOp::TokenMatch,
                "all_tokens" => FilterOp::AllTokens,
                "any_tokens" => FilterOp::AnyTokens,
                _ => return Err(unsupported(format!("function {name}() in WHERE"))),
            };
            let mut args = args.into_iter();
            let (Some(target), Some(pattern), None) = (args.next(), args.next(), args.next())
            else {
                return Err(semantic(format!(
                    "{name}() takes (<variable>.<property>, <string>)"
                )));
            };
            let (var, prop) = property_of(target)?;
            out.push(Pred::Filter(
                var,
                prop,
                filter(Some(op), Some(literal(pattern)?)),
            ));
            Ok(())
        }
        Expr::ElementId(_) | Expr::Prop(..) | Expr::Value(_) => {
            Err(semantic("a WHERE term must be a comparison"))
        }
    }
}

fn lower_comparison(op: Cmp, l: Expr, r: Expr, out: &mut Vec<Pred>) -> Result<(), Error> {
    if let Expr::ElementId(id) = l {
        return match (op, r) {
            (Cmp::Eq, r) => {
                out.push(Pred::Ids(id, vec![integer(r)?]));
                Ok(())
            }
            (Cmp::In, r) => {
                let ids = integer_list(r)?;
                out.push(Pred::Ids(id, ids));
                Ok(())
            }
            _ => Err(unsupported(
                "element_id() with anything but =, IN, or BETWEEN",
            )),
        };
    }
    if matches!(r, Expr::Prop(..) | Expr::ElementId(_)) {
        return Err(unsupported(
            "comparing two variables (cross-variable predicates)",
        ));
    }
    let (var, prop) = property_of(l)?;
    let value = literal(r)?;
    let filter_op = match op {
        Cmp::Eq => None,
        Cmp::Ne => return Err(unsupported("<> (inequality)")),
        Cmp::Lt => Some(FilterOp::Lt),
        Cmp::Lte => Some(FilterOp::Lte),
        Cmp::Gt => Some(FilterOp::Gt),
        Cmp::Gte => Some(FilterOp::Gte),
        Cmp::In => Some(FilterOp::In),
        Cmp::StartsWith => Some(FilterOp::StartsWith),
        Cmp::EndsWith => Some(FilterOp::EndsWith),
        Cmp::Contains => Some(FilterOp::Contains),
    };
    out.push(Pred::Filter(var, prop, filter(filter_op, Some(value))));
    Ok(())
}

fn property_of(expr: Expr) -> Result<(String, String), Error> {
    match expr {
        Expr::Prop(var, prop) => Ok((var, prop)),
        Expr::ElementId(_) => Err(unsupported(
            "element_id() with anything but =, IN, or BETWEEN",
        )),
        Expr::Value(_) => Err(semantic("the property must be on the left-hand side")),
        _ => Err(unsupported("nested expressions as comparison operands")),
    }
}

fn literal(expr: Expr) -> Result<serde_json::Value, Error> {
    match expr {
        Expr::Value(v) => Ok(v),
        Expr::Prop(..) | Expr::ElementId(_) => Err(unsupported(
            "comparing two variables (cross-variable predicates)",
        )),
        _ => Err(unsupported("nested expressions as comparison operands")),
    }
}

fn integer(expr: Expr) -> Result<i64, Error> {
    match literal(expr)? {
        serde_json::Value::Number(n) if n.is_i64() => Ok(n.as_i64().expect("checked")),
        other => Err(semantic(format!(
            "element_id() expects an integer, got {other}"
        ))),
    }
}

fn integer_list(expr: Expr) -> Result<Vec<i64>, Error> {
    let serde_json::Value::Array(items) = literal(expr)? else {
        return Err(semantic("element_id() IN expects a list of integers"));
    };
    if items.is_empty() {
        return Err(semantic("element_id() IN needs at least one id"));
    }
    items
        .iter()
        .map(|v| {
            v.as_i64()
                .ok_or_else(|| semantic(format!("element_id() expects integers, got {v}")))
        })
        .collect()
}

pub(crate) enum ReturnItem {
    Var(String, Option<String>),
    /// `count(*)`; a row count, which the DSL spells as `count` of a node.
    CountAll(Option<String>),
    AllProps(String),
    Prop(String, String, Option<String>),
    Trunc(TruncateUnit, String, String, Option<String>),
    Agg(AggExpr, Option<String>),
}

#[derive(Default)]
struct Returned {
    vars: Vec<(String, Option<String>)>,
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
        if let Pred::Filter(_, prop, filter) = &pred
            && let Some(value) = &filter.value
        {
            check_filter_value(prop, value)?;
        }
        match pred {
            Pred::Ids(target, ids) => self.id_target(target)?.node_ids.extend(ids),
            Pred::IdRange(target, start, end) => {
                let node = self.id_target(target)?;
                if node.id_range.is_some() {
                    return Err(semantic(format!(
                        "element_id({}) BETWEEN given more than once",
                        node.id
                    )));
                }
                node.id_range = Some(InputIdRange { start, end });
            }
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

    fn id_target(&mut self, target: ElementId) -> Result<&mut InputNode, Error> {
        let node = self.node_mut(&target.var)?;
        let wanted = target
            .property
            .unwrap_or_else(|| DEFAULT_PRIMARY_KEY.to_string());
        let pinned = !node.node_ids.is_empty() || node.id_range.is_some();
        if pinned && node.id_property != wanted {
            return Err(semantic(format!(
                "element_id({}) is matched on both {} and {wanted}",
                node.id, node.id_property
            )));
        }
        node.id_property = wanted;
        Ok(node)
    }

    fn select_all_columns(&mut self, var: &str) -> Result<(), Error> {
        let node = self.node_mut(var)?;
        node.columns = Some(ColumnSelection::All);
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
                && n.id_range.is_none()
                && n.columns.is_none()
        })
    }
}

pub(crate) fn assemble(
    shortest: bool,
    patterns: Vec<Pattern>,
    filter: Option<Expr>,
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
    let mut preds = Vec::new();
    if let Some(expr) = filter {
        lower_where(expr, &mut preds)?;
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
            ReturnItem::Var(v, alias) => {
                g.node_mut(&v)?;
                returned.vars.push((v, alias));
            }
            ReturnItem::AllProps(v) => {
                g.select_all_columns(&v)?;
                returned.vars.push((v, None));
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
            // With one node, counting it is the row count. With more, the
            // DSL's per-node count drives hop predicates, so `*` is ambiguous.
            ReturnItem::CountAll(alias) => {
                let [node] = g.nodes.as_slice() else {
                    return Err(unsupported(
                        "count(*) over more than one node (count a specific variable)",
                    ));
                };
                let expr = AggExpr::Count(TargetRef {
                    node: node.id.clone(),
                    property: None,
                });
                metrics.push(InputAggregationMetric { expr, alias });
            }
        }
    }

    let mut input = Input::default();
    if let Some(limit) = limit {
        if !(1..=MAX_LIMIT).contains(&limit) {
            return Err(semantic(format!("LIMIT must be between 1 and {MAX_LIMIT}")));
        }
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
    for (v, alias) in &returned.vars {
        projection.push(OutputColumn::node_as(v, alias.clone()));
    }
    for (v, p, alias) in returned.props {
        projection.push(OutputColumn::property(&v, &p, alias));
        g.select_column(&v, p)?;
    }
    let returned_vars: Vec<String> = returned.vars.into_iter().map(|(v, _)| v).collect();
    if let Some(neighbors) = detect_neighbors(&mut g, &returned_vars)? {
        input.query_type = QueryType::Neighbors;
        input.neighbors = Some(neighbors);
    } else {
        input.query_type = QueryType::Traversal;
        input.relationships = std::mem::take(&mut g.rels);
    }
    input.order_by = traversal_order(order)?;
    input.nodes = labelled(g.nodes)?;
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
    input.nodes = labelled(g.nodes)?;
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
            for (v, alias) in returned_vars {
                let key = keys
                    .iter_mut()
                    .find(|k| matches!(k, InputGroupByKey::Node { .. }) && k.node() == v);
                let Some(InputGroupByKey::Node {
                    alias: key_alias, ..
                }) = key
                else {
                    return Err(semantic(format!(
                        "RETURN {v} is neither aggregated nor a GROUP BY key"
                    )));
                };
                projection.push(OutputColumn::node_as(&v, alias.clone()));
                *key_alias = alias;
            }
            for (v, p, alias) in returned_props {
                let key = keys
                    .iter_mut()
                    .find(|k| k.node() == v && k.property() == Some(p.as_str()));
                projection.push(OutputColumn::property(&v, &p, alias.clone()));
                match key {
                    Some(InputGroupByKey::Property {
                        alias: key_alias, ..
                    }) => *key_alias = alias,
                    Some(InputGroupByKey::Node { .. }) => {
                        unreachable!("node keys have no property")
                    }
                    None => g.select_column(&v, p)?,
                }
            }
            keys
        }
        None => {
            let mut keys = Vec::new();
            for (node, alias) in returned_vars {
                projection.push(OutputColumn::node_as(&node, alias.clone()));
                keys.push(InputGroupByKey::Node { node, alias });
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
    input.nodes = labelled(g.nodes)?;
    Ok(Parsed { input, projection })
}

fn same_truncation(key: &InputGroupByKey, wanted: &InputGroupByKey) -> bool {
    key.node() == wanted.node()
        && key.property() == wanted.property()
        && key.truncate() == wanted.truncate()
}

/// The DSL requires `entity` on every node; only the neighbours blank may
/// be unlabelled, and it has been removed from the graph by this point.
fn labelled(nodes: Vec<InputNode>) -> Result<Vec<InputNode>, Error> {
    if let Some(node) = nodes.iter().find(|n| n.entity.is_none()) {
        return Err(semantic(format!(
            "node {} needs a label, e.g. ({}:Project)",
            node.id, node.id
        )));
    }
    Ok(nodes)
}

fn check_filter_value(prop: &str, value: &serde_json::Value) -> Result<(), Error> {
    match value {
        serde_json::Value::String(s) if s.len() > MAX_FILTER_VALUE_LEN => Err(semantic(format!(
            "filter value on {prop} exceeds {MAX_FILTER_VALUE_LEN} characters"
        ))),
        serde_json::Value::Array(items) if items.len() > MAX_IN_VALUES => Err(semantic(format!(
            "filter list on {prop} exceeds {MAX_IN_VALUES} values"
        ))),
        serde_json::Value::Array(items) => {
            items.iter().try_for_each(|v| check_filter_value(prop, v))
        }
        _ => Ok(()),
    }
}
