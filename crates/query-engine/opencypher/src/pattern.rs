//! The MATCH half of a statement: node and relationship drafts collected
//! before the statement is classified, with WHERE predicates attached.

use std::collections::HashMap;

use compiler::FilterOp;
use serde_json::Value as Json;

use crate::syntax::{P, Rule};
use crate::tree::{Ctx, Named, Result, child, children, first, start};

const TOKEN_FUNCTIONS: &[(&str, FilterOp)] = &[
    ("token_match", FilterOp::TokenMatch),
    ("all_tokens", FilterOp::AllTokens),
    ("any_tokens", FilterOp::AnyTokens),
];
const TEMPORAL_FUNCTIONS: &[&str] = &["date", "datetime"];

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum VarKind {
    Node(usize),
    Edge(usize),
    Path,
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum Arrow {
    Left,
    Right,
    Undirected,
}

pub struct Node {
    pub var: Named,
    pub label: Option<Named>,
    pub preds: Vec<Pred>,
}

pub struct Range {
    pub min: Option<i64>,
    pub max: Option<i64>,
    pub at: usize,
}

pub struct Edge {
    pub left: usize,
    pub right: usize,
    pub arrow: Arrow,
    pub var: Option<Named>,
    pub types: Vec<Named>,
    pub range: Option<Range>,
    pub preds: Vec<Pred>,
    pub at: usize,
}

pub struct Pred {
    pub property: Named,
    pub op: FilterOp,
    pub value: Option<Json>,
    pub at: usize,
}

#[derive(Default)]
pub struct Graph {
    pub nodes: Vec<Node>,
    pub edges: Vec<Edge>,
    pub vars: HashMap<String, VarKind>,
    pub is_path: bool,
}

impl Graph {
    pub fn collect(ctx: &Ctx<'_>, clause: &P<'_>) -> Result<Graph> {
        let mut graph = Graph::default();
        let patterns: Vec<P<'_>> = children(clause, Rule::pattern).collect();
        for pattern in &patterns {
            let chain = match child(pattern, Rule::path_function) {
                Some(path_fn) => {
                    if patterns.len() > 1 {
                        return ctx.fail(start(pattern), "shortestPath() must be the only pattern");
                    }
                    let chain = child(&path_fn, Rule::chain).expect("path function chain");
                    if children(&chain, Rule::node_pattern).count() != 2 {
                        return ctx.fail(
                            start(pattern),
                            "shortestPath() takes two nodes joined by one relationship",
                        );
                    }
                    if let Some(variable) = child(pattern, Rule::ident) {
                        graph.bind(ctx, &ctx.named(&variable)?, VarKind::Path)?;
                    }
                    graph.is_path = true;
                    chain
                }
                None => child(pattern, Rule::chain).expect("pattern chain"),
            };
            let mut previous: Option<usize> = None;
            let mut pending_edge: Option<P<'_>> = None;
            for element in chain.into_inner() {
                match element.as_rule() {
                    Rule::node_pattern => {
                        let index = graph.add_node(ctx, &element)?;
                        if let (Some(left), Some(edge)) = (previous, pending_edge.take()) {
                            graph.add_edge(ctx, &edge, left, index)?;
                        }
                        previous = Some(index);
                    }
                    Rule::edge_pattern => pending_edge = Some(element),
                    _ => {}
                }
            }
        }
        Ok(graph)
    }

    fn bind(&mut self, ctx: &Ctx<'_>, variable: &Named, kind: VarKind) -> Result<()> {
        if self.vars.insert(variable.name.clone(), kind).is_some() {
            return ctx.fail(
                variable.at,
                format!("variable `{}` is already bound", variable.name),
            );
        }
        Ok(())
    }

    /// The node a variable names; an error for relationships, paths, and unbound names.
    pub fn node_index(&self, ctx: &Ctx<'_>, variable: &Named) -> Result<usize> {
        match self.vars.get(&variable.name) {
            Some(VarKind::Node(i)) => Ok(*i),
            Some(_) => ctx.fail(variable.at, format!("`{}` is not a node", variable.name)),
            None => ctx.unbound(variable),
        }
    }

    fn add_node(&mut self, ctx: &Ctx<'_>, node: &P<'_>) -> Result<usize> {
        let variable =
            ctx.named(&child(node, Rule::ident).expect("anonymous nodes are rejected earlier"))?;
        let label = match child(node, Rule::node_label) {
            Some(l) => Some(ctx.named(&child(&l, Rule::schema_name).expect("label name"))?),
            None => None,
        };
        // A variable mentioned again in a later pattern part is the same node;
        // that is how star-shaped traversals are spelled.
        let index = match self.vars.get(&variable.name) {
            Some(VarKind::Node(index)) => {
                let existing = &mut self.nodes[*index];
                match (&existing.label, label) {
                    (None, Some(label)) => existing.label = Some(label),
                    (Some(have), Some(label)) if have.name != label.name => {
                        return ctx.fail(
                            label.at,
                            format!("`{}` is already labeled :{}", variable.name, have.name),
                        );
                    }
                    _ => {}
                }
                *index
            }
            Some(_) => {
                return ctx.fail(
                    variable.at,
                    format!("`{}` is already a relationship or path", variable.name),
                );
            }
            None => {
                self.vars
                    .insert(variable.name.clone(), VarKind::Node(self.nodes.len()));
                self.nodes.push(Node {
                    var: variable,
                    label,
                    preds: Vec::new(),
                });
                self.nodes.len() - 1
            }
        };
        if let Some(map) = child(node, Rule::property_map) {
            for kv in children(&map, Rule::property_kv) {
                let pred = map_entry(ctx, &kv)?;
                self.nodes[index].preds.push(pred);
            }
        }
        Ok(index)
    }

    fn add_edge(&mut self, ctx: &Ctx<'_>, edge: &P<'_>, left: usize, right: usize) -> Result<()> {
        let form = first(&first(edge));
        let arrow = match form.as_rule() {
            Rule::edge_left | Rule::arrow_left => Arrow::Left,
            Rule::edge_right | Rule::arrow_right => Arrow::Right,
            _ => Arrow::Undirected,
        };
        let mut draft = Edge {
            left,
            right,
            arrow,
            var: None,
            types: Vec::new(),
            range: None,
            preds: Vec::new(),
            at: start(edge),
        };
        if let Some(body) = child(&form, Rule::edge_body) {
            if let Some(variable) = child(&body, Rule::ident) {
                let named = ctx.named(&variable)?;
                self.bind(ctx, &named, VarKind::Edge(self.edges.len()))?;
                draft.var = Some(named);
            }
            for t in child(&body, Rule::type_spec)
                .iter()
                .flat_map(|t| children(t, Rule::schema_name))
            {
                draft.types.push(ctx.named(&t)?);
            }
            draft.range = child(&body, Rule::range_literal).map(|r| range(&r));
            for kv in child(&body, Rule::property_map)
                .iter()
                .flat_map(|m| children(m, Rule::property_kv))
            {
                draft.preds.push(map_entry(ctx, &kv)?);
            }
        }
        self.edges.push(draft);
        Ok(())
    }

    pub fn apply_where(&mut self, ctx: &Ctx<'_>, where_clause: &P<'_>) -> Result<()> {
        let mut predicates = Vec::new();
        flatten(
            &child(where_clause, Rule::condition).expect("WHERE condition"),
            &mut predicates,
        );
        for pair in predicates {
            let (variable, pred) = predicate(ctx, &pair)?;
            match self.vars.get(&variable.name) {
                Some(VarKind::Node(i)) => self.nodes[*i].preds.push(pred),
                Some(VarKind::Edge(i)) => self.edges[*i].preds.push(pred),
                Some(VarKind::Path) => {
                    return ctx.fail(variable.at, "a path has no properties to filter on");
                }
                None => return ctx.unbound(&variable),
            }
        }
        Ok(())
    }
}

/// `variable.property` as (variable, property).
pub fn property_ref(ctx: &Ctx<'_>, pair: &P<'_>) -> Result<(Named, Named)> {
    let variable = ctx.named(&child(pair, Rule::ident).expect("property variable"))?;
    let property = ctx.named(&child(pair, Rule::schema_name).expect("property name"))?;
    Ok((variable, property))
}

fn range(pair: &P<'_>) -> Range {
    let dots = pair.as_str().find("..").map(|i| start(pair) + i);
    let mut out = Range {
        min: None,
        max: None,
        at: start(pair),
    };
    for int in children(pair, Rule::integer) {
        let value = int.as_str().parse::<i64>().ok();
        match dots {
            Some(d) if start(&int) > d => out.max = value,
            Some(_) => out.min = value,
            None => (out.min, out.max) = (value, value),
        }
    }
    out
}

/// `{key: value}` is an equality filter, as a bare JSON filter value is.
fn map_entry(ctx: &Ctx<'_>, kv: &P<'_>) -> Result<Pred> {
    let key = ctx.named(&child(kv, Rule::schema_name).expect("property key"))?;
    let value = ctx.json(&child(kv, Rule::value).expect("property value"))?;
    if value.is_array() {
        return ctx.fail(
            start(kv),
            format!(
                "`{{{0}: [...]}}` is a list equality; use `WHERE x.{0} IN [...]`",
                key.name
            ),
        );
    }
    Ok(Pred {
        property: key,
        op: FilterOp::Eq,
        value: Some(value),
        at: start(kv),
    })
}

/// The condition is a conjunction (NOT/OR/XOR were rejected earlier), so
/// parentheses only group.
fn flatten<'i>(condition: &P<'i>, out: &mut Vec<P<'i>>) {
    for term in children(condition, Rule::term) {
        let inner = first(&term);
        match inner.as_rule() {
            Rule::paren_condition => flatten(&first(&inner), out),
            _ => out.push(inner),
        }
    }
}

fn predicate(ctx: &Ctx<'_>, pred: &P<'_>) -> Result<(Named, Pred)> {
    let lhs = first(&child(pred, Rule::operand).expect("predicate operand"));
    let tail = child(pred, Rule::predicate_tail);
    if lhs.as_rule() == Rule::func_call {
        return token_function(ctx, &lhs, tail.is_some(), start(pred));
    }
    if lhs.as_rule() != Rule::property_ref {
        return ctx.fail(
            start(&lhs),
            "the left side of a predicate must be `variable.property`",
        );
    }
    let (variable, property) = property_ref(ctx, &lhs)?;
    let Some(tail) = tail else {
        return ctx.fail(
            start(pred),
            format!(
                "`{}` alone is not a predicate; compare it, e.g. `{} = true`",
                lhs.as_str(),
                lhs.as_str()
            ),
        );
    };
    let head = first(&tail);
    let op = match head.as_rule() {
        Rule::comp_op => match head.as_str() {
            "=" => FilterOp::Eq,
            "<" => FilterOp::Lt,
            ">" => FilterOp::Gt,
            "<=" => FilterOp::Lte,
            _ => FilterOp::Gte,
        },
        Rule::kw_in => FilterOp::In,
        Rule::null_test if child(&head, Rule::kw_not).is_some() => FilterOp::IsNotNull,
        Rule::null_test => FilterOp::IsNull,
        Rule::kw_starts => FilterOp::StartsWith,
        Rule::kw_ends => FilterOp::EndsWith,
        _ => FilterOp::Contains,
    };
    let value = match child(&tail, Rule::operand) {
        Some(operand) => Some(rhs(ctx, &operand, op == FilterOp::In)?),
        None => None,
    };
    Ok((
        variable,
        Pred {
            property,
            op,
            value,
            at: start(pred),
        },
    ))
}

fn token_function(ctx: &Ctx<'_>, call: &P<'_>, has_tail: bool, at: usize) -> Result<(Named, Pred)> {
    let name = ctx.named(&child(call, Rule::ident).expect("function name"))?;
    let Some((_, op)) = TOKEN_FUNCTIONS
        .iter()
        .find(|(n, _)| name.name.eq_ignore_ascii_case(n))
    else {
        return ctx.fail(name.at, format!("function `{}` is not allowed in WHERE; only token_match, all_tokens, any_tokens are", name.name));
    };
    if has_tail {
        return ctx.fail(
            at,
            format!(
                "`{}(...)` is already a predicate; it cannot be compared",
                name.name
            ),
        );
    }
    let args: Vec<P<'_>> = children(call, Rule::func_arg).map(|a| first(&a)).collect();
    let [property, value] = args.as_slice() else {
        return ctx.fail(
            at,
            format!(
                "`{0}` takes two arguments: `{0}(variable.property, 'text')`",
                name.name
            ),
        );
    };
    let property = first(property);
    if property.as_rule() != Rule::property_ref {
        return ctx.fail(
            start(&property),
            "the first argument must be `variable.property`",
        );
    }
    let (variable, property) = property_ref(ctx, &property)?;
    let value = Some(rhs(ctx, value, false)?);
    Ok((
        variable,
        Pred {
            property,
            op: *op,
            value,
            at,
        },
    ))
}

/// The right side of a predicate: a literal, a parameter, or `date('...')`.
fn rhs(ctx: &Ctx<'_>, operand: &P<'_>, want_list: bool) -> Result<Json> {
    let inner = first(operand);
    let value = match inner.as_rule() {
        Rule::value => ctx.json(&inner)?,
        Rule::func_call => {
            let name = child(&inner, Rule::ident).expect("function name");
            let args: Vec<P<'_>> = children(&inner, Rule::func_arg).collect();
            let arg = args.first().map(|a| first(&first(a)));
            match arg {
                Some(v) if args.len() == 1 && v.as_rule() == Rule::value && first(&v).as_rule() == Rule::string
                    && TEMPORAL_FUNCTIONS.iter().any(|t| name.as_str().eq_ignore_ascii_case(t)) => ctx.json(&v)?,
                _ => return ctx.fail(start(&name), format!("`{}` is not allowed here; the right side is a literal, a parameter, or date('...')", name.as_str())),
            }
        }
        Rule::property_ref => return ctx.fail(start(&inner), "comparing two properties is not supported; the right side must be a literal or parameter"),
        _ => return ctx.fail(start(&inner), "the right side of a predicate must be a literal or parameter"),
    };
    if value.is_array() != want_list {
        return ctx.fail(
            start(&inner),
            if want_list {
                "IN takes a list `[...]` or a list parameter"
            } else {
                "a list is only valid after IN"
            },
        );
    }
    Ok(value)
}
