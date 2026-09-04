//! pest parse tree to [`compiler::Input`], plus syntax-error rendering.
//!
//! There is no intermediate AST: the pest [`Pair`] tree already carries rule,
//! text, span, and children, so lowering walks it directly. Classification
//! (traversal, aggregation, path finding, neighbors) needs the whole pattern
//! and RETURN list, so the pattern is first collected into small drafts and
//! the RETURN items are viewed twice; pairs are `Rc` handles, so that is cheap.
//!
//! Everything lands in the `Input` through constructors and JSON-visible
//! fields only, so the result is the structure the JSON frontend deserializes
//! from the equivalent DSL document and every later compiler pass runs
//! unchanged.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::hash::{Hash, Hasher};

use compiler::input::{
    AggExpr, Direction, HopRange, InputAggSort, InputAggregation, InputAggregationMetric,
    InputGroupByKey, InputIdRange, InputNeighbors, InputOrderBy, InputPath, InputRelationship,
    OrderDirection, PathType, PropertyRef, TargetRef, TruncateUnit,
};
use compiler::{ColumnSelection, FilterOp, Input, InputFilter, InputNode, QueryError, QueryType};
use ontology::{Ontology, TRAVERSAL_PATH_COLUMN};
use pest::Parser;
use pest::iterators::Pair;
use pest_derive::Parser;
use serde_json::Value as Json;
use strum::VariantNames;

use crate::{MAX_FILTER_STRING_CHARS, MAX_IN_VALUES, MAX_LIMIT, Parameters};

#[derive(Parser)]
#[grammar = "grammar.pest"]
struct CypherParser;

type Result<T> = std::result::Result<T, QueryError>;
type P<'i> = Pair<'i, Rule>;

const ID_COLUMN: &str = "id";
const WILDCARD_TYPE: &str = "*";
const DEFAULT_PATH_MAX_DEPTH: u32 = 3;
const MAX_ID_STRING_CHARS: usize = 20;
const AGGREGATE_FUNCTIONS: &[&str] = &["count", "sum", "avg", "min", "max", "collect"];
const TOKEN_FUNCTIONS: &[(&str, FilterOp)] = &[
    ("token_match", FilterOp::TokenMatch),
    ("all_tokens", FilterOp::AllTokens),
    ("any_tokens", FilterOp::AnyTokens),
];
const TEMPORAL_FUNCTIONS: &[&str] = &["date", "datetime"];
const DATE_TRUNC: &str = "date_trunc";
const SHORTEST_PATH: &str = "shortestPath";

/// Parse, substitute parameters, and lower. Returns the `Input` and the
/// cursor-binding hash of the substituted token stream.
pub fn lower(source: &str, params: &Parameters, ontology: &Ontology) -> Result<(Input, u64)> {
    let statement = CypherParser::parse(Rule::statement, source)
        .map_err(|e| QueryError::Syntax(render_error(source, &e)))?
        .next()
        .expect("statement rule produces one pair");
    let lowerer = Lowerer {
        ontology,
        source,
        params,
    };
    let query_hash = lowerer.token_hash(&statement);
    let input = lowerer.lower(&statement)?;
    Ok((input, query_hash))
}

pub fn line_col(source: &str, offset: usize) -> (usize, usize) {
    let offset = offset.min(source.len());
    let before = &source[..offset];
    let line = before.matches('\n').count() + 1;
    let col = before.rfind('\n').map_or(offset, |i| offset - i - 1) + 1;
    (line, col)
}

// ── Pair helpers ────────────────────────────────────────────────────────────

fn child<'i>(pair: &P<'i>, rule: Rule) -> Option<P<'i>> {
    pair.clone().into_inner().find(|c| c.as_rule() == rule)
}

fn children<'i>(pair: &P<'i>, rule: Rule) -> impl Iterator<Item = P<'i>> {
    pair.clone()
        .into_inner()
        .filter(move |c| c.as_rule() == rule)
}

fn first<'i>(pair: &P<'i>) -> P<'i> {
    pair.clone()
        .into_inner()
        .next()
        .unwrap_or_else(|| panic!("{:?} has an inner pair", pair.as_rule()))
}

fn start(pair: &P<'_>) -> usize {
    pair.as_span().start()
}

fn ident_name(pair: &P<'_>) -> String {
    let raw = pair.as_str();
    raw.strip_prefix('`')
        .and_then(|s| s.strip_suffix('`'))
        .unwrap_or(raw)
        .to_string()
}

fn unescape(quoted: &str) -> String {
    let quote = quoted
        .chars()
        .next()
        .expect("quoted string has a delimiter");
    let body = &quoted[1..quoted.len() - 1];
    let mut out = String::with_capacity(body.len());
    let mut chars = body.chars().peekable();
    while let Some(c) = chars.next() {
        if c == quote && chars.peek() == Some(&quote) {
            chars.next();
            out.push(quote);
            continue;
        }
        if c != '\\' {
            out.push(c);
            continue;
        }
        match chars.next() {
            Some('n') => out.push('\n'),
            Some('t') => out.push('\t'),
            Some('r') => out.push('\r'),
            Some('b') => out.push('\u{8}'),
            Some('f') => out.push('\u{c}'),
            Some('u') => {
                let hex: String = chars.by_ref().take(4).collect();
                match u32::from_str_radix(&hex, 16).ok().and_then(char::from_u32) {
                    Some(ch) => out.push(ch),
                    None => {
                        out.push_str("\\u");
                        out.push_str(&hex);
                    }
                }
            }
            Some(other) => out.push(other),
            None => out.push('\\'),
        }
    }
    out
}

// ── Drafts (the pattern half of the statement, collected before classification) ──

#[derive(Clone, Copy, PartialEq, Eq)]
enum VarKind {
    Node(usize),
    Edge(usize),
    Path,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Arrow {
    Left,
    Right,
    Undirected,
}

struct Named {
    name: String,
    at: usize,
}

struct NodeDraft {
    var: Named,
    label: Option<Named>,
    preds: Vec<Pred>,
}

struct Range {
    min: Option<i64>,
    max: Option<i64>,
    at: usize,
}

struct EdgeDraft {
    left: usize,
    right: usize,
    arrow: Arrow,
    var: Option<Named>,
    types: Vec<Named>,
    range: Option<Range>,
    preds: Vec<Pred>,
    at: usize,
}

struct Pred {
    property: Named,
    op: FilterOp,
    value: Option<Json>,
    at: usize,
}

struct Graph {
    nodes: Vec<NodeDraft>,
    edges: Vec<EdgeDraft>,
    vars: HashMap<String, VarKind>,
    is_path: bool,
}

/// A RETURN, WHERE, or ORDER BY expression, viewed once per use.
enum Expr<'i> {
    Property {
        var: Named,
        prop: Named,
        at: usize,
    },
    Variable(Named),
    Value(P<'i>),
    Function {
        name: Named,
        distinct: bool,
        args: Vec<Arg<'i>>,
        at: usize,
    },
    GqlTemporal {
        keyword: String,
        at: usize,
    },
}

enum Arg<'i> {
    Star(usize),
    Expr(Expr<'i>),
}

impl Expr<'_> {
    fn at(&self) -> usize {
        match self {
            Self::Property { at, .. }
            | Self::Function { at, .. }
            | Self::GqlTemporal { at, .. } => *at,
            Self::Variable(v) => v.at,
            Self::Value(p) => start(p),
        }
    }

    /// Span-free rendering used to match an ORDER BY key against RETURN items.
    fn render(&self) -> String {
        match self {
            Self::Property { var, prop, .. } => format!("{}.{}", var.name, prop.name),
            Self::Variable(v) => v.name.clone(),
            Self::Value(p) => p.as_str().split_whitespace().collect(),
            Self::Function {
                name,
                distinct,
                args,
                ..
            } => {
                let args: Vec<String> = args
                    .iter()
                    .map(|a| match a {
                        Arg::Star(_) => "*".to_string(),
                        Arg::Expr(e) => e.render(),
                    })
                    .collect();
                format!(
                    "{}({}{})",
                    name.name.to_ascii_lowercase(),
                    if *distinct { "distinct " } else { "" },
                    args.join(", ")
                )
            }
            Self::GqlTemporal { keyword, .. } => keyword.clone(),
        }
    }
}

#[derive(Default)]
struct ColumnDraft {
    bare: bool,
    properties: Vec<String>,
}

impl ColumnDraft {
    fn selection(&self) -> Option<ColumnSelection> {
        if !self.properties.is_empty() {
            Some(ColumnSelection::List(self.properties.clone()))
        } else if self.bare {
            Some(ColumnSelection::All)
        } else {
            None
        }
    }
}

struct Lowerer<'a> {
    ontology: &'a Ontology,
    source: &'a str,
    params: &'a Parameters,
}

impl<'i> Lowerer<'_> {
    fn at(&self, offset: usize, msg: impl AsRef<str>) -> String {
        let (line, col) = line_col(self.source, offset);
        format!("line {line}, column {col}: {}", msg.as_ref())
    }

    fn validation(&self, offset: usize, msg: impl AsRef<str>) -> QueryError {
        QueryError::Validation(self.at(offset, msg))
    }

    fn named(&self, ident: &P<'_>) -> Result<Named> {
        let name = ident_name(ident);
        if !is_valid_identifier(&name) {
            return Err(self.validation(
                start(ident),
                format!("identifier `{name}` must match ^[a-zA-Z_][a-zA-Z0-9_]{{0,63}}$"),
            ));
        }
        Ok(Named {
            name,
            at: start(ident),
        })
    }

    // ── Hash ─────────────────────────────────────────────────────────────

    /// FNV-1a over the token stream with parameters substituted, mirroring
    /// `compiler::passes::cursor::canonical_hash`. Leaves hash their text
    /// (keywords case-folded, identifiers unbackticked, strings unescaped),
    /// so whitespace and comments do not move the hash.
    fn token_hash(&self, statement: &P<'_>) -> u64 {
        struct Fnv1a(u64);
        impl Hasher for Fnv1a {
            fn finish(&self) -> u64 {
                self.0
            }
            fn write(&mut self, bytes: &[u8]) {
                for b in bytes {
                    self.0 ^= u64::from(*b);
                    self.0 = self.0.wrapping_mul(0x100000001b3);
                }
            }
        }
        fn walk(pair: &P<'_>, params: &Parameters, h: &mut Fnv1a) {
            let rule = pair.as_rule();
            rule.hash(h);
            let mut inner = pair.clone().into_inner().peekable();
            match rule {
                Rule::parameter => {
                    let name = &pair.as_str()[1..];
                    match params.get(name) {
                        Some(v) => v.to_string().hash(h),
                        None => name.hash(h),
                    }
                }
                Rule::ident => ident_name(pair).hash(h),
                Rule::string => unescape(pair.as_str()).hash(h),
                _ if inner.peek().is_none() => pair.as_str().to_ascii_uppercase().hash(h),
                _ => {
                    for c in inner {
                        walk(&c, params, h);
                    }
                }
            }
        }
        let mut hasher = Fnv1a(0xcbf29ce484222325);
        walk(statement, self.params, &mut hasher);
        hasher.finish()
    }

    // ── Entry ────────────────────────────────────────────────────────────

    fn lower(&self, statement: &P<'i>) -> Result<Input> {
        let clause = self.single_match(statement)?;
        let mut graph = self.collect_graph(&clause)?;
        if let Some(where_clause) = child(&clause, Rule::where_clause) {
            let condition = first(&where_clause);
            let mut predicates = Vec::new();
            self.flatten(&condition, &mut predicates)?;
            for predicate in predicates {
                self.assign_predicate(&predicate, &mut graph)?;
            }
        }
        let ret = child(statement, Rule::return_clause).expect("grammar requires RETURN");
        self.check_return_shape(&ret)?;
        let items = self.return_items(&ret)?;

        let has_aggregate = items.iter().any(|(expr, _)| self.is_aggregate(expr));
        let unlabeled: Vec<usize> = (0..graph.nodes.len())
            .filter(|i| graph.nodes[*i].label.is_none())
            .collect();

        if graph.is_path {
            if has_aggregate {
                return Err(self.validation(
                    start(&ret),
                    "shortestPath() cannot be combined with aggregate functions",
                ));
            }
            if let Some(&i) = unlabeled.first() {
                return Err(self.unlabeled_node(&graph.nodes[i]));
            }
            return self.lower_path(&graph, &ret, &items);
        }
        if graph.edges.len() == 1 && graph.nodes.len() == 2 && unlabeled.len() == 1 {
            if has_aggregate {
                return Err(self.validation(
                    start(&ret),
                    "a neighbors pattern (one endpoint without a label) cannot be aggregated; \
                     give both nodes a label",
                ));
            }
            return self.lower_neighbors(&graph, &ret, &items, unlabeled[0]);
        }
        if let Some(&i) = unlabeled.first() {
            return Err(self.unlabeled_node(&graph.nodes[i]));
        }
        if has_aggregate {
            self.lower_aggregation(&graph, &ret, &items)
        } else {
            self.lower_traversal(&graph, &ret, &items)
        }
    }

    fn unlabeled_node(&self, node: &NodeDraft) -> QueryError {
        self.validation(
            node.var.at,
            format!(
                "node `{}` needs a label; a node without a label is only allowed as the far \
                 endpoint of a single-relationship neighbors pattern",
                node.var.name
            ),
        )
    }

    fn single_match(&self, statement: &P<'i>) -> Result<P<'i>> {
        let mut matches = children(statement, Rule::match_clause);
        let clause = matches.next().expect("grammar requires MATCH");
        if let Some(extra) = matches.next() {
            return Err(self.validation(
                start(&extra),
                "only one MATCH clause is supported; join the patterns with a comma or a shared \
                 variable instead",
            ));
        }
        if let Some(optional) = child(&clause, Rule::kw_optional) {
            return Err(self.validation(
                start(&optional),
                "OPTIONAL MATCH is not supported; every pattern is required",
            ));
        }
        Ok(clause)
    }

    fn check_return_shape(&self, ret: &P<'_>) -> Result<()> {
        if let Some(distinct) = child(ret, Rule::kw_distinct) {
            return Err(self.validation(
                start(&distinct),
                "DISTINCT is not supported; traversal results are already deduplicated entity \
                 sets and the DSL has no distinct aggregate",
            ));
        }
        if let Some(skip) = child(ret, Rule::skip_clause) {
            return Err(self.validation(
                start(&skip),
                "SKIP/OFFSET are not supported; the DSL paginates with a keyset cursor supplied \
                 beside the statement",
            ));
        }
        if let Some(order_by) = child(ret, Rule::order_by_clause) {
            let mut keys = children(&order_by, Rule::sort_item);
            let key = keys.next().expect("ORDER BY has a key");
            if let Some(extra) = keys.next() {
                return Err(self.validation(start(&extra), "ORDER BY takes exactly one key"));
            }
            if let Some(nulls) = child(&key, Rule::nulls_order) {
                return Err(self.validation(
                    start(&nulls),
                    "NULLS FIRST/LAST is not supported; NULL sort keys always sort last",
                ));
            }
        }
        Ok(())
    }

    // ── Patterns ─────────────────────────────────────────────────────────

    fn collect_graph(&self, clause: &P<'i>) -> Result<Graph> {
        let mut graph = Graph {
            nodes: Vec::new(),
            edges: Vec::new(),
            vars: HashMap::new(),
            is_path: false,
        };
        let patterns: Vec<P<'i>> = children(clause, Rule::pattern).collect();
        for pattern in &patterns {
            let variable = child(pattern, Rule::ident);
            let chain = match child(pattern, Rule::path_function) {
                Some(path_fn) => {
                    if patterns.len() > 1 {
                        return Err(self.validation(
                            start(pattern),
                            "shortestPath() must be the only pattern in the MATCH clause",
                        ));
                    }
                    let name = child(&path_fn, Rule::path_fn_name).expect("path function name");
                    if !name.as_str().eq_ignore_ascii_case(SHORTEST_PATH) {
                        return Err(self.validation(
                            start(&name),
                            "allShortestPaths() is not supported; only shortestPath() exists \
                             (path.type: shortest)",
                        ));
                    }
                    let chain = child(&path_fn, Rule::chain).expect("path function chain");
                    if children(&chain, Rule::node_pattern).count() != 2 {
                        return Err(self.validation(
                            start(pattern),
                            "shortestPath() takes exactly two nodes joined by one relationship",
                        ));
                    }
                    if let Some(variable) = &variable {
                        let named = self.named(variable)?;
                        self.bind(&mut graph, named, VarKind::Path)?;
                    }
                    graph.is_path = true;
                    chain
                }
                None => {
                    if let Some(variable) = &variable {
                        return Err(self.validation(
                            start(variable),
                            "path variables are only supported with shortestPath(); traversal \
                             results are entity sets, not paths",
                        ));
                    }
                    child(pattern, Rule::chain).expect("pattern chain")
                }
            };
            let mut previous: Option<usize> = None;
            let mut pending_edge: Option<P<'i>> = None;
            for element in chain.into_inner() {
                match element.as_rule() {
                    Rule::node_pattern => {
                        let index = self.add_node(&mut graph, &element)?;
                        if let (Some(left), Some(edge)) = (previous, pending_edge.take()) {
                            self.add_edge(&mut graph, &edge, left, index)?;
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

    fn bind(&self, graph: &mut Graph, variable: Named, kind: VarKind) -> Result<()> {
        if graph.vars.insert(variable.name.clone(), kind).is_some() {
            return Err(self.validation(
                variable.at,
                format!(
                    "variable `{}` is already bound to a different pattern element",
                    variable.name
                ),
            ));
        }
        Ok(())
    }

    fn add_node(&self, graph: &mut Graph, node: &P<'i>) -> Result<usize> {
        let Some(variable) = child(node, Rule::ident) else {
            return Err(self.validation(
                start(node),
                "every node needs a variable, e.g. `(n:Label)`; the DSL uses it as the node \
                 selector id and in output column names",
            ));
        };
        let variable = self.named(&variable)?;
        if let Some(inline) = child(node, Rule::inline_where) {
            return Err(self.validation(
                start(&inline),
                "WHERE inside a node pattern is GQL syntax; move the predicate to the statement \
                 WHERE clause",
            ));
        }
        let mut labels = children(node, Rule::node_label);
        let label = match labels.next() {
            Some(l) => Some(self.named(&child(&l, Rule::ident).expect("label identifier"))?),
            None => None,
        };
        if let Some(extra) = labels.next() {
            return Err(self.validation(
                start(&extra),
                "a node has exactly one label (one entity per selector)",
            ));
        }
        let index = match graph.vars.get(&variable.name) {
            Some(VarKind::Node(index)) => {
                let existing = &mut graph.nodes[*index];
                match (&existing.label, label) {
                    (None, Some(label)) => existing.label = Some(label),
                    (Some(have), Some(label)) if have.name != label.name => {
                        return Err(self.validation(
                            label.at,
                            format!("node `{}` is already labeled :{}", variable.name, have.name),
                        ));
                    }
                    _ => {}
                }
                *index
            }
            Some(_) => {
                return Err(self.validation(
                    variable.at,
                    format!(
                        "variable `{}` is already bound to a relationship or path",
                        variable.name
                    ),
                ));
            }
            None => {
                graph
                    .vars
                    .insert(variable.name.clone(), VarKind::Node(graph.nodes.len()));
                graph.nodes.push(NodeDraft {
                    var: variable,
                    label,
                    preds: Vec::new(),
                });
                graph.nodes.len() - 1
            }
        };
        if let Some(map) = child(node, Rule::property_map) {
            for kv in children(&map, Rule::property_kv) {
                let pred = self.map_entry(&kv)?;
                graph.nodes[index].preds.push(pred);
            }
        }
        Ok(index)
    }

    fn add_edge(&self, graph: &mut Graph, edge: &P<'i>, left: usize, right: usize) -> Result<()> {
        if let Some(quantifier) = child(edge, Rule::gql_quantifier) {
            return Err(self.validation(
                start(&quantifier),
                "`{m,n}` is the GQL quantifier; openCypher spells a hop range `*m..n` inside the \
                 brackets, e.g. -[:TYPE*1..3]->",
            ));
        }
        let form = first(&first(edge));
        let arrow = match form.as_rule() {
            Rule::edge_left | Rule::arrow_left | Rule::gql_arrow_left => Arrow::Left,
            Rule::edge_right | Rule::arrow_right | Rule::gql_arrow_right => Arrow::Right,
            _ => Arrow::Undirected,
        };
        if matches!(
            form.as_rule(),
            Rule::gql_arrow_left | Rule::gql_arrow_right | Rule::gql_dash
        ) {
            return Err(self.validation(
                start(edge),
                "`->`, `<-`, and `-` are GQL abbreviations; openCypher spells them `-->`, `<--`, \
                 and `--`",
            ));
        }
        let mut draft = EdgeDraft {
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
            if let Some(inline) = child(&body, Rule::inline_where) {
                return Err(self.validation(
                    start(&inline),
                    "WHERE inside a relationship pattern is GQL syntax; move the predicate to the \
                     statement WHERE clause",
                ));
            }
            if let Some(variable) = child(&body, Rule::ident) {
                let named = self.named(&variable)?;
                let index = graph.edges.len();
                self.bind(
                    graph,
                    Named {
                        name: named.name.clone(),
                        at: named.at,
                    },
                    VarKind::Edge(index),
                )?;
                draft.var = Some(named);
            }
            if let Some(types) = child(&body, Rule::type_spec) {
                for t in children(&types, Rule::ident) {
                    draft.types.push(self.named(&t)?);
                }
            }
            if let Some(range) = child(&body, Rule::range_literal) {
                draft.range = Some(self.range(&range));
            }
            if let Some(map) = child(&body, Rule::property_map) {
                for kv in children(&map, Rule::property_kv) {
                    draft.preds.push(self.map_entry(&kv)?);
                }
            }
        }
        graph.edges.push(draft);
        Ok(())
    }

    fn range(&self, range: &P<'_>) -> Range {
        let text = range.as_str();
        let dots = text.find("..").map(|i| start(range) + i);
        let mut out = Range {
            min: None,
            max: None,
            at: start(range),
        };
        for int in children(range, Rule::integer) {
            let value = int.as_str().parse::<i64>().ok();
            match dots {
                Some(d) if start(&int) > d => out.max = value,
                Some(_) => out.min = value,
                None => {
                    out.min = value;
                    out.max = value;
                }
            }
        }
        out
    }

    fn map_entry(&self, kv: &P<'_>) -> Result<Pred> {
        let key = self.named(&child(kv, Rule::ident).expect("property key"))?;
        let value_pair = child(kv, Rule::value).expect("property value");
        let value = self.json(&value_pair)?;
        if value.is_array() {
            return Err(self.validation(
                start(&value_pair),
                format!(
                    "`{{{}: [...]}}` is a list equality; use `WHERE x.{} IN [...]`",
                    key.name, key.name
                ),
            ));
        }
        Ok(Pred {
            at: start(kv),
            property: key,
            op: FilterOp::Eq,
            value: Some(value),
        })
    }

    // ── Expressions and values ───────────────────────────────────────────

    fn expr(&self, operand: &P<'i>) -> Result<Expr<'i>> {
        let inner = first(operand);
        Ok(match inner.as_rule() {
            Rule::property_ref => {
                let mut idents = children(&inner, Rule::ident);
                let var = self.named(&idents.next().expect("property variable"))?;
                let prop = self.named(&idents.next().expect("property name"))?;
                Expr::Property {
                    var,
                    prop,
                    at: start(&inner),
                }
            }
            Rule::ident => Expr::Variable(self.named(&inner)?),
            Rule::value => Expr::Value(inner),
            Rule::func_call => {
                let name = self.named(&child(&inner, Rule::ident).expect("function name"))?;
                let mut args = Vec::new();
                for arg in children(&inner, Rule::func_arg) {
                    let arg = first(&arg);
                    args.push(match arg.as_rule() {
                        Rule::star => Arg::Star(start(&arg)),
                        _ => Arg::Expr(self.expr(&arg)?),
                    });
                }
                Expr::Function {
                    name,
                    distinct: child(&inner, Rule::kw_distinct).is_some(),
                    args,
                    at: start(&inner),
                }
            }
            Rule::gql_temporal => Expr::GqlTemporal {
                keyword: child(&inner, Rule::gql_temporal_kw)
                    .map(|k| k.as_str().to_ascii_uppercase())
                    .unwrap_or_default(),
                at: start(&inner),
            },
            other => unreachable!("operand form {other:?}"),
        })
    }

    /// A literal or parameter as the JSON frontend would carry it.
    fn json(&self, value: &P<'_>) -> Result<Json> {
        let inner = first(value);
        let at = start(&inner);
        Ok(match inner.as_rule() {
            Rule::string => {
                let s = unescape(inner.as_str());
                self.check_string(&s, at)?;
                Json::String(s)
            }
            Rule::integer => match inner.as_str().parse::<i64>() {
                Ok(i) => Json::from(i),
                Err(_) => return Err(self.validation(at, "integer literal is out of range")),
            },
            Rule::float => inner
                .as_str()
                .parse::<f64>()
                .ok()
                .and_then(serde_json::Number::from_f64)
                .map(Json::Number)
                .ok_or_else(|| self.validation(at, "number literal is out of range"))?,
            Rule::boolean => Json::Bool(inner.as_str().eq_ignore_ascii_case("true")),
            Rule::null => {
                return Err(
                    self.validation(at, "NULL is not a filter value; use IS NULL or IS NOT NULL")
                );
            }
            Rule::list => {
                let items: Vec<P<'_>> = children(&inner, Rule::value).collect();
                self.check_list_len(items.len(), at)?;
                let mut out = Vec::with_capacity(items.len());
                for item in &items {
                    if first(item).as_rule() == Rule::list {
                        return Err(self.validation(start(item), "lists cannot be nested"));
                    }
                    out.push(self.json(item)?);
                }
                Json::Array(out)
            }
            Rule::parameter => {
                let name = &inner.as_str()[1..];
                let Some(bound) = self.params.get(name) else {
                    return Err(self.validation(at, format!("parameter ${name} is not bound")));
                };
                self.param_json(bound, at, true)?
            }
            other => unreachable!("value form {other:?}"),
        })
    }

    fn param_json(&self, bound: &Json, at: usize, allow_list: bool) -> Result<Json> {
        match bound {
            Json::Null => Err(self.validation(
                at,
                "a NULL parameter is not a filter value; use IS NULL or IS NOT NULL",
            )),
            Json::Bool(_) => Ok(bound.clone()),
            Json::Number(n) => {
                if n.is_i64() || n.is_f64() {
                    Ok(bound.clone())
                } else {
                    Err(self.validation(at, format!("parameter value {n} is out of range")))
                }
            }
            Json::String(s) => {
                self.check_string(s, at)?;
                Ok(bound.clone())
            }
            Json::Array(items) if allow_list => {
                self.check_list_len(items.len(), at)?;
                items
                    .iter()
                    .map(|item| self.param_json(item, at, false))
                    .collect::<Result<Vec<_>>>()
                    .map(Json::Array)
            }
            Json::Array(_) => Err(self.validation(at, "lists cannot be nested")),
            Json::Object(_) => Err(self.validation(
                at,
                "parameter value must be a scalar or a list, not an object",
            )),
        }
    }

    fn check_string(&self, s: &str, at: usize) -> Result<()> {
        let len = s.chars().count();
        if len > MAX_FILTER_STRING_CHARS {
            return Err(self.validation(
                at,
                format!(
                    "string literal is {len} characters; the maximum is {MAX_FILTER_STRING_CHARS}"
                ),
            ));
        }
        Ok(())
    }

    fn check_list_len(&self, len: usize, at: usize) -> Result<()> {
        if len > MAX_IN_VALUES {
            return Err(QueryError::LimitExceeded(self.at(
                at,
                format!("list has {len} values; the maximum is {MAX_IN_VALUES}"),
            )));
        }
        Ok(())
    }

    // ── WHERE ────────────────────────────────────────────────────────────

    fn flatten(&self, condition: &P<'i>, out: &mut Vec<P<'i>>) -> Result<()> {
        for part in condition.clone().into_inner() {
            match part.as_rule() {
                Rule::kw_not => {
                    return Err(self.validation(
                        start(&part),
                        "NOT is not supported; filters are conjunctions of single-property \
                         comparisons (use IS NOT NULL or IN with the complementary set)",
                    ));
                }
                Rule::bool_op => {
                    if first(&part).as_rule() != Rule::kw_and {
                        return Err(self.validation(
                            start(&part),
                            "OR and XOR are not supported; filters on one selector AND-combine \
                             and there is no disjunction across selectors",
                        ));
                    }
                }
                Rule::term => {
                    let inner = first(&part);
                    match inner.as_rule() {
                        Rule::paren_condition => self.flatten(&first(&inner), out)?,
                        _ => out.push(inner),
                    }
                }
                _ => {}
            }
        }
        Ok(())
    }

    fn assign_predicate(&self, predicate: &P<'i>, graph: &mut Graph) -> Result<()> {
        let (variable, pred) = self.lower_predicate(predicate)?;
        match graph.vars.get(&variable.name) {
            Some(VarKind::Node(i)) => graph.nodes[*i].preds.push(pred),
            Some(VarKind::Edge(i)) => graph.edges[*i].preds.push(pred),
            Some(VarKind::Path) => {
                return Err(self.validation(
                    variable.at,
                    format!(
                        "path variable `{}` has no properties to filter on",
                        variable.name
                    ),
                ));
            }
            None => return Err(self.unbound(&variable)),
        }
        Ok(())
    }

    fn unbound(&self, variable: &Named) -> QueryError {
        QueryError::ReferenceError(self.at(
            variable.at,
            format!(
                "variable `{}` is not bound in the MATCH pattern",
                variable.name
            ),
        ))
    }

    fn lower_predicate(&self, predicate: &P<'i>) -> Result<(Named, Pred)> {
        let lhs = self.expr(&child(predicate, Rule::operand).expect("predicate operand"))?;
        let tail = child(predicate, Rule::predicate_tail);
        let (var, prop) = match lhs {
            Expr::Property { var, prop, .. } => (var, prop),
            Expr::Function { name, args, at, .. } => {
                return self.lower_token_function(predicate, name, args, at, tail.is_some());
            }
            Expr::Variable(v) => {
                return Err(self.validation(
                    v.at,
                    format!(
                        "`{}` is a variable; predicates compare `variable.property` with a value",
                        v.name
                    ),
                ));
            }
            other => {
                return Err(self.validation(
                    other.at(),
                    "the left side of a predicate must be `variable.property`",
                ));
            }
        };
        let Some(tail) = tail else {
            return Err(self.validation(
                start(predicate),
                format!(
                    "`{}.{}` alone is not a predicate; compare it explicitly, e.g. `{}.{} = true`",
                    var.name, prop.name, var.name, prop.name
                ),
            ));
        };
        let mut parts = tail.clone().into_inner();
        let head = parts.next().expect("predicate tail form");
        let rhs = || parts.clone().find(|p| p.as_rule() == Rule::operand);
        let (op, value) = match head.as_rule() {
            Rule::comp_op => {
                let op = match head.as_str() {
                    "=" => FilterOp::Eq,
                    "<" => FilterOp::Lt,
                    ">" => FilterOp::Gt,
                    "<=" => FilterOp::Lte,
                    ">=" => FilterOp::Gte,
                    "<>" | "!=" => {
                        return Err(self.validation(
                            start(&head),
                            "`<>`/`!=` are not supported (the DSL has no not-equal operator); \
                             for enumerations use IN with the complementary set",
                        ));
                    }
                    _ => {
                        return Err(self.validation(
                            start(&head),
                            "`=~` regex matching is not supported; use STARTS WITH, ENDS WITH, \
                             CONTAINS, or the token functions",
                        ));
                    }
                };
                let rhs = rhs().expect("comparison rhs");
                if first(&rhs).as_rule() == Rule::value
                    && first(&first(&rhs)).as_rule() == Rule::null
                {
                    return Err(self.validation(
                        start(&rhs),
                        "comparing with NULL is always false in openCypher; use IS NULL or \
                         IS NOT NULL",
                    ));
                }
                (op, Some(self.rhs_scalar(&rhs)?))
            }
            Rule::kw_in => {
                let rhs = rhs().expect("IN rhs");
                let value = self.rhs_value(&rhs)?;
                if !value.is_array() {
                    return Err(self.validation(
                        start(&rhs),
                        "IN takes a list literal `[...]` or a list parameter",
                    ));
                }
                (FilterOp::In, Some(value))
            }
            Rule::null_test => {
                if child(&head, Rule::kw_not).is_some() {
                    (FilterOp::IsNotNull, None)
                } else {
                    (FilterOp::IsNull, None)
                }
            }
            Rule::kw_starts => (
                FilterOp::StartsWith,
                Some(self.rhs_scalar(&rhs().expect("rhs"))?),
            ),
            Rule::kw_ends => (
                FilterOp::EndsWith,
                Some(self.rhs_scalar(&rhs().expect("rhs"))?),
            ),
            _ => (
                FilterOp::Contains,
                Some(self.rhs_scalar(&rhs().expect("rhs"))?),
            ),
        };
        Ok((
            var,
            Pred {
                property: prop,
                op,
                value,
                at: start(predicate),
            },
        ))
    }

    fn lower_token_function(
        &self,
        predicate: &P<'i>,
        name: Named,
        args: Vec<Arg<'i>>,
        at: usize,
        has_tail: bool,
    ) -> Result<(Named, Pred)> {
        let lower_name = name.name.to_ascii_lowercase();
        let Some((_, op)) = TOKEN_FUNCTIONS.iter().find(|(n, _)| *n == lower_name) else {
            let hint = if AGGREGATE_FUNCTIONS.contains(&lower_name.as_str()) {
                "; aggregate functions belong in RETURN"
            } else {
                "; the only functions allowed in WHERE are token_match, all_tokens, and any_tokens"
            };
            return Err(self.validation(
                name.at,
                format!("function `{}` is not supported in WHERE{hint}", name.name),
            ));
        };
        if has_tail {
            return Err(self.validation(
                start(predicate),
                format!(
                    "`{}(...)` is already a predicate; it cannot be compared",
                    name.name
                ),
            ));
        }
        let mut args = args.into_iter();
        let (Some(Arg::Expr(Expr::Property { var, prop, .. })), Some(Arg::Expr(rhs)), None) =
            (args.next(), args.next(), args.next())
        else {
            return Err(self.validation(
                at,
                format!(
                    "`{}` takes two arguments: `{}(variable.property, 'text')`",
                    name.name, name.name
                ),
            ));
        };
        let value = self.rhs_scalar_expr(&rhs)?;
        Ok((
            var,
            Pred {
                property: prop,
                op: *op,
                value: Some(value),
                at: start(predicate),
            },
        ))
    }

    fn rhs_scalar(&self, operand: &P<'i>) -> Result<Json> {
        self.rhs_scalar_expr(&self.expr(operand)?)
    }

    fn rhs_scalar_expr(&self, expr: &Expr<'i>) -> Result<Json> {
        let value = self.rhs_value_expr(expr)?;
        if value.is_array() {
            return Err(self.validation(expr.at(), "a list is only valid on the right side of IN"));
        }
        Ok(value)
    }

    fn rhs_value(&self, operand: &P<'i>) -> Result<Json> {
        self.rhs_value_expr(&self.expr(operand)?)
    }

    fn rhs_value_expr(&self, expr: &Expr<'i>) -> Result<Json> {
        match expr {
            Expr::Value(value) => self.json(value),
            Expr::Function { name, args, at, .. } => {
                let lower_name = name.name.to_ascii_lowercase();
                if !TEMPORAL_FUNCTIONS.contains(&lower_name.as_str()) {
                    return Err(self.validation(
                        name.at,
                        format!(
                            "function `{}` is not supported here; the right side of a predicate \
                             is a literal, a parameter, or date('...')/datetime('...')",
                            name.name
                        ),
                    ));
                }
                match args.as_slice() {
                    [Arg::Expr(Expr::Value(v))] if first(v).as_rule() == Rule::string => {
                        self.json(v)
                    }
                    _ => Err(self.validation(
                        *at,
                        format!("`{}` takes a single string argument", name.name),
                    )),
                }
            }
            Expr::Property { at, .. } => Err(self.validation(
                *at,
                "comparing two properties is not supported; the right side must be a literal or \
                 parameter",
            )),
            Expr::Variable(v) => Err(self.validation(
                v.at,
                format!(
                    "`{}` is a variable; the right side of a predicate must be a literal or \
                     parameter",
                    v.name
                ),
            )),
            Expr::GqlTemporal { keyword, at } => Err(self.validation(
                *at,
                format!(
                    "`{keyword} '...'` is the GQL temporal literal; write {}('...') or a plain \
                     string, the column type drives the binding",
                    keyword.to_ascii_lowercase()
                ),
            )),
        }
    }

    // ── Filters against the ontology ─────────────────────────────────────

    fn node_filters(
        &self,
        node: &NodeDraft,
        entity: &str,
    ) -> Result<(
        BTreeMap<String, Vec<InputFilter>>,
        Vec<i64>,
        Option<InputIdRange>,
    )> {
        let mut node_ids = Vec::new();
        let mut range_start: Option<(i64, &Pred)> = None;
        let mut range_end: Option<(i64, &Pred)> = None;
        let mut rest = Vec::new();
        for pred in &node.preds {
            self.check_property(entity, &pred.property)?;
            if pred.property.name != ID_COLUMN {
                rest.push(pred);
                continue;
            }
            match (pred.op, &pred.value) {
                (FilterOp::Eq, Some(value)) => node_ids.push(self.node_id(value, pred.at)?),
                (FilterOp::In, Some(Json::Array(items))) => {
                    for item in items {
                        node_ids.push(self.node_id(item, pred.at)?);
                    }
                }
                (FilterOp::Gte, Some(value)) if range_start.is_none() => {
                    range_start = Some((self.node_id(value, pred.at)?, pred));
                }
                (FilterOp::Lte, Some(value)) if range_end.is_none() => {
                    range_end = Some((self.node_id(value, pred.at)?, pred));
                }
                _ => rest.push(pred),
            }
        }
        let id_range = match (range_start, range_end) {
            (Some((start, _)), Some((end, _))) => Some(InputIdRange { start, end }),
            (Some((_, pred)), None) | (None, Some((_, pred))) => {
                rest.push(pred);
                None
            }
            (None, None) => None,
        };
        Ok((filter_map(&rest), node_ids, id_range))
    }

    fn node_id(&self, value: &Json, at: usize) -> Result<i64> {
        match value {
            Json::Number(n) if n.is_i64() => Ok(n.as_i64().expect("checked i64")),
            Json::String(s)
                if !s.is_empty()
                    && s.len() <= MAX_ID_STRING_CHARS
                    && s.bytes().all(|b| b.is_ascii_digit()) =>
            {
                s.parse::<i64>()
                    .map_err(|_| self.validation(at, format!("id {s} is out of range")))
            }
            other => Err(self.validation(
                at,
                format!("`id` takes an integer (or a digit string), got {other}"),
            )),
        }
    }

    fn check_property(&self, entity: &str, property: &Named) -> Result<()> {
        let name = property.name.as_str();
        if name == ID_COLUMN || self.ontology.has_field(entity, name) {
            return Ok(());
        }
        if name == TRAVERSAL_PATH_COLUMN
            && self
                .ontology
                .get_node(entity)
                .is_some_and(|n| n.has_traversal_path)
        {
            return Ok(());
        }
        let candidates = self
            .ontology
            .get_node(entity)
            .map(|n| n.fields.iter().map(|f| f.name.as_str()).collect::<Vec<_>>())
            .unwrap_or_default();
        Err(QueryError::AllowlistRejected(self.at(
            property.at,
            format!(
                "unknown property `{name}` on {entity}{}",
                suggestion(name, candidates.iter().copied())
            ),
        )))
    }

    fn check_label(&self, label: &Named) -> Result<()> {
        if self.ontology.has_node(&label.name) {
            return Ok(());
        }
        Err(QueryError::AllowlistRejected(self.at(
            label.at,
            format!(
                "unknown node label `{}`{}",
                label.name,
                suggestion(&label.name, self.ontology.node_names())
            ),
        )))
    }

    fn check_column(&self, node: &NodeDraft, entity: &str, column: &str) -> Result<()> {
        self.ontology.validate_field(entity, column).map_err(|e| {
            QueryError::AllowlistRejected(self.at(
                node.var.at,
                format!("invalid column for `{}`: {e}", node.var.name),
            ))
        })
    }

    fn rel_types(&self, types: &[Named]) -> Result<Vec<String>> {
        types
            .iter()
            .map(|t| {
                if self.ontology.has_edge(&t.name) {
                    Ok(t.name.clone())
                } else {
                    Err(QueryError::AllowlistRejected(self.at(
                        t.at,
                        format!(
                            "unknown relationship type `{}`{}",
                            t.name,
                            suggestion(&t.name, self.ontology.edge_names())
                        ),
                    )))
                }
            })
            .collect()
    }

    fn hops(&self, range: Option<&Range>) -> Result<HopRange> {
        let Some(range) = range else {
            return Ok(HopRange::default());
        };
        let bounded = |bound: Option<i64>| -> Result<Option<u32>> {
            match bound {
                None => Ok(None),
                Some(n) if n >= 1 => Ok(Some(u32::try_from(n).unwrap_or(u32::MAX))),
                Some(_) => Err(self.validation(
                    range.at,
                    "hop bounds start at 1; the DSL has no zero-length hops",
                )),
            }
        };
        let max = bounded(range.max)?.ok_or_else(|| {
            self.validation(
                range.at,
                "unbounded hop ranges (`*`, `*1..`) are not supported; give an upper bound of at \
                 most 3, e.g. `*1..3`",
            )
        })?;
        let min = bounded(range.min)?.unwrap_or(1);
        if min > max {
            return Err(self.validation(range.at, format!("hop range *{min}..{max} is inverted")));
        }
        Ok(HopRange { min, max })
    }

    /// Canonical `from`/`to`: a left-pointing edge is the same relationship as
    /// its mirror image, so `(a)<-[:T]-(b)` builds `{from: b, to: a}` exactly
    /// as a JSON author would write it.
    fn endpoints(&self, graph: &Graph, edge: &EdgeDraft) -> (String, String, Direction) {
        let left = graph.nodes[edge.left].var.name.clone();
        let right = graph.nodes[edge.right].var.name.clone();
        match edge.arrow {
            Arrow::Right => (left, right, Direction::Outgoing),
            Arrow::Left => (right, left, Direction::Outgoing),
            Arrow::Undirected => (left, right, Direction::Both),
        }
    }

    fn relationships(&self, graph: &Graph) -> Result<Vec<InputRelationship>> {
        graph
            .edges
            .iter()
            .map(|edge| {
                let mut types = self.rel_types(&edge.types)?;
                if types.is_empty() {
                    types.push(WILDCARD_TYPE.to_string());
                }
                let (from, to, direction) = self.endpoints(graph, edge);
                Ok(InputRelationship::new(
                    types,
                    from,
                    to,
                    self.hops(edge.range.as_ref())?,
                    direction,
                    filter_map(&edge.preds.iter().collect::<Vec<_>>()),
                ))
            })
            .collect()
    }

    fn input_node(&self, node: &NodeDraft, columns: Option<&ColumnDraft>) -> Result<InputNode> {
        let label = node.label.as_ref().expect("classification requires labels");
        self.check_label(label)?;
        let (filters, node_ids, id_range) = self.node_filters(node, &label.name)?;
        let columns = columns.and_then(ColumnDraft::selection);
        if let Some(ColumnSelection::List(cols)) = &columns {
            for col in cols {
                self.check_column(node, &label.name, col)?;
            }
        }
        Ok(InputNode {
            id: node.var.name.clone(),
            entity: Some(label.name.clone()),
            columns,
            filters,
            node_ids,
            id_range,
            ..Default::default()
        })
    }

    fn input_nodes(
        &self,
        graph: &Graph,
        columns: &HashMap<usize, ColumnDraft>,
    ) -> Result<Vec<InputNode>> {
        graph
            .nodes
            .iter()
            .enumerate()
            .map(|(i, node)| self.input_node(node, columns.get(&i)))
            .collect()
    }

    // ── RETURN ───────────────────────────────────────────────────────────

    fn return_items(&self, ret: &P<'i>) -> Result<Vec<(Expr<'i>, Option<Named>)>> {
        let body = child(ret, Rule::return_body).expect("RETURN body");
        let mut items = Vec::new();
        for item in children(&body, Rule::return_item) {
            let expr = self.expr(&child(&item, Rule::operand).expect("return operand"))?;
            let alias = match child(&item, Rule::ident) {
                Some(alias) => Some(self.named(&alias)?),
                None => None,
            };
            items.push((expr, alias));
        }
        Ok(items)
    }

    fn return_star(&self, ret: &P<'i>) -> bool {
        child(ret, Rule::return_body).is_some_and(|body| child(&body, Rule::star).is_some())
    }

    fn is_aggregate(&self, expr: &Expr<'_>) -> bool {
        matches!(expr, Expr::Function { name, .. }
            if AGGREGATE_FUNCTIONS.contains(&name.name.to_ascii_lowercase().as_str()))
    }

    /// Bare variables and `variable.property` items become the node's
    /// `columns`; a bare variable alone selects every column.
    fn column_drafts(
        &self,
        graph: &Graph,
        ret: &P<'i>,
        items: &[(Expr<'i>, Option<Named>)],
        skip: impl Fn(&Expr<'i>) -> bool,
    ) -> Result<HashMap<usize, ColumnDraft>> {
        let mut drafts: HashMap<usize, ColumnDraft> = HashMap::new();
        if self.return_star(ret) {
            for i in 0..graph.nodes.len() {
                drafts.entry(i).or_default().bare = true;
            }
        }
        for (expr, _) in items {
            if skip(expr) {
                continue;
            }
            match expr {
                Expr::Variable(v) => match graph.vars.get(&v.name) {
                    Some(VarKind::Node(i)) => drafts.entry(*i).or_default().bare = true,
                    Some(VarKind::Path) => {}
                    Some(VarKind::Edge(_)) => {
                        return Err(self.validation(
                            v.at,
                            format!(
                                "relationship `{}` cannot be returned; the DSL returns entities",
                                v.name
                            ),
                        ));
                    }
                    None => return Err(self.unbound(v)),
                },
                Expr::Property { var, prop, at } => match graph.vars.get(&var.name) {
                    Some(VarKind::Node(i)) => {
                        drafts
                            .entry(*i)
                            .or_default()
                            .properties
                            .push(prop.name.clone());
                    }
                    Some(_) => {
                        return Err(self.validation(*at, "only node properties can be returned"));
                    }
                    None => return Err(self.unbound(var)),
                },
                Expr::Function { name, .. } => {
                    return Err(self.validation(
                        name.at,
                        format!(
                            "function `{}` is not supported in RETURN; return `variable` or \
                             `variable.property`",
                            name.name
                        ),
                    ));
                }
                Expr::Value(_) | Expr::GqlTemporal { .. } => {
                    return Err(self.validation(expr.at(), "literals cannot be returned"));
                }
            }
        }
        Ok(drafts)
    }

    fn reject_aliases(&self, items: &[(Expr<'i>, Option<Named>)]) -> Result<()> {
        if let Some(alias) = items.iter().find_map(|(_, alias)| alias.as_ref()) {
            return Err(self.validation(
                alias.at,
                "AS aliases are only supported in aggregation statements; traversal columns keep \
                 their property names",
            ));
        }
        Ok(())
    }

    fn limit(&self, ret: &P<'_>) -> Result<Option<u32>> {
        let Some(limit) = child(ret, Rule::limit_clause) else {
            return Ok(None);
        };
        let value = child(&limit, Rule::value).expect("LIMIT value");
        let at = start(&value);
        match self.json(&value)? {
            Json::Number(n)
                if n.as_i64()
                    .is_some_and(|i| i >= 1 && i <= i64::from(MAX_LIMIT)) =>
            {
                Ok(Some(
                    u32::try_from(n.as_i64().expect("checked")).expect("bounded"),
                ))
            }
            Json::Number(n) if n.is_i64() => Err(self.validation(
                at,
                format!("LIMIT {n} is out of range; limit must be between 1 and {MAX_LIMIT}"),
            )),
            _ => Err(self.validation(at, "LIMIT takes an integer")),
        }
    }

    fn sort_key(&self, ret: &P<'i>) -> Result<Option<(Expr<'i>, OrderDirection, usize)>> {
        let Some(order_by) = child(ret, Rule::order_by_clause) else {
            return Ok(None);
        };
        let key = child(&order_by, Rule::sort_item).expect("ORDER BY key");
        let expr = self.expr(&child(&key, Rule::operand).expect("sort operand"))?;
        let descending = child(&key, Rule::sort_direction)
            .is_some_and(|d| matches!(first(&d).as_rule(), Rule::kw_desc | Rule::kw_descending));
        let direction = if descending {
            OrderDirection::Desc
        } else {
            OrderDirection::Asc
        };
        Ok(Some((expr, direction, start(&key))))
    }

    fn order_by(&self, graph: &Graph, ret: &P<'i>) -> Result<Option<InputOrderBy>> {
        let Some((expr, direction, at)) = self.sort_key(ret)? else {
            return Ok(None);
        };
        let Expr::Property { var, prop, .. } = expr else {
            return Err(self.validation(at, "ORDER BY takes `variable.property`"));
        };
        match graph.vars.get(&var.name) {
            Some(VarKind::Node(_)) => {}
            Some(_) => return Err(self.validation(at, "ORDER BY takes a node property")),
            None => return Err(self.unbound(&var)),
        }
        Ok(Some(InputOrderBy {
            node: var.name,
            property: prop.name,
            direction,
        }))
    }

    fn base_input(&self, query_type: QueryType, ret: &P<'_>) -> Result<Input> {
        let mut input = Input {
            query_type,
            ..Input::default()
        };
        if let Some(limit) = self.limit(ret)? {
            input.limit = limit;
        }
        Ok(input)
    }

    // ── Query types ──────────────────────────────────────────────────────

    fn lower_traversal(
        &self,
        graph: &Graph,
        ret: &P<'i>,
        items: &[(Expr<'i>, Option<Named>)],
    ) -> Result<Input> {
        self.reject_aliases(items)?;
        let columns = self.column_drafts(graph, ret, items, |_| false)?;
        let mut input = self.base_input(QueryType::Traversal, ret)?;
        input.nodes = self.input_nodes(graph, &columns)?;
        input.relationships = self.relationships(graph)?;
        input.order_by = self.order_by(graph, ret)?;
        Ok(input)
    }

    fn lower_aggregation(
        &self,
        graph: &Graph,
        ret: &P<'i>,
        items: &[(Expr<'i>, Option<Named>)],
    ) -> Result<Input> {
        if self.return_star(ret) {
            return Err(self.validation(
                start(ret),
                "RETURN * cannot be combined with aggregate functions; name the group keys",
            ));
        }
        for edge in &graph.edges {
            if edge.arrow == Arrow::Undirected {
                return Err(self.validation(
                    edge.at,
                    "an undirected relationship is not supported in an aggregation (the OR join \
                     defeats index use); use -[]-> or <-[]-",
                ));
            }
        }
        let bare_nodes: HashSet<&str> = items
            .iter()
            .filter_map(|(expr, _)| match expr {
                Expr::Variable(v) => Some(v.name.as_str()),
                _ => None,
            })
            .collect();
        let node_var = |v: &Named| -> Result<()> {
            match graph.vars.get(&v.name) {
                Some(VarKind::Node(_)) => Ok(()),
                Some(_) => Err(self.validation(
                    v.at,
                    format!("`{}` is not a node; only nodes and node properties can be grouped or aggregated", v.name),
                )),
                None => Err(self.unbound(v)),
            }
        };

        let mut aggregation = InputAggregation::default();
        let mut outputs: Vec<(String, String)> = Vec::new();
        for (expr, alias) in items {
            let alias_name = alias.as_ref().map(|a| a.name.clone());
            match expr {
                Expr::Function { .. } if self.is_aggregate(expr) => {
                    let metric = InputAggregationMetric {
                        expr: self.aggregate(graph, expr)?,
                        alias: alias_name,
                    };
                    outputs.push((expr.render(), metric.output_name()));
                    aggregation.metrics.push(metric);
                }
                Expr::Function { name, .. } if name.name.eq_ignore_ascii_case(DATE_TRUNC) => {
                    let key = self.date_trunc(graph, expr, alias_name)?;
                    outputs.push((expr.render(), key.output_name()));
                    aggregation.group_by.push(key);
                }
                Expr::Function { name, .. } => {
                    return Err(self.validation(
                        name.at,
                        format!(
                            "function `{}` is not supported in RETURN; aggregates are count, sum, \
                             avg, min, max; date_trunc('unit', x.prop) buckets a group key",
                            name.name
                        ),
                    ));
                }
                Expr::Variable(v) => {
                    node_var(v)?;
                    let key = InputGroupByKey::Node {
                        node: v.name.clone(),
                        alias: alias_name,
                    };
                    outputs.push((expr.render(), key.output_name()));
                    aggregation.group_by.push(key);
                }
                Expr::Property { var, prop, .. } => {
                    node_var(var)?;
                    // Beside a bare `u`, `u.prop` selects a column of the grouped
                    // node (result-equivalent, since the properties are determined
                    // by the node); alone it is a group key.
                    if bare_nodes.contains(var.name.as_str()) {
                        if let Some(alias) = alias {
                            return Err(self.validation(
                                alias.at,
                                "a column of a grouped node cannot be aliased",
                            ));
                        }
                        continue;
                    }
                    let key = InputGroupByKey::Property {
                        node: var.name.clone(),
                        property: prop.name.clone(),
                        truncate: None,
                        alias: alias_name,
                    };
                    outputs.push((expr.render(), key.output_name()));
                    aggregation.group_by.push(key);
                }
                Expr::Value(_) | Expr::GqlTemporal { .. } => {
                    return Err(self.validation(expr.at(), "literals cannot be returned"));
                }
            }
        }
        aggregation.sort = self.aggregation_sort(ret, &outputs)?;

        let columns = self.column_drafts(graph, ret, items, |expr| match expr {
            // Group keys and metrics are not column selections; only bare
            // nodes and the properties beside them are.
            Expr::Variable(_) => false,
            Expr::Property { var, .. } => !bare_nodes.contains(var.name.as_str()),
            _ => true,
        })?;
        let mut input = self.base_input(QueryType::Aggregation, ret)?;
        input.nodes = self.input_nodes(graph, &columns)?;
        input.relationships = self.relationships(graph)?;
        input.aggregation = aggregation;
        Ok(input)
    }

    fn aggregate(&self, graph: &Graph, expr: &Expr<'i>) -> Result<AggExpr> {
        let Expr::Function {
            name,
            distinct,
            args,
            at,
        } = expr
        else {
            unreachable!("is_aggregate checked");
        };
        if *distinct {
            return Err(self.validation(*at, "DISTINCT aggregates are not supported"));
        }
        let function = name.name.to_ascii_lowercase();
        let arg = match args.as_slice() {
            [Arg::Star(star)] => {
                return Err(self.validation(
                    *star,
                    format!(
                        "count(*) is not supported; name the node to count, e.g. count({})",
                        graph.nodes.first().map_or("n", |n| n.var.name.as_str())
                    ),
                ));
            }
            [Arg::Expr(arg)] => arg,
            _ => {
                return Err(
                    self.validation(*at, format!("`{}` takes exactly one argument", name.name))
                );
            }
        };
        let node_of = |v: &Named| -> Result<String> {
            match graph.vars.get(&v.name) {
                Some(VarKind::Node(_)) => Ok(v.name.clone()),
                Some(_) => Err(self.validation(
                    v.at,
                    format!("`{}` is not a node; aggregates target nodes", v.name),
                )),
                None => Err(self.unbound(v)),
            }
        };
        let property_ref = |var: &Named, prop: &Named| -> Result<PropertyRef> {
            Ok(PropertyRef {
                node: node_of(var)?,
                property: prop.name.clone(),
            })
        };
        Ok(match (function.as_str(), arg) {
            ("count", Expr::Variable(v)) => AggExpr::Count(TargetRef {
                node: node_of(v)?,
                property: None,
            }),
            ("count", Expr::Property { var, prop, .. }) => AggExpr::Count(TargetRef {
                node: node_of(var)?,
                property: Some(prop.name.clone()),
            }),
            ("sum", Expr::Property { var, prop, .. }) => AggExpr::Sum(property_ref(var, prop)?),
            ("avg", Expr::Property { var, prop, .. }) => AggExpr::Avg(property_ref(var, prop)?),
            ("min", Expr::Property { var, prop, .. }) => AggExpr::Min(property_ref(var, prop)?),
            ("max", Expr::Property { var, prop, .. }) => AggExpr::Max(property_ref(var, prop)?),
            ("collect", Expr::Property { var, prop, .. }) => {
                AggExpr::Collect(property_ref(var, prop)?)
            }
            ("count", other) => {
                return Err(self.validation(
                    other.at(),
                    "count takes a node variable or `variable.property`",
                ));
            }
            (_, other) => {
                return Err(self.validation(
                    other.at(),
                    format!("`{}` takes `variable.property`", name.name),
                ));
            }
        })
    }

    fn date_trunc(
        &self,
        graph: &Graph,
        expr: &Expr<'i>,
        alias: Option<String>,
    ) -> Result<InputGroupByKey> {
        let Expr::Function { args, at, .. } = expr else {
            unreachable!("caller matched a function");
        };
        let [
            Arg::Expr(Expr::Value(unit)),
            Arg::Expr(Expr::Property { var, prop, .. }),
        ] = args.as_slice()
        else {
            return Err(self.validation(
                *at,
                "date_trunc takes a unit string and a property: date_trunc('month', x.created_at)",
            ));
        };
        let Json::String(unit_name) = self.json(unit)? else {
            return Err(self.validation(start(unit), "date_trunc unit must be a string"));
        };
        let truncate: TruncateUnit =
            serde_json::from_value(Json::String(unit_name.to_ascii_lowercase())).map_err(|_| {
                self.validation(
                    start(unit),
                    format!(
                        "unknown truncation unit '{unit_name}' (one of: {})",
                        TruncateUnit::VARIANTS.join(", ")
                    ),
                )
            })?;
        match graph.vars.get(&var.name) {
            Some(VarKind::Node(_)) => {}
            Some(_) => return Err(self.validation(var.at, "date_trunc takes a node property")),
            None => return Err(self.unbound(var)),
        }
        Ok(InputGroupByKey::Property {
            node: var.name.clone(),
            property: prop.name.clone(),
            truncate: Some(truncate),
            alias,
        })
    }

    fn aggregation_sort(
        &self,
        ret: &P<'i>,
        outputs: &[(String, String)],
    ) -> Result<Option<InputAggSort>> {
        let Some((expr, direction, at)) = self.sort_key(ret)? else {
            return Ok(None);
        };
        let rendered = expr.render();
        let column = outputs
            .iter()
            .find(|(item, _)| *item == rendered)
            .map(|(_, output)| output.clone())
            .or_else(|| match &expr {
                Expr::Variable(v) if outputs.iter().any(|(_, o)| *o == v.name) => {
                    Some(v.name.clone())
                }
                _ => None,
            })
            .ok_or_else(|| {
                QueryError::ReferenceError(self.at(
                    at,
                    "ORDER BY in an aggregation must name an alias, an output column, or repeat a \
                     RETURN item verbatim",
                ))
            })?;
        Ok(Some(InputAggSort { column, direction }))
    }

    fn lower_path(
        &self,
        graph: &Graph,
        ret: &P<'i>,
        items: &[(Expr<'i>, Option<Named>)],
    ) -> Result<Input> {
        self.reject_aliases(items)?;
        let edge = &graph.edges[0];
        if let Some(variable) = &edge.var {
            return Err(self.validation(
                variable.at,
                "the path relationship cannot be bound to a variable; the response carries the \
                 path",
            ));
        }
        if !edge.preds.is_empty() {
            return Err(self.validation(
                edge.at,
                "path finding has no relationship filters; only relationship types bound the \
                 search",
            ));
        }
        if edge.types.is_empty() {
            return Err(self.validation(
                edge.at,
                "the path relationship needs at least one type, e.g. -[:CONTAINS*..3]->; the \
                 frontier would otherwise fan out over every edge kind",
            ));
        }
        let (from, to) = match edge.arrow {
            Arrow::Right => (edge.left, edge.right),
            Arrow::Left => (edge.right, edge.left),
            Arrow::Undirected => {
                return Err(self.validation(
                    edge.at,
                    "path search is directed; use -[:TYPE*..3]-> or <-[:TYPE*..3]-",
                ));
            }
        };
        let max_depth = match &edge.range {
            None => DEFAULT_PATH_MAX_DEPTH,
            Some(range) => {
                let hops = self.hops(Some(range))?;
                if hops.min != 1 {
                    return Err(self.validation(
                        range.at,
                        "a path search always starts at depth 1; write `*..n` or `*1..n`",
                    ));
                }
                hops.max
            }
        };
        let columns = self.column_drafts(graph, ret, items, |_| false)?;
        let mut input = self.base_input(QueryType::PathFinding, ret)?;
        input.nodes = self.input_nodes(graph, &columns)?;
        input.path = Some(InputPath::new(
            PathType::Shortest,
            graph.nodes[from].var.name.clone(),
            graph.nodes[to].var.name.clone(),
            max_depth,
            self.rel_types(&edge.types)?,
        ));
        input.order_by = self.order_by(graph, ret)?;
        Ok(input)
    }

    fn lower_neighbors(
        &self,
        graph: &Graph,
        ret: &P<'i>,
        items: &[(Expr<'i>, Option<Named>)],
        far: usize,
    ) -> Result<Input> {
        self.reject_aliases(items)?;
        let center = 1 - far;
        let edge = &graph.edges[0];
        let far_node = &graph.nodes[far];
        if let Some(pred) = far_node.preds.first() {
            return Err(self.validation(
                pred.at,
                format!(
                    "neighbors have no filters: `{}` is the discovered endpoint and its type is \
                     only known at runtime; give it a label to make this a traversal",
                    far_node.var.name
                ),
            ));
        }
        if let Some(pred) = edge.preds.first() {
            return Err(self.validation(
                pred.at,
                "a neighbors relationship has no filters; only its types and direction are \
                 configurable",
            ));
        }
        if let Some(range) = &edge.range {
            return Err(self.validation(
                range.at,
                "a neighbors relationship is always one hop; give the far node a label for a \
                 multi-hop traversal",
            ));
        }
        let direction = match (edge.arrow, edge.left == center) {
            (Arrow::Undirected, _) => Direction::Both,
            (Arrow::Right, true) | (Arrow::Left, false) => Direction::Outgoing,
            (Arrow::Right, false) | (Arrow::Left, true) => Direction::Incoming,
        };
        let center_node = &graph.nodes[center];
        let center_var = center_node.var.name.as_str();
        for (expr, _) in items {
            if let Expr::Property { var, at, .. } = expr
                && var.name != center_var
            {
                return Err(self.validation(
                    *at,
                    format!(
                        "`{}` is discovered at runtime, so its properties cannot be selected here; \
                         column selection for neighbors comes from options.dynamic_columns",
                        var.name
                    ),
                ));
            }
        }
        let columns = self.column_drafts(
            graph,
            ret,
            items,
            |expr| matches!(expr, Expr::Variable(v) if v.name != center_var),
        )?;
        let mut input = self.base_input(QueryType::Neighbors, ret)?;
        input.nodes = vec![self.input_node(center_node, columns.get(&center))?];
        input.neighbors = Some(InputNeighbors {
            direction,
            rel_types: self.rel_types(&edge.types)?,
        });
        input.order_by = self.order_by(graph, ret)?;
        Ok(input)
    }
}

/// Groups predicates by property. Within a property the entries are ordered
/// by operator name, which is the order `serde_json`'s sorted map hands the
/// JSON frontend an operator object in, so both frontends build the same
/// `Vec<InputFilter>`.
fn filter_map(preds: &[&Pred]) -> BTreeMap<String, Vec<InputFilter>> {
    let mut map: BTreeMap<String, Vec<InputFilter>> = BTreeMap::new();
    for pred in preds {
        map.entry(pred.property.name.clone())
            .or_default()
            .push(InputFilter::new(pred.op, pred.value.clone()));
    }
    for filters in map.values_mut() {
        filters.sort_by_cached_key(|f| {
            f.op.map_or_else(|| "eq".to_owned(), |op| op.as_ref().to_owned())
        });
    }
    map
}

fn suggestion<'a>(name: &str, candidates: impl Iterator<Item = &'a str>) -> String {
    candidates
        .filter(|c| c.eq_ignore_ascii_case(name) && *c != name)
        .map(|c| format!("; did you mean `{c}`?"))
        .next()
        .unwrap_or_default()
}

/// Mirrors the DSL `Identifier` schema pattern `^[a-zA-Z_][a-zA-Z0-9_]{0,63}$`.
pub fn is_valid_identifier(name: &str) -> bool {
    let mut chars = name.chars();
    let Some(first) = chars.next() else {
        return false;
    };
    (first.is_ascii_alphabetic() || first == '_')
        && name.len() <= 64
        && chars.all(|c| c.is_ascii_alphanumeric() || c == '_')
}

// ── Syntax errors ───────────────────────────────────────────────────────────

fn render_error(source: &str, err: &pest::error::Error<Rule>) -> String {
    let offset = match err.location {
        pest::error::InputLocation::Pos(p) => p,
        pest::error::InputLocation::Span((s, _)) => s,
    };
    let (line, col) = line_col(source, offset);
    let found = token_at(source, offset);
    let mut msg = if found.is_empty() {
        format!("line {line}, column {col}: unexpected end of statement")
    } else {
        format!("line {line}, column {col}: unexpected `{found}`")
    };
    if let pest::error::ErrorVariant::ParsingError { positives, .. } = &err.variant {
        let mut expected: Vec<&str> = positives.iter().map(|r| describe_rule(*r)).collect();
        expected.sort_unstable();
        expected.dedup();
        if !expected.is_empty() {
            msg.push_str("; expected ");
            msg.push_str(&expected.join(", "));
        }
    }
    if let Some(hint) = hint_for(&found) {
        msg.push_str(". ");
        msg.push_str(hint);
    }
    msg
}

fn token_at(source: &str, offset: usize) -> String {
    let rest = source[offset.min(source.len())..].trim_start();
    let Some(first) = rest.chars().next() else {
        return String::new();
    };
    if first.is_alphanumeric() || first == '_' {
        rest.chars()
            .take_while(|c| c.is_alphanumeric() || *c == '_')
            .collect()
    } else {
        let two: String = rest.chars().take(2).collect();
        match two.as_str() {
            "<>" | "!=" | "=~" | "<-" | "->" | "--" | ".." | "{." => two,
            _ => first.to_string(),
        }
    }
}

fn describe_rule(rule: Rule) -> &'static str {
    match rule {
        Rule::EOI => "end of statement",
        Rule::match_clause | Rule::kw_match => "MATCH",
        Rule::return_clause | Rule::kw_return => "RETURN",
        Rule::where_clause | Rule::kw_where => "WHERE",
        Rule::order_by_clause | Rule::kw_order => "ORDER BY",
        Rule::kw_by => "BY",
        Rule::limit_clause | Rule::kw_limit => "LIMIT",
        Rule::skip_clause | Rule::kw_skip | Rule::offset_kw => "SKIP",
        Rule::kw_as => "AS",
        Rule::kw_distinct => "DISTINCT",
        Rule::kw_optional => "OPTIONAL",
        Rule::sort_direction
        | Rule::kw_asc
        | Rule::kw_ascending
        | Rule::kw_desc
        | Rule::kw_descending => "ASC or DESC",
        Rule::nulls_order | Rule::nulls_kw => "NULLS",
        Rule::first_kw | Rule::last_kw => "FIRST or LAST",
        Rule::pattern | Rule::chain | Rule::node_pattern => "a node pattern `(var:Label)`",
        Rule::path_function | Rule::path_fn_name => "shortestPath(",
        Rule::edge_pattern | Rule::bracket_edge | Rule::arrow_edge => {
            "a relationship pattern `-[:TYPE]->`"
        }
        Rule::edge_left | Rule::arrow_left | Rule::gql_arrow_left => "<-",
        Rule::edge_right | Rule::arrow_right | Rule::gql_arrow_right => "->",
        Rule::edge_undirected | Rule::arrow_undirected | Rule::gql_dash => "-",
        Rule::edge_body => "a relationship body `[var:TYPE*1..3 {prop: value}]`",
        Rule::node_label | Rule::type_spec => ":Label",
        Rule::range_literal => "*min..max",
        Rule::property_map => "{prop: value}",
        Rule::property_kv => "prop: value",
        Rule::gql_quantifier => "{",
        Rule::inline_where => "WHERE",
        Rule::condition | Rule::term | Rule::predicate | Rule::paren_condition => "a predicate",
        Rule::predicate_tail | Rule::comp_op => "a comparison operator",
        Rule::bool_op | Rule::kw_and => "AND",
        Rule::kw_or => "OR",
        Rule::kw_xor => "XOR",
        Rule::kw_not => "NOT",
        Rule::kw_in => "IN",
        Rule::null_test | Rule::kw_is => "IS NULL",
        Rule::kw_null | Rule::null => "NULL",
        Rule::kw_starts => "STARTS WITH",
        Rule::kw_ends => "ENDS WITH",
        Rule::kw_with => "WITH",
        Rule::kw_contains => "CONTAINS",
        Rule::operand | Rule::return_item | Rule::return_body | Rule::sort_item => {
            "an expression (`var`, `var.property`, or a function call)"
        }
        Rule::func_call | Rule::func_arg => "a function call",
        Rule::property_ref => "var.property",
        Rule::star => "*",
        Rule::value | Rule::list => "a value",
        Rule::string | Rule::sq_string | Rule::dq_string | Rule::escape => "a string",
        Rule::integer => "an integer",
        Rule::float | Rule::exponent => "a number",
        Rule::boolean => "TRUE or FALSE",
        Rule::parameter | Rule::parameter_name => "$parameter",
        Rule::ident | Rule::plain_ident | Rule::backtick_ident => "an identifier",
        Rule::gql_temporal | Rule::gql_temporal_kw => "date(",
        Rule::statement
        | Rule::reserved_word
        | Rule::ident_start
        | Rule::ident_char
        | Rule::WHITESPACE
        | Rule::COMMENT => "a statement",
    }
}

/// Hints keyed by the token the parser choked on. Each names the DSL
/// limitation or the openCypher spelling, never a parser state.
fn hint_for(token: &str) -> Option<&'static str> {
    let upper = token.to_ascii_uppercase();
    Some(match upper.as_str() {
        "CREATE" | "MERGE" | "SET" | "REMOVE" | "DELETE" | "DETACH" | "INSERT" | "DROP"
        | "ALTER" => "The openCypher frontend is read-only: only MATCH ... RETURN is accepted.",
        "CALL" | "YIELD" | "USE" | "SESSION" | "COMMIT" | "ROLLBACK" => {
            "Procedures, graph selection, and transactions are not supported."
        }
        "START" => "START is not supported; a query is a single MATCH ... RETURN statement.",
        "WITH" | "UNWIND" | "NEXT" | "LET" | "FILTER" => {
            "One MATCH ... RETURN statement per query; WITH/UNWIND chaining is not supported."
        }
        "FOR" => "FOR is not supported; one MATCH ... RETURN statement per query.",
        "UNION" | "EXCEPT" | "INTERSECT" | "OTHERWISE" => {
            "Composite queries (UNION, EXCEPT, INTERSECT) are not supported."
        }
        "OPTIONAL" => "OPTIONAL MATCH is not supported; every pattern is required.",
        "EXISTS" | "CASE" | "WHEN" | "THEN" | "ELSE" | "COALESCE" => {
            "Expressions are limited to `var.property <op> value` predicates; EXISTS, CASE, and \
             COALESCE are not supported."
        }
        "END" => {
            "`end` is a reserved word in openCypher (CASE ... END); write `end` in backticks to \
             use it as a variable."
        }
        "ALL" | "ANY" | "SHORTEST" | "WALK" | "TRAIL" | "SIMPLE" | "ACYCLIC" => {
            "GQL path search prefixes and path modes are not supported; use \
             shortestPath((a:Label {id: 1})-[:TYPE*..3]->(b:Label {id: 2}))."
        }
        "GROUP" => {
            "GROUP BY is not openCypher; grouping is implicit: every non-aggregate RETURN item is \
             a group key."
        }
        "OF" | "ON" | "DO" | "ADD" | "UNIQUE" | "REQUIRE" | "CONSTRAINT" | "MANDATORY"
        | "SCALAR" => {
            "This word is reserved in openCypher; write it in backticks to use it as a variable."
        }
        "~" => {
            "`~` is not supported: use -[:TYPE]- for an undirected edge; regex matching (=~) has \
             no DSL equivalent."
        }
        "+" | "/" | "%" | "^" => {
            "Arithmetic is not supported; compare a property with a literal or parameter."
        }
        "*" => {
            "Arithmetic is not supported; `*` is only valid as RETURN *, count(*) (rejected), or \
             a hop range `*1..3`."
        }
        "&" | "!" => {
            "Label expressions (&, !, %) are not supported; a node has exactly one label and \
             relationship types combine with `|`."
        }
        "|" => {
            "Label disjunction is not supported on nodes (one entity per node); relationship \
             types may be combined as :A|B."
        }
        "$" => {
            "A parameter may stand in for a value, an IN list, or LIMIT; parameter maps \
             `(n $props)` and parameterized labels are not supported."
        }
        "[" => {
            "List comprehensions and list indexing are not supported; `[...]` is only valid as \
             the right side of IN."
        }
        "{" | "{." => {
            "Map projection `u {.a, .b}` is not openCypher 9; write `u, u.a, u.b`. A property map \
             `{prop: value}` belongs inside a node or relationship pattern."
        }
        "." => "Nested property access is not supported; use `var.property`.",
        ">" => "A relationship has one direction: use -[]->, <-[]-, or -[]-.",
        ":" => {
            "Relationship types combine with `|` (`:A|B`), not `:`; a node has exactly one label."
        }
        _ => return None,
    })
}
