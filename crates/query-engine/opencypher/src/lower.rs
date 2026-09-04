//! Classification and query-type lowering to [`compiler::Input`].
//!
//! There is no intermediate AST: the pest pair tree already carries rule,
//! text, span, and children. Classification needs the whole pattern and
//! RETURN list, so the pattern is collected into [`Graph`] drafts first and
//! RETURN items are viewed once as [`Item`]s. Everything lands in the `Input`
//! through constructors and JSON-visible fields only, so the result is the
//! structure the JSON frontend deserializes from the equivalent DSL document.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::hash::{Hash, Hasher};

use compiler::input::{
    AggExpr, Direction, HopRange, InputAggSort, InputAggregationMetric, InputGroupByKey,
    InputIdRange, InputNeighbors, InputOrderBy, InputPath, InputRelationship, OrderDirection,
    PathType, TruncateUnit,
};
use compiler::{ColumnSelection, FilterOp, Input, InputFilter, InputNode, QueryType};
use ontology::{Ontology, TRAVERSAL_PATH_COLUMN};
use serde_json::Value as Json;
use strum::VariantNames;

use crate::pattern::{Arrow, Graph, Node, Range};
use crate::syntax::{P, Rule};
use crate::tree::{Ctx, Named, Result, child, children, first, ident_name, start, unescape};
use crate::{MAX_LIMIT, Parameters};

const ID_COLUMN: &str = "id";
const WILDCARD_TYPE: &str = "*";
const DEFAULT_PATH_MAX_DEPTH: u32 = 3;
const MAX_ID_STRING_CHARS: usize = 20;
const DATE_TRUNC: &str = "date_trunc";

/// Parse, substitute parameters, and lower. Returns the `Input` and the
/// cursor-binding hash of the substituted token stream.
pub fn lower(source: &str, params: &Parameters, ontology: &Ontology) -> Result<(Input, u64)> {
    let statement = crate::syntax::parse(source)?;
    crate::syntax::reject_unsupported(source, &statement)?;
    let ctx = Ctx {
        source,
        params,
        ontology,
    };
    let hash = token_hash(&statement, params);
    let clause = child(&statement, Rule::match_clause).expect("grammar requires MATCH");
    let mut graph = Graph::collect(&ctx, &clause)?;
    if let Some(where_clause) = child(&clause, Rule::where_clause) {
        graph.apply_where(&ctx, &where_clause)?;
    }
    let ret = child(&statement, Rule::return_clause).expect("grammar requires RETURN");
    let items = children(
        &child(&ret, Rule::return_body).expect("RETURN body"),
        Rule::return_item,
    )
    .map(|item| Item::new(&ctx, &item))
    .collect::<Result<Vec<_>>>()?;
    let lowerer = Lowerer {
        ctx,
        graph,
        ret,
        items,
    };
    Ok((lowerer.lower()?, hash))
}

/// FNV-1a over the token stream, mirroring
/// `compiler::passes::cursor::canonical_hash`. Leaves hash their text
/// (keywords case-folded, identifiers unbackticked, strings unescaped), so
/// whitespace and comments do not move the hash. A bound `$parameter` hashes
/// as the literal it stands for, so substitution happens before hashing.
fn token_hash(statement: &P<'_>, params: &Parameters) -> u64 {
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
        let mut inner = pair.clone().into_inner().peekable();
        match rule {
            Rule::parameter => match params.get(&pair.as_str()[1..]) {
                Some(bound) => literal(bound, h),
                None => {
                    rule.hash(h);
                    pair.as_str().hash(h);
                }
            },
            Rule::ident => {
                rule.hash(h);
                ident_name(pair).hash(h);
            }
            Rule::string => {
                rule.hash(h);
                unescape(pair.as_str()).hash(h);
            }
            _ if inner.peek().is_none() => {
                rule.hash(h);
                pair.as_str().to_ascii_uppercase().hash(h);
            }
            _ => {
                rule.hash(h);
                for child in inner {
                    walk(&child, params, h);
                }
            }
        }
    }
    /// The token path a literal spelling of `value` would have produced.
    fn literal(value: &Json, h: &mut Fnv1a) {
        match value {
            Json::Null => {
                Rule::null.hash(h);
                "NULL".hash(h);
            }
            Json::Bool(b) => {
                Rule::boolean.hash(h);
                if *b { "TRUE" } else { "FALSE" }.hash(h);
            }
            Json::Number(n) if n.is_i64() || n.is_u64() => {
                Rule::integer.hash(h);
                n.to_string().hash(h);
            }
            Json::Number(n) => {
                Rule::float.hash(h);
                n.to_string().to_ascii_uppercase().hash(h);
            }
            Json::String(s) => {
                Rule::string.hash(h);
                s.hash(h);
            }
            Json::Array(items) => {
                Rule::list.hash(h);
                for item in items {
                    Rule::value.hash(h);
                    literal(item, h);
                }
            }
            Json::Object(_) => {
                Rule::parameter.hash(h);
                value.to_string().hash(h);
            }
        }
    }
    let mut hasher = Fnv1a(0xcbf29ce484222325);
    walk(statement, params, &mut hasher);
    hasher.finish()
}

/// One RETURN item, classified by its outermost form.
struct Item<'i> {
    kind: Kind<'i>,
    alias: Option<Named>,
    /// Whitespace-free source text, for matching ORDER BY keys to items.
    text: String,
    at: usize,
}

enum Kind<'i> {
    Var(Named),
    Prop(Named, Named),
    Func { name: Named, args: Vec<P<'i>> },
    Other,
}

impl<'i> Item<'i> {
    fn new(ctx: &Ctx<'_>, item: &P<'i>) -> Result<Self> {
        let operand = child(item, Rule::operand).expect("return operand");
        let alias = match child(item, Rule::ident) {
            Some(alias) => Some(ctx.named(&alias)?),
            None => None,
        };
        Ok(Self {
            kind: kind(ctx, &operand)?,
            alias,
            text: operand.as_str().split_whitespace().collect(),
            at: start(&operand),
        })
    }
}

fn kind<'i>(ctx: &Ctx<'_>, operand: &P<'i>) -> Result<Kind<'i>> {
    let inner = first(operand);
    Ok(match inner.as_rule() {
        Rule::ident => Kind::Var(ctx.named(&inner)?),
        Rule::property_ref => {
            let (variable, property) = crate::pattern::property_ref(ctx, &inner)?;
            Kind::Prop(variable, property)
        }
        Rule::func_call => Kind::Func {
            name: ctx.named(&child(&inner, Rule::ident).expect("function name"))?,
            args: children(&inner, Rule::func_arg)
                .map(|a| first(&a))
                .collect(),
        },
        _ => Kind::Other,
    })
}

fn is_aggregate(name: &str) -> bool {
    AggExpr::try_new(&name.to_ascii_lowercase(), "n", Some("p")).is_some()
}

/// `(filters, node_ids, id_range)` for one node.
type NodeFilters = (
    BTreeMap<String, Vec<InputFilter>>,
    Vec<i64>,
    Option<InputIdRange>,
);

/// What RETURN says about one node. A bare variable alone leaves `columns`
/// unset, which is the DSL default (`id` only, or the entity's default
/// columns for a grouped node); `RETURN *` selects every column.
#[derive(Default)]
struct Columns {
    all: bool,
    bare: bool,
    properties: Vec<String>,
}

impl Columns {
    fn selection(&self) -> Option<ColumnSelection> {
        match (self.properties.is_empty(), self.all) {
            (false, _) => Some(ColumnSelection::List(self.properties.clone())),
            (true, true) => Some(ColumnSelection::All),
            (true, false) => None,
        }
    }
}

struct Lowerer<'i> {
    ctx: Ctx<'i>,
    graph: Graph,
    ret: P<'i>,
    items: Vec<Item<'i>>,
}

impl<'i> Lowerer<'i> {
    fn lower(&self) -> Result<Input> {
        let has_aggregate = self
            .items
            .iter()
            .any(|i| matches!(&i.kind, Kind::Func { name, .. } if is_aggregate(&name.name)));
        let unlabeled: Vec<usize> = (0..self.graph.nodes.len())
            .filter(|i| self.graph.nodes[*i].label.is_none())
            .collect();
        let is_neighbors =
            self.graph.edges.len() == 1 && self.graph.nodes.len() == 2 && unlabeled.len() == 1;
        if has_aggregate && (self.graph.is_path || is_neighbors) {
            return self.ctx.fail(
                start(&self.ret),
                "aggregates cannot be combined with shortestPath() or an unlabeled endpoint",
            );
        }
        if is_neighbors {
            return self.neighbors(unlabeled[0]);
        }
        if let Some(&i) = unlabeled.first() {
            let node = &self.graph.nodes[i];
            return self.ctx.fail(node.var.at, format!("node `{}` needs a label; only the far endpoint of a single-relationship neighbors pattern may omit it", node.var.name));
        }
        if self.graph.is_path {
            self.path()
        } else if has_aggregate {
            self.aggregation()
        } else {
            self.traversal()
        }
    }

    // ── Ontology checks ──────────────────────────────────────────────────

    fn check_label(&self, label: &Named) -> Result<()> {
        if self.ctx.ontology.has_node(&label.name) {
            return Ok(());
        }
        let hint = suggestion(&label.name, self.ctx.ontology.node_names());
        self.ctx.allowlist(
            label.at,
            format!("unknown node label `{}`{hint}", label.name),
        )
    }

    fn check_property(&self, entity: &str, property: &Named) -> Result<()> {
        let name = property.name.as_str();
        let node = self.ctx.ontology.get_node(entity);
        if name == ID_COLUMN
            || self.ctx.ontology.has_field(entity, name)
            || (name == TRAVERSAL_PATH_COLUMN && node.is_some_and(|n| n.has_traversal_path))
        {
            return Ok(());
        }
        let hint = suggestion(
            name,
            std::iter::once(ID_COLUMN).chain(
                node.into_iter()
                    .flat_map(|n| n.fields.iter().map(|f| f.name.as_str())),
            ),
        );
        self.ctx.allowlist(
            property.at,
            format!("unknown property `{name}` on {entity}{hint}"),
        )
    }

    fn rel_types(&self, types: &[Named]) -> Result<Vec<String>> {
        types
            .iter()
            .map(|t| {
                if self.ctx.ontology.has_edge(&t.name) {
                    return Ok(t.name.clone());
                }
                let hint = suggestion(&t.name, self.ctx.ontology.edge_names());
                self.ctx.allowlist(
                    t.at,
                    format!("unknown relationship type `{}`{hint}", t.name),
                )
            })
            .collect()
    }

    // ── Nodes ────────────────────────────────────────────────────────────

    fn input_node(&self, node: &Node, columns: Option<&Columns>) -> Result<InputNode> {
        let label = node.label.as_ref().expect("classification requires labels");
        self.check_label(label)?;
        let (filters, node_ids, id_range) = self.node_filters(node, &label.name)?;
        let columns = columns.and_then(Columns::selection);
        if let Some(ColumnSelection::List(cols)) = &columns {
            for col in cols {
                if let Err(e) = self.ctx.ontology.validate_field(&label.name, col) {
                    return self.ctx.allowlist(
                        node.var.at,
                        format!("invalid column for `{}`: {e}", node.var.name),
                    );
                }
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

    fn input_nodes(&self, columns: &HashMap<usize, Columns>) -> Result<Vec<InputNode>> {
        self.graph
            .nodes
            .iter()
            .enumerate()
            .map(|(i, n)| self.input_node(n, columns.get(&i)))
            .collect()
    }

    /// `id = v` and `id IN [...]` become `node_ids`; `id >= a AND id <= b`
    /// becomes `id_range`; everything else stays a filter. Plain filters would
    /// be result-equivalent but lose the planner's narrowing paths.
    fn node_filters(&self, node: &Node, entity: &str) -> Result<NodeFilters> {
        let mut node_ids = Vec::new();
        let (mut lo, mut hi) = (None, None);
        let mut rest = Vec::new();
        for pred in &node.preds {
            self.check_property(entity, &pred.property)?;
            if pred.property.name != ID_COLUMN {
                rest.push(pred);
                continue;
            }
            match (pred.op, &pred.value) {
                (FilterOp::Eq, Some(v)) => node_ids.push(self.node_id(v, pred.at)?),
                (FilterOp::In, Some(Json::Array(items))) => {
                    for item in items {
                        node_ids.push(self.node_id(item, pred.at)?);
                    }
                }
                (FilterOp::Gte, Some(v)) if lo.is_none() => {
                    lo = Some((self.node_id(v, pred.at)?, pred))
                }
                (FilterOp::Lte, Some(v)) if hi.is_none() => {
                    hi = Some((self.node_id(v, pred.at)?, pred))
                }
                _ => rest.push(pred),
            }
        }
        let id_range = match (lo, hi) {
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
        let digits = value.as_str().filter(|s| {
            !s.is_empty() && s.len() <= MAX_ID_STRING_CHARS && s.bytes().all(|b| b.is_ascii_digit())
        });
        match (value.as_i64(), digits.and_then(|s| s.parse::<i64>().ok())) {
            (Some(i), _) | (None, Some(i)) => Ok(i),
            _ => self.ctx.fail(
                at,
                format!("`id` takes an integer or a digit string, got {value}"),
            ),
        }
    }

    // ── Relationships ────────────────────────────────────────────────────

    fn hops(&self, range: Option<&Range>) -> Result<HopRange> {
        let Some(range) = range else {
            return Ok(HopRange::default());
        };
        let bound = |b: Option<i64>| match b {
            Some(n) if n < 1 => self.ctx.fail(
                range.at,
                "hop bounds start at 1; the DSL has no zero-length hops",
            ),
            Some(n) => Ok(Some(u32::try_from(n).unwrap_or(u32::MAX))),
            None => Ok(None),
        };
        let Some(max) = bound(range.max)? else {
            return self.ctx.fail(range.at, "unbounded hop ranges (`*`, `*1..`) are not supported; give an upper bound of at most 3, e.g. `*1..3`");
        };
        let min = bound(range.min)?.unwrap_or(1);
        if min > max {
            return self
                .ctx
                .fail(range.at, format!("hop range *{min}..{max} is inverted"));
        }
        Ok(HopRange { min, max })
    }

    fn relationships(&self) -> Result<Vec<InputRelationship>> {
        self.graph
            .edges
            .iter()
            .map(|edge| {
                let mut types = self.rel_types(&edge.types)?;
                if types.is_empty() {
                    types.push(WILDCARD_TYPE.to_string());
                }
                let left = self.graph.nodes[edge.left].var.name.clone();
                let right = self.graph.nodes[edge.right].var.name.clone();
                // A left-pointing edge is its mirror image, so `(a)<-[:T]-(b)`
                // builds `{from: b, to: a}` exactly as a JSON author writes it.
                let (from, to, direction) = match edge.arrow {
                    Arrow::Right => (left, right, Direction::Outgoing),
                    Arrow::Left => (right, left, Direction::Outgoing),
                    Arrow::Undirected => (left, right, Direction::Both),
                };
                let filters = filter_map(&edge.preds.iter().collect::<Vec<_>>());
                Ok(InputRelationship::new(
                    types,
                    from,
                    to,
                    self.hops(edge.range.as_ref())?,
                    direction,
                    filters,
                ))
            })
            .collect()
    }

    // ── RETURN, ORDER BY, LIMIT ──────────────────────────────────────────

    fn returns_star(&self) -> bool {
        child(&self.ret, Rule::return_body).is_some_and(|b| child(&b, Rule::star).is_some())
    }

    /// `variable.property` items become the node's `columns`; `RETURN *`
    /// selects every column of every node. `skip` excludes items that mean
    /// something else in the current query type.
    fn columns(&self, skip: impl Fn(&Item<'i>) -> bool) -> Result<HashMap<usize, Columns>> {
        let mut drafts: HashMap<usize, Columns> = HashMap::new();
        if self.returns_star() {
            for i in 0..self.graph.nodes.len() {
                drafts.entry(i).or_default().all = true;
            }
        }
        for item in self.items.iter().filter(|i| !skip(i)) {
            match &item.kind {
                Kind::Var(v) if self.graph.vars.get(&v.name) == Some(&crate::pattern::VarKind::Path) => {}
                Kind::Var(v) => drafts.entry(self.graph.node_index(&self.ctx, v)?).or_default().bare = true,
                Kind::Prop(v, p) => drafts.entry(self.graph.node_index(&self.ctx, v)?).or_default().properties.push(p.name.clone()),
                Kind::Func { name, .. } => return self.ctx.fail(name.at, format!("function `{}` is not supported here; return `variable` or `variable.property`", name.name)),
                Kind::Other => return self.ctx.fail(item.at, "only variables and `variable.property` can be returned"),
            }
        }
        Ok(drafts)
    }

    fn reject_aliases(&self) -> Result<()> {
        match self.items.iter().find_map(|i| i.alias.as_ref()) {
            Some(alias) => self.ctx.fail(
                alias.at,
                "AS is only supported in aggregations; traversal columns keep their property names",
            ),
            None => Ok(()),
        }
    }

    fn base_input(&self, query_type: QueryType) -> Result<Input> {
        let mut input = Input {
            query_type,
            ..Input::default()
        };
        if let Some(limit) = child(&self.ret, Rule::limit_clause) {
            let value = child(&limit, Rule::value).expect("LIMIT value");
            input.limit = match self.ctx.json(&value)?.as_i64() {
                Some(n) if (1..=i64::from(MAX_LIMIT)).contains(&n) => {
                    u32::try_from(n).expect("bounded")
                }
                Some(n) => {
                    return self.ctx.fail(
                        start(&value),
                        format!("LIMIT {n} is out of range 1..={MAX_LIMIT}"),
                    );
                }
                None => return self.ctx.fail(start(&value), "LIMIT takes an integer"),
            };
        }
        Ok(input)
    }

    /// The single ORDER BY key as (operand, direction).
    fn sort_key(&self) -> Option<(P<'i>, OrderDirection)> {
        let key = child(&child(&self.ret, Rule::order_by_clause)?, Rule::sort_item)?;
        let descending = child(&key, Rule::sort_direction)
            .is_some_and(|d| matches!(first(&d).as_rule(), Rule::kw_desc | Rule::kw_descending));
        let direction = if descending {
            OrderDirection::Desc
        } else {
            OrderDirection::Asc
        };
        Some((child(&key, Rule::operand)?, direction))
    }

    fn order_by(&self) -> Result<Option<InputOrderBy>> {
        let Some((operand, direction)) = self.sort_key() else {
            return Ok(None);
        };
        let Kind::Prop(variable, property) = kind(&self.ctx, &operand)? else {
            return self
                .ctx
                .fail(start(&operand), "ORDER BY takes `variable.property`");
        };
        self.graph.node_index(&self.ctx, &variable)?;
        Ok(Some(InputOrderBy {
            node: variable.name,
            property: property.name,
            direction,
        }))
    }

    // ── Query types ──────────────────────────────────────────────────────

    fn traversal(&self) -> Result<Input> {
        self.reject_aliases()?;
        let mut input = self.base_input(QueryType::Traversal)?;
        input.nodes = self.input_nodes(&self.columns(|_| false)?)?;
        input.relationships = self.relationships()?;
        input.order_by = self.order_by()?;
        Ok(input)
    }

    fn aggregation(&self) -> Result<Input> {
        if self.returns_star() {
            return self.ctx.fail(
                start(&self.ret),
                "RETURN * cannot be combined with aggregates; name the group keys",
            );
        }
        if let Some(edge) = self
            .graph
            .edges
            .iter()
            .find(|e| e.arrow == Arrow::Undirected)
        {
            return self.ctx.fail(edge.at, "an undirected relationship is not supported in an aggregation (the OR join defeats index use)");
        }
        let bare: HashSet<&str> = self
            .items
            .iter()
            .filter_map(|i| match &i.kind {
                Kind::Var(v) => Some(v.name.as_str()),
                _ => None,
            })
            .collect();
        // Beside a bare `u`, `u.prop` selects a column of the grouped node
        // (result-equivalent, since the node determines its properties);
        // alone it is a group key.
        let is_column = |item: &Item<'_>| matches!(&item.kind, Kind::Prop(v, _) if bare.contains(v.name.as_str()));

        let mut input = self.base_input(QueryType::Aggregation)?;
        let mut outputs: Vec<(&str, String)> = Vec::new();
        for item in &self.items {
            let alias = item.alias.as_ref().map(|a| a.name.clone());
            let output = match &item.kind {
                Kind::Func { name, args } if is_aggregate(&name.name) => {
                    let metric = InputAggregationMetric { expr: self.aggregate(name, args)?, alias };
                    let output = metric.output_name();
                    input.aggregation.metrics.push(metric);
                    output
                }
                Kind::Func { name, args } if name.name.eq_ignore_ascii_case(DATE_TRUNC) => {
                    let key = self.date_trunc(name, args, alias)?;
                    let output = key.output_name();
                    input.aggregation.group_by.push(key);
                    output
                }
                Kind::Func { name, .. } => return self.ctx.fail(name.at, format!("function `{}` is not supported; aggregates are count, sum, avg, min, max and date_trunc('unit', x.prop) buckets a key", name.name)),
                Kind::Var(v) => {
                    self.graph.node_index(&self.ctx, v)?;
                    let key = InputGroupByKey::Node { node: v.name.clone(), alias };
                    let output = key.output_name();
                    input.aggregation.group_by.push(key);
                    output
                }
                Kind::Prop(..) if is_column(item) => {
                    if let Some(alias) = &item.alias {
                        return self.ctx.fail(alias.at, "a column of a grouped node cannot be aliased");
                    }
                    continue;
                }
                Kind::Prop(v, p) => {
                    self.graph.node_index(&self.ctx, v)?;
                    let key = InputGroupByKey::Property { node: v.name.clone(), property: p.name.clone(), truncate: None, alias };
                    let output = key.output_name();
                    input.aggregation.group_by.push(key);
                    output
                }
                Kind::Other => return self.ctx.fail(item.at, "only variables, properties, and aggregates can be returned"),
            };
            outputs.push((&item.text, output));
        }
        input.aggregation.sort = self.aggregation_sort(&outputs)?;
        let columns =
            self.columns(|item| !matches!(item.kind, Kind::Var(_)) && !is_column(item))?;
        input.nodes = self.input_nodes(&columns)?;
        input.relationships = self.relationships()?;
        Ok(input)
    }

    fn aggregate(&self, name: &Named, args: &[P<'i>]) -> Result<AggExpr> {
        let [arg] = args else {
            return self.ctx.fail(
                name.at,
                format!("`{}` takes exactly one argument", name.name),
            );
        };
        let (node, property) = match kind(&self.ctx, arg)? {
            Kind::Var(v) => (v, None),
            Kind::Prop(v, p) => (v, Some(p)),
            _ => {
                return self.ctx.fail(
                    start(arg),
                    format!(
                        "`{}` takes a node variable or `variable.property`",
                        name.name
                    ),
                );
            }
        };
        self.graph.node_index(&self.ctx, &node)?;
        AggExpr::try_new(
            &name.name.to_ascii_lowercase(),
            &node.name,
            property.as_ref().map(|p| p.name.as_str()),
        )
        .ok_or_else(|| {
            self.ctx
                .fail::<()>(
                    name.at,
                    format!("`{}` takes `variable.property`", name.name),
                )
                .unwrap_err()
        })
    }

    fn date_trunc(
        &self,
        name: &Named,
        args: &[P<'i>],
        alias: Option<String>,
    ) -> Result<InputGroupByKey> {
        let (unit, prop) = match args {
            [unit, prop] if first(unit).as_rule() == Rule::value => {
                (self.ctx.json(&first(unit))?, kind(&self.ctx, prop)?)
            }
            _ => return self.ctx.fail(
                name.at,
                "date_trunc takes a unit string and a property: date_trunc('month', x.created_at)",
            ),
        };
        let Kind::Prop(variable, property) = prop else {
            return self
                .ctx
                .fail(start(&args[1]), "date_trunc takes `variable.property`");
        };
        self.graph.node_index(&self.ctx, &variable)?;
        let unit_name = unit.as_str().unwrap_or_default().to_ascii_lowercase();
        let Ok(truncate) = serde_json::from_value::<TruncateUnit>(Json::String(unit_name.clone()))
        else {
            return self.ctx.fail(
                start(&args[0]),
                format!(
                    "unknown truncation unit '{unit_name}' (one of: {})",
                    TruncateUnit::VARIANTS.join(", ")
                ),
            );
        };
        Ok(InputGroupByKey::Property {
            node: variable.name,
            property: property.name,
            truncate: Some(truncate),
            alias,
        })
    }

    /// Resolves the ORDER BY key to an output column: an alias or output name,
    /// or a RETURN item repeated verbatim.
    fn aggregation_sort(&self, outputs: &[(&str, String)]) -> Result<Option<InputAggSort>> {
        let Some((operand, direction)) = self.sort_key() else {
            return Ok(None);
        };
        let text: String = operand.as_str().split_whitespace().collect();
        let column = outputs
            .iter()
            .find(|(item, output)| *item == text || *output == text)
            .map(|(_, output)| output.clone());
        match column {
            Some(column) => Ok(Some(InputAggSort { column, direction })),
            None => Err(compiler::QueryError::ReferenceError(
                crate::syntax::located(
                    self.ctx.source,
                    start(&operand),
                    "ORDER BY in an aggregation must name an alias, an output column, or repeat a RETURN item verbatim",
                ),
            )),
        }
    }

    fn path(&self) -> Result<Input> {
        self.reject_aliases()?;
        let edge = &self.graph.edges[0];
        if let Some(variable) = &edge.var {
            return self.ctx.fail(variable.at, "the path relationship cannot be bound to a variable; the response carries the path");
        }
        if !edge.preds.is_empty() {
            return self.ctx.fail(edge.at, "path finding has no relationship filters; only relationship types bound the search");
        }
        if edge.types.is_empty() {
            return self.ctx.fail(edge.at, "the path relationship needs a type, e.g. -[:CONTAINS*..3]->, or the frontier fans out over every edge kind");
        }
        let (from, to) = match edge.arrow {
            Arrow::Right => (edge.left, edge.right),
            Arrow::Left => (edge.right, edge.left),
            Arrow::Undirected => {
                return self.ctx.fail(
                    edge.at,
                    "path search is directed; use -[:TYPE*..3]-> or <-[:TYPE*..3]-",
                );
            }
        };
        let max_depth = match &edge.range {
            None => DEFAULT_PATH_MAX_DEPTH,
            Some(range) => match self.hops(Some(range))? {
                HopRange { min: 1, max } => max,
                _ => {
                    return self.ctx.fail(
                        range.at,
                        "a path search always starts at depth 1; write `*..n` or `*1..n`",
                    );
                }
            },
        };
        let mut input = self.base_input(QueryType::PathFinding)?;
        input.nodes = self.input_nodes(&self.columns(|_| false)?)?;
        input.path = Some(InputPath::new(
            PathType::Shortest,
            self.graph.nodes[from].var.name.clone(),
            self.graph.nodes[to].var.name.clone(),
            max_depth,
            self.rel_types(&edge.types)?,
        ));
        input.order_by = self.order_by()?;
        Ok(input)
    }

    fn neighbors(&self, far: usize) -> Result<Input> {
        self.reject_aliases()?;
        let center = 1 - far;
        let edge = &self.graph.edges[0];
        let far_node = &self.graph.nodes[far];
        if let Some(pred) = far_node.preds.first() {
            return self.ctx.fail(pred.at, format!("`{}` is the discovered endpoint and has no filters; give it a label to make this a traversal", far_node.var.name));
        }
        if let Some(pred) = edge.preds.first() {
            return self.ctx.fail(pred.at, "a neighbors relationship has no filters; only its types and direction are configurable");
        }
        if let Some(range) = &edge.range {
            return self.ctx.fail(range.at, "a neighbors relationship is always one hop; give the far node a label for a multi-hop traversal");
        }
        let direction = match (edge.arrow, edge.left == center) {
            (Arrow::Undirected, _) => Direction::Both,
            (Arrow::Right, true) | (Arrow::Left, false) => Direction::Outgoing,
            _ => Direction::Incoming,
        };
        let center_var = self.graph.nodes[center].var.name.as_str();
        if let Some(item) = self
            .items
            .iter()
            .find(|i| matches!(&i.kind, Kind::Prop(v, _) if v.name != center_var))
        {
            return self.ctx.fail(item.at, "neighbor and relationship properties are only known at runtime; select them with options.dynamic_columns");
        }
        let columns = self.columns(|i| matches!(&i.kind, Kind::Var(v) if v.name != center_var))?;
        let mut input = self.base_input(QueryType::Neighbors)?;
        input.nodes = vec![self.input_node(&self.graph.nodes[center], columns.get(&center))?];
        input.neighbors = Some(InputNeighbors {
            direction,
            rel_types: self.rel_types(&edge.types)?,
        });
        input.order_by = self.order_by()?;
        Ok(input)
    }
}

/// Groups predicates by property. Within a property the entries are ordered
/// by operator name, the order `serde_json`'s sorted map hands the JSON
/// frontend an operator object in, so both frontends build the same vector.
fn filter_map(preds: &[&crate::pattern::Pred]) -> BTreeMap<String, Vec<InputFilter>> {
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

/// A case-insensitive near miss plus the full allow-list, the same
/// information the JSON frontend's schema error carries.
fn suggestion<'a>(name: &str, candidates: impl Iterator<Item = &'a str>) -> String {
    let mut valid: Vec<&str> = candidates.collect();
    valid.sort_unstable();
    let near = valid
        .iter()
        .find(|c| c.eq_ignore_ascii_case(name) && **c != name)
        .map(|c| format!("; did you mean `{c}`?"))
        .unwrap_or_default();
    format!("{near}. Valid values: {}", valid.join(", "))
}
