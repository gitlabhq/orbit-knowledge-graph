use compiler::input::{
    AggExpr, AggFunction, Direction, FilterOp, HopRange, InputFilter, InputGroupByKey, InputNode,
    InputRelationship, OrderDirection, PropertyRef, TargetRef, TruncateUnit,
};
use serde_json::Value;

use crate::Params;

use crate::assemble::{OrderTarget, Pattern, Pred, RelPart, ReturnItem, assemble};

const MAX_IDENTIFIER_LEN: usize = 64;

const RESERVED: &[&str] = &[
    "MATCH", "WHERE", "FILTER", "RETURN", "GROUP", "BY", "ORDER", "LIMIT", "AND", "OR", "NOT",
    "IN", "IS", "NULL", "TRUE", "FALSE", "STARTS", "ENDS", "WITH", "CONTAINS", "AS", "ASC", "DESC",
    "ANY", "ALL", "SHORTEST", "NEXT", "OPTIONAL", "LET", "YIELD", "UNION", "SKIP", "OFFSET",
];

fn eq_filter(value: Value) -> InputFilter {
    InputFilter {
        op: None,
        value: Some(value),
        ..Default::default()
    }
}

fn op_filter(op: FilterOp, value: Option<Value>) -> InputFilter {
    InputFilter {
        op: Some(op),
        value,
        ..Default::default()
    }
}

type RelBody = (
    Option<String>,
    Vec<String>,
    Option<HopRange>,
    Vec<(String, InputFilter)>,
);

fn relationship(
    types: Vec<String>,
    hops: Option<HopRange>,
    direction: Direction,
) -> InputRelationship {
    InputRelationship {
        types,
        from: String::new(),
        to: String::new(),
        hops: hops.unwrap_or_default(),
        direction,
        filters: Default::default(),
        fk_column: None,
        scope_prefix: None,
        scope_preserving: false,
    }
}

peg::parser! {
    pub(crate) grammar gql(params: &Params) for str {
        rule _() = quiet!{ [' ' | '\t' | '\n' | '\r']* }
        rule __() = quiet!{ [' ' | '\t' | '\n' | '\r']+ }

        rule kw(k: &'static str)
            = w:$(['a'..='z' | 'A'..='Z' | '_']+) {? if w.eq_ignore_ascii_case(k) { Ok(()) } else { Err(k) } }

        rule ident() -> String
            = quiet!{ w:name()
                {? if RESERVED.iter().any(|r| r.eq_ignore_ascii_case(&w)) { Err("identifier") } else { Ok(w) } } }
            / expected!("identifier")

        rule name() -> String
            = quiet!{ w:$(['a'..='z' | 'A'..='Z' | '_'] ['a'..='z' | 'A'..='Z' | '0'..='9' | '_']*)
                {? if w.len() > MAX_IDENTIFIER_LEN { Err("name of at most 64 characters") } else { Ok(w.to_string()) } } }
            / expected!("name")

        rule uint() -> u32
            = n:$(['0'..='9']+) {? n.parse().or(Err("integer")) }

        rule int() -> i64
            = n:$("-"? ['0'..='9']+) {? n.parse().or(Err("integer")) }

        rule number() -> Value
            = n:$("-"? ['0'..='9']+ "." ['0'..='9']+) {? n.parse::<f64>().ok().and_then(serde_json::Number::from_f64).map(Value::Number).ok_or("number") }
            / n:int() { Value::from(n) }

        rule string() -> Value
            = "'" s:$(("\\'" / [^ '\''])*) "'" { Value::String(s.replace("\\'", "'")) }
            / "\"" s:$(("\\\"" / [^ '"'])*) "\"" { Value::String(s.replace("\\\"", "\"")) }

        rule value() -> Value
            = string()
            / number()
            / kw("TRUE") { Value::Bool(true) }
            / kw("FALSE") { Value::Bool(false) }
            / kw("NULL") { Value::Null }
            / "[" _ items:(value() ** (_ "," _)) _ "]" { Value::Array(items) }
            / "$" name:ident() {? params.get(&name).cloned().ok_or("bound parameter") }

        rule prop_map() -> Vec<(String, InputFilter)>
            = "{" _ entries:((k:name() _ ":" _ v:value() { (k, eq_filter(v)) }) ** (_ "," _)) _ "}" { entries }

        rule label() -> String
            = ":" _ l:name() { l }
            / kw("IS") __ l:name() { l }

        rule node() -> InputNode
            = "(" _ var:ident()? _ label:label()? _ props:prop_map()? _ ")" {
                let mut node = InputNode { id: var.unwrap_or_default(), entity: label, ..Default::default() };
                for (k, f) in props.unwrap_or_default() {
                    node.filters.entry(k).or_default().push(f);
                }
                node
            }

        rule rel_types() -> Vec<String>
            = ":" _ t:(name() ++ (_ "|" _)) { t }

        rule var_len() -> HopRange
            = "*" min:uint()? ".." max:uint() { HopRange { min: min.unwrap_or(1), max } }
            / "*" n:uint() { HopRange { min: n, max: n } }
            / "*" {? Err("bounded quantifier (`*..N` or `{M,N}`)") }

        rule quantifier() -> HopRange
            = "{" _ min:uint() _ "," _ max:uint() _ "}" { HopRange { min, max } }
            / "{" _ n:uint() _ "}" { HopRange { min: n, max: n } }

        rule rel_body() -> RelBody
            = var:ident()? _ types:rel_types()? _ vl:var_len()? _ props:prop_map()? {
                (var, types.unwrap_or_default(), vl, props.unwrap_or_default())
            }

        rule relation() -> RelPart
            = "-[" _ b:rel_body() _ "]->" q:quantifier()? { RelPart::new(b, q, Direction::Outgoing) }
            / "<-[" _ b:rel_body() _ "]-" q:quantifier()? { RelPart::new(b, q, Direction::Incoming) }
            / "-[" _ b:rel_body() _ "]-" q:quantifier()? { RelPart::new(b, q, Direction::Both) }
            / "-->" q:quantifier()? { RelPart::bare(q, Direction::Outgoing) }
            / "<--" q:quantifier()? { RelPart::bare(q, Direction::Incoming) }
            / "--" q:quantifier()? { RelPart::bare(q, Direction::Both) }

        rule pattern() -> Pattern
            = start:node() chain:(_ r:relation() _ n:node() { (r, n) })* { Pattern { start, chain } }

        rule path_prefix() -> bool
            = kw("ANY") __ kw("SHORTEST") __ { true }
            / kw("SHORTEST") __ { true }
            / kw("ALL") __ kw("SHORTEST") {? Err("ANY SHORTEST (ALL SHORTEST is not supported)") }

        rule match_clause() -> (bool, Vec<Pattern>)
            = kw("MATCH") __ shortest:path_prefix()? patterns:(pattern() ++ (_ "," _)) {
                (shortest.unwrap_or(false), patterns)
            }

        rule cmp_op() -> Option<FilterOp>
            = ">=" { Some(FilterOp::Gte) }
            / "<=" { Some(FilterOp::Lte) }
            / ("<>" / "!=") {? Err("=, <, <=, >, >= (inequality is not supported)") }
            / "=" { None }
            / ">" { Some(FilterOp::Gt) }
            / "<" { Some(FilterOp::Lt) }

        rule id_list() -> Vec<i64>
            = "[" _ ids:(int() ++ (_ "," _)) _ "]" { ids }
            / "$" name:ident() {?
                params.get(&name)
                    .and_then(Value::as_array)
                    .map(|a| a.iter().filter_map(Value::as_i64).collect())
                    .ok_or("bound integer list parameter")
            }

        rule id_value() -> i64
            = int()
            / "$" name:ident() {? params.get(&name).and_then(Value::as_i64).ok_or("bound integer parameter") }

        rule property() -> (String, String)
            = v:ident() "." p:name() { (v, p) }

        rule predicate() -> Pred
            = kw("ELEMENT_ID") _ "(" _ v:ident() _ ")" _ "=" _ id:id_value() { Pred::Ids(v, vec![id]) }
            / kw("ELEMENT_ID") _ "(" _ v:ident() _ ")" __ kw("IN") __ ids:id_list() { Pred::Ids(v, ids) }
            / p:property() __ kw("IS") __ kw("NOT") __ kw("NULL") { Pred::Filter(p.0, p.1, op_filter(FilterOp::IsNotNull, None)) }
            / p:property() __ kw("IS") __ kw("NULL") { Pred::Filter(p.0, p.1, op_filter(FilterOp::IsNull, None)) }
            / p:property() __ kw("STARTS") __ kw("WITH") __ v:value() { Pred::Filter(p.0, p.1, op_filter(FilterOp::StartsWith, Some(v))) }
            / p:property() __ kw("ENDS") __ kw("WITH") __ v:value() { Pred::Filter(p.0, p.1, op_filter(FilterOp::EndsWith, Some(v))) }
            / p:property() __ kw("CONTAINS") __ v:value() { Pred::Filter(p.0, p.1, op_filter(FilterOp::Contains, Some(v))) }
            / p:property() __ kw("IN") __ v:value() { Pred::Filter(p.0, p.1, op_filter(FilterOp::In, Some(v))) }
            / p:property() _ op:cmp_op() _ v:value() {
                Pred::Filter(p.0, p.1, match op { Some(op) => op_filter(op, Some(v)), None => eq_filter(v) })
            }
            / kw("NOT") {? Err("a property predicate (NOT is not supported)") }
            / "(" {? Err("a property predicate (parenthesised predicates are not supported)") }

        rule conjunction()
            = __ kw("AND") __
            / __ kw("OR") {? Err("AND (OR is not supported)") }

        rule where_clause() -> Vec<Pred>
            = __ (kw("WHERE") / kw("FILTER")) __ preds:(predicate() ++ conjunction()) { preds }

        rule agg_fn() -> AggFunction
            = f:$(['a'..='z' | 'A'..='Z']+) &(_ "(") {?
                match f.to_ascii_lowercase().as_str() {
                    "count" => Ok(AggFunction::Count),
                    "sum" => Ok(AggFunction::Sum),
                    "avg" => Ok(AggFunction::Avg),
                    "min" => Ok(AggFunction::Min),
                    "max" => Ok(AggFunction::Max),
                    "collect" => Ok(AggFunction::Collect),
                    _ => Err("aggregate function"),
                }
            }

        rule alias() -> String
            = __ kw("AS") __ a:ident() { a }

        rule truncate_unit() -> TruncateUnit
            = "'" u:$(['a'..='z' | 'A'..='Z']+) "'" {?
                serde_json::from_value(Value::String(u.to_ascii_lowercase())).or(Err("truncation unit (minute, hour, day, week, month, quarter, year)"))
            }

        rule date_trunc() -> (TruncateUnit, String, String)
            = kw("DATE_TRUNC") _ "(" _ unit:truncate_unit() _ "," _ p:property() _ ")" { (unit, p.0, p.1) }

        rule return_item() -> ReturnItem
            = t:date_trunc() alias:alias()? { ReturnItem::Trunc(t.0, t.1, t.2, alias) }
            / f:agg_fn() _ "(" _ "*" _ ")" {? Err("count(<variable>) (count(*) is not supported)") }
            / f:agg_fn() _ "(" _ v:ident() p:("." p:name() { p })? _ ")" alias:alias()? {?
                let expr = match (f, p) {
                    (AggFunction::Count, p) => AggExpr::Count(TargetRef { node: v, property: p }),
                    (_, None) => return Err("<variable>.<property> inside a non-count aggregate"),
                    (AggFunction::Sum, Some(p)) => AggExpr::Sum(PropertyRef { node: v, property: p }),
                    (AggFunction::Avg, Some(p)) => AggExpr::Avg(PropertyRef { node: v, property: p }),
                    (AggFunction::Min, Some(p)) => AggExpr::Min(PropertyRef { node: v, property: p }),
                    (AggFunction::Max, Some(p)) => AggExpr::Max(PropertyRef { node: v, property: p }),
                    (AggFunction::Collect, Some(p)) => AggExpr::Collect(PropertyRef { node: v, property: p }),
                    (AggFunction::Count, _) => unreachable!(),
                };
                Ok(ReturnItem::Agg(expr, alias))
            }
            / p:property() alias:alias()? { ReturnItem::Prop(p.0, p.1, alias) }
            / v:ident() { ReturnItem::Var(v) }

        rule return_clause() -> Vec<ReturnItem>
            = kw("RETURN") __ items:(return_item() ++ (_ "," _)) { items }

        rule group_key() -> InputGroupByKey
            = t:date_trunc() { InputGroupByKey::Property { node: t.1, property: t.2, truncate: Some(t.0), alias: None } }
            / p:property() { InputGroupByKey::Property { node: p.0, property: p.1, truncate: None, alias: None } }
            / v:ident() { InputGroupByKey::Node { node: v, alias: None } }

        rule group_by() -> Vec<InputGroupByKey>
            = __ kw("GROUP") __ kw("BY") __ keys:(group_key() ++ (_ "," _)) { keys }

        rule order_dir() -> OrderDirection
            = __ kw("DESC") { OrderDirection::Desc }
            / __ kw("ASC") { OrderDirection::Asc }

        rule order_key() -> (OrderTarget, OrderDirection)
            = p:property() d:order_dir()? { (OrderTarget::Prop(p.0, p.1), d.unwrap_or_default()) }
            / c:ident() d:order_dir()? { (OrderTarget::Column(c), d.unwrap_or_default()) }

        rule order_by() -> Vec<(OrderTarget, OrderDirection)>
            = __ kw("ORDER") __ kw("BY") __ keys:(order_key() ++ (_ "," _)) { keys }

        rule limit() -> u32
            = __ kw("LIMIT") __ n:uint() { n }

        pub rule query() -> Result<crate::Parsed, crate::Error>
            = _ m:match_clause() preds:where_clause()? _ ret:return_clause()
              group:group_by()? order:order_by()? limit:limit()? _ ![_]
            {
                assemble(m.0, m.1, preds.unwrap_or_default(), ret, group, order.unwrap_or_default(), limit)
            }
    }
}

impl RelPart {
    fn new(
        (var, types, var_len, props): RelBody,
        quantifier: Option<HopRange>,
        direction: Direction,
    ) -> Self {
        let hops = var_len.or(quantifier);
        let mut rel = relationship(types, hops, direction);
        for (k, f) in props {
            rel.filters.entry(k).or_default().push(f);
        }
        RelPart {
            var,
            rel,
            quantified: hops.is_some(),
        }
    }

    fn bare(quantifier: Option<HopRange>, direction: Direction) -> Self {
        RelPart {
            var: None,
            rel: relationship(Vec::new(), quantifier, direction),
            quantified: quantifier.is_some(),
        }
    }
}
