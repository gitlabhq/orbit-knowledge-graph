use std::collections::HashSet;

use ontology::constants::*;
use ontology::Ontology;

use crate::ast::*;
use crate::constants::*;
use crate::error::Result;
use crate::input::*;
use crate::passes::normalize::is_wildcard;
use crate::passes::plan_v2::PhysOp;
use crate::passes::shared::{
    deleted_false, denorm_tag_expr, edge_select_columns, edge_select_columns_with_prefix,
    filter_to_expr, id_list_predicate, id_range_predicate, rel_kind_filter, requested_columns,
};

pub fn lower(op: PhysOp, input: &Input, ontology: &Ontology) -> Result<Node> {
    let ctx = LowerCtx::new(input, ontology);
    let mut q = ctx.emit(op);
    ctx.append_join_predicates(&mut q);
    ctx.append_projections(&mut q);
    ctx.append_aggregation(&mut q);
    ctx.apply_order_limit(&mut q);
    Ok(Node::Query(Box::new(q)))
}

struct LowerCtx<'a> {
    input: &'a Input,
    excerpt_cols: std::collections::HashMap<String, HashSet<String>>,
    excerpt_max_chars: u32,
}

impl<'a> LowerCtx<'a> {
    fn new(input: &'a Input, ontology: &Ontology) -> Self {
        let limit = input.fetch_limit();
        let max_chars = (8 * 1024 * 1024 / 4 / u64::from(limit.max(1))) as u32;
        let mut excerpt_cols = std::collections::HashMap::new();
        for node in &input.nodes {
            if let Some(ref entity) = node.entity {
                excerpt_cols.insert(node.id.clone(), text_excerpt_columns(entity, ontology));
            }
        }
        Self { input, excerpt_cols, excerpt_max_chars: max_chars }
    }

    fn emit(&self, op: PhysOp) -> Query {
        match op {
            PhysOp::Scan { table, alias, dedup } => {
                let predicates = self.scan_predicates(&alias, &table);
                let from = if dedup {
                    TableRef::scan_final(&table, &alias)
                } else {
                    TableRef::scan(&table, &alias)
                };
                Query {
                    from,
                    where_clause: Expr::conjoin(predicates),
                    ..Default::default()
                }
            }

            PhysOp::Join { left, right, left_col, right_col } => {
                let lq = self.emit(*left);
                let rq = self.emit(*right);
                let on = Expr::eq(
                    Expr::col(&left_col.0, &left_col.1),
                    Expr::col(&right_col.0, &right_col.1),
                );
                let (rhs, rhs_wh) = self.wrap_if_needed(rq);
                Query {
                    from: TableRef::join(JoinType::Inner, lq.from, rhs, on),
                    where_clause: merge_where(lq.where_clause, rhs_wh),
                    ctes: lq.ctes,
                    ..Default::default()
                }
            }

            PhysOp::Union { arms, alias } => {
                let queries: Vec<Query> = arms.into_iter().map(|a| {
                    let mut q = self.emit(a);
                    if q.select.is_empty() { q.select.push(SelectExpr::star()); }
                    q
                }).collect();
                Query {
                    from: TableRef::union_all(queries, &alias),
                    ..Default::default()
                }
            }

            PhysOp::Cte { name, body, consumer } => {
                let body_q = {
                    let mut q = self.emit(*body);
                    if q.select.is_empty() { q.select.push(SelectExpr::star()); }
                    q
                };
                let mut consumer_q = self.emit(*consumer);
                consumer_q.ctes.insert(0, Cte::new(&name, body_q));
                consumer_q
            }

            PhysOp::TopN { input, limit } => {
                let mut q = self.emit(*input);
                q.limit = Some(limit);
                q
            }

            PhysOp::Aggregate { input, limit } => {
                let mut q = self.emit(*input);
                q.limit = Some(limit);
                q
            }
        }
    }

    fn scan_predicates(&self, alias: &str, _table: &str) -> Vec<Expr> {
        if let Some(node) = self.input.nodes.iter().find(|n| n.id == alias) {
            return self.node_predicates(alias, node);
        }
        if let Some((_idx, rel)) = self.find_edge_rel(alias) {
            return self.edge_predicates(alias, rel);
        }
        vec![deleted_false(alias)]
    }

    fn find_edge_rel(&self, alias: &str) -> Option<(usize, &InputRelationship)> {
        if !alias.starts_with('e') { return None; }
        let idx: usize = alias[1..].parse().ok()?;
        self.input.relationships.get(idx).map(|r| (idx, r))
    }

    fn node_predicates(&self, alias: &str, node: &InputNode) -> Vec<Expr> {
        let mut p = Vec::new();
        let mut props: Vec<_> = node.filters.iter().collect();
        props.sort_unstable_by_key(|(k, _)| *k);
        for (prop, fs) in props {
            for f in fs { p.push(filter_to_expr(alias, prop, f)); }
        }
        if !node.node_ids.is_empty() {
            p.push(id_list_predicate(alias, DEFAULT_PRIMARY_KEY, &node.node_ids));
        }
        if let Some(ref r) = node.id_range { p.push(id_range_predicate(alias, r)); }
        p.push(deleted_false(alias));
        p
    }

    fn edge_predicates(&self, alias: &str, rel: &InputRelationship) -> Vec<Expr> {
        let mut p = Vec::new();
        let (sc, ec) = rel.direction.edge_columns();

        if let Some(f) = rel_kind_filter(alias, &rel.types) { p.push(f); }
        for (nid, ic) in [(&rel.from, sc), (&rel.to, ec)] {
            if let Some(n) = self.input.nodes.iter().find(|n| &n.id == nid) {
                if let Some(ref ent) = n.entity {
                    let kc = if ic == SOURCE_ID_COLUMN { SOURCE_KIND_COLUMN } else { TARGET_KIND_COLUMN };
                    p.push(Expr::eq(Expr::col(alias, kc), Expr::string(ent)));
                }
            }
        }
        p.push(deleted_false(alias));

        if let Some(ref pfx) = rel.scope_prefix { p.push(pfx.predicate(alias)); }

        for (nid, ic) in [(&rel.from, sc), (&rel.to, ec)] {
            if let Some(n) = self.input.nodes.iter().find(|n| &n.id == nid) {
                if !n.node_ids.is_empty() { p.push(id_list_predicate(alias, ic, &n.node_ids)); }
                if let Some(ref r) = n.id_range {
                    p.push(Expr::and(
                        Expr::binary(Op::Ge, Expr::col(alias, ic), Expr::int(r.start)),
                        Expr::binary(Op::Le, Expr::col(alias, ic), Expr::int(r.end)),
                    ));
                }
            }
        }

        let meta = &self.input.compiler;
        let et_key = meta.edge_table_for_rel.iter()
            .find(|(k, _)| rel.types.contains(k))
            .map(|(_, v)| v.as_str())
            .unwrap_or(&meta.default_edge_table);
        if let Some(ecols) = meta.table_columns.get(et_key) {
            let reserved: HashSet<&str> = EDGE_RESERVED_COLUMNS.iter().copied().collect();
            let mut seen: HashSet<&str> = HashSet::new();
            for nid in [&rel.from, &rel.to] {
                if let Some(n) = self.input.nodes.iter().find(|n| &n.id == nid) {
                    for (prop, fs) in &n.filters {
                        if ecols.contains(prop) && !reserved.contains(prop.as_str()) && seen.insert(prop.as_str()) {
                            for f in fs { p.push(filter_to_expr(alias, prop, f)); }
                        }
                    }
                }
            }
        }

        if !is_wildcard(&rel.types) {
            for (nid, ic) in [(&rel.from, sc), (&rel.to, ec)] {
                if let Some(n) = self.input.nodes.iter().find(|n| &n.id == nid) {
                    let ent = n.entity.as_deref().unwrap_or("");
                    let dir = if ic == SOURCE_ID_COLUMN { "source" } else { "target" };
                    for (prop, fs) in &n.filters {
                        let key = (ent.to_string(), prop.clone(), dir.to_string());
                        let carries = meta.denorm_rel_kinds.get(&key).is_some_and(|kinds| {
                            rel.types.iter().any(|t| kinds.iter().any(|k| k == t))
                        });
                        if !carries { continue; }
                        if let Some((tc, tk)) = meta.denormalized_columns.get(&key) {
                            for f in fs {
                                if let Some(expr) = denorm_tag_expr(alias, tc, tk, f) { p.push(expr); }
                            }
                        }
                    }
                }
            }
        }
        p
    }

    fn wrap_if_needed(&self, q: Query) -> (TableRef, Option<Expr>) {
        let alias = extract_alias(&q.from);
        if q.where_clause.is_some() || !q.ctes.is_empty() || q.limit.is_some() {
            let a = alias.unwrap_or_else(|| "_r".to_string());
            let mut w = q;
            if w.select.is_empty() { w.select.push(SelectExpr::star()); }
            (TableRef::subquery(w, &a), None)
        } else {
            (q.from, None)
        }
    }

    fn append_join_predicates(&self, q: &mut Query) {
        for jp in &self.input.join_predicates {
            let filter = InputFilter {
                op: Some(jp.op),
                rhs_column: Some((jp.rhs_node.clone(), jp.rhs_prop.clone())),
                ..Default::default()
            };
            let pred = filter_to_expr(&jp.lhs_node, &jp.lhs_prop, &filter);
            q.where_clause = Some(match q.where_clause.take() {
                Some(existing) => Expr::and(existing, pred),
                None => pred,
            });
        }
    }

    fn append_projections(&self, q: &mut Query) {
        let input = self.input;
        if input.query_type == QueryType::Traversal && !input.relationships.is_empty() {
            for (i, rel) in input.relationships.iter().enumerate() {
                let ea = format!("e{i}");
                if rel.hops.max > 1 {
                    let prefix = format!("hop_{ea}");
                    q.select.extend(edge_select_columns_with_prefix(&ea, &prefix));
                    q.select.push(SelectExpr::new(
                        Expr::col(&ea, PATH_NODES_COLUMN),
                        format!("{prefix}_{PATH_NODES_COLUMN}"),
                    ));
                } else {
                    q.select.extend(edge_select_columns(&ea));
                }
            }
        }
        if input.query_type == QueryType::Traversal {
            for node in &input.nodes {
                for col in requested_columns(&node.columns) {
                    q.select.push(SelectExpr::new(
                        self.text_excerpt_expr(&node.id, &col),
                        format!("{}_{col}", node.id),
                    ));
                }
            }
        }
    }

    fn append_aggregation(&self, q: &mut Query) {
        let input = self.input;
        if input.query_type != QueryType::Aggregation { return; }

        let names = group_by_names(&input.aggregation.group_by);
        for (g, name) in input.aggregation.group_by.iter().zip(names) {
            match g {
                InputGroupByKey::Property { node, property, truncate, .. } => {
                    let col = Expr::col(node, property);
                    let expr = match truncate {
                        Some(unit) => {
                            let tr = Expr::func(unit.ch_function(), vec![col]);
                            match unit {
                                TruncateUnit::Minute | TruncateUnit::Hour =>
                                    Expr::func("toDateTime64", vec![tr, Expr::ident("0")]),
                                _ => Expr::func("toDate32", vec![tr]),
                            }
                        }
                        None => col,
                    };
                    q.select.push(SelectExpr::new(expr.clone(), name));
                    if !q.group_by.contains(&expr) { q.group_by.push(expr); }
                }
                InputGroupByKey::Node { node, .. } => {
                    let cols = input.nodes.iter()
                        .find(|n| &n.id == node)
                        .map(|n| requested_columns(&n.columns))
                        .unwrap_or_default();
                    for c in cols {
                        let expr = Expr::col(node, &c);
                        if !q.group_by.contains(&expr) { q.group_by.push(expr); }
                    }
                }
            }
        }

        for agg in &input.aggregation.metrics {
            let expr = match &agg.expr {
                AggExpr::Count(t) => match t.property.as_ref() {
                    Some(p) => Expr::func("COUNT", vec![Expr::col(&t.node, p)]),
                    None => Expr::func("COUNT", vec![]),
                },
                AggExpr::Sum(p) => Expr::func("SUM", vec![Expr::col(&p.node, &p.property)]),
                AggExpr::Avg(p) => Expr::func("AVG", vec![Expr::col(&p.node, &p.property)]),
                AggExpr::Min(p) => Expr::func("MIN", vec![Expr::col(&p.node, &p.property)]),
                AggExpr::Max(p) => Expr::func("MAX", vec![Expr::col(&p.node, &p.property)]),
                AggExpr::Collect(p) => Expr::func("groupArray", vec![Expr::col(&p.node, &p.property)]),
            };
            q.select.push(SelectExpr::new(expr, agg.output_name()));
        }

        if let Some(ref sort) = input.aggregation.sort {
            q.order_by.push(if matches!(sort.direction, OrderDirection::Desc) {
                OrderExpr::desc(Expr::ident(&sort.column))
            } else {
                OrderExpr::asc(Expr::ident(&sort.column))
            });
        }
        if input.cursor.is_some() {
            q.order_by.extend(q.group_by.iter().map(|e| OrderExpr::asc(e.clone())));
        }
    }

    fn apply_order_limit(&self, q: &mut Query) {
        if q.order_by.is_empty() {
            if let Some(ref ob) = self.input.order_by {
                q.order_by.push(if matches!(ob.direction, OrderDirection::Desc) {
                    OrderExpr::desc(Expr::col(&ob.node, &ob.property))
                } else {
                    OrderExpr::asc(Expr::col(&ob.node, &ob.property))
                });
            }
        }
    }

    fn text_excerpt_expr(&self, alias: &str, col: &str) -> Expr {
        let value = Expr::col(alias, col);
        let is_excerpt = self.excerpt_cols.get(alias).is_some_and(|c| c.contains(col));
        if !is_excerpt || self.excerpt_max_chars == 0 { return value; }
        let excerpt = Expr::func("substringUTF8", vec![value.clone(), Expr::lit(1), Expr::lit(self.excerpt_max_chars)]);
        let shortened = Expr::binary(Op::Gt, Expr::func("length", vec![value]), Expr::func("length", vec![excerpt.clone()]));
        Expr::func("concat", vec![
            excerpt,
            Expr::func("if", vec![shortened, Expr::string(" [truncated]"), Expr::string("")]),
        ])
    }
}

fn extract_alias(tr: &TableRef) -> Option<String> {
    match tr {
        TableRef::Scan { alias, .. }
        | TableRef::Subquery { alias, .. }
        | TableRef::Union { alias, .. } => Some(alias.clone()),
        TableRef::Join { .. } => None,
    }
}

fn merge_where(a: Option<Expr>, b: Option<Expr>) -> Option<Expr> {
    match (a, b) {
        (Some(a), Some(b)) => Some(Expr::and(a, b)),
        (a, b) => a.or(b),
    }
}

fn text_excerpt_columns(entity: &str, ontology: &Ontology) -> HashSet<String> {
    let Some(node) = ontology.get_node(entity) else { return HashSet::new() };
    let mut cols: HashSet<String> = node.fields.iter()
        .filter(|f| f.column_name().is_some() && f.data_type == ontology::DataType::String)
        .map(|f| f.name.clone()).collect();
    for f in &node.fields {
        if let ontology::FieldSource::Virtual(src) = &f.source {
            for dep in &src.depends_on { cols.remove(dep); }
        }
    }
    cols
}

fn group_by_names(keys: &[InputGroupByKey]) -> Vec<String> {
    keys.iter().map(|k| match k {
        InputGroupByKey::Property { alias: Some(a), .. } => a.clone(),
        InputGroupByKey::Property { node, property, .. } => format!("{node}_{property}"),
        InputGroupByKey::Node { alias: Some(a), .. } => a.clone(),
        InputGroupByKey::Node { node, .. } => node.clone(),
    }).collect()
}
