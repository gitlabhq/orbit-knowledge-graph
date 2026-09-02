use orbit_server_config::QueryConfig;

use crate::ast::{Expr, Node, Query, TableRef};

pub fn resolve(query_type: &str) -> QueryConfig {
    orbit_server_config::query::for_query_type(query_type)
}

pub fn drop_unused_settings(config: &mut QueryConfig, node: &Node) {
    let Node::Query(query) = node else {
        return;
    };
    let mut shape = QueryShape::default();
    shape.scan_query(query);
    if !shape.has_final_scan {
        config.optimize_move_to_prewhere_if_final = None;
    }
    if !shape.has_in_subquery {
        config.use_index_for_in_with_subqueries_max_values = None;
    }
}

#[derive(Default)]
struct QueryShape {
    has_final_scan: bool,
    has_in_subquery: bool,
}

impl QueryShape {
    fn scan_query(&mut self, q: &Query) {
        self.scan_table(&q.from);
        for cte in &q.ctes {
            self.scan_query(&cte.query);
        }
        for union in &q.union_all {
            self.scan_query(union);
        }
        q.select
            .iter()
            .map(|s| &s.expr)
            .chain(&q.where_clause)
            .chain(&q.group_by)
            .chain(&q.having)
            .chain(q.order_by.iter().map(|o| &o.expr))
            .chain(q.limit_by.iter().flat_map(|(_, cols)| cols))
            .for_each(|e| self.scan_expr(e));
    }

    fn scan_table(&mut self, t: &TableRef) {
        match t {
            TableRef::Scan { final_, .. } => self.has_final_scan |= *final_,
            TableRef::Join {
                left, right, on, ..
            } => {
                self.scan_table(left);
                self.scan_table(right);
                self.scan_expr(on);
            }
            TableRef::Union { queries, .. } => queries.iter().for_each(|q| self.scan_query(q)),
            TableRef::Subquery { query, .. } => self.scan_query(query),
        }
    }

    fn scan_expr(&mut self, e: &Expr) {
        match e {
            Expr::InSubquery { expr, .. } => {
                self.has_in_subquery = true;
                self.scan_expr(expr);
            }
            Expr::InSelect { expr, query } => {
                self.has_in_subquery = true;
                self.scan_expr(expr);
                self.scan_query(query);
            }
            Expr::FuncCall { args, .. } => args.iter().for_each(|a| self.scan_expr(a)),
            Expr::BinaryOp { left, right, .. } => {
                self.scan_expr(left);
                self.scan_expr(right);
            }
            Expr::UnaryOp { expr, .. } | Expr::Lambda { body: expr, .. } => self.scan_expr(expr),
            Expr::Column { .. }
            | Expr::Identifier(_)
            | Expr::Literal(_)
            | Expr::Param { .. }
            | Expr::Star => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ast::{ChType, Cte, SelectExpr};

    fn select_id(from: TableRef, where_clause: Option<Expr>) -> Node {
        Node::Query(Box::new(Query {
            select: vec![SelectExpr::col("t", "id")],
            from,
            where_clause,
            ..Default::default()
        }))
    }

    fn settings_for(node: &Node) -> QueryConfig {
        let mut config = QueryConfig::default();
        drop_unused_settings(&mut config, node);
        config
    }

    #[test]
    fn resolve_returns_default_for_unknown_type() {
        let cfg = resolve("nonexistent");
        assert_eq!(cfg, QueryConfig::default());
    }

    #[test]
    fn plain_scan_with_literal_in_drops_both_settings() {
        let node = select_id(
            TableRef::scan("gl_project", "t"),
            Expr::col_in("t", "id", ChType::Int64, vec![1.into(), 2.into()]),
        );
        let cfg = settings_for(&node);
        assert_eq!(cfg.optimize_move_to_prewhere_if_final, None);
        assert_eq!(cfg.use_index_for_in_with_subqueries_max_values, None);
    }

    #[test]
    fn final_scan_inside_cte_keeps_prewhere_setting() {
        let user_ids = Query {
            select: vec![SelectExpr::col("u", "id")],
            from: TableRef::scan_final("gl_user", "u"),
            ..Default::default()
        };
        let node = Node::Query(Box::new(Query {
            ctes: vec![Cte::new("c", user_ids)],
            select: vec![SelectExpr::col("t", "id")],
            from: TableRef::scan("gl_project", "t"),
            ..Default::default()
        }));
        let cfg = settings_for(&node);
        assert_eq!(cfg.optimize_move_to_prewhere_if_final, Some(true));
        assert_eq!(cfg.use_index_for_in_with_subqueries_max_values, None);
    }

    #[test]
    fn in_subquery_keeps_index_cap() {
        let node = select_id(
            TableRef::scan("gl_project", "t"),
            Some(Expr::and(
                Expr::lit(true),
                Expr::InSubquery {
                    expr: Box::new(Expr::col("t", "id")),
                    cte_name: "ids".into(),
                    column: "id".into(),
                },
            )),
        );
        let cfg = settings_for(&node);
        assert_eq!(cfg.optimize_move_to_prewhere_if_final, None);
        assert_eq!(
            cfg.use_index_for_in_with_subqueries_max_values,
            QueryConfig::default().use_index_for_in_with_subqueries_max_values
        );
    }
}
