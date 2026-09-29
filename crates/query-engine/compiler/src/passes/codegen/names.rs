use std::collections::{HashMap, HashSet};

use crate::ast::visit::{visit_expressions, visit_queries};
use crate::ast::{Expr, Identifier, Node, Symbol, TableRef};

#[derive(Default)]
pub(super) struct Names {
    reserved: HashSet<String>,
    generated: HashMap<Symbol, String>,
    next: usize,
}

impl Names {
    pub fn new(node: &Node) -> Self {
        let mut names = Self::default();
        match node {
            Node::Query(query) => {
                visit_queries(query, &mut |query| {
                    for cte in &query.ctes {
                        names.reserve(&cte.name);
                    }
                    for select in &query.select {
                        if let Some(alias) = &select.alias {
                            names.reserve(alias);
                        }
                        names.expression(&select.expr);
                    }
                    for expression in query
                        .where_clause
                        .iter()
                        .chain(&query.having)
                        .chain(&query.group_by)
                        .chain(query.order_by.iter().map(|order| &order.expr))
                        .chain(query.limit_by.iter().flat_map(|(_, keys)| keys))
                    {
                        names.expression(expression);
                    }
                    names.relation(&query.from);
                    Ok(())
                })
                .expect("identifier collection is infallible");
            }
            Node::Insert(insert) => {
                names.reserved.insert(insert.table().into());
                names.reserved.extend(insert.columns().iter().cloned());
                for value in insert.values().iter().flatten() {
                    names.expression(value);
                }
            }
        }
        names
    }

    fn reserve(&mut self, identifier: &Identifier) {
        if let Some(name) = identifier.name() {
            self.reserved.insert(name.to_ascii_lowercase());
        }
    }

    fn expression(&mut self, expression: &Expr) {
        visit_expressions(expression, &mut |expression| {
            match expression {
                Expr::Column { table, column } => {
                    self.reserve(table);
                    self.reserve(column);
                }
                Expr::Identifier(name) => self.reserve(name),
                Expr::InSubquery {
                    cte_name, column, ..
                } => {
                    self.reserve(cte_name);
                    self.reserve(column);
                }
                Expr::Lambda { param, .. } => {
                    self.reserved.insert(param.to_ascii_lowercase());
                }
                _ => {}
            }
            Ok(())
        })
        .expect("identifier collection is infallible");
    }

    fn relation(&mut self, table: &TableRef) {
        match table {
            TableRef::Scan { table, alias, .. } => {
                self.reserved.insert(table.to_ascii_lowercase());
                self.reserve(alias);
            }
            TableRef::Reference { name, alias } => {
                self.reserve(name);
                self.reserve(alias);
            }
            TableRef::Join {
                left, right, on, ..
            } => {
                self.relation(left);
                self.relation(right);
                self.expression(on);
            }
            TableRef::Subquery { alias, .. } | TableRef::Union { alias, .. } => self.reserve(alias),
        }
    }

    pub fn resolve(&mut self, identifier: &Identifier) -> String {
        let Identifier::Generated(symbol) = identifier else {
            return identifier.name().expect("named identifier").into();
        };
        if let Some(name) = self.generated.get(symbol) {
            return name.clone();
        }

        loop {
            let name = format!("_q{}", self.next);
            self.next += 1;
            if self.reserved.insert(name.clone()) {
                self.generated.insert(*symbol, name.clone());
                return name;
            }
        }
    }
}
