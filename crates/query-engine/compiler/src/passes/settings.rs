use std::collections::HashMap;

use orbit_server_config::QueryConfig;

use crate::ast::visit::{
    query_expressions, visit_expressions, visit_queries, visit_queries_mut, visit_relations,
};
use crate::ast::{Expr, Query, TableRef};
use crate::error::Result;

pub fn resolve(query_type: &str) -> QueryConfig {
    orbit_server_config::query::for_query_type(query_type)
}

pub fn materialize_shared_ctes(query: &mut Query) -> Result<()> {
    visit_queries_mut(query, &mut |query| {
        if query.ctes.is_empty() {
            return Ok(());
        }
        let references = cte_references(query)?;
        for cte in &mut query.ctes {
            cte.materialized =
                !cte.recursive && references.get(&cte.name).is_some_and(|count| *count > 1);
        }
        Ok(())
    })
}

fn cte_references(query: &Query) -> Result<HashMap<String, usize>> {
    let mut references = HashMap::new();
    visit_queries(query, &mut |query| {
        let mut join_conditions = Vec::new();
        visit_relations(&query.from, &mut |relation| match relation {
            TableRef::Cte { name, .. } => *references.entry(name.clone()).or_default() += 1,
            TableRef::Join { on, .. } => join_conditions.push(on),
            _ => {}
        });
        for expression in query_expressions(query).chain(join_conditions) {
            visit_expressions(expression, &mut |expression| {
                if let Expr::InSubquery { cte_name, .. } = expression {
                    *references.entry(cte_name.clone()).or_default() += 1;
                }
                Ok(())
            })?;
        }
        Ok(())
    })?;
    Ok(references)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resolve_returns_default_for_unknown_type() {
        let cfg = resolve("nonexistent");
        assert_eq!(cfg, QueryConfig::default());
    }
}
