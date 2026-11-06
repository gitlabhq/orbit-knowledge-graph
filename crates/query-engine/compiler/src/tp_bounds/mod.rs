mod derive;
mod keys;
mod propagate;
#[cfg(test)]
mod tests;

use std::collections::{HashMap, HashSet};

use keys::TpLookupValue;

#[derive(Clone, Default)]
pub struct TpBounds {
    nodes: HashMap<String, TpBound>,
    relationships: Vec<Option<TpBound>>,
    self_seeded: HashSet<String>,
}

impl TpBounds {
    pub(crate) fn node(&self, alias: &str) -> Option<&TpBound> {
        if self.self_seeded.contains(alias) {
            return None;
        }
        self.nodes.get(alias)
    }

    pub(crate) fn relationship(&self, index: usize) -> Option<&TpBound> {
        self.relationships.get(index).and_then(Option::as_ref)
    }
}

use ontology::constants::{DELETED_COLUMN, TRAVERSAL_PATH_COLUMN, VERSION_COLUMN};

use crate::constants::scope_lookup_alias;
use crate::passes::lower::build::{self, Built};
use orbit_utils::query_types::SqlType;
use query_data_model::implementations::relational::RelationalBackend;
use query_data_model::implementations::relational::plan::{self, Relation, RelationalBuilder};

const UNRESOLVED_PATH: &str = "0/";

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct TpBound {
    lookups: Vec<TpLookup>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct TpLookup {
    source_table: String,
    key_column: String,
    value: TpLookupValue,
}

impl TpBound {
    fn from_lookups(lookups: Vec<TpLookup>) -> Self {
        Self { lookups }
    }
}

pub(crate) fn bound<B: RelationalBackend>(
    q: &mut RelationalBuilder<'_, B>,
    scope: &[&Relation],
    bound: &TpBound,
    alias: &str,
) -> Built<plan::Expr> {
    let mut matches = Vec::new();
    for lookup in &bound.lookups {
        let column = build::col(scope, alias, TRAVERSAL_PATH_COLUMN)?;
        let path = lookup_path(q, lookup)?;
        matches.push(build::call(plan::Function::StartsWith, vec![column, path]));
    }
    for lookup in &bound.lookups {
        let path = lookup_path(q, lookup)?;
        matches.push(build::eq(path, build::string(UNRESOLVED_PATH)));
    }
    Ok(build::disjoin(matches).expect("TP bound has at least one lookup"))
}

fn lookup_path<B: RelationalBackend>(
    q: &mut RelationalBuilder<'_, B>,
    lookup: &TpLookup,
) -> Built<plan::Expr> {
    let alias = scope_lookup_alias();
    let (key, final_) = match &lookup.value {
        TpLookupValue::Numeric(id) => (build::param::<B>(SqlType::Int64, (*id).into()), false),
        TpLookupValue::Text(text) => (
            build::param::<B>(SqlType::String, text.clone().into()),
            true,
        ),
    };
    let body = q.subquery("subquery", |q| {
        let rows = build::scan(q, &lookup.source_table, alias, final_)?;
        let key_column = build::col(&[&rows], alias, &lookup.key_column)?;
        let rows = q.filter(rows, build::eq(key_column, key))?;
        let latest = |column: &str| -> Built<plan::Measure> {
            Ok(plan::Measure {
                function: plan::AggregateFunction::ArgMaxOrNull,
                args: vec![
                    build::col(&[&rows], alias, column)?,
                    build::col(&[&rows], alias, VERSION_COLUMN)?,
                ],
                distinct: false,
                condition: None,
            })
        };
        let measures = vec![latest(DELETED_COLUMN)?, latest(TRAVERSAL_PATH_COLUMN)?];
        let aggregated = q.aggregate(rows, Vec::new(), measures)?;
        let (deleted, path) = (aggregated.measures[0].expr(), aggregated.measures[1].expr());
        let path = build::call(
            plan::Function::Coalesce,
            vec![
                build::call(plan::Function::If, vec![deleted, plan::Expr::Null, path]),
                build::string(UNRESOLVED_PATH),
            ],
        );
        let items = vec![(TRAVERSAL_PATH_COLUMN.to_string(), path)];
        let (rows, columns) = build::computed(q, aggregated.rows, items)?;
        q.select(rows, columns)
    })?;
    Ok(body.scalar())
}
