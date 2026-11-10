//! A binding as the planner reads it: its source table, pinned ids, id range, and the filters
//! attached to it, each bound to the storage facts that decide how it lowers.

use query_data_model::Catalog;
use query_data_model::constants::DEFAULT_PRIMARY_KEY;
use query_data_model::implementations::relational::RelationalBackend;
use query_data_model::implementations::relational::plan::Expr;

use super::filters::{BoundFilter, bind_facts, unindex_if_key_narrowed};
use super::predicates::Column;
use crate::logical::Query;
use crate::logical::columns::redaction_column;
use crate::logical::facts::Anchor;
use crate::passes::codegen::SqlBackend;
use crate::passes::plan::build::Built;
use crate::passes::plan::build::expr::{self, Filter};
use crate::passes::plan::build::sources::Scope;

pub(crate) struct Node<'p> {
    pub alias: &'p str,
    pub entity: &'p str,
    pub table: Option<&'p str>,
    pub redaction: &'p str,
    pub pinned: Option<Vec<i64>>,
    pub range: Option<(i64, i64)>,
    pub filters: Vec<(String, BoundFilter)>,
    pub has_traversal_path: bool,
}

impl<'p> Node<'p> {
    pub fn new<B: SqlBackend>(query: Query<'p>, anchor: Anchor<'p>, model: &'p Catalog<B>) -> Self {
        let binding = anchor.binding;
        let graph = query.model.graph();
        let source = model.source(binding.entity).map(|(_, source)| source);
        let mut filters = bind_facts(query, &anchor, binding.name, model);
        unindex_if_key_narrowed(&mut filters);
        Self {
            alias: binding.name,
            entity: &graph.entity(binding.entity).name,
            table: source.map(|source| source.table.as_str()),
            redaction: redaction_column(model, binding.entity).unwrap_or(DEFAULT_PRIMARY_KEY),
            pinned: anchor.pinned,
            range: anchor.range,
            filters,
            has_traversal_path: source.is_some_and(|source| source.path_column.is_some()),
        }
    }

    pub fn is_id_pinned_only(&self) -> bool {
        self.pinned.is_some() && self.filters.is_empty() && self.range.is_none()
    }

    pub fn uses_default_pk(&self) -> bool {
        self.redaction == DEFAULT_PRIMARY_KEY
    }
}

impl BoundFilter {
    pub fn lower<B: RelationalBackend>(&self, scope: &Scope<'_>, column: &Column) -> Built<Expr> {
        expr::filter::<B>(
            scope,
            Filter {
                column,
                test: &self.test,
                data_type: self.data_type.as_ref(),
                in_sort_key: self.in_sort_key,
                text_indexed: self.text_indexed,
                null_as_epoch: self.null_as_epoch,
                negated: false,
            },
        )
    }
}
