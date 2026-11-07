//! A binding as the planner reads it: its source table, pinned ids, id range, and the filters
//! attached to it, each bound to the storage facts that decide how it lowers.

use ontology::constants::DEFAULT_PRIMARY_KEY;
use query_data_model::implementations::relational::RelationalBackend;
use query_data_model::implementations::relational::plan::Expr;
use query_data_model::plan::logical::Expr as Logical;

use super::filters::{BoundFilter, FilterOwner, Test, bind_filter, unindex_if_key_narrowed};
use super::predicates::Column;
use crate::logical::Query;
use crate::logical::facts::Anchor;
use crate::passes::codegen::RelationalModel;
use crate::passes::lower::build::Built;
use crate::passes::lower::build::expr::{self, Filter};
use crate::passes::lower::build::sources::Scope;

pub(crate) struct Node<'p> {
    pub alias: &'p str,
    pub entity: &'p str,
    pub table: Option<&'p str>,
    pub redaction: &'p str,
    pub pinned: Vec<i64>,
    pub range: Option<(i64, i64)>,
    pub filters: Vec<(String, BoundFilter)>,
    pub has_traversal_path: bool,
}

impl<'p> Node<'p> {
    pub fn new<M: RelationalModel>(query: Query<'p>, anchor: Anchor<'p>, model: &'p M) -> Self {
        let binding = anchor.binding;
        let graph = query.model.graph();
        let source = model
            .catalog()
            .source(binding.entity)
            .map(|(_, source)| source);
        let mut filters = bind_facts(query, &anchor, binding.name, model);
        filters.sort_by(|(left, _), (right, _)| left.cmp(right));
        unindex_if_key_narrowed(&mut filters);
        Self {
            alias: binding.name,
            entity: &graph.entity(binding.entity).name,
            table: source.map(|source| source.table.as_str()),
            redaction: model
                .redaction_id_column(binding.entity)
                .unwrap_or(DEFAULT_PRIMARY_KEY),
            pinned: anchor.pinned,
            range: anchor.range,
            filters,
            has_traversal_path: source.is_some_and(|source| source.path_column.is_some()),
        }
    }

    pub fn is_id_pinned_only(&self) -> bool {
        !self.pinned.is_empty() && self.filters.is_empty() && self.range.is_none()
    }

    pub fn uses_default_pk(&self) -> bool {
        self.redaction == DEFAULT_PRIMARY_KEY
    }
}

/// The anchor's attached filters, each bound to its property on the binding's entity.
pub(crate) fn bind_facts<M: RelationalModel + ?Sized>(
    query: Query<'_>,
    anchor: &Anchor<'_>,
    alias: &str,
    model: &M,
) -> Vec<(String, BoundFilter)> {
    let graph = query.model.graph();
    let owner = FilterOwner::Entity(anchor.binding.entity);
    anchor
        .filters
        .iter()
        .filter_map(|fact| {
            let Logical::Property { property, .. } = fact.left else {
                return None;
            };
            let name = &graph.property(property).name;
            let mut rhs = None;
            let test = Test::of(*fact, |right| {
                let Logical::Property { property, .. } = right else {
                    unreachable!("attached comparisons stay on one binding")
                };
                rhs = Some(property);
                Column::new(alias, &graph.property(property).name)
            })?;
            let mut bound = bind_filter(name, test, &owner, model);
            bound.null_as_epoch.rhs = rhs
                .filter(|rhs| model.null_as_epoch(*rhs))
                .map(|rhs| graph.property(rhs).data_type);
            Some((name.clone(), bound))
        })
        .collect()
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
