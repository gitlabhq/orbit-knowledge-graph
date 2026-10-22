//! Step for a relation stored as a column on one node: read the holder table, filter its key column
//! by the other endpoint, and expose the pair as edge columns.

use ontology::constants::*;
use query_data_model::OrbitQueryModel;

use super::super::{EdgeRelation, Fk};
use super::RelationBuilder;
use crate::error::{QueryError, Result};
use crate::input::ColumnSelection;
use crate::passes::plan::predicates::*;

impl<M: OrbitQueryModel + ?Sized> RelationBuilder<'_, M> {
    pub(super) fn add_fk(&mut self, relation: &EdgeRelation, key: &Fk) -> Result<()> {
        let pattern = self.pattern;
        let joined_through_endpoint = self.ensure_holder(relation, key)?;
        let holder = self
            .binding(&key.node)
            .cloned()
            .expect("foreign key holder binding");
        let alias = holder
            .property_source
            .as_deref()
            .expect("foreign key holder table");
        let endpoint = relation.other_end(&key.node);
        let key_column = Column::new(alias, &key.column);
        let mut predicates = pattern.nodes[endpoint].id_predicates(key_column.clone());
        if self.binding(endpoint).is_none() {
            let selective = pattern.endpoint_is_selective(endpoint);
            let unfiltered = !selective && !pattern.has_uncovered_filter(&pattern.nodes[endpoint]);
            let mut reached = if selective {
                None
            } else {
                self.ids_through_holder(&key.node, &key.column)
            };
            if reached.is_none()
                && unfiltered
                && let Some(read) = self.nodes.get(&key.node).and_then(|state| state.read)
            {
                reached = Some(self.ids_reached_by(read, &key.column));
            }
            let read_later = pattern.nodes[endpoint].joined || pattern.holds_fk(endpoint);
            match reached {
                Some(reached) if read_later && unfiltered => self.set_ids(endpoint, reached),
                None if read_later && unfiltered => {}
                reached if self.ids(endpoint).is_none() => {
                    let mut node = pattern.nodes[endpoint].clone();
                    node.columns = Some(ColumnSelection::List(vec![]));
                    if let Some(reached) = reached {
                        node.predicates.push(membership(
                            Column::new(&node.alias, DEFAULT_PRIMARY_KEY),
                            reached,
                        ));
                    }
                    let ids = self.node_ids_from(&node)?;
                    predicates.push(membership(key_column.clone(), ids));
                }
                _ => {}
            }
        } else if let Some(ids) = self.ids(endpoint).cloned() {
            predicates.push(membership(key_column.clone(), ids));
        }
        match self.binding(endpoint) {
            Some(binding) if !joined_through_endpoint => predicates.push(column_eq(
                Column::new(&binding.alias, &binding.column),
                key_column,
            )),
            Some(_) => {}
            None => self.set_binding(endpoint, alias, &key.column, None, None),
        }
        if let Some(root) = self.root.take() {
            self.root = Some(self.graph.filter(root, predicates));
        }
        self.push_fk_outputs(relation, key, alias);
        Ok(())
    }

    /// Reads the holder table if it is not in the chain yet; returns whether it joined through the other endpoint.
    fn ensure_holder(&mut self, relation: &EdgeRelation, key: &Fk) -> Result<bool> {
        if self.has_table(&key.node) {
            return Ok(false);
        }
        let pattern = self.pattern;
        let mut node = pattern.nodes[&key.node].clone();
        let endpoint = relation.other_end(&key.node);
        let key_column = Column::new(&node.alias, &key.column);
        node.predicates
            .extend(pattern.nodes[endpoint].id_predicates(key_column.clone()));
        if self.binding(endpoint).is_none()
            && self.ids(endpoint).is_none()
            && pattern.has_selective_uncovered_filter(endpoint)
        {
            let mut far = pattern.nodes[endpoint].clone();
            far.columns = Some(ColumnSelection::List(vec![]));
            let ids = self.node_ids_from(&far)?;
            node.predicates.push(membership(key_column.clone(), ids));
        } else if let Some(ids) = self.endpoint_ids(endpoint) {
            node.predicates.push(membership(key_column.clone(), ids));
        }
        if let Some(ids) = self.reached_ids(&key.node) {
            node.predicates.push(membership(
                Column::new(&node.alias, DEFAULT_PRIMARY_KEY),
                ids,
            ));
        }
        let holder_binding = self.binding(&key.node).cloned();
        let joined_through_endpoint = self.root.is_some() && holder_binding.is_none();
        self.attach_table(&node, |builder| match &holder_binding {
            Some(binding) => Ok((
                Column::new(&node.alias, DEFAULT_PRIMARY_KEY),
                Column::new(&binding.alias, &binding.column),
            )),
            None => {
                let endpoint = builder
                    .binding(endpoint)
                    .ok_or_else(|| QueryError::Lowering("node relation is disconnected".into()))?;
                Ok((key_column, Column::new(&endpoint.alias, &endpoint.column)))
            }
        })?;
        Ok(joined_through_endpoint)
    }

    fn push_fk_outputs(&mut self, relation: &EdgeRelation, key: &Fk, alias: &str) {
        let pattern = self.pattern;
        let (source, target) = relation.stored_endpoints();
        let (source_id, target_id) = if key.node == source {
            (DEFAULT_PRIMARY_KEY, key.column.as_str())
        } else {
            (key.column.as_str(), DEFAULT_PRIMARY_KEY)
        };
        let entity =
            |node: &str| OutputValue::Text(pattern.nodes[node].entity.clone().unwrap_or_default());
        self.push_relation_outputs(
            relation,
            [
                OutputValue::Text(relation.rel_types.first().cloned().unwrap_or_default()),
                OutputValue::Column(Column::new(alias, source_id)),
                entity(source),
                OutputValue::Column(Column::new(alias, target_id)),
                entity(target),
            ],
        );
    }
}
