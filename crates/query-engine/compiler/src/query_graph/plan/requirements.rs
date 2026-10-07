use super::*;
use crate::input::{Direction, Input, InputNode, QueryType, node_group_ids};

pub(super) struct NodeRequirements<'a> {
    identity: Option<StoredColumnRef<'a>>,
    role: Option<(StoredColumnRef<'a>, u32)>,
}

impl NodeRequirements<'_> {
    pub(super) fn needs_stored_row(&self) -> bool {
        self.identity.is_some() || self.role.is_some()
    }
}

pub(super) fn node_requirements<'a>(
    catalog: &'a (impl QueryDataModel + ?Sized),
    input: &Input,
    node: &InputNode,
) -> Result<NodeRequirements<'a>> {
    let entity = node
        .entity
        .as_deref()
        .and_then(|name| catalog.entity(name))
        .ok_or(GraphError::MissingOutput)?;
    let table = catalog
        .entity_table(&entity.name)
        .and_then(|name| catalog.stored_table(name))
        .ok_or(GraphError::MissingOutput)?;
    let returned = match input.query_type {
        QueryType::Traversal | QueryType::Neighbors => true,
        QueryType::Aggregation => {
            node_group_ids(&input.aggregation.group_by).any(|alias| alias == node.id)
        }
        QueryType::PathFinding | QueryType::Hydration => false,
    };
    let identity = if returned {
        match catalog.redaction_id_column(entity.id) {
            Some(name) if name != "id" && edge_identity(catalog, input, node, name).is_none() => {
                Some(table.column(name).ok_or(GraphError::MissingOutput)?)
            }
            _ => None,
        }
    } else {
        None
    };
    let role = match catalog.entity_minimum_access_level(&entity.name) {
        Some(level) if level > crate::types::DEFAULT_PATH_ACCESS_LEVEL => Some((
            table
                .column(ontology::TRAVERSAL_PATH_COLUMN)
                .ok_or(GraphError::MissingOutput)?,
            level,
        )),
        _ => None,
    };
    Ok(NodeRequirements { identity, role })
}

fn edge_identity<'a>(
    catalog: &'a (impl QueryDataModel + ?Sized),
    input: &Input,
    node: &InputNode,
    name: &str,
) -> Option<(usize, StoredColumnRef<'a>)> {
    input
        .relationships
        .iter()
        .enumerate()
        .find_map(|(index, relationship)| {
            if relationship.hops.max != 1
                || relationship.direction == Direction::Both
                || (relationship.from != node.id && relationship.to != node.id)
                || relationship.types.is_any()
                || relationship.types.is_empty()
            {
                return None;
            }
            let from = input
                .nodes
                .iter()
                .find(|node| node.id == relationship.from)?
                .entity
                .as_deref()?;
            let to = input
                .nodes
                .iter()
                .find(|node| node.id == relationship.to)?
                .entity
                .as_deref()?;
            let (source, target) = if relationship.direction == Direction::Incoming {
                (to, from)
            } else {
                (from, to)
            };
            if !relationship.types.iter().all(|kind| {
                catalog
                    .variant_scope(kind, source, target)
                    .is_some_and(ontology::EdgeVariantScope::is_scope_preserving)
            }) {
                return None;
            }
            let table = catalog.relationship_table_for_query(relationship.types.as_slice());
            if ontology::EDGE_RESERVED_COLUMNS.contains(&name) {
                return None;
            }
            let column = catalog.stored_table(table)?.column(name)?;
            let node_table =
                catalog.stored_table(catalog.entity_table(node.entity.as_deref()?)?)?;
            let node_column = node_table.column(name)?;
            if node_column.data_type() != column.data_type()
                || node_column.is_array() != column.is_array()
            {
                return None;
            }
            Some((index, column))
        })
}

impl<'a, M: QueryDataModel + ?Sized, L> QueryGraph<'a, M, L> {
    pub fn authorization_identity(
        &self,
        root: BlockId,
        input: &Input,
        index: usize,
    ) -> Result<ColumnRef<'a>> {
        let node = input.nodes.get(index).ok_or(GraphError::MissingOutput)?;
        let entity = node
            .entity
            .as_deref()
            .and_then(|name| self.catalog.entity(name))
            .ok_or(GraphError::MissingOutput)?;
        let name = self.catalog.redaction_id_column(entity.id).unwrap_or("id");
        if name == "id" {
            return self.input_identity(root, input, index);
        }
        if let Ok(relation) = self.input_node(root, index) {
            return self.stored_column(relation, name);
        }
        let (relationship, column) =
            edge_identity(self.catalog, input, node, name).ok_or(GraphError::MissingOutput)?;
        let relation = self
            .relations(root)?
            .find(|relation| {
                self.relation(*relation).is_ok_and(|relation| {
                    relation.input == Some(ScanInput::Relationship(relationship))
                })
            })
            .ok_or(GraphError::MissingOutput)?;
        self.stored_port(relation, column)
    }
}
