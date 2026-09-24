use crate::input::{Direction, Input, InputNode};
use crate::passes::logical_v3::{column, ColumnRef, Expr, LogicalPlan, LogicalRelationSource, RelationId};
use ontology::constants::{
    DEFAULT_PRIMARY_KEY, RELATIONSHIP_KIND_COLUMN, SOURCE_ID_COLUMN, SOURCE_KIND_COLUMN,
    TARGET_ID_COLUMN, TARGET_KIND_COLUMN,
};
use ontology::Ontology;
use std::collections::BTreeMap;

pub struct PhysicalCatalog<'a> {
    input: &'a Input,
    ontology: &'a Ontology,
    relations: BTreeMap<RelationId, Relation<'a>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DedupRequirement {
    None,
    Final,
    LimitBy,
}

pub enum Relation<'a> {
    Node {
        input: &'a InputNode,
        table: String,
        sort_key: Vec<String>,
    },
    Edge {
        input_index: Option<usize>,
        relationships: Vec<String>,
        tables: Vec<String>,
    },
}

pub struct ForeignKey<'a> {
    pub edge: RelationId,
    pub variant: &'a ontology::EdgeEntity,
    pub holder: RelationId,
    pub referenced: RelationId,
}

pub struct DenormalizedFilter {
    pub edge: RelationId,
    pub column: String,
    pub token: String,
}

pub struct ScopeAnchor<'a> {
    pub relation: RelationId,
    pub edge: RelationId,
    pub prefix: &'a crate::ScopePrefix,
}

impl<'a> PhysicalCatalog<'a> {
    pub fn new(logical: &LogicalPlan, input: &'a Input, ontology: &'a Ontology) -> Self {
        let relations = logical
            .relations
            .iter()
            .map(|(id, relation)| {
                let relation = match relation.source {
                    LogicalRelationSource::Node(index) => {
                        let input = &input.nodes[index];
                        let table = input
                            .entity
                            .as_deref()
                            .and_then(|entity| ontology.table_name(entity).ok())
                            .unwrap_or_default()
                            .to_string();
                        let sort_key = ontology
                            .sort_key_for_table(&table)
                            .unwrap_or_default()
                            .to_vec();
                        Relation::Node {
                            input,
                            table,
                            sort_key,
                        }
                    }
                    LogicalRelationSource::Edge(index) => {
                        let relationships = index
                            .map(|index| input.relationships[index].types.clone())
                            .unwrap_or_else(|| {
                                let config = input.neighbors.as_ref();
                                config.map(|config| config.rel_types.clone()).unwrap_or_else(|| {
                                    input
                                        .path
                                        .as_ref()
                                        .map(|path| path.rel_types.clone())
                                        .unwrap_or_default()
                                })
                            });
                        let mut tables: Vec<_> = if relationships.is_empty()
                            || relationships.iter().any(|relationship| relationship == "*")
                        {
                            ontology.edge_tables().into_iter().map(str::to_string).collect()
                        } else {
                            relationships
                                .iter()
                                .map(|relationship| {
                                    ontology
                                        .edge_table_for_relationship(relationship)
                                        .to_string()
                                })
                                .collect()
                        };
                        tables.sort();
                        tables.dedup();
                        Relation::Edge {
                            input_index: index,
                            relationships,
                            tables,
                        }
                    }
                };
                (*id, relation)
            })
            .collect();
        Self {
            input,
            ontology,
            relations,
        }
    }

    pub fn relation(&self, id: RelationId) -> Option<&Relation<'a>> {
        self.relations.get(&id)
    }

    pub fn node(&self, id: RelationId) -> Option<&'a InputNode> {
        let Relation::Node { input, .. } = self.relation(id)? else {
            return None;
        };
        Some(*input)
    }

    pub fn node_table(&self, id: RelationId) -> Option<&str> {
        let Relation::Node { table, .. } = self.relation(id)? else {
            return None;
        };
        Some(table)
    }

    pub fn sort_key(&self, id: RelationId) -> &[String] {
        match self.relation(id) {
            Some(Relation::Node { sort_key, .. }) => sort_key,
            _ => &[],
        }
    }

    pub fn edge_tables(&self, id: RelationId) -> &[String] {
        match self.relation(id) {
            Some(Relation::Edge { tables, .. }) => tables,
            _ => &[],
        }
    }

    pub fn dedup(&self, relation: RelationId, edge_count: usize) -> DedupRequirement {
        match self.relation(relation) {
            Some(Relation::Node { .. }) if self.input.query_type == crate::input::QueryType::Hydration => {
                DedupRequirement::LimitBy
            }
            Some(Relation::Node { .. }) => DedupRequirement::Final,
            Some(Relation::Edge { .. })
                if self.input.query_type == crate::input::QueryType::Aggregation
                    && edge_count == 1 => DedupRequirement::LimitBy,
            Some(Relation::Edge { .. }) if edge_count > 1 => DedupRequirement::Final,
            _ => DedupRequirement::None,
        }
    }

    pub fn selective(&self, relation: RelationId) -> bool {
        match self.relation(relation) {
            Some(Relation::Node { input, .. }) => {
                !input.node_ids.is_empty() || input.id_range.is_some() || !input.filters.is_empty()
            }
            Some(Relation::Edge {
                input_index: Some(index),
                ..
            }) => {
                let edge = &self.input.relationships[*index];
                !edge.filters.is_empty()
                    || [&edge.from, &edge.to].into_iter().any(|alias| {
                        self.input.nodes.iter().find(|node| node.id == **alias).is_some_and(
                            |node| {
                                !node.node_ids.is_empty()
                                    || node.id_range.is_some()
                                    || !node.filters.is_empty()
                            },
                        )
                    })
            }
            _ => false,
        }
    }

    pub fn text_index(&self, column: &ColumnRef) -> Option<&str> {
        let table = self.node_table(column.relation)?;
        self.input
            .compiler
            .text_indexes
            .get(&(table.to_string(), column.name.clone()))
            .map(|index| index.tokenizer.as_str())
    }

    pub fn node_sources<B: crate::passes::physical_v3::Backend>(
        &self,
        plan: &crate::passes::physical_v3::PhysicalPlan<B>,
    ) -> std::collections::HashMap<String, (String, String)> {
        let visible = plan.visible_relations();
        self.relations
            .iter()
            .filter_map(|(relation, physical)| {
                let Relation::Node { input, .. } = physical else {
                    return None;
                };
                visible.contains(relation).then(|| {
                    (
                        input.id.clone(),
                        (
                            crate::passes::physical_v3::relation_alias(*relation),
                            DEFAULT_PRIMARY_KEY.into(),
                        ),
                    )
                })
            })
            .collect()
    }

    pub fn denormalized_filters(&self, edge: RelationId) -> Vec<DenormalizedFilter> {
        let Some(Relation::Edge {
            input_index: Some(index),
            relationships,
            ..
        }) = self.relation(edge)
        else {
            return Vec::new();
        };
        let relationship = &self.input.relationships[*index];
        let (source, target) = (self.node_id(&relationship.from), self.node_id(&relationship.to));
        [(source, ontology::DenormDirection::Source), (target, ontology::DenormDirection::Target)]
            .into_iter()
            .flat_map(|(node, direction)| {
                let Some(node) = node.and_then(|node| self.node(node)) else {
                    return Vec::new();
                };
                let Some(entity) = node.entity.as_deref() else {
                    return Vec::new();
                };
                node.filters
                    .iter()
                    .flat_map(|(property_name, filters)| {
                        let direction = direction.clone();
                        filters.iter().filter_map(move |filter| {
                            let denormalized = self.ontology.denormalized_properties().iter().find(
                                |property| {
                                    property.node_kind == entity
                                        && property.property_name == *property_name
                                        && property.direction == direction
                                        && relationships.contains(&property.relationship_kind)
                                },
                            )?;
                            let value = filter.value.as_ref()?;
                            let value = value
                                .as_str()
                                .map(str::to_string)
                                .unwrap_or_else(|| value.to_string());
                            Some(DenormalizedFilter {
                                edge,
                                column: denormalized.edge_column.clone(),
                                token: format!("{}:{value}", denormalized.tag_key),
                            })
                        })
                    })
                    .collect::<Vec<_>>()
            })
            .collect()
    }

    pub fn scope_anchor(&self) -> Option<ScopeAnchor<'a>> {
        if self.input.query_type != crate::input::QueryType::Aggregation {
            return None;
        }
        self.input.relationships.iter().enumerate().find_map(|(index, edge)| {
            let prefix = edge.scope_prefix.as_ref()?;
            if !edge.scope_preserving || !edge.filters.is_empty() {
                return None;
            }
            let relation = [&edge.from, &edge.to].into_iter().find_map(|alias| {
                let relation = self.node_id(alias)?;
                crate::scope::is_scope_only(self.node(relation)?).then_some(relation)
            })?;
            let edge_relation = self.relations.iter().find_map(|(id, relation)| {
                matches!(relation, Relation::Edge { input_index: Some(i), .. } if *i == index)
                    .then_some(*id)
            })?;
            Some(ScopeAnchor {
                relation,
                edge: edge_relation,
                prefix,
            })
        })
    }

    pub fn foreign_key(&self, edge: RelationId) -> Option<ForeignKey<'a>> {
        let Relation::Edge {
            input_index: Some(index),
            relationships,
            ..
        } = self.relation(edge)?
        else {
            return None;
        };
        let input = &self.input.relationships[*index];
        if input.hops.max > 1 || input.direction == Direction::Both || !input.filters.is_empty() {
            return None;
        }
        let from = self.node_id(&input.from)?;
        let to = self.node_id(&input.to)?;
        let from_entity = self.node(from)?.entity.as_deref()?;
        let to_entity = self.node(to)?.entity.as_deref()?;
        let mut variants = relationships.iter().map(|relationship| {
            self.ontology.get_edge(relationship)?.iter().find(|variant| {
                variant.source_kind == from_entity && variant.target_kind == to_entity
            })
        });
        let first = variants.next()??;
        let foreign_key = first.fk_column.as_deref()?;
        if variants.any(|variant| {
            variant.is_none_or(|variant| variant.fk_column.as_deref() != Some(foreign_key))
        }) {
            return None;
        }
        let from_has_key = self
            .ontology
            .get_node(from_entity)
            .is_some_and(|node| node.storage.columns.iter().any(|column| column.name == foreign_key));
        let (holder, referenced) = if from_has_key { (from, to) } else { (to, from) };
        Some(ForeignKey {
            edge,
            variant: first,
            holder,
            referenced,
        })
    }

    pub fn foreign_keys(
        &self,
        relations: impl IntoIterator<Item = RelationId>,
    ) -> Option<Vec<ForeignKey<'a>>> {
        relations
            .into_iter()
            .filter(|relation| matches!(self.relation(*relation), Some(Relation::Edge { input_index: Some(_), .. })))
            .map(|relation| self.foreign_key(relation))
            .collect::<Option<Vec<_>>>()
            .filter(|keys| !keys.is_empty())
    }

    fn node_id(&self, alias: &str) -> Option<RelationId> {
        self.relations.iter().find_map(|(id, relation)| match relation {
            Relation::Node { input, .. } if input.id == alias => Some(*id),
            _ => None,
        })
    }
}

impl ForeignKey<'_> {
    pub fn condition(&self) -> Expr {
        column(self.holder, self.variant.fk_column.as_deref().unwrap())
            .eq(column(self.referenced, DEFAULT_PRIMARY_KEY))
    }

    pub fn substitutions(&self, catalog: &PhysicalCatalog<'_>) -> Vec<(ColumnRef, Expr)> {
        let source = catalog.node_id(&self.variant.source).unwrap_or(self.holder);
        let target = catalog.node_id(&self.variant.target).unwrap_or(self.referenced);
        [
            (SOURCE_ID_COLUMN, column(source, DEFAULT_PRIMARY_KEY)),
            (TARGET_ID_COLUMN, column(target, DEFAULT_PRIMARY_KEY)),
            (
                SOURCE_KIND_COLUMN,
                Expr::Literal(self.variant.source_kind.clone().into()),
            ),
            (
                TARGET_KIND_COLUMN,
                Expr::Literal(self.variant.target_kind.clone().into()),
            ),
            (
                RELATIONSHIP_KIND_COLUMN,
                Expr::Literal(self.variant.relationship_kind.clone().into()),
            ),
        ]
        .into_iter()
        .map(|(name, expression)| {
            (
                ColumnRef {
                    relation: self.edge,
                    name: name.into(),
                },
                expression,
            )
        })
        .collect()
    }
}
