use std::collections::HashMap;

use ontology::constants::DEFAULT_PRIMARY_KEY;

use crate::{Authz, DataModelError, EntityId, GraphCatalog, PropertyId, RelationshipVariantId};

#[derive(Debug)]
pub struct EntityAuthorization {
    pub resource_type: String,
    pub ability: String,
    pub id_property: PropertyId,
    pub owner_entity: Option<EntityId>,
    pub required_access_level: u32,
    pub global: bool,
}

#[derive(Debug)]
pub struct GitLabAuthzCatalog {
    entities: HashMap<EntityId, EntityAuthorization>,
    admin_only: HashMap<PropertyId, bool>,
    variant_scopes: HashMap<RelationshipVariantId, ontology::EdgeVariantScope>,
}

impl GitLabAuthzCatalog {
    pub fn entity(&self, id: EntityId) -> Option<&EntityAuthorization> {
        self.entities.get(&id)
    }

    pub fn is_admin_only(&self, id: PropertyId) -> bool {
        self.admin_only.get(&id).copied().unwrap_or(false)
    }

    pub fn variant_scope(&self, id: RelationshipVariantId) -> Option<ontology::EdgeVariantScope> {
        self.variant_scopes.get(&id).copied()
    }

    pub fn entities(&self) -> impl Iterator<Item = (EntityId, &EntityAuthorization)> {
        self.entities.iter().map(|(id, policy)| (*id, policy))
    }
}

pub struct GitLabAuthz;

impl Authz for GitLabAuthz {
    type Catalog = GitLabAuthzCatalog;

    fn derive(
        ontology: &ontology::Ontology,
        graph: &GraphCatalog,
    ) -> Result<Self::Catalog, DataModelError> {
        let owners: HashMap<&str, EntityId> = ontology
            .nodes()
            .filter_map(|node| {
                let redaction = node.redaction.as_ref()?;
                (redaction.id_column == DEFAULT_PRIMARY_KEY).then(|| {
                    graph
                        .entity_id(&node.name)
                        .map(|id| (redaction.resource_type.as_str(), id))
                })?
            })
            .collect();

        let mut entities = HashMap::new();
        let mut admin_only = HashMap::new();
        for node in ontology.nodes() {
            let entity_id =
                graph
                    .entity_id(&node.name)
                    .ok_or_else(|| DataModelError::UnknownReference {
                        kind: "entity",
                        name: node.name.clone(),
                    })?;
            for field in &node.fields {
                if field.admin_only {
                    let property_id =
                        graph.property_id(entity_id, &field.name).ok_or_else(|| {
                            DataModelError::UnknownReference {
                                kind: "property",
                                name: format!("{}.{}", node.name, field.name),
                            }
                        })?;
                    admin_only.insert(property_id, true);
                }
            }
            if let Some(redaction) = &node.redaction
                && let Some(id_property) = graph.property_id(entity_id, &redaction.id_column)
            {
                entities.insert(
                    entity_id,
                    EntityAuthorization {
                        resource_type: redaction.resource_type.clone(),
                        ability: redaction.ability.clone(),
                        id_property,
                        owner_entity: (redaction.id_column != DEFAULT_PRIMARY_KEY)
                            .then(|| owners.get(redaction.resource_type.as_str()).copied())
                            .flatten(),
                        required_access_level: redaction.required_role.as_access_level(),
                        global: node.global,
                    },
                );
            }
        }

        let mut variant_scopes = HashMap::new();
        for edge in ontology.edges() {
            let Some(scope) = edge.scope else {
                continue;
            };
            let relationship = graph
                .relationship_id(&edge.relationship_kind)
                .ok_or_else(|| DataModelError::UnknownReference {
                    kind: "relationship",
                    name: edge.relationship_kind.clone(),
                })?;
            let source = graph.entity_id(&edge.source_kind).ok_or_else(|| {
                DataModelError::UnknownReference {
                    kind: "source entity",
                    name: edge.source_kind.clone(),
                }
            })?;
            let target = graph.entity_id(&edge.target_kind).ok_or_else(|| {
                DataModelError::UnknownReference {
                    kind: "target entity",
                    name: edge.target_kind.clone(),
                }
            })?;
            if let Some(variant) = graph.variant_id(relationship, source, target) {
                variant_scopes.insert(variant, scope);
            }
        }

        Ok(GitLabAuthzCatalog {
            entities,
            admin_only,
            variant_scopes,
        })
    }
}

#[derive(Debug, Default)]
pub struct TrustedLocalCatalog;

pub struct TrustedLocal;

impl Authz for TrustedLocal {
    type Catalog = TrustedLocalCatalog;

    fn derive(
        _ontology: &ontology::Ontology,
        _graph: &GraphCatalog,
    ) -> Result<Self::Catalog, DataModelError> {
        Ok(TrustedLocalCatalog)
    }
}
