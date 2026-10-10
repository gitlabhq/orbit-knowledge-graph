use std::collections::HashMap;

use ontology::constants::DEFAULT_PRIMARY_KEY;

use crate::{
    DataModelError, EntityId, GraphCatalog, PropertyId, QueryAuthorizationCatalog,
    RelationshipVariantId,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EntityAuthConfig {
    pub resource_type: String,
    pub ability: String,
    pub auth_id_column: String,
    pub owner_entity: Option<String>,
    pub required_access_level: u32,
}

#[derive(Debug)]
pub struct EntityAuthorization {
    pub resource_type: String,
    pub ability: String,
    pub id_column: String,
    pub owner_entity: Option<EntityId>,
    pub required_access_level: u32,
}

#[derive(Debug, Clone)]
struct PropertyPolicy {
    admin_only: bool,
    filterable: bool,
    like_allowed: bool,
    hidden: bool,
}

#[derive(Debug)]
pub struct GitLabAuthzCatalog {
    entities: Vec<Option<EntityAuthorization>>,
    entity_auth: HashMap<String, EntityAuthConfig>,
    properties: Vec<PropertyPolicy>,
    variant_scopes: Vec<Option<ontology::EdgeVariantScope>>,
    anchor_foreign_keys: HashMap<String, EntityId>,
}

impl GitLabAuthzCatalog {
    pub fn entity(&self, id: EntityId) -> Option<&EntityAuthorization> {
        self.entities.get(id.index())?.as_ref()
    }

    pub fn is_admin_only(&self, id: PropertyId) -> bool {
        self.properties
            .get(id.index())
            .is_some_and(|policy| policy.admin_only)
    }

    pub fn variant_scope(&self, id: RelationshipVariantId) -> Option<ontology::EdgeVariantScope> {
        *self.variant_scopes.get(id.index())?
    }

    pub fn entities(&self) -> impl Iterator<Item = (EntityId, &EntityAuthorization)> {
        self.entities
            .iter()
            .enumerate()
            .filter_map(|(id, policy)| Some((EntityId(id), policy.as_ref()?)))
    }

    pub fn entity_auth(&self) -> &HashMap<String, EntityAuthConfig> {
        &self.entity_auth
    }
}

impl QueryAuthorizationCatalog for GitLabAuthzCatalog {
    fn derive(ontology: &ontology::Ontology, graph: &GraphCatalog) -> Result<Self, DataModelError> {
        Self::from_ontology(ontology, graph)
    }

    fn variant_scope(&self, variant: RelationshipVariantId) -> Option<ontology::EdgeVariantScope> {
        GitLabAuthzCatalog::variant_scope(self, variant)
    }

    fn anchor_foreign_keys(&self) -> &HashMap<String, EntityId> {
        &self.anchor_foreign_keys
    }

    fn is_admin_only(&self, property: PropertyId) -> bool {
        GitLabAuthzCatalog::is_admin_only(self, property)
    }

    fn is_filterable(&self, property: PropertyId) -> bool {
        self.properties
            .get(property.index())
            .is_some_and(|policy| policy.filterable)
    }

    fn is_like_allowed(&self, property: PropertyId) -> bool {
        self.properties
            .get(property.index())
            .is_some_and(|policy| policy.like_allowed)
    }

    fn is_hidden(&self, property: PropertyId) -> bool {
        self.properties
            .get(property.index())
            .is_some_and(|policy| policy.hidden)
    }

    fn entity_auth(&self) -> &HashMap<String, EntityAuthConfig> {
        GitLabAuthzCatalog::entity_auth(self)
    }

    fn redaction_id_column(&self, entity: EntityId) -> Option<&str> {
        self.entity(entity).map(|policy| policy.id_column.as_str())
    }

    fn required_access_level(&self, entity: EntityId) -> Option<u32> {
        self.entity(entity)
            .map(|policy| policy.required_access_level)
    }
}

impl GitLabAuthzCatalog {
    fn from_ontology(
        ontology: &ontology::Ontology,
        graph: &GraphCatalog,
    ) -> Result<Self, DataModelError> {
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

        let mut entities = std::iter::repeat_with(|| None)
            .take(graph.entities().count())
            .collect::<Vec<_>>();
        let mut entity_auth = HashMap::new();
        let properties = derive_property_policy(ontology, graph)?;
        for node in ontology.nodes() {
            let entity_id =
                graph
                    .entity_id(&node.name)
                    .ok_or_else(|| DataModelError::UnknownReference {
                        kind: "entity",
                        name: node.name.clone(),
                    })?;
            if let Some(redaction) = &node.redaction {
                entities[entity_id.index()] = Some(EntityAuthorization {
                    resource_type: redaction.resource_type.clone(),
                    ability: redaction.ability.clone(),
                    id_column: redaction.id_column.clone(),
                    owner_entity: (redaction.id_column != DEFAULT_PRIMARY_KEY)
                        .then(|| owners.get(redaction.resource_type.as_str()).copied())
                        .flatten(),
                    required_access_level: redaction.required_role.as_access_level(),
                });
                entity_auth.insert(
                    node.name.clone(),
                    EntityAuthConfig {
                        resource_type: redaction.resource_type.clone(),
                        ability: redaction.ability.clone(),
                        auth_id_column: redaction.id_column.clone(),
                        owner_entity: (redaction.id_column != DEFAULT_PRIMARY_KEY)
                            .then(|| owners.get(redaction.resource_type.as_str()).copied())
                            .flatten()
                            .map(|owner| graph.entity(owner).name.clone()),
                        required_access_level: redaction.required_role.as_access_level(),
                    },
                );
            }
        }

        let mut variant_scopes = vec![None; graph.variants().count()];
        let mut anchor_foreign_keys = HashMap::new();
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
                variant_scopes[variant.index()] = Some(scope);
            }
            if scope == ontology::EdgeVariantScope::NamespaceAnchor
                && let Some(column) = &edge.fk_column
            {
                anchor_foreign_keys.entry(column.clone()).or_insert(target);
            }
        }

        Ok(GitLabAuthzCatalog {
            entities,
            entity_auth,
            properties,
            variant_scopes,
            anchor_foreign_keys,
        })
    }
}

#[derive(Debug)]
pub struct TrustedLocalCatalog {
    properties: Vec<PropertyPolicy>,
}

impl QueryAuthorizationCatalog for TrustedLocalCatalog {
    fn derive(ontology: &ontology::Ontology, graph: &GraphCatalog) -> Result<Self, DataModelError> {
        Ok(Self {
            properties: derive_property_policy(ontology, graph)?,
        })
    }

    fn variant_scope(&self, _variant: RelationshipVariantId) -> Option<ontology::EdgeVariantScope> {
        None
    }

    fn anchor_foreign_keys(&self) -> &HashMap<String, EntityId> {
        static EMPTY: std::sync::LazyLock<HashMap<String, EntityId>> =
            std::sync::LazyLock::new(HashMap::new);
        &EMPTY
    }

    fn is_admin_only(&self, _property: PropertyId) -> bool {
        false
    }

    fn is_filterable(&self, property: PropertyId) -> bool {
        self.properties
            .get(property.index())
            .is_some_and(|policy| policy.filterable)
    }

    fn is_like_allowed(&self, property: PropertyId) -> bool {
        self.properties
            .get(property.index())
            .is_some_and(|policy| policy.like_allowed)
    }

    fn is_hidden(&self, _property: PropertyId) -> bool {
        false
    }

    fn entity_auth(&self) -> &HashMap<String, EntityAuthConfig> {
        static EMPTY: std::sync::LazyLock<HashMap<String, EntityAuthConfig>> =
            std::sync::LazyLock::new(HashMap::new);
        &EMPTY
    }

    fn redaction_id_column(&self, _entity: EntityId) -> Option<&str> {
        None
    }

    fn required_access_level(&self, _entity: EntityId) -> Option<u32> {
        None
    }
}

fn derive_property_policy(
    ontology: &ontology::Ontology,
    graph: &GraphCatalog,
) -> Result<Vec<PropertyPolicy>, DataModelError> {
    let mut properties = vec![
        PropertyPolicy {
            admin_only: false,
            filterable: true,
            like_allowed: true,
            hidden: false,
        };
        graph.properties().count()
    ];
    for node in ontology.nodes() {
        let entity =
            graph
                .entity_id(&node.name)
                .ok_or_else(|| DataModelError::UnknownReference {
                    kind: "entity",
                    name: node.name.clone(),
                })?;
        for field in &node.fields {
            let property = graph.property_id(entity, &field.name).ok_or_else(|| {
                DataModelError::UnknownReference {
                    kind: "property",
                    name: format!("{}.{}", node.name, field.name),
                }
            })?;
            properties[property.index()] = PropertyPolicy {
                admin_only: field.admin_only,
                filterable: field.filterable,
                like_allowed: field.like_allowed,
                hidden: field.hidden,
            };
        }
    }
    Ok(properties)
}
