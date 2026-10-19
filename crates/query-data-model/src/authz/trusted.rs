use std::collections::HashMap;

use super::gitlab::{EntityAuthConfig, GitLabPolicy};
use super::{PropertyPolicy, derive_property_policy};
use crate::{DataModelError, EntityId, GraphCatalog, PropertyId, RelationshipVariantId};

#[derive(Debug)]
pub struct TrustedLocalCatalog {
    properties: Vec<PropertyPolicy>,
}

impl TrustedLocalCatalog {
    pub(crate) fn from_ontology(
        ontology: &ontology::Ontology,
        graph: &GraphCatalog,
    ) -> Result<Self, DataModelError> {
        Ok(Self {
            properties: derive_property_policy(ontology, graph)?,
        })
    }
}

impl GitLabPolicy for TrustedLocalCatalog {
    fn is_hidden(&self, _property: PropertyId) -> bool {
        false
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
