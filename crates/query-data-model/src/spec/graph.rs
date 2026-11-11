use std::collections::BTreeMap;

use indexmap::IndexMap;
use serde::{Deserialize, Serialize};

use super::{Naming, Raw, Resolved, Short, ShortForm, is_false};
use crate::generic::{AttributeId, Style};
use crate::types::Cardinality;
use crate::{
    EntityId, EnumType, FieldSelectivity, PropertyId, RelationshipId, RelationshipVariantId,
};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GraphFragment {
    pub package: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub domain: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub domain_description: Option<String>,
    #[serde(default, skip_serializing_if = "IndexMap::is_empty")]
    pub entities: IndexMap<String, Entity<Raw>>,
    #[serde(default, skip_serializing_if = "IndexMap::is_empty")]
    pub relationships: IndexMap<String, Relationship<Raw>>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, bound = "")]
pub struct Entity<S: Naming = Resolved> {
    #[serde(skip)]
    pub id: S::Loaded<EntityId>,
    #[serde(skip)]
    pub name: S::Loaded<String>,
    #[serde(skip)]
    pub package: S::Loaded<String>,
    #[serde(skip)]
    pub domain: S::Loaded<Option<String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    pub label: String,
    #[serde(default)]
    pub defaults: Vec<S::Property>,
    #[serde(default)]
    pub style: Style,
    pub properties: S::Members<Short<Property<S>>, PropertyId>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, bound = "")]
pub struct Property<S: Naming = Resolved> {
    #[serde(skip)]
    pub id: S::Loaded<PropertyId>,
    #[serde(skip)]
    pub entity: S::Loaded<EntityId>,
    #[serde(skip)]
    pub name: S::Loaded<String>,
    #[serde(rename = "type")]
    pub data_type: S::Type,
    #[serde(skip)]
    pub nullable: S::Loaded<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(default, skip_serializing_if = "is_false")]
    pub unique: bool,
    #[serde(default, rename = "values", skip_serializing_if = "Option::is_none")]
    pub enum_values: Option<BTreeMap<i64, String>>,
    #[serde(default, rename = "stored_as_text", with = "stored_as_text")]
    pub enum_type: EnumType,
    #[serde(default, rename = "terminal", skip_serializing_if = "Vec::is_empty")]
    pub terminal_values: Vec<String>,
    pub selectivity: S::Defaulted<FieldSelectivity>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub default: Option<DefaultValue>,
}

/// The value a row takes when its source omits the property.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum DefaultValue {
    Bool(bool),
    Int(i64),
    Float(f64),
    String(String),
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, bound = "")]
pub struct Relationship<S: Naming = Resolved> {
    #[serde(skip)]
    pub id: S::Loaded<RelationshipId>,
    #[serde(skip)]
    pub name: S::Loaded<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    pub variants: S::Members<RelationshipVariant<S>, RelationshipVariantId>,
    #[serde(default)]
    pub attributes: S::Members<Short<Attribute<S>>, AttributeId>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, bound = "")]
pub struct RelationshipVariant<S: Naming = Resolved> {
    #[serde(skip)]
    pub id: S::Loaded<RelationshipVariantId>,
    #[serde(skip)]
    pub relationship: S::Loaded<RelationshipId>,
    #[serde(skip)]
    pub source: S::Loaded<EntityId>,
    #[serde(skip)]
    pub target: S::Loaded<EntityId>,
    pub cardinality: Cardinality,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, bound = "")]
pub struct Attribute<S: Naming = Resolved> {
    #[serde(skip)]
    pub id: S::Loaded<AttributeId>,
    #[serde(skip)]
    pub relationship: S::Loaded<RelationshipId>,
    #[serde(skip)]
    pub name: S::Loaded<String>,
    #[serde(rename = "type")]
    pub data_type: S::Type,
    #[serde(skip)]
    pub nullable: S::Loaded<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
}

impl<S: Naming> ShortForm for Property<S> {
    const KEY: &'static str = "type";
}

impl<S: Naming> ShortForm for Attribute<S> {
    const KEY: &'static str = "type";
}

impl RelationshipVariant {
    pub fn endpoint(&self, side: crate::Endpoint) -> EntityId {
        match side {
            crate::Endpoint::Source => self.source,
            crate::Endpoint::Target => self.target,
        }
    }
}

impl DefaultValue {
    /// The value as a SQL literal.
    pub fn literal(&self) -> String {
        match self {
            Self::Bool(value) => value.to_string(),
            Self::Int(value) => value.to_string(),
            Self::Float(value) => value.to_string(),
            Self::String(value) => format!("'{}'", value.replace('\'', "''")),
        }
    }
}

mod stored_as_text {
    use serde::{Deserialize, Deserializer, Serializer};

    use crate::EnumType;

    pub fn serialize<S: Serializer>(kind: &EnumType, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_bool(*kind == EnumType::String)
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<EnumType, D::Error> {
        Ok(match bool::deserialize(deserializer)? {
            true => EnumType::String,
            false => EnumType::Int,
        })
    }
}
