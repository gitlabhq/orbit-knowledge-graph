use std::collections::BTreeMap;

use indexmap::IndexMap;
use serde::{Deserialize, Serialize};

use super::is_false;
use crate::{FieldSelectivity, Style};

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GraphFragment {
    pub package: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub domain: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub domain_description: Option<String>,
    #[serde(default, skip_serializing_if = "IndexMap::is_empty")]
    pub entities: IndexMap<String, EntitySpec>,
    #[serde(default, skip_serializing_if = "IndexMap::is_empty")]
    pub relationships: IndexMap<String, RelationshipSpec>,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EntitySpec {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub label: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub defaults: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub style: Option<Style>,
    pub properties: IndexMap<String, PropertySpec>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum PropertySpec {
    Type(String),
    Detail(PropertyDetail),
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PropertyDetail {
    #[serde(rename = "type")]
    pub data_type: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(default, skip_serializing_if = "is_false")]
    pub immutable: bool,
    #[serde(default, skip_serializing_if = "is_false")]
    pub unique: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub values: Option<BTreeMap<i64, String>>,
    #[serde(default, skip_serializing_if = "is_false")]
    pub stored_as_text: bool,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub terminal: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub references: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub selectivity: Option<FieldSelectivity>,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RelationshipSpec {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    pub variants: Vec<String>,
    #[serde(default, skip_serializing_if = "IndexMap::is_empty")]
    pub attributes: IndexMap<String, AttributeSpec>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum AttributeSpec {
    Type(String),
    Detail(AttributeDetail),
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AttributeDetail {
    #[serde(rename = "type")]
    pub data_type: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
}

impl PropertySpec {
    pub fn data_type(&self) -> &str {
        match self {
            Self::Type(data_type) => data_type,
            Self::Detail(detail) => &detail.data_type,
        }
    }

    pub fn detail(&self) -> Option<&PropertyDetail> {
        match self {
            Self::Type(_) => None,
            Self::Detail(detail) => Some(detail),
        }
    }
}

impl AttributeSpec {
    pub fn data_type(&self) -> &str {
        match self {
            Self::Type(data_type) => data_type,
            Self::Detail(detail) => &detail.data_type,
        }
    }

    pub fn description(&self) -> Option<&str> {
        match self {
            Self::Type(_) => None,
            Self::Detail(detail) => detail.description.as_deref(),
        }
    }
}
