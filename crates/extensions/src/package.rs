//! The package document of the `v1alpha1` contract: its typed model, JSON Schema
//! validation, and the load rules that one document must satisfy on its own.
//!
//! Cross-package rules (an entity declared once across all packages and the core
//! model, a relationship that points only at known entities) live in [`crate::loader`],
//! because they need every loaded package and the core model.

use std::collections::BTreeMap;
use std::sync::LazyLock;

use serde::Deserialize;
use sha2::{Digest, Sha256};

pub const API_VERSION: &str = "orbit.gitlab.com/extensions/v1alpha1";
pub const KIND: &str = "Package";

/// The one property every entity must declare. It is the entity id, and the
/// graph stores it as `Int64`.
pub const ID_PROPERTY: &str = "id";

const PACKAGE_SCHEMA_JSON: &str = include_str!("../schema/package.v1alpha1.json");

static PACKAGE_SCHEMA: LazyLock<jsonschema::Validator> = LazyLock::new(|| {
    let schema: serde_json::Value =
        serde_json::from_str(PACKAGE_SCHEMA_JSON).expect("package schema is valid JSON");
    jsonschema::validator_for(&schema).expect("package schema compiles")
});

#[derive(Debug, thiserror::Error)]
#[error("package rejected ({}):\n  - {}", .name, .errors.join("\n  - "))]
pub struct PackageError {
    pub name: String,
    pub errors: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct Package {
    pub digest: String,
    pub document_yaml: String,
    pub document: PackageDocument,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PackageDocument {
    #[serde(rename = "apiVersion")]
    pub api_version: String,
    pub kind: String,
    pub metadata: Metadata,
    pub graph: Graph,
    pub authorization: Authorization,
    pub storage: Storage,
    pub sources: Sources,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Metadata {
    pub name: String,
    pub version: String,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Graph {
    pub entities: BTreeMap<String, Entity>,
    #[serde(default)]
    pub relationships: BTreeMap<String, Relationship>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Entity {
    pub label: String,
    pub properties: BTreeMap<String, Property>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Property {
    #[serde(rename = "type")]
    pub property_type: PropertyType,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum PropertyType {
    Int64,
    String,
    Bool,
    Float,
    Timestamp,
    Date,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Relationship {
    pub variants: Vec<RelationshipVariant>,
}

/// One edge kind `from -> to`. Several packages may add variants to the same
/// relationship name; a `(name, from, to)` triple is declared once across all of
/// them. A variant carries exactly one of `fk` or `table`.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RelationshipVariant {
    pub from: String,
    pub to: String,
    #[serde(default)]
    pub fk: Option<ForeignKey>,
    #[serde(default)]
    pub table: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ForeignKey {
    pub column: String,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Authorization {
    pub entities: BTreeMap<String, AuthorizationEntity>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AuthorizationEntity {
    pub scope: String,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Storage {
    pub store: String,
    pub entities: BTreeMap<String, StorageEntity>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StorageEntity {
    pub columns: BTreeMap<String, String>,
    pub sort_key: Vec<String>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Sources {
    pub source: String,
    pub entities: BTreeMap<String, SourceEntity>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SourceEntity {
    pub tables: Vec<String>,
    pub order_by: Vec<String>,
}

impl Package {
    /// Parses and validates a package document against the JSON Schema and the
    /// intra-document load rules. Every violation is reported, not just the first.
    pub fn parse(document_yaml: &str, digest: String) -> Result<Self, PackageError> {
        let value: serde_json::Value =
            orbit_utils::yaml::from_str(document_yaml).map_err(|e| PackageError {
                name: "<unparsed>".into(),
                errors: vec![e.to_string()],
            })?;

        let name = value
            .pointer("/metadata/name")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("<unnamed>")
            .to_string();

        let schema_errors: Vec<String> = PACKAGE_SCHEMA
            .iter_errors(&value)
            .map(|e| format!("{} at {}", e, e.instance_path()))
            .collect();
        if !schema_errors.is_empty() {
            return Err(PackageError {
                name,
                errors: schema_errors,
            });
        }

        let document: PackageDocument =
            serde_json::from_value(value).map_err(|e| PackageError {
                name: name.clone(),
                errors: vec![e.to_string()],
            })?;

        let rule_errors = document.check_load_rules();
        if !rule_errors.is_empty() {
            return Err(PackageError {
                name,
                errors: rule_errors,
            });
        }

        Ok(Self {
            digest,
            document_yaml: document_yaml.to_string(),
            document,
        })
    }

    pub fn name(&self) -> &str {
        &self.document.metadata.name
    }
}

impl PropertyType {
    /// The ClickHouse column type Orbit would infer for a property. The package
    /// still declares the storage column itself; this is used only where a
    /// column must exist without a declaration, such as the scopes join.
    pub fn clickhouse_type(self) -> &'static str {
        match self {
            Self::Int64 => "Int64",
            Self::String => "String",
            Self::Bool => "Bool",
            Self::Float => "Float64",
            Self::Timestamp => "DateTime64(6, 'UTC')",
            Self::Date => "Date",
        }
    }
}

/// The first 8 hex characters of the sha256 of a package document. Orbit puts
/// this in each extension table name, so a changed document yields new tables.
pub fn short_digest(document_yaml: &str) -> String {
    full_digest(document_yaml).chars().take(8).collect()
}

pub fn full_digest(document_yaml: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(document_yaml.as_bytes());
    hex(&hasher.finalize())
}

impl PackageDocument {
    fn check_load_rules(&self) -> Vec<String> {
        let mut errors = Vec::new();

        if self.api_version != API_VERSION {
            errors.push(format!(
                "apiVersion must be {API_VERSION}, got {}",
                self.api_version
            ));
        }
        if self.kind != KIND {
            errors.push(format!("kind must be {KIND}, got {}", self.kind));
        }

        for (name, entity) in &self.graph.entities {
            match entity.properties.get(ID_PROPERTY) {
                Some(id) if id.property_type == PropertyType::Int64 => {}
                Some(_) => errors.push(format!("entity {name}: property `id` must be int64")),
                None => errors.push(format!(
                    "entity {name}: a required `id` property is missing"
                )),
            }
            self.check_entity_coverage(name, entity, &mut errors);
        }

        for (name, relationship) in &self.graph.relationships {
            self.check_relationship(name, relationship, &mut errors);
        }

        errors
    }

    fn check_entity_coverage(&self, name: &str, entity: &Entity, errors: &mut Vec<String>) {
        if !self.authorization.entities.contains_key(name) {
            errors.push(format!("entity {name}: no authorization guard declared"));
        }
        if !self.sources.entities.contains_key(name) {
            errors.push(format!("entity {name}: no source table declared"));
        }
        match self.storage.entities.get(name) {
            None => errors.push(format!("entity {name}: no storage declared")),
            Some(storage) => {
                for property in entity.properties.keys() {
                    if !storage.columns.contains_key(property) {
                        errors.push(format!(
                            "entity {name}: property `{property}` has no storage column"
                        ));
                    }
                }
                for column in storage.columns.keys() {
                    if !entity.properties.contains_key(column) {
                        errors.push(format!(
                            "entity {name}: storage column `{column}` has no matching property"
                        ));
                    }
                }
            }
        }
    }

    fn check_relationship(
        &self,
        name: &str,
        relationship: &Relationship,
        errors: &mut Vec<String>,
    ) {
        for variant in &relationship.variants {
            if variant.fk.is_some() == variant.table.is_some() {
                errors.push(format!(
                    "relationship {name} variant {} -> {}: exactly one of fk or table is required",
                    variant.from, variant.to
                ));
            }
        }
    }
}

fn hex(bytes: &[u8]) -> String {
    use std::fmt::Write;
    bytes
        .iter()
        .fold(String::with_capacity(bytes.len() * 2), |mut acc, byte| {
            let _ = write!(acc, "{byte:02x}");
            acc
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    const WIDGETS: &str = include_str!("../tests/fixtures/example.widgets.yaml");

    fn parse_widgets() -> Package {
        let digest = short_digest(WIDGETS);
        Package::parse(WIDGETS, digest).expect("fixture loads")
    }

    #[test]
    fn parses_the_fixture_package() {
        let package = parse_widgets();
        assert_eq!(package.name(), "example.widgets");
        assert!(package.document.graph.entities.contains_key("Widget"));
        assert!(package.document.graph.entities.contains_key("Gadget"));
    }

    #[test]
    fn reads_both_relationship_variant_kinds() {
        let package = parse_widgets();
        let relationships = &package.document.graph.relationships;
        let part_of = &relationships["PART_OF"].variants[0];
        assert_eq!(
            (part_of.from.as_str(), part_of.to.as_str()),
            ("Gadget", "Widget")
        );
        assert!(part_of.fk.is_some(), "PART_OF variant is a fk variant");
        assert!(
            relationships["LINKS"].variants[0].table.is_some(),
            "LINKS variant is a table variant"
        );
    }

    #[test]
    fn rejects_a_variant_with_both_fk_and_table() {
        let yaml = WIDGETS.replace(
            "        - { from: Widget, to: Gadget, table: ex_widget_links }",
            "        - { from: Widget, to: Gadget, table: ex_widget_links, fk: { column: gadget_id } }",
        );
        let err = Package::parse(&yaml, "d".into()).unwrap_err();
        assert!(
            err.errors
                .iter()
                .any(|e| e.contains("exactly one of fk or table") || e.contains("oneOf")),
            "got {:?}",
            err.errors
        );
    }

    #[test]
    fn rejects_a_wrong_api_version() {
        let yaml = WIDGETS.replace(API_VERSION, "orbit.gitlab.com/extensions/v2");
        let err = Package::parse(&yaml, "d".into()).unwrap_err();
        assert!(
            err.errors
                .iter()
                .any(|e| e.contains("apiVersion") || e.contains("const"))
        );
    }

    #[test]
    fn rejects_a_property_with_no_storage_column() {
        let yaml = WIDGETS.replace("        name: { type: string }\n", "");
        let err = Package::parse(&yaml, "d".into()).unwrap_err();
        assert!(
            err.errors
                .iter()
                .any(|e| e.contains("storage column `name`")),
            "got {:?}",
            err.errors
        );
    }

    #[test]
    fn rejects_an_entity_without_id() {
        let yaml = WIDGETS.replace("        id: { type: int64 }\n", "");
        let err = Package::parse(&yaml, "d".into()).unwrap_err();
        assert!(
            err.errors.iter().any(|e| e.contains("`id`")),
            "got {:?}",
            err.errors
        );
    }

    #[test]
    fn short_digest_changes_with_the_document() {
        assert_ne!(short_digest("a"), short_digest("b"));
        assert_eq!(short_digest(WIDGETS).len(), 8);
    }
}
