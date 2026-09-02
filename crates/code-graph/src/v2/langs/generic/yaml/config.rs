//! Embedded document-type configs, validated against
//! `config/schemas/yaml_document_type.schema.json` when first loaded.

use std::sync::LazyLock;

use rust_embed::Embed;
use serde::Deserialize;

use super::{N, find_pair, scalar_text};
use treesitter_visit::Axis::*;
use treesitter_visit::Match::*;

#[derive(Embed)]
#[folder = "src/v2/langs/generic/yaml/document_types"]
struct ConfigFiles;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct DocumentType {
    pub(super) name: String,
    #[serde(rename = "match")]
    pub(super) matcher: Matcher,
    #[serde(default)]
    pub(super) keywords: Vec<String>,
    #[serde(default)]
    pub(super) definitions: Vec<DefinitionRule>,
    #[serde(default)]
    pub(super) imports: Vec<KeyRule>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct DefinitionRule {
    #[serde(rename = "type")]
    pub(super) definition_type: String,
    #[serde(flatten)]
    pub(super) shape: Shape,
}

#[derive(Deserialize)]
#[serde(rename_all = "snake_case")]
pub(super) enum Shape {
    ChildrenOf(String),
    ItemsOf(String),
    RootKeys(bool),
    ValueOf(String),
    AllKeys(bool),
}

impl Shape {
    pub(super) fn claims(&self, doc_type: &DocumentType, root_key: &str) -> bool {
        match self {
            Shape::ChildrenOf(parent) | Shape::ItemsOf(parent) => parent == root_key,
            Shape::ValueOf(path) => path.split('.').next() == Some(root_key),
            Shape::RootKeys(enabled) => {
                *enabled && !doc_type.keywords.iter().any(|k| k == root_key)
            }
            Shape::AllKeys(enabled) => *enabled,
        }
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Matcher {
    #[serde(default)]
    pub(super) filename_suffixes: Vec<String>,
    #[serde(default)]
    pub(super) filename_prefixes: Vec<String>,
    #[serde(default)]
    pub(super) directory_prefixes: Vec<String>,
    #[serde(default)]
    pub(super) document_keys: std::collections::BTreeMap<String, KeyCondition>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct KeyCondition {
    #[serde(rename = "in", default)]
    pub(super) any_of: Vec<String>,
    pub(super) starts_with: Option<String>,
}

impl KeyCondition {
    fn holds(&self, value: &str) -> bool {
        (self.any_of.is_empty() || self.any_of.iter().any(|v| v == value))
            && self
                .starts_with
                .as_deref()
                .is_none_or(|prefix| value.starts_with(prefix))
    }
}

impl Matcher {
    pub(super) fn matches(&self, node: &N<'_>, file_path: &str) -> bool {
        let filename = file_path.rsplit('/').next().unwrap_or(file_path);
        if self.filename_suffixes.iter().any(|suffix| {
            filename == suffix || (suffix.starts_with('.') && filename.ends_with(suffix.as_str()))
        }) || self
            .filename_prefixes
            .iter()
            .any(|prefix| filename.starts_with(prefix.as_str()))
            || self
                .directory_prefixes
                .iter()
                .any(|prefix| file_path.starts_with(prefix.as_str()))
        {
            return true;
        }
        if self.document_keys.is_empty() {
            return false;
        }
        let Some(mapping) = node
            .find(Ancestor, Kind("document"))
            .and_then(|document| document.find(Child, Kind("block_node")))
            .and_then(|block| block.find(Child, Kind("block_mapping")))
        else {
            return false;
        };
        self.document_keys.iter().all(|(key, condition)| {
            find_pair(&mapping, key)
                .and_then(|pair| pair.field("value").as_ref().and_then(scalar_text))
                .is_some_and(|value| condition.holds(&value))
        })
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct KeyRule {
    pub(super) key: String,
    #[serde(default)]
    pub(super) also_under: Vec<String>,
    pub(super) scalar_type: Option<String>,
    #[serde(default)]
    pub(super) mapping_forms: Vec<MappingForm>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct MappingForm {
    #[serde(rename = "type")]
    pub(super) import_type: String,
    pub(super) path_key: String,
    #[serde(default)]
    pub(super) name_keys: Vec<String>,
    pub(super) alias_key: Option<String>,
    pub(super) version_split: Option<char>,
}

const CONFIG_SCHEMA: &str = include_str!(concat!(
    env!("SCHEMA_DIR"),
    "/yaml_document_type.schema.json"
));

pub(super) fn config_validator() -> jsonschema::Validator {
    let schema: serde_json::Value =
        serde_json::from_str(CONFIG_SCHEMA).expect("yaml_document_type.schema.json must be JSON");
    jsonschema::validator_for(&schema).expect("yaml_document_type.schema.json must be a schema")
}

pub(super) static DOCUMENT_TYPES: LazyLock<Vec<DocumentType>> = LazyLock::new(|| {
    let validator = config_validator();
    ConfigFiles::iter()
        .map(|path| {
            let file = ConfigFiles::get(&path).expect("iterated embedded file must exist");
            let document: serde_json::Value = orbit_utils::yaml::from_slice(&file.data)
                .unwrap_or_else(|e| panic!("document-type config {path} must parse: {e}"));
            let errors: Vec<String> = validator
                .iter_errors(&document)
                .map(|e| format!("{}: {e}", e.instance_path()))
                .collect();
            assert!(
                errors.is_empty(),
                "document-type config {path} violates yaml_document_type.schema.json: {}",
                errors.join("; ")
            );
            let doc_type: DocumentType = serde_json::from_value(document)
                .unwrap_or_else(|e| panic!("document-type config {path} must deserialize: {e}"));
            let stem = path.trim_end_matches(".yaml").trim_end_matches(".yml");
            assert_eq!(
                doc_type.name, stem,
                "document-type config {path} must be named after its `name`"
            );
            doc_type
        })
        .collect()
});
