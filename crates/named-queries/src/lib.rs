//! Server-defined named queries.
//!
//! Named queries are graph query templates committed under
//! `config/named_queries/` so the query text lives with the engine
//! `gkg-server`'s build script compiles every template against the ontology,
//! and the same files are embedded into the binary so the server can execute
//! them by name at runtime.
//!
//! Templates may contain `{ "$binding": "<name>" }` placeholders. Bindings
//! resolve exclusively from trusted request context ([`BindingValues`]),
//! never from client-supplied input.

use std::collections::{BTreeMap, HashSet};
use std::path::Path;

use rust_embed::Embed;
use serde_json::Value;

#[derive(Embed)]
#[folder = "$NAMED_QUERIES_DIR"]
struct EmbeddedNamedQueries;

const BINDING_KEY: &str = "$binding";
const CURRENT_USER_ID: &str = "current_user_id";

#[derive(Debug, thiserror::Error)]
pub enum NamedQueryError {
    #[error("failed to read {path}: {message}")]
    Read { path: String, message: String },

    #[error("failed to parse {path}: {source}")]
    Parse {
        path: String,
        #[source]
        source: serde_yaml::Error,
    },

    #[error("named query `{name}`: {message}")]
    Invalid { name: String, message: String },

    #[error("unknown named query `{name}`; available named queries: {}", available.join(", "))]
    Unknown {
        name: String,
        available: Vec<String>,
    },

    #[error("no named query YAML files found in {location}")]
    Empty { location: String },
}

#[derive(Debug, Clone, Copy)]
pub struct BindingValues {
    pub current_user_id: u64,
}

impl BindingValues {
    fn resolve(&self, binding: &str) -> Option<Value> {
        match binding {
            CURRENT_USER_ID => Some(Value::from(self.current_user_id)),
            _ => None,
        }
    }
}

#[derive(Debug, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct NamedQueryYaml {
    name: String,
    description: String,
    #[serde(default)]
    bindings: Vec<String>,
    query: Value,
}

#[derive(Debug)]
pub struct NamedQuery {
    pub name: String,
    pub description: String,
    bindings: Vec<String>,
    query: Value,
}

impl NamedQuery {
    fn from_yaml(path: &str, content: &str) -> Result<Self, NamedQueryError> {
        let yaml: NamedQueryYaml =
            serde_yaml::from_str(content).map_err(|source| NamedQueryError::Parse {
                path: path.to_string(),
                source,
            })?;

        let stem = Path::new(path)
            .file_stem()
            .map(|s| s.to_string_lossy().to_string())
            .unwrap_or_default();
        if yaml.name != stem {
            return Err(NamedQueryError::Invalid {
                name: yaml.name,
                message: format!("`name` must match the file stem of {path}"),
            });
        }
        if yaml.description.trim().is_empty() {
            return Err(NamedQueryError::Invalid {
                name: yaml.name,
                message: "needs a non-empty description".to_string(),
            });
        }

        let query = Self {
            name: yaml.name,
            description: yaml.description,
            bindings: yaml.bindings,
            query: yaml.query,
        };
        query.validate()?;
        Ok(query)
    }

    fn validate(&self) -> Result<(), NamedQueryError> {
        let mut probe = self.query.clone();
        let mut used = HashSet::new();
        self.substitute(&mut probe, &BindingValues { current_user_id: 0 }, &mut used)?;
        for declared in &self.bindings {
            if !used.contains(declared.as_str()) {
                return Err(NamedQueryError::Invalid {
                    name: self.name.clone(),
                    message: format!(
                        "declares binding `{declared}` but never uses it; remove it from `bindings:`"
                    ),
                });
            }
        }
        Ok(())
    }

    pub fn render(&self, values: &BindingValues) -> Result<String, NamedQueryError> {
        let mut rendered = self.query.clone();
        self.substitute(&mut rendered, values, &mut HashSet::new())?;
        Ok(rendered.to_string())
    }

    fn substitute(
        &self,
        value: &mut Value,
        values: &BindingValues,
        used: &mut HashSet<String>,
    ) -> Result<(), NamedQueryError> {
        match value {
            Value::Object(map) => {
                if let Some(binding) = map.get(BINDING_KEY) {
                    if map.len() != 1 {
                        return Err(
                            self.invalid(format!("a {BINDING_KEY} object must have no other keys"))
                        );
                    }
                    let Some(binding) = binding.as_str() else {
                        return Err(self.invalid(format!("{BINDING_KEY} value must be a string")));
                    };
                    if !self.bindings.iter().any(|b| b == binding) {
                        return Err(self.invalid(format!(
                            "uses undeclared binding `{binding}`; declare it under `bindings:`"
                        )));
                    }
                    let Some(resolved) = values.resolve(binding) else {
                        return Err(self.invalid(format!("uses unknown binding `{binding}`")));
                    };
                    used.insert(binding.to_string());
                    *value = resolved;
                    return Ok(());
                }
                for nested in map.values_mut() {
                    self.substitute(nested, values, used)?;
                }
            }
            Value::Array(items) => {
                for item in items {
                    self.substitute(item, values, used)?;
                }
            }
            _ => {}
        }
        Ok(())
    }

    fn invalid(&self, message: String) -> NamedQueryError {
        NamedQueryError::Invalid {
            name: self.name.clone(),
            message,
        }
    }
}

#[derive(Debug)]
pub struct NamedQueries {
    queries: BTreeMap<String, NamedQuery>,
}

impl NamedQueries {
    pub fn load_embedded() -> Result<Self, NamedQueryError> {
        let mut files = Vec::new();
        for path in EmbeddedNamedQueries::iter() {
            if !path.ends_with(".yaml") {
                continue;
            }
            let file = EmbeddedNamedQueries::get(&path).ok_or_else(|| NamedQueryError::Read {
                path: path.to_string(),
                message: "embedded file disappeared".to_string(),
            })?;
            let content =
                String::from_utf8(file.data.to_vec()).map_err(|e| NamedQueryError::Read {
                    path: path.to_string(),
                    message: e.to_string(),
                })?;
            files.push((path.to_string(), content));
        }
        Self::from_files(env!("NAMED_QUERIES_DIR"), files)
    }

    pub fn load_from_dir(dir: &Path) -> Result<Self, NamedQueryError> {
        let read_err = |message: String| NamedQueryError::Read {
            path: dir.display().to_string(),
            message,
        };

        let mut paths: Vec<_> = std::fs::read_dir(dir)
            .map_err(|e| read_err(e.to_string()))?
            .map(|entry| entry.map(|e| e.path()))
            .collect::<Result<_, _>>()
            .map_err(|e| read_err(e.to_string()))?;
        paths.retain(|p| p.extension().is_some_and(|ext| ext == "yaml"));
        paths.sort();

        let mut files = Vec::new();
        for path in paths {
            let content = std::fs::read_to_string(&path).map_err(|e| NamedQueryError::Read {
                path: path.display().to_string(),
                message: e.to_string(),
            })?;
            files.push((path.display().to_string(), content));
        }
        Self::from_files(&dir.display().to_string(), files)
    }

    fn from_files(
        location: &str,
        files: impl IntoIterator<Item = (String, String)>,
    ) -> Result<Self, NamedQueryError> {
        let mut queries = BTreeMap::new();
        for (path, content) in files {
            let query = NamedQuery::from_yaml(&path, &content)?;
            let name = query.name.clone();
            if queries.insert(name.clone(), query).is_some() {
                return Err(NamedQueryError::Invalid {
                    name,
                    message: "duplicate named query".to_string(),
                });
            }
        }
        if queries.is_empty() {
            return Err(NamedQueryError::Empty {
                location: location.to_string(),
            });
        }
        Ok(Self { queries })
    }

    pub fn get(&self, name: &str) -> Option<&NamedQuery> {
        self.queries.get(name)
    }

    pub fn render(&self, name: &str, values: &BindingValues) -> Result<String, NamedQueryError> {
        let Some(query) = self.queries.get(name) else {
            return Err(NamedQueryError::Unknown {
                name: name.to_string(),
                available: self.names().map(String::from).collect(),
            });
        };
        query.render(values)
    }

    pub fn iter(&self) -> impl Iterator<Item = &NamedQuery> {
        self.queries.values()
    }

    pub fn names(&self) -> impl Iterator<Item = &str> {
        self.queries.keys().map(String::as_str)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(yaml: &str) -> Result<NamedQuery, NamedQueryError> {
        NamedQuery::from_yaml("q.yaml", yaml)
    }

    const VALID: &str = r#"
name: q
description: A query.
bindings: [current_user_id]
query:
  node_ids:
    - { $binding: current_user_id }
"#;

    #[test]
    fn load_embedded_contains_committed_queries() {
        let queries = NamedQueries::load_embedded().expect("embedded named queries load");
        for name in [
            "my_neighbors",
            "my_mrs_with_pipelines",
            "recent_merges",
            "top_mr_authors",
            "mrs_fixing_vulnerabilities",
        ] {
            assert!(queries.get(name).is_some(), "missing named query `{name}`");
        }
    }

    #[test]
    fn render_by_name_substitutes_current_user_id() {
        let queries = NamedQueries::load_embedded().expect("embedded named queries load");
        let rendered = queries
            .render(
                "my_neighbors",
                &BindingValues {
                    current_user_id: 42,
                },
            )
            .expect("known named query renders");
        assert!(rendered.contains("\"node_ids\":[42]"), "{rendered}");
        assert!(!rendered.contains("$binding"), "{rendered}");
    }

    #[test]
    fn render_by_name_rejects_unknown_name_and_lists_available() {
        let queries = NamedQueries::load_embedded().expect("embedded named queries load");
        let err = queries
            .render(
                "nonexistent",
                &BindingValues {
                    current_user_id: 42,
                },
            )
            .unwrap_err();
        assert!(err.to_string().contains("nonexistent"), "{err}");
        assert!(err.to_string().contains("my_neighbors"), "{err}");
    }

    #[test]
    fn render_substitutes_current_user_id() {
        let query = parse(VALID).expect("valid template");
        let rendered = query
            .render(&BindingValues {
                current_user_id: 42,
            })
            .expect("render succeeds");
        assert_eq!(rendered, r#"{"node_ids":[42]}"#);
    }

    #[test]
    fn name_must_match_file_stem() {
        let err = parse(&VALID.replace("name: q", "name: other")).unwrap_err();
        assert!(err.to_string().contains("file stem"), "{err}");
    }

    #[test]
    fn description_must_be_non_empty() {
        let err = parse(&VALID.replace("A query.", "''")).unwrap_err();
        assert!(err.to_string().contains("description"), "{err}");
    }

    #[test]
    fn unknown_binding_is_rejected() {
        let yaml = VALID.replace("current_user_id", "current_project_id");
        let err = parse(&yaml).unwrap_err();
        assert!(err.to_string().contains("unknown binding"), "{err}");
    }

    #[test]
    fn undeclared_binding_is_rejected() {
        let err = parse(&VALID.replace("bindings: [current_user_id]", "")).unwrap_err();
        assert!(err.to_string().contains("undeclared binding"), "{err}");
    }

    #[test]
    fn unused_declared_binding_is_rejected() {
        let yaml = r#"
name: q
description: A query.
bindings: [current_user_id]
query:
  limit: 1
"#;
        let err = parse(yaml).unwrap_err();
        assert!(err.to_string().contains("never uses it"), "{err}");
    }

    #[test]
    fn binding_object_must_have_single_key() {
        let yaml = r#"
name: q
description: A query.
bindings: [current_user_id]
query:
  node_ids:
    - { $binding: current_user_id, extra: 1 }
"#;
        let err = parse(yaml).unwrap_err();
        assert!(err.to_string().contains("no other keys"), "{err}");
    }

    #[test]
    fn binding_value_must_be_string() {
        let yaml = r#"
name: q
description: A query.
bindings: [current_user_id]
query:
  node_ids:
    - { $binding: 7 }
"#;
        let err = parse(yaml).unwrap_err();
        assert!(err.to_string().contains("must be a string"), "{err}");
    }

    #[test]
    fn unknown_top_level_yaml_key_is_rejected() {
        let err = parse(&format!("{VALID}extra_key: 1\n")).unwrap_err();
        assert!(matches!(err, NamedQueryError::Parse { .. }), "{err}");
    }

    #[test]
    fn duplicate_names_are_rejected() {
        let err = NamedQueries::from_files(
            "test",
            [
                ("a/q.yaml".to_string(), VALID.to_string()),
                ("b/q.yaml".to_string(), VALID.to_string()),
            ],
        )
        .unwrap_err();
        assert!(err.to_string().contains("duplicate"), "{err}");
    }

    #[test]
    fn empty_set_error_names_the_actual_location() {
        let err = NamedQueries::from_files("/tmp/my_queries", []).unwrap_err();
        assert!(matches!(err, NamedQueryError::Empty { .. }), "{err}");
        assert!(err.to_string().contains("/tmp/my_queries"), "{err}");
    }
}
