//! Server-defined named queries.
//!
//! Named queries are graph query templates committed under
//! `config/named_queries/` so the query text lives with the engine
//! `gkg-server`'s build script compiles every template against the ontology,
//! and the same files are embedded into the binary so the server can execute
//! them by name at runtime.
//!
//! Templates may contain two placeholder kinds:
//!
//! - `{ "$binding": "<name>" }` — identity values resolved exclusively from
//!   trusted request context ([`BindingValues`]), never from client input.
//! - `{ "$param": "<name>" }` — selection values supplied by the client
//!   (e.g. the entity and ids of a clicked node) and validated against the
//!   JSON Schema each template declares for the parameter.

mod query;

use std::collections::BTreeMap;
use std::path::Path;

use rust_embed::Embed;
use serde_json::{Map, Value};

pub use query::{BindingValues, NamedQuery};

#[derive(Embed)]
#[folder = "$NAMED_QUERIES_DIR"]
struct EmbeddedNamedQueries;

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

    #[error("invalid named query request: {message}")]
    InvalidRequest { message: String },

    #[error("no named query YAML files found in {location}")]
    Empty { location: String },
}

pub(crate) fn invalid(name: &str, message: String) -> NamedQueryError {
    NamedQueryError::Invalid {
        name: name.to_string(),
        message,
    }
}

fn read_err(path: impl std::fmt::Display, message: impl std::fmt::Display) -> NamedQueryError {
    NamedQueryError::Read {
        path: path.to_string(),
        message: message.to_string(),
    }
}

#[derive(Debug)]
pub struct NamedQueries {
    queries: BTreeMap<String, NamedQuery>,
}

impl NamedQueries {
    pub fn load_embedded() -> Result<Self, NamedQueryError> {
        let mut files = Vec::new();
        for path in EmbeddedNamedQueries::iter().filter(|p| p.ends_with(".yaml")) {
            let file = EmbeddedNamedQueries::get(&path)
                .ok_or_else(|| read_err(&path, "embedded file disappeared"))?;
            let content = String::from_utf8(file.data.to_vec()).map_err(|e| read_err(&path, e))?;
            files.push((path.to_string(), content));
        }
        Self::from_files(env!("NAMED_QUERIES_DIR"), files)
    }

    pub fn load_from_dir(dir: &Path) -> Result<Self, NamedQueryError> {
        let mut paths = std::fs::read_dir(dir)
            .and_then(|entries| {
                entries
                    .map(|e| e.map(|e| e.path()))
                    .collect::<std::io::Result<Vec<_>>>()
            })
            .map_err(|e| read_err(dir.display(), e))?;
        paths.retain(|p| p.extension().is_some_and(|ext| ext == "yaml"));
        paths.sort();

        let mut files = Vec::new();
        for path in paths {
            let content =
                std::fs::read_to_string(&path).map_err(|e| read_err(path.display(), e))?;
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
            if let Some(existing) = queries.insert(query.name.clone(), query) {
                return Err(invalid(&existing.name, "duplicate named query".into()));
            }
        }
        if queries.is_empty() {
            return Err(NamedQueryError::Empty {
                location: location.to_string(),
            });
        }
        let defaults: Vec<&str> = queries
            .values()
            .filter(|q| q.default)
            .map(|q| q.name.as_str())
            .collect();
        if defaults.len() > 1 {
            return Err(invalid(
                defaults[0],
                format!(
                    "at most one named query may set `default: true`, but {} do: {}",
                    defaults.len(),
                    defaults.join(", ")
                ),
            ));
        }
        Ok(Self { queries })
    }

    pub fn get(&self, name: &str) -> Option<&NamedQuery> {
        self.queries.get(name)
    }

    pub fn render_request(
        &self,
        request: &str,
        values: &BindingValues,
    ) -> Result<String, NamedQueryError> {
        let (name, params) = parse_request(request)?;
        let Some(query) = self.queries.get(&name) else {
            return Err(NamedQueryError::Unknown {
                name,
                available: self.names().map(String::from).collect(),
            });
        };
        query.render(values, &params)
    }

    pub fn iter(&self) -> impl Iterator<Item = &NamedQuery> {
        self.queries.values()
    }

    pub fn names(&self) -> impl Iterator<Item = &str> {
        self.queries.keys().map(String::as_str)
    }
}

fn parse_request(request: &str) -> Result<(String, Map<String, Value>), NamedQueryError> {
    #[derive(serde::Deserialize)]
    #[serde(deny_unknown_fields)]
    struct Envelope {
        name: String,
        #[serde(default)]
        parameters: Map<String, Value>,
    }

    let envelope: Envelope =
        serde_json::from_str(request).map_err(|e| NamedQueryError::InvalidRequest {
            message: e.to_string(),
        })?;
    Ok((envelope.name, envelope.parameters))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn values() -> BindingValues {
        BindingValues {
            current_user_id: 42,
        }
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
            "expand_neighbors",
        ] {
            assert!(queries.get(name).is_some(), "missing named query `{name}`");
        }
    }

    #[test]
    fn render_request_without_parameters_substitutes_current_user_id() {
        let queries = NamedQueries::load_embedded().expect("embedded named queries load");
        let rendered = queries
            .render_request(r#"{"name": "my_neighbors"}"#, &values())
            .expect("known named query renders");
        assert!(rendered.contains("\"node_ids\":[42]"), "{rendered}");
        assert!(!rendered.contains("$binding"), "{rendered}");
    }

    #[test]
    fn render_request_rejects_unknown_name_and_lists_available() {
        let queries = NamedQueries::load_embedded().expect("embedded named queries load");
        let err = queries
            .render_request(r#"{"name": "nonexistent"}"#, &values())
            .unwrap_err();
        assert!(err.to_string().contains("nonexistent"), "{err}");
        assert!(err.to_string().contains("my_neighbors"), "{err}");
    }

    #[test]
    fn render_request_rejects_bare_name() {
        let queries = NamedQueries::load_embedded().expect("embedded named queries load");
        let err = queries
            .render_request("my_neighbors", &values())
            .unwrap_err();
        assert!(
            matches!(err, NamedQueryError::InvalidRequest { .. }),
            "{err}"
        );
    }

    #[test]
    fn render_request_envelope_substitutes_parameters() {
        let queries = NamedQueries::load_embedded().expect("embedded named queries load");
        let rendered = queries
            .render_request(
                r#"{"name": "expand_neighbors", "parameters": {"entity": "Project", "node_ids": [7, 9], "limit": 50}}"#,
                &values(),
            )
            .expect("parameterized named query renders");
        assert!(rendered.contains("\"entity\":\"Project\""), "{rendered}");
        assert!(rendered.contains("\"node_ids\":[7,9]"), "{rendered}");
        assert!(rendered.contains("\"limit\":50"), "{rendered}");
        assert!(!rendered.contains("$param"), "{rendered}");
    }

    #[test]
    fn render_request_rejects_malformed_envelope() {
        let queries = NamedQueries::load_embedded().expect("embedded named queries load");
        let err = queries
            .render_request(
                r#"{"name": "expand_neighbors", "unexpected": 1}"#,
                &values(),
            )
            .unwrap_err();
        assert!(
            matches!(err, NamedQueryError::InvalidRequest { .. }),
            "{err}"
        );
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
    fn multiple_defaults_are_rejected() {
        let default_yaml = |name: &str| {
            format!(
                "name: {name}\ndescription: A query.\ndefault: true\nbindings: [current_user_id]\nquery:\n  node_ids:\n    - {{ $binding: current_user_id }}\n"
            )
        };
        let err = NamedQueries::from_files(
            "test",
            [
                ("first.yaml".to_string(), default_yaml("first")),
                ("second.yaml".to_string(), default_yaml("second")),
            ],
        )
        .unwrap_err();
        assert!(err.to_string().contains("at most one"), "{err}");
        assert!(err.to_string().contains("first, second"), "{err}");
    }

    #[test]
    fn empty_set_error_names_the_actual_location() {
        let err = NamedQueries::from_files("/tmp/my_queries", []).unwrap_err();
        assert!(matches!(err, NamedQueryError::Empty { .. }), "{err}");
        assert!(err.to_string().contains("/tmp/my_queries"), "{err}");
    }
}
