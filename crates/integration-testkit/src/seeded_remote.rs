//! Seeded fakes of the Orbit Next code service and the Data Extraction Service, so scenarios
//! run code and live plans end to end the way `SeededColumnResolver` fakes Gitaly.

use std::collections::BTreeMap;

use compiler::remote::Namespaces;
use orbit_utils::traversal_path::TraversalPath;
use query_data_model::catalog::{ExecuteError, Row};
use query_data_model::implementations::live::{DataExtraction, ExtractionRequest};
use query_data_model::implementations::orbit_code::{
    CodeSearch, RequestField, StructuralSearchRequest,
};
use serde::Deserialize;
use serde_json::Value;

const SEED_DIR: &str = env!("SEEDS_DIR");

fn seed<T: for<'de> Deserialize<'de>>(file: &str) -> T {
    let path = format!("{SEED_DIR}/{file}");
    let yaml = std::fs::read_to_string(&path)
        .unwrap_or_else(|error| panic!("seed not found at {path}: {error}"));
    orbit_utils::yaml::from_str(&yaml).unwrap_or_else(|error| panic!("parsing {path}: {error}"))
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SeededMatch {
    pattern: String,
    language: String,
    path: String,
    line: u64,
    captures: BTreeMap<String, String>,
    /// The branch whose head holds the match; every repository indexes `main`.
    #[serde(default = "main")]
    branch: String,
}

fn main() -> String {
    "main".to_string()
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Repository {
    traversal_path: String,
    matches: Vec<SeededMatch>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SeededCodeSearch {
    repositories: BTreeMap<i64, Repository>,
}

impl SeededCodeSearch {
    pub fn from_seed_file() -> Self {
        seed("code_search.yaml")
    }
}

impl CodeSearch for SeededCodeSearch {
    fn search(&self, request: &StructuralSearchRequest) -> Result<Vec<Row>, ExecuteError> {
        let matches = request
            .repositories
            .iter()
            .filter_map(|project| Some((*project, self.repositories.get(project)?)))
            .flat_map(|(project, repository)| {
                repository.matches.iter().map(move |found| (project, found))
            })
            .filter(|(_, found)| found.pattern == request.pattern)
            .filter(|(project, found)| {
                request.heads.is_empty()
                    || request
                        .heads
                        .iter()
                        .any(|(head, branch)| head == project && *branch == found.branch)
            })
            .filter(|(_, found)| {
                request.languages.is_empty() || request.languages.contains(&found.language)
            });
        let limit = request.limit.map_or(usize::MAX, |limit| limit as usize);
        Ok(matches
            .take(limit)
            .map(|(project, found)| {
                request
                    .fields
                    .iter()
                    .map(|field| match field {
                        RequestField::Path => {
                            ("path".to_string(), Value::from(found.path.as_str()))
                        }
                        RequestField::Line => ("line".to_string(), Value::from(found.line)),
                        RequestField::Repository(name) => (name.clone(), Value::from(project)),
                        RequestField::Branch(name) => {
                            (name.clone(), Value::from(found.branch.as_str()))
                        }
                        RequestField::Capture(name) => {
                            (name.clone(), found.captures.get(name).cloned().into())
                        }
                    })
                    .collect()
            })
            .collect())
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Workspace {
    traversal_path: String,
    messages: Vec<BTreeMap<String, Value>>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SeededDataExtraction {
    workspaces: BTreeMap<String, Workspace>,
}

impl SeededDataExtraction {
    pub fn from_seed_file() -> Self {
        seed("data_extraction.yaml")
    }
}

fn text(value: Option<&Value>) -> &str {
    value.and_then(Value::as_str).unwrap_or_default()
}

impl DataExtraction for SeededDataExtraction {
    fn extract(&self, user: &str, request: &ExtractionRequest) -> Result<Vec<Row>, ExecuteError> {
        if request.operation != "search.messages" {
            return Err(ExecuteError(format!(
                "unknown operation {}",
                request.operation
            )));
        }
        let visible = |message: &&BTreeMap<String, Value>| {
            message
                .get("visible_to")
                .and_then(Value::as_array)
                .is_some_and(|users| users.iter().any(|seen| seen.as_str() == Some(user)))
        };
        let filtered = |message: &&BTreeMap<String, Value>| {
            request.filters.iter().all(|filter| {
                let field = message.get(&filter.field);
                match filter.op {
                    "contains" => text(field).contains(text(Some(&filter.value))),
                    "in" => filter
                        .value
                        .as_array()
                        .is_some_and(|values| field.is_some_and(|field| values.contains(field))),
                    _ => field == Some(&filter.value),
                }
            })
        };
        let windowed = |message: &&BTreeMap<String, Value>| {
            request.window.as_ref().is_none_or(|window| {
                let at = text(message.get(&window.field));
                window.from.as_deref().is_none_or(|from| at >= from)
                    && window.to.as_deref().is_none_or(|to| at <= to)
            })
        };
        let limit = request.limit.map_or(usize::MAX, |limit| limit as usize);
        Ok(request
            .workspaces
            .iter()
            .filter_map(|workspace| self.workspaces.get(workspace))
            .flat_map(|workspace| &workspace.messages)
            .filter(visible)
            .filter(filtered)
            .filter(windowed)
            .take(limit)
            .map(|message| {
                request
                    .fields
                    .iter()
                    .map(|field| {
                        (
                            field.clone(),
                            message.get(field).cloned().unwrap_or_default(),
                        )
                    })
                    .collect()
            })
            .collect())
    }
}

/// The traversal path of every seeded repository and workspace, as the lookup on the graph
/// connection would resolve them.
pub fn seeded_namespaces() -> Namespaces {
    let code = SeededCodeSearch::from_seed_file();
    let live = SeededDataExtraction::from_seed_file();
    Namespaces {
        repositories: code
            .repositories
            .iter()
            .map(|(project, repository)| {
                (
                    *project,
                    TraversalPath::new_unchecked(repository.traversal_path.as_str()),
                )
            })
            .collect(),
        workspaces: live
            .workspaces
            .iter()
            .map(|(workspace, seeded)| {
                (
                    workspace.clone(),
                    TraversalPath::new_unchecked(seeded.traversal_path.as_str()),
                )
            })
            .collect(),
    }
}
