//! A fake of Orbit Next's code indexing web servers: it takes the structural search request the
//! code executor renders and matches its pattern against the seeded file contents.

use axum::Json;
use axum::extract::State;
use axum::http::StatusCode;
use query_data_model::catalog::Row;
use regex::Regex;
use serde::Deserialize;
use serde_json::Value;

use super::Shared;
use crate::seed::{OrbitNext, language};

pub const PATH: &str = "/v1/structural_search";

/// `StructuralSearchRequest` as it arrives on the wire.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Search {
    pub repositories: Vec<i64>,
    pub heads: Vec<(i64, String)>,
    pub languages: Vec<String>,
    pub trigrams: Vec<String>,
    pub pattern: String,
    pub limit: Option<u64>,
    pub fields: Vec<Field>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Field {
    Path,
    Line,
    Capture(String),
    Repository(String),
    Branch(String),
}

pub async fn search(
    State(store): State<Shared<OrbitNext>>,
    Json(request): Json<Search>,
) -> Result<Json<Vec<Row>>, (StatusCode, String)> {
    let store = super::current(&store);
    let (pattern, captures) =
        matcher(&request.pattern).map_err(|error| (StatusCode::BAD_REQUEST, error))?;
    let limit = request.limit.map_or(usize::MAX, |limit| limit as usize);
    let mut rows = Vec::new();
    for project in &request.repositories {
        let Some(repository) = store.repositories.get(project) else {
            continue;
        };
        for (branch, files) in &repository.branches {
            let searched = match request.heads.is_empty() {
                true => branch == "main",
                false => request.heads.contains(&(*project, branch.clone())),
            };
            if !searched {
                continue;
            }
            for file in files {
                let lang = language(&file.path);
                if !request.languages.is_empty() && !request.languages.iter().any(|l| l == lang) {
                    continue;
                }
                for (index, text) in file.content.lines().enumerate() {
                    let Some(found) = pattern.captures(text) else {
                        continue;
                    };
                    let row = request.fields.iter().map(|field| match field {
                        Field::Path => ("path".to_string(), Value::from(file.path.as_str())),
                        Field::Line => ("line".to_string(), Value::from(index as u64 + 1)),
                        Field::Repository(name) => (name.clone(), Value::from(*project)),
                        Field::Branch(name) => (name.clone(), Value::from(branch.as_str())),
                        Field::Capture(name) => {
                            let text = captures
                                .contains(name)
                                .then(|| found.name(name).map(|m| m.as_str().trim()))
                                .flatten();
                            (name.clone(), text.map(Value::from).unwrap_or_default())
                        }
                    });
                    rows.push(row.collect());
                    if rows.len() == limit {
                        return Ok(Json(rows));
                    }
                }
            }
        }
    }
    Ok(Json(rows))
}

/// An ast-grep pattern as a one-line regex: `$$$NAME` takes any text, `$NAME` one identifier,
/// and whitespace matches any run of whitespace.
fn matcher(pattern: &str) -> Result<(Regex, Vec<String>), String> {
    let mut regex = String::new();
    let mut names = Vec::new();
    let mut chars = pattern.chars().peekable();
    while let Some(c) = chars.next() {
        if c.is_whitespace() {
            while chars.peek().is_some_and(|c| c.is_whitespace()) {
                chars.next();
            }
            regex.push_str(r"\s*");
            continue;
        }
        if c != '$' {
            regex.push_str(&regex::escape(&c.to_string()));
            continue;
        }
        let mut many = false;
        while chars.peek() == Some(&'$') {
            chars.next();
            many = true;
        }
        let mut name = String::new();
        while let Some(c) = chars
            .peek()
            .filter(|c| c.is_ascii_uppercase() || **c == '_')
        {
            name.push(*c);
            chars.next();
        }
        let body = if many {
            ".*?"
        } else {
            "[A-Za-z_][A-Za-z0-9_]*"
        };
        if name.is_empty() || name == "_" {
            regex.push_str(&format!("(?:{body})"));
        } else if names.contains(&name) {
            return Err(format!(
                "the fake matches each of {pattern}'s metavariables once"
            ));
        } else {
            regex.push_str(&format!("(?P<{name}>{body})"));
            names.push(name);
        }
    }
    let regex = Regex::new(&regex).map_err(|error| format!("{pattern}: {error}"))?;
    Ok((regex, names))
}
