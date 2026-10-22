use std::fmt::Write as _;

use serde_json::{Value, json};

use super::client::OrbitClient;
use super::error::{EXIT_GENERIC, RemoteError};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Code {
    Symbol(String),
    File {
        path: String,
        lines: Option<(usize, usize)>,
    },
}

impl Code {
    pub(crate) fn parse(kind: &str, value: &str) -> Self {
        match kind {
            "symbol" | "def" => Code::Symbol(value.to_string()),
            _ => match value.rsplit_once(':').and_then(|(path, range)| {
                let (start, end) = range.split_once('-').unwrap_or((range, range));
                Some((path, (start.parse().ok()?, end.parse().ok()?)))
            }) {
                Some((path, (start, end))) if start >= 1 && end >= start => Code::File {
                    path: path.to_string(),
                    lines: Some((start, end)),
                },
                _ => Code::File {
                    path: value.to_string(),
                    lines: None,
                },
            },
        }
    }
}

impl std::fmt::Display for Code {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Code::Symbol(name) => write!(f, "symbol:{name}"),
            Code::File { path, lines: None } => write!(f, "file:{path}"),
            Code::File {
                path,
                lines: Some((start, end)),
            } => write!(f, "file:{path}:{start}-{end}"),
        }
    }
}

pub(crate) async fn split_project_path(
    client: &OrbitClient,
    path: &str,
) -> Option<(String, String)> {
    let parts: Vec<&str> = path.split('/').collect();
    for end in 2..parts.len() {
        let project = parts[..end].join("/");
        let found = client
            .get_api(&format!("projects/{}", project.replace('/', "%2F")), &[])
            .await;
        if found.is_ok() {
            return Some((project, parts[end..].join("/")));
        }
    }
    None
}

fn error(message: impl Into<String>) -> RemoteError {
    RemoteError::new(EXIT_GENERIC, message.into())
}

pub(crate) async fn read(
    client: &OrbitClient,
    project: &str,
    code: &Code,
) -> Result<String, RemoteError> {
    let body = client
        .get_api(&format!("projects/{}", project.replace('/', "%2F")), &[])
        .await?;
    let found: Value =
        serde_json::from_slice(&body).map_err(|e| error(format!("unreadable project: {e}")))?;
    let id = found["id"]
        .as_i64()
        .ok_or_else(|| error(format!("{project}: no project id")))?;
    let branch = found["default_branch"]
        .as_str()
        .unwrap_or("main")
        .to_string();
    let query = match code {
        Code::Symbol(name) => definition_query(id, &branch, name),
        Code::File { path, .. } => file_query(id, &branch, path),
    };
    let request = serde_json::to_vec(&json!({"query": query, "response_format": "raw"}))
        .map_err(|e| error(e.to_string()))?;
    let response = client.query_raw(request).await?;
    let response: Value = serde_json::from_slice(&response)
        .map_err(|e| error(format!("unreadable graph response: {e}")))?;
    let nodes = response["result"]["nodes"]
        .as_array()
        .cloned()
        .unwrap_or_default();
    Ok(render(project, &branch, code, &nodes))
}

fn definition_query(project_id: i64, branch: &str, name: &str) -> Value {
    let filter = match name.contains(['.', ':', '#']) {
        true => json!({"fqn": {"ends_with": name}}),
        false => json!({"name": name}),
    };
    let mut filters = json!({"project_id": project_id, "branch": branch});
    filters
        .as_object_mut()
        .expect("filters is an object")
        .extend(filter.as_object().expect("filter is an object").clone());
    json!({
        "query_type": "traversal",
        "nodes": [{
            "id": "d",
            "entity": "Definition",
            "filters": filters,
            "columns": ["fqn", "definition_type", "file_path", "start_line", "end_line", "content"],
        }],
        "limit": 5,
    })
}

fn file_query(project_id: i64, branch: &str, path: &str) -> Value {
    json!({
        "query_type": "traversal",
        "nodes": [{
            "id": "f",
            "entity": "File",
            "filters": {"project_id": project_id, "branch": branch, "path": {"ends_with": path}},
            "columns": ["path", "content"],
        }],
        "limit": 5,
    })
}

fn numbered(out: &mut String, content: &str, first: usize, range: Option<(usize, usize)>) {
    for (offset, line) in content.lines().enumerate() {
        let number = first + offset;
        if range.is_none_or(|(start, end)| (start..=end).contains(&number)) {
            let _ = writeln!(out, "{number}|{line}");
        }
    }
}

fn render(project: &str, branch: &str, code: &Code, nodes: &[Value]) -> String {
    let mut out = String::new();
    if nodes.is_empty() {
        let _ = writeln!(out, "{code} in {project}@{branch}: not found in the graph");
        return out;
    }
    for node in nodes {
        let text = |key: &str| node[key].as_str().unwrap_or_default();
        match code {
            Code::Symbol(_) => {
                let start = node["start_line"].as_u64().unwrap_or(1) as usize;
                let _ = writeln!(
                    out,
                    "{project}/{}:{}-{} {} {}",
                    text("file_path"),
                    start,
                    node["end_line"].as_u64().unwrap_or(0),
                    text("definition_type"),
                    text("fqn")
                );
                numbered(&mut out, text("content"), start, None);
            }
            Code::File { lines, .. } => {
                let _ = writeln!(out, "{project}/{}", text("path"));
                numbered(&mut out, text("content"), 1, *lines);
            }
        }
    }
    out
}
