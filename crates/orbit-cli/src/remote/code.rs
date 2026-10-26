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

/// Splits `group/project/path/to/file` at the longest project path the graph knows, in one query.
pub(crate) async fn split_project_path(
    client: &OrbitClient,
    path: &str,
) -> Option<(String, String)> {
    let parts: Vec<&str> = path.split('/').collect();
    let candidates: Vec<String> = (2..parts.len()).map(|end| parts[..end].join("/")).collect();
    if candidates.is_empty() {
        return None;
    }
    let list: Vec<String> = candidates.iter().map(|path| gql_string(path)).collect();
    let query = format!(
        "MATCH (p:Project) WHERE p.full_path IN [{}] RETURN p.full_path LIMIT {}",
        list.join(", "),
        candidates.len()
    );
    let nodes = query_nodes(client, &query).await.ok()?;
    let project = nodes
        .iter()
        .filter_map(|node| node["full_path"].as_str())
        .filter(|found| candidates.iter().any(|candidate| candidate == found))
        .max_by_key(|found| found.len())?;
    Some((project.to_string(), path[project.len() + 1..].to_string()))
}

fn error(message: impl Into<String>) -> RemoteError {
    RemoteError::new(EXIT_GENERIC, message.into())
}

async fn query_nodes(client: &OrbitClient, query: &str) -> Result<Vec<Value>, RemoteError> {
    let request = serde_json::to_vec(&json!({"query": query, "response_format": "raw"}))
        .map_err(|e| error(e.to_string()))?;
    let response = client.query_raw(request).await?;
    let response: Value = serde_json::from_slice(&response)
        .map_err(|e| error(format!("unreadable graph response: {e}")))?;
    Ok(response["result"]["nodes"]
        .as_array()
        .cloned()
        .unwrap_or_default())
}

/// The project id and default branch name from the graph.
async fn project_branch(client: &OrbitClient, project: &str) -> Result<(i64, String), RemoteError> {
    let query = format!(
        "MATCH (p:Project {{full_path: {}}})-[:CONTAINS]->(b:Branch) WHERE b.is_default = true \
         RETURN b.project_id, b.name LIMIT 1",
        gql_string(project)
    );
    let nodes = query_nodes(client, &query).await?;
    let branch = nodes
        .iter()
        .find(|node| node["name"].is_string() && !node["project_id"].is_null())
        .ok_or_else(|| {
            error(format!(
                "{project}'s code isn't indexed in Orbit yet; Orbit indexes it once enabled for the top-level group"
            ))
        })?;
    let id = match &branch["project_id"] {
        Value::Number(id) => id.as_i64(),
        Value::String(id) => id.parse().ok(),
        _ => None,
    }
    .ok_or_else(|| error(format!("{project}: no project id")))?;
    Ok((id, branch["name"].as_str().unwrap_or_default().to_string()))
}

pub(crate) async fn read(
    client: &OrbitClient,
    project: &str,
    code: &Code,
) -> Result<String, RemoteError> {
    let (id, branch) = project_branch(client, project).await?;
    let nodes = match code {
        Code::Symbol(name) => query_nodes(client, &definition_query(id, &branch, name)).await?,
        Code::File { path, .. } => file_nodes(client, id, &branch, path).await?,
    };
    Ok(render(project, &branch, code, &nodes))
}

/// Finds the file without content, which costs no Gitaly call, then reads only the one it picks.
async fn file_nodes(
    client: &OrbitClient,
    id: i64,
    branch: &str,
    path: &str,
) -> Result<Vec<Value>, RemoteError> {
    let found = query_nodes(client, &file_query(id, branch, path, false)).await?;
    let Some(chosen) = found
        .iter()
        .filter_map(|node| node["path"].as_str())
        .min_by_key(|found| (*found != path, found.len()))
    else {
        return Ok(Vec::new());
    };
    query_nodes(client, &file_query(id, branch, chosen, true)).await
}

fn definition_query(project_id: i64, branch: &str, name: &str) -> String {
    let by = match name.contains(['.', ':', '#']) {
        true => format!("d.fqn ENDS WITH {}", gql_string(name)),
        false => format!("d.name = {}", gql_string(name)),
    };
    format!(
        "MATCH (d:Definition {{project_id: {project_id}, branch: {}}}) WHERE {by} \
         RETURN d.fqn, d.definition_type, d.file_path, d.start_line, d.end_line, d.content LIMIT 5",
        gql_string(branch)
    )
}

fn file_query(project_id: i64, branch: &str, path: &str, content: bool) -> String {
    let by = match content || path.contains('/') {
        true => format!("f.path = {}", gql_string(path)),
        false => format!("f.path ENDS WITH {}", gql_string(&format!("/{path}"))),
    };
    let (returned, limit) = match content {
        true => ("f.path, f.content", 1),
        false => ("f.path", 20),
    };
    format!(
        "MATCH (f:File {{project_id: {project_id}, branch: {}}}) WHERE {by} RETURN {returned} LIMIT {limit}",
        gql_string(branch)
    )
}

fn gql_string(value: &str) -> String {
    format!("'{}'", value.replace('\\', "\\\\").replace('\'', "\\'"))
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
        let _ = writeln!(
            out,
            "{code} in {project}@{branch}: not found in the graph or the repository"
        );
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
