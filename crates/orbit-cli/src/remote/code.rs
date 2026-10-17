//! Reads another project's code from the graph: `symbol:` returns indexed definitions with their
//! source, `file:` returns a file's content, optionally a line range. Content comes from Gitaly
//! through the graph, so no checkout is needed.

use std::fmt::Write as _;

use serde_json::{Value, json};

use super::client::OrbitClient;
use super::error::{EXIT_GENERIC, RemoteError};

const MAX_DEFINITIONS: usize = 5;
const MAX_FILES: usize = 3;
const MAX_FILE_LINES: usize = 400;

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
        "limit": MAX_DEFINITIONS,
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
        "limit": MAX_FILES,
    })
}

fn numbered(out: &mut String, content: &str, first: usize, range: Option<(usize, usize)>) {
    let (start, end) = range.unwrap_or((first, first + MAX_FILE_LINES - 1));
    let mut shown = 0;
    for (offset, line) in content.lines().enumerate() {
        let number = first + offset;
        if number < start || number > end {
            continue;
        }
        let _ = writeln!(out, "{number}|{line}");
        shown += 1;
    }
    let total = content.lines().count();
    if range.is_none() && total > shown {
        let _ = writeln!(
            out,
            "... {} more lines; add a range, e.g. file:path:{}-{}",
            total - shown,
            first + shown,
            first + shown + 199
        );
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_file_ranges() {
        assert_eq!(
            Code::parse("file", "a/b.go:10-20"),
            Code::File {
                path: "a/b.go".into(),
                lines: Some((10, 20))
            }
        );
        assert_eq!(
            Code::parse("path", "a/b.go:7"),
            Code::File {
                path: "a/b.go".into(),
                lines: Some((7, 7))
            }
        );
        assert_eq!(
            Code::parse("file", "a/b.go"),
            Code::File {
                path: "a/b.go".into(),
                lines: None
            }
        );
        assert_eq!(
            Code::parse("def", "Evaluate"),
            Code::Symbol("Evaluate".into())
        );
    }

    #[test]
    fn qualified_symbols_match_on_fqn() {
        let query = definition_query(5, "main", "policy.New");
        assert_eq!(
            query["nodes"][0]["filters"]["fqn"]["ends_with"],
            "policy.New"
        );
        let query = definition_query(5, "main", "New");
        assert_eq!(query["nodes"][0]["filters"]["name"], "New");
    }

    #[test]
    fn renders_definitions_and_file_ranges() {
        let definition = json!({"fqn": "pkg.Evaluate", "definition_type": "Function", "file_path": "x.go",
            "start_line": 10, "end_line": 11, "content": "func Evaluate() {\n}"});
        let text = render(
            "g/lib",
            "main",
            &Code::Symbol("Evaluate".into()),
            &[definition],
        );
        assert_eq!(
            text,
            "g/lib/x.go:10-11 Function pkg.Evaluate\n10|func Evaluate() {\n11|}\n"
        );
        let file = json!({"path": "x.go", "content": "a\nb\nc\nd"});
        let code = Code::parse("file", "x.go:2-3");
        assert_eq!(
            render("g/lib", "main", &code, &[file]),
            "g/lib/x.go\n2|b\n3|c\n"
        );
        assert_eq!(
            render("g/lib", "main", &code, &[]),
            "file:x.go:2-3 in g/lib@main: not found in the graph\n"
        );
    }
}
