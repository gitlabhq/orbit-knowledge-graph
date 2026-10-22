use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;
use std::sync::LazyLock;

use anyhow::Result;
use regex::Regex;

use crate::database::GraphReader;
use crate::workspace::GitInfo;

#[derive(Debug, Default, PartialEq, Eq)]
pub(crate) struct Anchors {
    pub(crate) spans: Vec<(String, i64, i64)>,
    pub(crate) names: Vec<String>,
}

const DEFINITION_LINES: i64 = 60;
const WINDOW_LINES: i64 = 8;
const BUDGET_LINES: usize = 220;
const NAMED_DEFINITIONS: usize = 10;
const CALLERS_SHOWN: usize = 8;

static CHANGED_FILE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"(?m)^\s+[AMR] (\S+)(?: \(from \S+\))? @ (.+)$").expect("valid pattern")
});

const PATH: &str = r"[\w./@+-]*\w\.[A-Za-z][A-Za-z0-9]{0,7}";

static PATH_LINES: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(&format!(r#"({PATH})"?(?::|,? line )(\d+)"#)).expect("valid pattern")
});

pub(crate) fn local_path(
    root: Option<&Path>,
    raw: &str,
    files: &BTreeSet<String>,
) -> Option<String> {
    let known =
        |path: &str| files.contains(path) || root.is_some_and(|root| root.join(path).is_file());
    let segments: Vec<&str> = raw
        .split('/')
        .filter(|segment| !segment.is_empty() && *segment != ".")
        .collect();
    if let [name] = segments.as_slice() {
        if known(name) {
            return Some(name.to_string());
        }
        let suffix = format!("/{name}");
        let mut found = files.iter().filter(|file| file.ends_with(&suffix));
        return match (found.next(), found.next()) {
            (Some(only), None) => Some(only.clone()),
            _ => None,
        };
    }
    (0..segments.len() - 1)
        .map(|start| segments[start..].join("/"))
        .find(|candidate| known(candidate))
}

fn identifier_words(text: &str) -> Vec<String> {
    let code_span = Regex::new(r"`([^`\n]{2,80})`").expect("valid pattern");
    let word =
        Regex::new(r"[A-Za-z_][A-Za-z0-9_]*[?!]?(?:(?:::|\.|#)[A-Za-z_][A-Za-z0-9_]*[?!]?)*")
            .expect("valid pattern");
    let path =
        Regex::new(r"[A-Za-z0-9_./@+-]*/[A-Za-z0-9_./@+-]+|[A-Za-z0-9_@+-]+\.[A-Za-z0-9]{1,8}:\d+")
            .expect("valid pattern");
    let text = path.replace_all(text, " ");
    let text = text.as_ref();
    let mut names = Vec::new();
    let mut push = |token: &str| {
        for name in token.split(['.', '#', ':']).rev() {
            if !name.is_empty() && !names.iter().any(|known: &String| known == name) {
                names.push(name.to_string());
            }
        }
    };
    for span in code_span.captures_iter(text) {
        for found in word.find_iter(&span[1]) {
            push(found.as_str());
        }
    }
    names
}

pub(crate) fn extract(text: &str, root: &Path, files: &BTreeSet<String>) -> Anchors {
    let mut anchors = Anchors::default();
    let mut push = |path: String, start: i64, end: i64| {
        if start > 0 && end >= start && !anchors.spans.contains(&(path.clone(), start, end)) {
            anchors.spans.push((path, start, end));
        }
    };
    for found in PATH_LINES.captures_iter(text) {
        if let (Some(path), Ok(line)) = (
            local_path(Some(root), &found[1], files),
            found[2].parse::<i64>(),
        ) {
            push(path, line, line);
        }
    }
    for found in CHANGED_FILE.captures_iter(text) {
        let Some(path) = local_path(Some(root), &found[1], files) else {
            continue;
        };
        for range in found[2].split(", ") {
            let (start, end) = range.split_once('-').unwrap_or((range, range));
            if let (Ok(start), Ok(end)) = (start.parse(), end.parse()) {
                push(path.clone(), start, end);
            }
        }
    }
    anchors.names = identifier_words(text);
    anchors
}

pub(crate) fn indexed_files(client: &GraphReader, git: &GitInfo) -> Result<BTreeSet<String>> {
    let batches = client.query_arrow_json(
        "SELECT DISTINCT file_path FROM gl_definition WHERE project_id = ?1 AND commit_sha = ?2",
        &[git.project_id.into(), git.commit_sha.clone().into()],
    )?;
    Ok(duckdb_client::string_column(&batches, "file_path")
        .into_iter()
        .collect())
}

#[derive(Clone)]
struct Found {
    id: i64,
    path: String,
    fqn: String,
    kind: String,
    start: i64,
    end: i64,
}

fn definitions(
    client: &GraphReader,
    git: &GitInfo,
    sql: &str,
    extra: Vec<serde_json::Value>,
) -> Result<Vec<Found>> {
    let mut params: Vec<serde_json::Value> =
        vec![git.project_id.into(), git.commit_sha.clone().into()];
    params.extend(extra);
    let batches = client.query_arrow_json(
        &format!(
            "SELECT CAST(id AS BIGINT) AS id, file_path, name, fqn, definition_type,
       CAST(start_line AS BIGINT) AS start_line, CAST(end_line AS BIGINT) AS end_line
FROM gl_definition WHERE project_id = ?1 AND commit_sha = ?2 AND fqn NOT LIKE '%@%' AND {sql}"
        ),
        &params,
    )?;
    let ids = duckdb_client::i64_column(&batches, "id");
    let paths = duckdb_client::string_column(&batches, "file_path");
    let fqns = duckdb_client::string_column(&batches, "fqn");
    let kinds = duckdb_client::string_column(&batches, "definition_type");
    let starts = duckdb_client::i64_column(&batches, "start_line");
    let ends = duckdb_client::i64_column(&batches, "end_line");
    Ok((0..ids.len())
        .map(|i| Found {
            id: ids[i],
            path: paths[i].clone(),
            fqn: fqns[i].clone(),
            kind: kinds[i].clone(),
            start: starts[i],
            end: ends[i],
        })
        .collect())
}

struct Region {
    definition: Option<Found>,
    path: String,
    spans: Vec<(i64, i64)>,
}

impl Region {
    fn lines(&self) -> BTreeSet<i64> {
        let (low, high) = self
            .definition
            .as_ref()
            .map_or((1, i64::MAX), |def| (def.start, def.end));
        if let Some(def) = &self.definition
            && def.end - def.start < DEFINITION_LINES
        {
            return (def.start..=def.end).collect();
        }
        self.spans
            .iter()
            .flat_map(|(start, end)| {
                (start - WINDOW_LINES).max(low)..=(end + WINDOW_LINES).min(high)
            })
            .collect()
    }
}

fn regions(client: &GraphReader, git: &GitInfo, anchors: &Anchors) -> Result<Vec<Region>> {
    let mut order: Vec<(String, Option<i64>)> = Vec::new();
    let mut grouped: BTreeMap<(String, Option<i64>), Region> = BTreeMap::new();
    for (path, start, end) in &anchors.spans {
        let enclosing = definitions(
            client,
            git,
            "file_path = ?3 AND start_line <= ?4 AND end_line >= ?4 ORDER BY end_line - start_line LIMIT 1",
            vec![path.clone().into(), (*start).into()],
        )?
        .into_iter()
        .next();
        let key = (path.clone(), enclosing.as_ref().map(|def| def.id));
        if !grouped.contains_key(&key) {
            order.push(key.clone());
        }
        grouped
            .entry(key)
            .or_insert_with(|| Region {
                definition: enclosing,
                path: path.clone(),
                spans: Vec::new(),
            })
            .spans
            .push((*start, *end));
    }
    Ok(order
        .into_iter()
        .filter_map(|key| grouped.remove(&key))
        .collect())
}

fn callers(client: &GraphReader, id: i64) -> Result<Vec<String>> {
    let batches = client.query_arrow_json(
        "SELECT DISTINCT d.name AS name FROM gl_edge e
JOIN gl_definition d ON d.id = e.source_id
WHERE e.relationship_kind = 'CALLS' AND e.source_kind = 'Definition' AND e.target_id = ?1
  AND e.source_id <> e.target_id ORDER BY d.name",
        &[id.into()],
    )?;
    Ok(duckdb_client::string_column(&batches, "name"))
}

fn spans_label(spans: &[(i64, i64)]) -> String {
    spans
        .iter()
        .map(|(start, end)| match start == end {
            true => start.to_string(),
            false => format!("{start}-{end}"),
        })
        .collect::<Vec<_>>()
        .join(", ")
}

pub(crate) fn render(client: &GraphReader, git: &GitInfo, anchors: &Anchors) -> Result<String> {
    let mut out = String::new();
    let mut used = 0;
    let mut skipped = Vec::new();
    let mut shown = BTreeSet::new();
    for region in regions(client, git, anchors)? {
        let lines = region.lines();
        let label = spans_label(&region.spans);
        if used + lines.len() > BUDGET_LINES {
            skipped.push(format!("{}:{label}", region.path));
            continue;
        }
        let content = std::fs::read_to_string(git.repo_path.join(&region.path)).unwrap_or_default();
        let source: Vec<&str> = content.lines().collect();
        match &region.definition {
            Some(def) => {
                shown.insert(def.id);
                out.push_str(&format!(
                    "{}:{}-{}  {}  {}  (at {label})\n",
                    region.path, def.start, def.end, def.kind, def.fqn
                ));
            }
            None => out.push_str(&format!("{}  (at {label})\n", region.path)),
        }
        let mut last = None;
        for number in &lines {
            let Some(text) = usize::try_from(number - 1).ok().and_then(|i| source.get(i)) else {
                continue;
            };
            if last.is_some_and(|last| number - last > 1) {
                out.push_str("  ...\n");
            }
            out.push_str(&format!("{number}|{}\n", text.trim_end()));
            last = Some(*number);
            used += 1;
        }
        if let Some(def) = &region.definition {
            let names = callers(client, def.id)?;
            if !names.is_empty() {
                let more = names.len().saturating_sub(CALLERS_SHOWN);
                let mut listed = names
                    .into_iter()
                    .take(CALLERS_SHOWN)
                    .collect::<Vec<_>>()
                    .join(", ");
                if more > 0 {
                    listed.push_str(&format!(", +{more} more"));
                }
                out.push_str(&format!("» called by {listed}\n"));
            }
        }
        out.push('\n');
    }
    let mut named = 0;
    for name in &anchors.names {
        if named == NAMED_DEFINITIONS {
            break;
        }
        let found = definitions(client, git, "name = ?3 LIMIT 2", vec![name.clone().into()])?;
        if let [def] = found.as_slice()
            && shown.insert(def.id)
        {
            out.push_str(&format!(
                "{}:{}-{}  {}  {}\n",
                def.path, def.start, def.end, def.kind, def.fqn
            ));
            named += 1;
        }
    }
    if !skipped.is_empty() {
        out.push_str(&format!("» not shown: {}\n", skipped.join(" ")));
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn finds_lines_and_names_in_gitlab_text() {
        let dir = tempfile::tempdir().unwrap();
        let files: BTreeSet<String> = ["executors/docker/pull.go".to_string()].into();
        let files: BTreeSet<String> = files
            .into_iter()
            .chain(["app/models.py".to_string(), "index.js".to_string()])
            .collect();
        let anchors = extract(
            "pull.go:88: expected 3; call `retry_backoff`\n  File \"/srv/app/models.py\", line 12\n  node_modules/x/index.js:4\n  changed_files[1]:\n    M app/models.py @ 9, 16-23",
            dir.path(),
            &files,
        );
        assert_eq!(
            anchors.spans,
            [
                ("executors/docker/pull.go".to_string(), 88, 88),
                ("app/models.py".to_string(), 12, 12),
                ("app/models.py".to_string(), 9, 9),
                ("app/models.py".to_string(), 16, 23)
            ]
        );
        assert!(anchors.names.contains(&"retry_backoff".to_string()));
    }
}
