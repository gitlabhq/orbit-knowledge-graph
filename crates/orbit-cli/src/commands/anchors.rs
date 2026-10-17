//! Code that GitLab text points at. An issue, MR, job log, or review thread names files, lines, and
//! symbols; this finds them in the local graph so `context` can return the code with the GitLab
//! context in one call.

use std::collections::BTreeSet;
use std::path::Path;

use anyhow::Result;
use regex::Regex;

use crate::database::GraphReader;
use crate::workspace::GitInfo;

/// Definitions shown per call; GitLab text can name dozens of symbols.
const MAX_DEFINITIONS: usize = 6;
/// Lines shown on each side of a cited line.
const AROUND: i64 = 2;
/// A name defined in more places than this is too ambiguous to guess at.
const MAX_SAME_NAME: i64 = 3;
const MIN_NAME_LEN: usize = 5;

#[derive(Debug, Default, PartialEq, Eq)]
pub(crate) struct Anchors {
    /// Repo-relative path and line, from stack traces, test output, and review anchors.
    pub(crate) lines: Vec<(String, i64)>,
    /// Symbol names, from code spans and identifier-shaped words.
    pub(crate) names: Vec<String>,
}

fn line_patterns() -> [Regex; 2] {
    [
        Regex::new(r"([A-Za-z0-9_./@+-]+\.[A-Za-z0-9]{1,8}):(\d+)").expect("valid pattern"),
        Regex::new(r#"File "([^"]+)", line (\d+)"#).expect("valid pattern"),
    ]
}

/// Paths in GitLab text may be absolute, `./`-prefixed, or a bare file name; keep those that exist.
fn local_path(root: &Path, raw: &str, files: &BTreeSet<String>) -> Option<String> {
    let trimmed = raw
        .trim_start_matches("./")
        .trim_start_matches("/app/")
        .trim_start_matches('/');
    if files.contains(trimmed) {
        return Some(trimmed.to_string());
    }
    if root.join(trimmed).is_file() {
        return Some(trimmed.to_string());
    }
    let suffix = format!("/{trimmed}");
    let mut found = files.iter().filter(|file| file.ends_with(&suffix));
    match (found.next(), found.next()) {
        (Some(only), None) => Some(only.clone()),
        _ => None,
    }
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
            if name.len() >= MIN_NAME_LEN && !names.iter().any(|known: &String| known == name) {
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
    for pattern in line_patterns() {
        for found in pattern.captures_iter(text) {
            let (Some(path), Ok(line)) =
                (local_path(root, &found[1], files), found[2].parse::<i64>())
            else {
                continue;
            };
            if line > 0 && !anchors.lines.contains(&(path.clone(), line)) {
                anchors.lines.push((path, line));
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

struct Found {
    id: i64,
    path: String,
    fqn: String,
    kind: String,
    start: i64,
    end: i64,
    /// The cited line, for anchors that came from a `file:line`.
    line: Option<i64>,
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
            line: None,
        })
        .collect())
}

/// The definition enclosing each cited line, then definitions named in code spans that are unique
/// enough to trust, at most [`MAX_DEFINITIONS`].
fn resolve(client: &GraphReader, git: &GitInfo, anchors: &Anchors) -> Result<Vec<Found>> {
    let mut found: Vec<Found> = Vec::new();
    for (path, line) in &anchors.lines {
        if found.len() >= MAX_DEFINITIONS {
            break;
        }
        let enclosing = definitions(
            client,
            git,
            "file_path = ?3 AND start_line <= ?4 AND end_line >= ?4 ORDER BY end_line - start_line LIMIT 1",
            vec![path.clone().into(), (*line).into()],
        )?;
        match enclosing.into_iter().next() {
            Some(def) if found.iter().any(|known| known.id == def.id) => {}
            Some(def) => found.push(Found {
                line: Some(*line),
                ..def
            }),
            None => found.push(Found {
                id: -1,
                path: path.clone(),
                fqn: String::new(),
                kind: String::new(),
                start: *line,
                end: *line,
                line: Some(*line),
            }),
        }
    }
    for name in &anchors.names {
        if found.len() >= MAX_DEFINITIONS {
            break;
        }
        let named = definitions(
            client,
            git,
            "name = ?3 ORDER BY (file_path LIKE '%test%' OR file_path LIKE '%spec%'), start_line LIMIT 4",
            vec![name.clone().into()],
        )?;
        if (named.len() as i64) <= MAX_SAME_NAME
            && let Some(def) = named.into_iter().next()
            && !found.iter().any(|known| known.id == def.id)
        {
            found.push(def);
        }
    }
    Ok(found)
}

fn neighbor_names(client: &GraphReader, sql: &str, id: i64) -> Result<(Vec<String>, i64)> {
    let batches = client.query_arrow_json(sql, &[id.into()])?;
    let names = duckdb_client::string_column(&batches, "name");
    let total = duckdb_client::i64_column(&batches, "total")
        .first()
        .copied()
        .unwrap_or(0);
    Ok((names, total))
}

fn listed(label: &str, (names, total): (Vec<String>, i64)) -> Option<String> {
    if names.is_empty() {
        return None;
    }
    let more = total - names.len() as i64;
    let suffix = if more > 0 {
        format!(" (+{more})")
    } else {
        String::new()
    };
    Some(format!("  {label}: {}{suffix}", names.join(", ")))
}

/// A compact view of the code GitLab text points at: per definition its header, signature, the
/// lines around a cited line, and its callers and callees by name. The agent reads the one it needs.
pub(crate) fn render(client: &GraphReader, git: &GitInfo, anchors: &Anchors) -> Result<String> {
    let mut out = String::new();
    for def in resolve(client, git, anchors)? {
        let content = std::fs::read_to_string(git.repo_path.join(&def.path)).unwrap_or_default();
        let lines: Vec<&str> = content.lines().collect();
        let show = |out: &mut String, number: i64| {
            if let Some(text) = usize::try_from(number - 1).ok().and_then(|i| lines.get(i)) {
                out.push_str(&format!("  {number}| {}\n", text.trim_end()));
            }
        };
        if def.id < 0 {
            out.push_str(&format!("{}:{}\n", def.path, def.start));
        } else {
            out.push_str(&format!(
                "{}:{}-{}  {}  {}\n",
                def.path, def.start, def.end, def.kind, def.fqn
            ));
            show(&mut out, def.start);
        }
        if let Some(line) = def.line {
            let from = (line - AROUND).max(def.start + 1).max(1);
            if from > def.start + 1 {
                out.push_str("  ...\n");
            }
            for number in from..=(line + AROUND).min(def.end.max(line + AROUND)) {
                show(&mut out, number);
            }
        }
        if def.id >= 0 {
            let callers = neighbor_names(
                client,
                "SELECT DISTINCT d.name AS name, COUNT(*) OVER () AS total FROM gl_edge e
JOIN gl_definition d ON d.id = e.source_id
WHERE e.relationship_kind = 'CALLS' AND e.source_kind = 'Definition' AND e.target_id = ?1
  AND e.source_id <> e.target_id LIMIT 6",
                def.id,
            )?;
            let callees = neighbor_names(
                client,
                "SELECT DISTINCT d.name AS name, COUNT(*) OVER () AS total FROM gl_edge e
JOIN gl_definition d ON d.id = e.target_id
WHERE e.relationship_kind = 'CALLS' AND e.target_kind = 'Definition' AND e.source_id = ?1
  AND e.source_id <> e.target_id LIMIT 6",
                def.id,
            )?;
            for row in [listed("called by", callers), listed("calls", callees)]
                .into_iter()
                .flatten()
            {
                out.push_str(&row);
                out.push('\n');
            }
        }
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn finds_lines_and_names_in_gitlab_text() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(dir.path().join("executors/docker")).unwrap();
        std::fs::write(dir.path().join("executors/docker/pull.go"), "x").unwrap();
        let files: BTreeSet<String> = [
            "executors/docker/pull.go".to_string(),
            "app/models/project.rb".to_string(),
        ]
        .into();
        let text = "--- FAIL: TestPullRetry\n    pull.go:88: expected 3\n\
                    File \"/app/app/models/project.rb\", line 12, in save\n\
                    We should call `retry_backoff` from `PullManager.Pull`, not loop.\n\
                    see executors/docker/pull.go:120 and nowhere.go:5";
        let anchors = extract(text, dir.path(), &files);
        assert_eq!(
            anchors.lines,
            [
                ("executors/docker/pull.go".to_string(), 88),
                ("executors/docker/pull.go".to_string(), 120),
                ("app/models/project.rb".to_string(), 12),
            ]
        );
        assert!(anchors.names.contains(&"retry_backoff".to_string()));
        assert!(anchors.names.contains(&"PullManager".to_string()));
        assert!(!anchors.names.contains(&"TestPullRetry".to_string()));
        assert!(!anchors.names.contains(&"should".to_string()));
        assert!(!anchors.names.contains(&"executors".to_string()));
        let ruby = extract(
            "`final_answer_streaming_cut_off?` in chat/step_executor.rb:95",
            dir.path(),
            &files,
        );
        assert_eq!(ruby.names, ["final_answer_streaming_cut_off?"]);
    }
}
