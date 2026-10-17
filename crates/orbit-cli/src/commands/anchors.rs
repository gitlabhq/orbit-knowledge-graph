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
/// Longer definitions show the lines around their anchor instead of the whole body.
const MAX_BODY_LINES: i64 = 120;
const AROUND_BEFORE: i64 = 15;
const AROUND_AFTER: i64 = 25;
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
    for found in word.find_iter(text) {
        let token = found.as_str();
        let shaped = token.split(['.', '#', ':']).any(|part| {
            let camel = part.chars().skip(1).any(char::is_uppercase)
                && part.chars().any(char::is_lowercase);
            let snake = part.contains('_') && part.chars().any(char::is_alphabetic);
            camel || snake
        });
        if shaped {
            push(token);
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

/// `context` targets for the anchors: the definition enclosing each line, then definitions named in
/// the text, at most [`MAX_DEFINITIONS`]. Long definitions become a range around their anchor.
pub(crate) fn targets(
    client: &GraphReader,
    git: &GitInfo,
    anchors: &Anchors,
) -> Result<Vec<String>> {
    let params =
        || -> Vec<serde_json::Value> { vec![git.project_id.into(), git.commit_sha.clone().into()] };
    let mut targets: Vec<String> = Vec::new();
    let mut seen_ids = BTreeSet::new();
    for (path, line) in &anchors.lines {
        if targets.len() >= MAX_DEFINITIONS {
            return Ok(targets);
        }
        let mut values = params();
        values.extend([path.clone().into(), (*line).into()]);
        let batches = client.query_arrow_json(
            "SELECT CAST(id AS BIGINT) AS id, CAST(start_line AS BIGINT) AS start_line,
       CAST(end_line AS BIGINT) AS end_line
FROM gl_definition
WHERE project_id = ?1 AND commit_sha = ?2 AND file_path = ?3 AND start_line <= ?4 AND end_line >= ?4
  AND fqn NOT LIKE '%@%'
ORDER BY end_line - start_line LIMIT 1",
            &values,
        )?;
        let (ids, starts, ends) = (
            duckdb_client::i64_column(&batches, "id"),
            duckdb_client::i64_column(&batches, "start_line"),
            duckdb_client::i64_column(&batches, "end_line"),
        );
        match (ids.first(), starts.first(), ends.first()) {
            (Some(id), Some(start), Some(end)) if end - start <= MAX_BODY_LINES => {
                if seen_ids.insert(*id) {
                    targets.push(format!("Definition:{id}"));
                }
            }
            _ => targets.push(format!(
                "{path}:{}-{}",
                (line - AROUND_BEFORE).max(1),
                line + AROUND_AFTER
            )),
        }
    }
    for name in &anchors.names {
        if targets.len() >= MAX_DEFINITIONS {
            break;
        }
        let mut values = params();
        values.push(name.clone().into());
        let batches = client.query_arrow_json(
            "SELECT CAST(id AS BIGINT) AS id, CAST(end_line - start_line AS BIGINT) AS span,
       COUNT(*) OVER () AS same_name
FROM gl_definition
WHERE project_id = ?1 AND commit_sha = ?2 AND name = ?3 AND fqn NOT LIKE '%@%'
ORDER BY (file_path LIKE '%test%' OR file_path LIKE '%spec%'), span LIMIT 1",
            &values,
        )?;
        let (ids, spans, counts) = (
            duckdb_client::i64_column(&batches, "id"),
            duckdb_client::i64_column(&batches, "span"),
            duckdb_client::i64_column(&batches, "same_name"),
        );
        if let (Some(id), Some(span), Some(count)) = (ids.first(), spans.first(), counts.first())
            && *count <= MAX_SAME_NAME
            && *span <= MAX_BODY_LINES
            && seen_ids.insert(*id)
        {
            targets.push(format!("Definition:{id}"));
        }
    }
    Ok(targets)
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
        assert!(anchors.names.contains(&"TestPullRetry".to_string()));
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
