use std::collections::{HashMap, HashSet};

use anyhow::Result;
use duckdb_client::search::{path_scope, text_line_table};
use duckdb_client::{DuckDbClient, i64_column, string_column};

use crate::workspace::GitInfo;

const MAX_FILES: usize = 12;
const SNIPPETS_PER_FILE: usize = 3;
const SNIPPETS_FETCHED: usize = 12;
const SNIPPET_CHARS: usize = 50;
const LINE_CHARS: usize = 170;
const FILE_LIMIT: usize = 400;
const VARIANT_MIN: usize = 3;
const PREFERRED_VARIANTS: &[&str] = &["en", "en-GB", "en-US", "en_US", "en_GB", "default"];

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct FileHits {
    pub(super) file: String,
    pub(super) lines: Vec<usize>,
    pub(super) snippets: Vec<(usize, String)>,
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub(super) struct Mentions {
    pub(super) total_lines: usize,
    pub(super) total_files: usize,
    pub(super) files: Vec<FileHits>,
    pub(super) unmatched: Vec<String>,
}

fn normalized(expr: &str) -> String {
    format!("lower(regexp_replace({expr}, '[_\\-\\s]', '', 'g'))")
}

pub(super) fn mentions(
    client: &DuckDbClient,
    git: &GitInfo,
    alternatives: &[String],
    paths: &[String],
) -> Result<Mentions> {
    let table = text_line_table(git.project_id);
    let present = i64_column(
        &client.query_arrow_json(
            "SELECT CAST(COUNT(*) AS BIGINT) AS n FROM duckdb_tables() WHERE table_name = ?1",
            &[table.clone().into()],
        )?,
        "n",
    );
    if present.first().copied().unwrap_or(0) == 0 || alternatives.is_empty() {
        return Ok(Mentions::default());
    }
    let filter = (0..alternatives.len())
        .map(|i| {
            format!(
                "contains({}, {})",
                normalized("text"),
                normalized(&format!("?{}", i + 2))
            )
        })
        .collect::<Vec<_>>()
        .join(" OR ");
    let params: Vec<serde_json::Value> = std::iter::once(git.commit_sha.clone().into())
        .chain(alternatives.iter().map(|a| a.clone().into()))
        .collect();
    let scope = path_scope("file_path", paths, true);
    let matched = format!(
        "SELECT file_path, line_no, text FROM {table} WHERE commit_sha = ?1 AND ({filter})\n{scope}"
    );
    let totals = client.query_arrow_json(
        &format!(
            "SELECT CAST(COUNT(*) AS BIGINT) AS lines, CAST(COUNT(DISTINCT file_path) AS BIGINT) AS files{per_term} FROM ({matched})",
            per_term = (0..alternatives.len())
                .map(|i| format!(
                    ", CAST(COUNT(*) FILTER (WHERE contains({}, {})) AS BIGINT) AS t{i}",
                    normalized("text"),
                    normalized(&format!("?{}", i + 2))
                ))
                .collect::<String>()
        ),
        &params,
    )?;
    let batches = client.query_arrow_json(
        &format!(
            "SELECT file_path, CAST(COUNT(*) AS BIGINT) AS n,
       list(line_no ORDER BY line_no) AS lines,
       list_slice(list(text ORDER BY line_no), 1, {SNIPPETS_FETCHED}) AS texts
FROM ({matched})
GROUP BY file_path
ORDER BY n DESC, file_path
LIMIT {FILE_LIMIT}"
        ),
        &params,
    )?;
    let files = string_column(&batches, "file_path");
    let lines = list_i64_column(&batches, "lines");
    let texts = list_string_column(&batches, "texts");
    Ok(Mentions {
        total_lines: i64_column(&totals, "lines").first().copied().unwrap_or(0) as usize,
        total_files: i64_column(&totals, "files").first().copied().unwrap_or(0) as usize,
        unmatched: alternatives
            .iter()
            .enumerate()
            .filter(|(i, _)| i64_column(&totals, &format!("t{i}")).first() == Some(&0))
            .map(|(_, alternative)| alternative.clone())
            .collect(),
        files: (0..files.len())
            .map(|i| {
                let numbers: Vec<usize> = lines[i].iter().map(|n| *n as usize).collect();
                FileHits {
                    file: files[i].clone(),
                    snippets: numbers
                        .iter()
                        .copied()
                        .zip(texts[i].iter().map(|t| t.trim().to_string()))
                        .collect(),
                    lines: numbers,
                }
            })
            .collect(),
    })
}

fn list_i64_column(batches: &[arrow::record_batch::RecordBatch], name: &str) -> Vec<Vec<i64>> {
    use arrow::array::{Array, Int64Array, ListArray};
    let mut out = Vec::new();
    for batch in batches {
        let Some(column) = batch.column_by_name(name) else {
            continue;
        };
        let Some(lists) = column.as_any().downcast_ref::<ListArray>() else {
            continue;
        };
        for i in 0..lists.len() {
            let values = lists.value(i);
            let values = values.as_any().downcast_ref::<Int64Array>();
            out.push(values.map_or_else(Vec::new, |v| v.iter().flatten().collect()));
        }
    }
    out
}

fn list_string_column(
    batches: &[arrow::record_batch::RecordBatch],
    name: &str,
) -> Vec<Vec<String>> {
    use arrow::array::{Array, ListArray, StringArray};
    let mut out = Vec::new();
    for batch in batches {
        let Some(column) = batch.column_by_name(name) else {
            continue;
        };
        let Some(lists) = column.as_any().downcast_ref::<ListArray>() else {
            continue;
        };
        for i in 0..lists.len() {
            let values = lists.value(i);
            let values = values.as_any().downcast_ref::<StringArray>();
            out.push(
                values.map_or_else(Vec::new, |v| v.iter().flatten().map(String::from).collect()),
            );
        }
    }
    out
}

fn collapse_variants(files: Vec<FileHits>) -> (Vec<(String, FileHits)>, usize, usize) {
    let mut templates: HashMap<String, Vec<usize>> = HashMap::new();
    for (index, hits) in files.iter().enumerate() {
        let parts: Vec<&str> = hits.file.split('/').collect();
        for slot in 0..parts.len().saturating_sub(1) {
            let mut key = parts.clone();
            key[slot] = "*";
            templates.entry(key.join("/")).or_default().push(index);
        }
    }
    let mut groups: Vec<(String, Vec<usize>)> = templates
        .into_iter()
        .filter(|(_, members)| members.len() >= VARIANT_MIN)
        .collect();
    groups.sort_by(|a, b| b.1.len().cmp(&a.1.len()).then(a.0.cmp(&b.0)));
    let mut taken = vec![false; files.len()];
    let mut labels: HashMap<usize, String> = HashMap::new();
    let mut merged = 0;
    let mut merged_lines = 0;
    for (template, members) in groups {
        let mut by_shape: HashMap<Vec<usize>, Vec<usize>> = HashMap::new();
        for index in members.into_iter().filter(|i| !taken[*i]) {
            by_shape
                .entry(files[index].lines.clone())
                .or_default()
                .push(index);
        }
        let Some(same) = by_shape.into_values().max_by_key(|group| group.len()) else {
            continue;
        };
        if same.len() < VARIANT_MIN {
            continue;
        }
        let slot = template
            .split('/')
            .position(|part| part == "*")
            .unwrap_or(0);
        let variant = |index: usize| {
            files[index]
                .file
                .split('/')
                .nth(slot)
                .unwrap_or("")
                .to_string()
        };
        let pick = same
            .iter()
            .copied()
            .min_by_key(|index| {
                PREFERRED_VARIANTS
                    .iter()
                    .position(|preferred| *preferred == variant(*index))
                    .unwrap_or(usize::MAX)
            })
            .unwrap_or(same[0]);
        for index in &same {
            taken[*index] = true;
        }
        merged += same.len() - 1;
        merged_lines += (same.len() - 1) * files[pick].lines.len();
        labels.insert(
            pick,
            template.replacen(
                '*',
                &format!("{{{},+{}}}", variant(pick), same.len() - 1),
                1,
            ),
        );
    }
    let rows = files
        .into_iter()
        .enumerate()
        .filter_map(|(index, hits)| match labels.remove(&index) {
            Some(label) => Some((label, hits)),
            None if taken[index] => None,
            None => Some((hits.file.clone(), hits)),
        })
        .collect();
    (rows, merged, merged_lines)
}

fn snippet(text: &str) -> String {
    let text = text.split_whitespace().collect::<Vec<_>>().join(" ");
    match text.chars().count() > SNIPPET_CHARS {
        true => format!("{}…", text.chars().take(SNIPPET_CHARS).collect::<String>()),
        false => text,
    }
}

fn clip(line: String) -> String {
    match line.chars().count() > LINE_CHARS {
        true => format!("{}…", line.chars().take(LINE_CHARS).collect::<String>()),
        false => line,
    }
}

pub(super) fn render(mentions: &Mentions, shown: &HashSet<(String, usize)>) -> String {
    let mut fresh = Vec::new();
    let (mut skipped_lines, mut skipped_files) = (0, 0);
    for hits in &mentions.files {
        let lines: Vec<usize> = hits
            .lines
            .iter()
            .copied()
            .filter(|line| !shown.contains(&(hits.file.clone(), *line)))
            .collect();
        skipped_lines += hits.lines.len() - lines.len();
        if lines.is_empty() {
            skipped_files += 1;
            continue;
        }
        let snippets = hits
            .snippets
            .iter()
            .filter(|(line, _)| lines.contains(line))
            .take(SNIPPETS_PER_FILE)
            .cloned()
            .collect();
        fresh.push(FileHits {
            file: hits.file.clone(),
            lines,
            snippets,
        });
    }
    if fresh.is_empty() {
        return String::new();
    }
    let (rows, merged, merged_lines) = collapse_variants(fresh);
    let files = mentions.total_files.saturating_sub(merged + skipped_files);
    let lines = mentions
        .total_lines
        .saturating_sub(merged_lines + skipped_lines);
    let mut out = format!("\nMentions — {lines} lines in {files} files:\n");
    for (label, hits) in rows.iter().take(MAX_FILES) {
        let mut parts: Vec<String> = hits
            .snippets
            .iter()
            .map(|(line, text)| format!(":{line} {}", snippet(text)))
            .collect();
        if hits.lines.len() > hits.snippets.len() {
            parts.push(format!("+{}", hits.lines.len() - hits.snippets.len()));
        }
        out.push_str(&clip(format!("  {label}   {}", parts.join("  "))));
        out.push('\n');
    }
    if files > MAX_FILES.min(rows.len()) {
        out.push_str(&format!(
            "  … {} more files. Narrow with --path.\n",
            files - MAX_FILES.min(rows.len())
        ));
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn file(path: &str, lines: &[usize], text: &str) -> FileHits {
        FileHits {
            file: path.into(),
            lines: lines.to_vec(),
            snippets: lines
                .iter()
                .take(SNIPPETS_PER_FILE)
                .map(|line| (*line, text.to_string()))
                .collect(),
        }
    }

    #[test]
    fn header_reports_exact_totals_and_extra_lines_are_counted() {
        let mentions = Mentions {
            total_lines: 7,
            total_files: 2,
            unmatched: Vec::new(),
            files: vec![
                file("docs/a.md", &[1, 2, 3, 4, 5], "x"),
                file(
                    "install/data/defaults.json",
                    &[130, 131],
                    "\"maintenanceMode\": 0,",
                ),
            ],
        };
        assert_eq!(
            render(&mentions, &HashSet::new()),
            "\nMentions — 7 lines in 2 files:\n  \
             docs/a.md   :1 x  :2 x  :3 x  +2\n  \
             install/data/defaults.json   :130 \"maintenanceMode\": 0,  :131 \"maintenanceMode\": 0,\n"
        );
    }

    #[test]
    fn locale_copies_collapse_preferring_english_but_distinct_files_do_not() {
        let mut files: Vec<FileHits> = ["ar", "de", "en-GB", "fr"]
            .iter()
            .map(|locale| {
                file(
                    &format!("public/language/{locale}/advanced.json"),
                    &[2],
                    "x",
                )
            })
            .collect();
        files.push(file("src/api/users.yaml", &[3], "x"));
        files.push(file("src/privileges/users.yaml", &[9], "x"));
        files.push(file("src/write/users.yaml", &[14], "x"));
        let (rows, merged, merged_lines) = collapse_variants(files);
        let names: Vec<&str> = rows.iter().map(|(label, _)| label.as_str()).collect();
        assert_eq!(
            names,
            [
                "public/language/{en-GB,+3}/advanced.json",
                "src/api/users.yaml",
                "src/privileges/users.yaml",
                "src/write/users.yaml",
            ]
        );
        assert_eq!((merged, merged_lines), (3, 3));
    }

    #[test]
    fn more_files_than_shown_says_how_many() {
        let mentions = Mentions {
            total_lines: 30,
            total_files: 30,
            unmatched: Vec::new(),
            files: (0..30)
                .map(|n| file(&format!("docs/f{n}.md"), &[n + 1], "x"))
                .collect(),
        };
        assert!(
            render(&mentions, &HashSet::new())
                .ends_with("  … 18 more files. Narrow with --path.\n")
        );
    }

    #[test]
    fn nothing_to_report_renders_nothing() {
        assert_eq!(render(&Mentions::default(), &HashSet::new()), "");
    }
}
