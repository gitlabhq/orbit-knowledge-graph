use std::collections::HashMap;

use anyhow::Result;
use duckdb_client::search::{path_scope, text_line_table};
use duckdb_client::{DuckDbClient, i64_column, string_column};

use crate::workspace::GitInfo;

const MAX_FILES: usize = 12;
const SNIPPETS_PER_FILE: usize = 3;
const SNIPPET_CHARS: usize = 50;
const LINE_CHARS: usize = 170;
const ROW_LIMIT: usize = 2000;
const VARIANT_MIN: usize = 3;
const PREFERRED_VARIANTS: &[&str] = &["en", "en-GB", "en-US", "en_US", "en_GB", "default"];

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct TextHit {
    pub(super) file: String,
    pub(super) line: usize,
    pub(super) text: String,
}

fn normalized(expr: &str) -> String {
    format!("lower(regexp_replace({expr}, '[_\\-\\s]', '', 'g'))")
}

pub(super) fn hits(
    client: &DuckDbClient,
    git: &GitInfo,
    alternatives: &[String],
    paths: &[String],
) -> Result<Vec<TextHit>> {
    let table = text_line_table(git.project_id);
    let present = i64_column(
        &client.query_arrow_json(
            "SELECT CAST(COUNT(*) AS BIGINT) AS n FROM duckdb_tables() WHERE table_name = ?1",
            &[table.clone().into()],
        )?,
        "n",
    );
    if present.first().copied().unwrap_or(0) == 0 || alternatives.is_empty() {
        return Ok(Vec::new());
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
    let batches = client.query_arrow_json(
        &format!(
            "SELECT file_path, line_no, text FROM {table}
WHERE commit_sha = ?1 AND ({filter})
{scope}ORDER BY file_path, line_no
LIMIT {ROW_LIMIT}",
            scope = path_scope("file_path", paths, true),
        ),
        &params,
    )?;
    let files = string_column(&batches, "file_path");
    let lines = i64_column(&batches, "line_no");
    let texts = string_column(&batches, "text");
    Ok((0..files.len())
        .map(|i| TextHit {
            file: files[i].clone(),
            line: lines[i] as usize,
            text: texts[i].trim().to_string(),
        })
        .collect())
}

fn group(hits: &[TextHit]) -> Vec<(String, Vec<&TextHit>)> {
    let mut files: Vec<(String, Vec<&TextHit>)> = Vec::new();
    for hit in hits {
        match files.iter_mut().find(|(file, _)| *file == hit.file) {
            Some((_, list)) => list.push(hit),
            None => files.push((hit.file.clone(), vec![hit])),
        }
    }
    collapse_variants(files)
}

fn collapse_variants(files: Vec<(String, Vec<&TextHit>)>) -> Vec<(String, Vec<&TextHit>)> {
    let mut templates: HashMap<String, Vec<usize>> = HashMap::new();
    for (index, (file, _)) in files.iter().enumerate() {
        let parts: Vec<&str> = file.split('/').collect();
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
    for (template, members) in groups {
        let mut by_shape: HashMap<Vec<usize>, Vec<usize>> = HashMap::new();
        for index in members.into_iter().filter(|i| !taken[*i]) {
            let shape = files[index].1.iter().map(|hit| hit.line).collect();
            by_shape.entry(shape).or_default().push(index);
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
                .0
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
        labels.insert(
            pick,
            template.replacen(
                '*',
                &format!("{{{},+{}}}", variant(pick), same.len() - 1),
                1,
            ),
        );
    }
    files
        .into_iter()
        .enumerate()
        .filter_map(|(index, (file, list))| match labels.remove(&index) {
            Some(label) => Some((label, list)),
            None if taken[index] => None,
            None => Some((file, list)),
        })
        .collect()
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

pub(super) fn render(hits: &[TextHit]) -> String {
    let files = group(hits);
    if files.is_empty() {
        return String::new();
    }
    let total: usize = files.iter().map(|(_, list)| list.len()).sum();
    let mut out = format!(
        "\nAlso in config, templates, and docs — {total} lines in {} files:\n",
        files.len()
    );
    for (file, list) in files.iter().take(MAX_FILES) {
        let mut parts: Vec<String> = list
            .iter()
            .take(SNIPPETS_PER_FILE)
            .map(|hit| format!(":{} {}", hit.line, snippet(&hit.text)))
            .collect();
        if list.len() > SNIPPETS_PER_FILE {
            parts.push(format!("+{}", list.len() - SNIPPETS_PER_FILE));
        }
        out.push_str(&clip(format!("  {file}   {}", parts.join("  "))));
        out.push('\n');
    }
    if files.len() > MAX_FILES {
        out.push_str(&format!(
            "  … {} more files. Narrow with --path.\n",
            files.len() - MAX_FILES
        ));
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn hit(file: &str, line: usize, text: &str) -> TextHit {
        TextHit {
            file: file.into(),
            line,
            text: text.into(),
        }
    }

    #[test]
    fn files_list_line_snippets_and_count_the_rest() {
        let hits = vec![
            hit("install/data/defaults.json", 130, "\"maintenanceMode\": 0,"),
            hit(
                "install/data/defaults.json",
                131,
                "\"maintenanceModeStatus\": 503,",
            ),
            hit("docs/a.md", 1, "a"),
            hit("docs/a.md", 2, "b"),
            hit("docs/a.md", 3, "c"),
            hit("docs/a.md", 4, "d"),
        ];
        assert_eq!(
            render(&hits),
            "\nAlso in config, templates, and docs — 6 lines in 2 files:\n  \
             install/data/defaults.json   :130 \"maintenanceMode\": 0,  :131 \"maintenanceModeStatus\": 503,\n  \
             docs/a.md   :1 a  :2 b  :3 c  +1\n"
        );
    }

    #[test]
    fn locale_copies_collapse_preferring_english_but_distinct_files_do_not() {
        let mut hits: Vec<TextHit> = ["ar", "de", "en-GB", "fr"]
            .iter()
            .map(|locale| {
                hit(
                    &format!("public/language/{locale}/advanced.json"),
                    2,
                    "\"maintenance-mode\": \"x\"",
                )
            })
            .collect();
        hits.push(hit("src/api/users.yaml", 3, "x"));
        hits.push(hit("src/privileges/users.yaml", 9, "x"));
        hits.push(hit("src/write/users.yaml", 14, "x"));
        let names: Vec<String> = group(&hits).into_iter().map(|(file, _)| file).collect();
        assert_eq!(
            names,
            [
                "public/language/{en-GB,+3}/advanced.json",
                "src/api/users.yaml",
                "src/privileges/users.yaml",
                "src/write/users.yaml",
            ]
        );
    }

    #[test]
    fn nothing_to_report_renders_nothing() {
        assert_eq!(render(&[]), "");
    }
}
