use std::collections::HashMap;

use anyhow::Result;
use duckdb_client::search::{NodeHydrator, path_scope, text_line_table};
use duckdb_client::{DuckDbClient, i64_column, sql_lit, string_column};

use crate::workspace::GitInfo;

const VARIANT_MIN: usize = 3;
const PREFERRED_VARIANTS: &[&str] = &["en", "en-GB", "en-US", "en_US", "en_GB", "default"];
const TOP_SOURCE_LINES: usize = 80;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct Def {
    pub(super) name: String,
    pub(super) start: usize,
    pub(super) end: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct Hit {
    pub(super) file: String,
    pub(super) line: usize,
    pub(super) text: String,
    pub(super) def: Option<Def>,
}

fn normalized(expr: &str) -> String {
    format!("lower(regexp_replace({expr}, '[_\\-\\s]', '', 'g'))")
}

pub(super) struct Term {
    pub(super) raw: String,
    regex: Option<(regex::Regex, regex::Regex)>,
}

impl Term {
    pub(super) fn parse(raw: &str) -> Self {
        let pattern = raw.contains([
            '\\', '.', '*', '+', '?', '(', ')', '[', ']', '{', '}', '^', '$',
        ]);
        let regex = pattern
            .then(|| {
                let line = regex::Regex::new(&format!("(?i){raw}")).ok()?;
                let whole = regex::Regex::new(&format!("(?i)^(?:{raw})$")).ok()?;
                Some((line, whole))
            })
            .flatten();
        Self {
            raw: raw.to_string(),
            regex,
        }
    }

    pub(super) fn literal(&self) -> Self {
        Self {
            raw: self.raw.clone(),
            regex: None,
        }
    }

    fn sql(&self, column: &str, param: &str) -> String {
        match self.regex {
            Some(_) => format!("regexp_matches({column}, {param})"),
            None => format!("contains({}, {})", normalized(column), normalized(param)),
        }
    }

    fn sql_name(&self, column: &str, param: &str) -> String {
        match self.regex {
            Some(_) => format!("regexp_full_match({column}, {param})"),
            None => format!("{} = {}", normalized(column), normalized(param)),
        }
    }

    fn param(&self) -> String {
        match self.regex {
            Some(_) => format!("(?i){}", self.raw),
            None => self.raw.clone(),
        }
    }

    fn matches(&self, text: &str) -> bool {
        match &self.regex {
            Some((line, _)) => line.is_match(text),
            None => compact(text).contains(&compact(&self.raw)),
        }
    }

    fn names(&self, name: &str) -> bool {
        match &self.regex {
            Some((_, whole)) => whole.is_match(name),
            None => compact(name) == compact(&self.raw),
        }
    }
}

fn compact(text: &str) -> String {
    text.chars()
        .filter(|c| !(c.is_whitespace() || *c == '_' || *c == '-'))
        .flat_map(char::to_lowercase)
        .collect()
}

pub(super) fn hits(
    client: &DuckDbClient,
    git: &GitInfo,
    alternatives: &[Term],
    paths: &[String],
    kinds: &[String],
) -> Result<Vec<Hit>> {
    let table = text_line_table(git.project_id);
    let present = client.query_arrow_json(
        "SELECT CAST(COUNT(*) AS BIGINT) AS n FROM duckdb_tables() WHERE table_name = ?1",
        &[table.clone().into()],
    )?;
    if i64_column(&present, "n").first().copied().unwrap_or(0) == 0 || alternatives.is_empty() {
        return Ok(Vec::new());
    }
    let def = NodeHydrator::embedded("Definition")?;
    let filter = alternatives
        .iter()
        .enumerate()
        .map(|(i, term)| term.sql("h.text", &format!("?{}", i + 2)))
        .collect::<Vec<_>>()
        .join(" OR ");
    let project = alternatives.len() + 2;
    let kind_filter = match kinds.is_empty() {
        true => String::new(),
        false => format!(
            "WHERE lower(kind) IN ({})",
            kinds
                .iter()
                .map(|k| sql_lit(&k.to_lowercase()))
                .collect::<Vec<_>>()
                .join(", ")
        ),
    };
    let mut params: Vec<serde_json::Value> = std::iter::once(git.commit_sha.clone().into())
        .chain(alternatives.iter().map(|a| a.param().into()))
        .collect();
    params.push(git.project_id.into());
    let batches = client.query_arrow_json(
        &format!(
            "SELECT * FROM (
  SELECT h.file_path, h.line_no, h.text, d.{name} AS def, d.{kind} AS kind,
         CAST(d.{start} AS BIGINT) AS def_start, CAST(d.{end} AS BIGINT) AS def_end
  FROM {table} h
  LEFT JOIN {dtable} d ON d.{project_col} = ?{project} AND d.{commit} = ?1
    AND d.{file} = h.file_path AND h.line_no BETWEEN d.{start} AND d.{end}
    AND d.{fqn} NOT LIKE '%@%'
  WHERE h.commit_sha = ?1 AND ({filter})
  {scope}QUALIFY row_number() OVER (
    PARTITION BY h.file_path, h.line_no
    ORDER BY (d.{start} = h.line_no AND ({named})) DESC, d.{end} > d.{start} DESC,
             d.{end} - d.{start} NULLS LAST, d.{id}
  ) = 1
) {kind_filter}
ORDER BY file_path, line_no",
            name = def.column("name")?,
            kind = def.column("definition_type")?,
            start = def.column("start_line")?,
            end = def.column("end_line")?,
            dtable = def.table(),
            project_col = def.column("project_id")?,
            commit = def.column("commit_sha")?,
            file = def.column("file_path")?,
            fqn = def.column("fqn")?,
            id = def.column("id")?,
            scope = path_scope("h.file_path", paths, true),
            named = alternatives
                .iter()
                .enumerate()
                .map(|(i, term)| term.sql_name(
                    &format!("d.{}", def.column("name").unwrap_or("name")),
                    &format!("?{}", i + 2)
                ))
                .collect::<Vec<_>>()
                .join(" OR "),
        ),
        &params,
    )?;
    let files = string_column(&batches, "file_path");
    let lines = i64_column(&batches, "line_no");
    let texts = string_column(&batches, "text");
    let names = optional_strings(&batches, "def");
    let starts = optional_i64s(&batches, "def_start");
    let ends = optional_i64s(&batches, "def_end");
    Ok((0..files.len())
        .map(|i| Hit {
            file: files[i].clone(),
            line: lines[i] as usize,
            text: texts[i].trim().to_string(),
            def: match (&names[i], starts[i], ends[i]) {
                (Some(name), Some(start), Some(end)) => Some(Def {
                    name: name.clone(),
                    start: start as usize,
                    end: end as usize,
                }),
                _ => None,
            },
        })
        .collect())
}

fn optional_strings(
    batches: &[arrow::record_batch::RecordBatch],
    name: &str,
) -> Vec<Option<String>> {
    use arrow::array::{Array, StringArray};
    batches
        .iter()
        .filter_map(|b| b.column_by_name(name))
        .flat_map(|column| {
            let values = column.as_any().downcast_ref::<StringArray>();
            (0..column.len())
                .map(|i| {
                    values
                        .filter(|v| !v.is_null(i))
                        .map(|v| v.value(i).to_string())
                })
                .collect::<Vec<_>>()
        })
        .collect()
}

fn optional_i64s(batches: &[arrow::record_batch::RecordBatch], name: &str) -> Vec<Option<i64>> {
    use arrow::array::{Array, Int64Array};
    batches
        .iter()
        .filter_map(|b| b.column_by_name(name))
        .flat_map(|column| {
            let values = column.as_any().downcast_ref::<Int64Array>();
            (0..column.len())
                .map(|i| values.filter(|v| !v.is_null(i)).map(|v| v.value(i)))
                .collect::<Vec<_>>()
        })
        .collect()
}

fn is_code(path: &str) -> bool {
    path.rsplit_once('.').is_some_and(|(_, ext)| {
        orbit_search::corpus::DEFAULT_SOURCE_EXTS.contains(&ext.to_ascii_lowercase().as_str())
    })
}

fn is_test(path: &str) -> bool {
    static TEST: std::sync::LazyLock<regex::Regex> = std::sync::LazyLock::new(|| {
        regex::Regex::new(
            r"(^|/)(tests?|spec|__tests__|fixtures?|scenarios|testdata|mocks?)/|(^|/)[\w-]*tests?[\w-]*/|(^|/)test_[^/]*$|_test\.|\.test\.|\.spec\.|-test\.",
        )
        .expect("valid test path regex")
    });
    TEST.is_match(path)
}

fn names(hit: &Hit, alternatives: &[Term]) -> bool {
    hit.def
        .as_ref()
        .is_some_and(|def| def.start == hit.line && alternatives.iter().any(|a| a.names(&def.name)))
}

fn assigns(hit: &Hit, alternatives: &[Term]) -> bool {
    alternatives
        .iter()
        .filter(|t| t.regex.is_none())
        .any(|term| {
            regex::Regex::new(&format!(
                r"(?i)^\s*(?:(?:const|let|var)\s+)?(?:[\w$]+\.)*{}\s*=[^=]",
                regex::escape(term.raw.trim())
            ))
            .is_ok_and(|re| re.is_match(&hit.text))
        })
}

fn defining_rank(hit: &Hit, alternatives: &[Term]) -> u8 {
    match (names(hit, alternatives), assigns(hit, alternatives)) {
        (true, _) => 0,
        (false, true) => 1,
        (false, false) => 2,
    }
}

const SEP: &str = " │ ";
const FULL_LINES: usize = 3;
const LINE_CHARS: usize = 160;

fn located(lines: &[&Hit]) -> String {
    let mut parts: Vec<String> = lines
        .iter()
        .take(FULL_LINES)
        .map(|h| match h.text.chars().count() > LINE_CHARS {
            true => format!(
                ":{} {}…",
                h.line,
                h.text.chars().take(LINE_CHARS).collect::<String>()
            ),
            false => format!(":{} {}", h.line, h.text),
        })
        .collect();
    let rest: Vec<usize> = lines.iter().skip(FULL_LINES).map(|h| h.line).collect();
    if !rest.is_empty() {
        parts.push(runs(&rest));
    }
    parts.join(SEP)
}

fn runs(lines: &[usize]) -> String {
    let mut parts = Vec::new();
    let mut index = 0;
    while index < lines.len() {
        let mut end = index;
        while end + 1 < lines.len() && lines[end + 1] == lines[end] + 1 {
            end += 1;
        }
        parts.push(match end > index {
            true => format!(":{}-{}", lines[index], lines[end]),
            false => format!(":{}", lines[index]),
        });
        index = end + 1;
    }
    parts.join(" ")
}

fn code_row(hits: &[&Hit]) -> String {
    let mut groups: Vec<(Option<&Def>, Vec<&Hit>)> = Vec::new();
    for hit in hits {
        match groups.last_mut() {
            Some((def, list)) if *def == hit.def.as_ref() => list.push(hit),
            _ => groups.push((hit.def.as_ref(), vec![hit])),
        }
    }
    groups
        .iter()
        .map(|(def, list)| {
            let label = def.map(|d| match d.end > d.start {
                true => format!("{}:{}-{} ", d.name, d.start, d.end),
                false => format!("{} ", d.name),
            });
            format!("{}{}", label.unwrap_or_default(), located(list))
        })
        .collect::<Vec<_>>()
        .join(SEP)
}

fn collapse_variants(files: Vec<(String, Vec<&Hit>)>) -> (Vec<(String, Vec<&Hit>)>, usize, usize) {
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
    let (mut merged_files, mut merged_lines) = (0, 0);
    for (template, members) in groups {
        let mut by_shape: HashMap<Vec<usize>, Vec<usize>> = HashMap::new();
        for index in members.into_iter().filter(|i| !taken[*i]) {
            by_shape
                .entry(files[index].1.iter().map(|h| h.line).collect())
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
        let variant = |i: usize| files[i].0.split('/').nth(slot).unwrap_or("").to_string();
        let pick = same
            .iter()
            .copied()
            .min_by_key(|i| {
                PREFERRED_VARIANTS
                    .iter()
                    .position(|p| *p == variant(*i))
                    .unwrap_or(usize::MAX)
            })
            .unwrap_or(same[0]);
        for index in &same {
            taken[*index] = true;
        }
        merged_files += same.len() - 1;
        merged_lines += (same.len() - 1) * files[pick].1.len();
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
        .filter_map(|(index, (file, list))| match labels.remove(&index) {
            Some(label) => Some((label, list)),
            None if taken[index] => None,
            None => Some((file, list)),
        })
        .collect();
    (rows, merged_files, merged_lines)
}

pub(super) fn render(hits: &[Hit], alternatives: &[Term]) -> String {
    let mut files: Vec<(String, Vec<&Hit>)> = Vec::new();
    for hit in hits {
        match files.last_mut() {
            Some((file, list)) if *file == hit.file => list.push(hit),
            _ => files.push((hit.file.clone(), vec![hit])),
        }
    }
    let (code, text): (Vec<_>, Vec<_>) = files.into_iter().partition(|(file, _)| is_code(file));
    let (text, _, merged_lines) = collapse_variants(text);
    let mut rows: Vec<(u8, usize, String)> = code
        .iter()
        .map(|(file, list)| {
            let best = list
                .iter()
                .map(|h| defining_rank(h, alternatives))
                .min()
                .unwrap_or(2);
            let class = if is_test(file) { 3 } else { best };
            (
                class,
                list.len(),
                format!("  {file}{SEP}{}", code_row(list)),
            )
        })
        .chain(text.iter().map(|(file, list)| {
            let class = if is_test(file) { 5 } else { 4 };
            (class, list.len(), format!("  {file}{SEP}{}", located(list)))
        }))
        .collect();
    rows.sort_by(|a, b| a.0.cmp(&b.0).then(b.1.cmp(&a.1)));
    let mut out = String::new();
    let missing: Vec<&str> = alternatives
        .iter()
        .filter(|a| !hits.iter().any(|h| a.matches(&h.text)))
        .map(|a| a.raw.as_str())
        .collect();
    if !missing.is_empty() {
        out.push_str(&format!("No matches: {}\n", missing.join(" | ")));
    }
    out.push_str(&format!(
        "{} lines in {} files\n",
        hits.len() - merged_lines,
        rows.len()
    ));
    for (_, _, row) in rows {
        out.push_str(&row);
        out.push('\n');
    }
    out
}

pub(super) fn top_source(repo: &std::path::Path, hits: &[Hit], alternatives: &[Term]) -> String {
    let Some(hit) = hits
        .iter()
        .filter(|h| is_code(&h.file) && !is_test(&h.file) && defining_rank(h, alternatives) < 2)
        .min_by_key(|h| defining_rank(h, alternatives))
    else {
        return String::new();
    };
    let Ok(content) = std::fs::read_to_string(repo.join(&hit.file)) else {
        return String::new();
    };
    let lines: Vec<&str> = content.lines().collect();
    let start = hit
        .def
        .as_ref()
        .filter(|d| d.start == hit.line)
        .map_or(hit.line, |d| d.start);
    let def_end = hit.def.as_ref().map_or(start, |d| d.end.max(start));
    let end = def_end.min(lines.len()).min(start + TOP_SOURCE_LINES - 1);
    let mut out = format!("\nSource — {}:{start}-{def_end}\n", hit.file);
    for number in start..=end {
        out.push_str(&format!("  {number}|{}\n", lines[number - 1]));
    }
    if def_end > end {
        out.push_str(&format!(
            "  rest: {} context {}:{}-{def_end}\n",
            crate::commands::setup::spec::launcher(),
            hit.file,
            end + 1
        ));
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn hit(file: &str, line: usize, text: &str, def: Option<(&str, usize, usize)>) -> Hit {
        Hit {
            file: file.into(),
            line,
            text: text.into(),
            def: def.map(|(name, start, end)| Def {
                name: name.into(),
                start,
                end,
            }),
        }
    }

    #[test]
    fn every_hit_is_listed_with_definitions_first_and_no_counts_hidden() {
        let terms = vec![Term::parse("maintenanceMode"), Term::parse("nosuchxyz")];
        let mut hits = vec![
            hit(
                "install/data/defaults.json",
                130,
                "\"maintenanceMode\": 0,",
                None,
            ),
            hit(
                "src/middleware/maintenance.js",
                10,
                "middleware.maintenanceMode = helpers.try(",
                Some(("default", 9, 41)),
            ),
            hit(
                "src/middleware/maintenance.js",
                11,
                "if (!meta.config.maintenanceMode) {",
                Some(("default", 9, 41)),
            ),
            hit(
                "test/controllers.js",
                1203,
                "meta.config.maintenanceMode = 1;",
                Some(("describe", 1201, 1230)),
            ),
        ];
        for line in 26..=37 {
            hits.push(hit(
                "src/routes/feeds.js",
                line,
                "app.get('/x', middleware.maintenanceMode, y);",
                Some(("default", 25, 38)),
            ));
        }
        hits.sort_by(|a, b| a.file.cmp(&b.file).then(a.line.cmp(&b.line)));
        let out = render(&hits, &terms);
        let lines: Vec<&str> = out.lines().collect();
        assert_eq!(lines[0], "No matches: nosuchxyz");
        assert_eq!(lines[1], "16 lines in 4 files");
        assert!(
            lines[2].starts_with("  src/middleware/maintenance.js │ default:9-41 :10 "),
            "{out}"
        );
        assert!(
            lines[2].ends_with(":11 if (!meta.config.maintenanceMode) {"),
            "{out}"
        );
        assert!(
            lines[3].starts_with("  src/routes/feeds.js │ default:25-38 :26 "),
            "{out}"
        );
        assert!(lines[3].ends_with(":29-37"), "{out}");
        assert!(lines[4].starts_with("  test/controllers.js"), "{out}");
        assert!(
            lines[5].starts_with("  install/data/defaults.json │ :130 "),
            "{out}"
        );
    }

    #[test]
    fn locale_copies_collapse_into_one_row() {
        let hits: Vec<Hit> = ["ar", "de", "en-GB", "fr"]
            .iter()
            .map(|l| {
                hit(
                    &format!("public/language/{l}/advanced.json"),
                    2,
                    "\"maintenance-mode\": \"x\"",
                    None,
                )
            })
            .collect();
        let out = render(&hits, &[Term::parse("maintenanceMode")]);
        assert!(
            out.contains("public/language/{en-GB,+3}/advanced.json"),
            "{out}"
        );
        assert!(out.starts_with("1 lines in 1 files\n"), "{out}");
    }

    #[test]
    fn regex_terms_match_like_ripgrep_and_bad_patterns_fall_back_to_literals() {
        assert!(Term::parse(r"\bCveDetail\b").matches("x := CveDetail{}"));
        assert!(!Term::parse(r"\bCveDetail\b").matches("CveDetails"));
        assert!(Term::parse(r"type .* struct\{\}").matches("type key struct{}"));
        assert!(Term::parse(r"route\(").matches(".route(\"/x\")"));
        assert!(Term::parse("route(").matches(".route(\"/x\")"));
        assert!(Term::parse("????").matches("x = '????'"));
        assert!(Term::parse("^rand").matches("rand = \"0.10\""));
        assert!(!Term::parse("^rand").matches("x.rand = 1"));
        assert!(Term::parse("mark_in_sync").matches("markInSync()"));
        assert!(Term::parse("on.*Login").names("onLogin"));
    }
}
