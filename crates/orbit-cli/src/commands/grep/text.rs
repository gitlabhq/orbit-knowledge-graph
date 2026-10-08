use std::collections::HashMap;

use std::path::Path;
use std::sync::Mutex;

use anyhow::Result;
use duckdb_client::search::NodeHydrator;
use duckdb_client::{DuckDbClient, i64_column, sql_lit, string_column};
use grep_regex::RegexMatcherBuilder;
use grep_searcher::{BinaryDetection, SearcherBuilder, sinks};

use crate::workspace::GitInfo;

const VARIANT_MIN: usize = 3;
const PREFERRED_VARIANTS: &[&str] = &["en", "en-GB", "en-US", "en_US", "en_GB", "default"];
const TOP_SOURCE_LINES: usize = 80;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct Def {
    pub(super) name: String,
    pub(super) start: usize,
    pub(super) end: usize,
    pub(super) id: i64,
}

/// Callers and callees of each listed definition, by definition id.
pub(super) type Connections = HashMap<i64, (Vec<String>, Vec<String>)>;

const CONNECTION_NAMES: usize = 8;
const LOOKUP_MAX_DEFINITIONS: usize = 3;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct Hit {
    pub(super) file: String,
    pub(super) line: usize,
    pub(super) text: String,
    pub(super) def: Option<Def>,
}

pub(super) struct Term {
    pub(super) raw: String,
    regex: Option<(regex::Regex, regex::Regex)>,
    assign: Option<regex::Regex>,
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
        let assign = regex.is_none().then(|| assignment(raw)).flatten();
        Self {
            raw: raw.to_string(),
            regex,
            assign,
        }
    }

    pub(super) fn literal(&self) -> Self {
        Self {
            raw: self.raw.clone(),
            regex: None,
            assign: assignment(&self.raw),
        }
    }

    fn pattern(&self) -> String {
        match self.regex {
            Some(_) => self.raw.clone(),
            None => compact(&self.raw)
                .chars()
                .map(|c| regex::escape(&c.to_string()))
                .collect::<Vec<_>>()
                .join("[-_\\t ]*"),
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

const LINE_TEXT_CHARS: usize = 400;
const DEF_FILES_PER_QUERY: usize = 500;

pub(super) fn matcher(alternatives: &[Term]) -> Result<grep_regex::RegexMatcher> {
    let pattern = alternatives
        .iter()
        .map(|term| format!("(?:{})", term.pattern()))
        .collect::<Vec<_>>()
        .join("|");
    Ok(RegexMatcherBuilder::new()
        .case_insensitive(true)
        .line_terminator(Some(b'\n'))
        .build(&pattern)?)
}

pub(super) fn scan(
    repo: &Path,
    paths: &[String],
    matcher: &grep_regex::RegexMatcher,
) -> Result<Vec<Hit>> {
    let globbed = paths.iter().any(|p| p.contains(['*', '?', '[']));
    let roots: Vec<std::path::PathBuf> = match paths.is_empty() || globbed {
        true => vec![repo.to_path_buf()],
        false => paths.iter().map(|p| repo.join(p)).collect(),
    };
    let mut walk = ignore::WalkBuilder::new(&roots[0]);
    for root in &roots[1..] {
        walk.add(root);
    }
    walk.hidden(false)
        .require_git(false)
        .filter_entry(|entry| entry.file_name() != ".git");
    let found = Mutex::new(Vec::new());
    walk.build_parallel().run(|| {
        let mut searcher = SearcherBuilder::new()
            .binary_detection(BinaryDetection::quit(0))
            .line_number(true)
            .build();
        let found = &found;
        Box::new(move |entry| {
            let Ok(entry) = entry else {
                return ignore::WalkState::Continue;
            };
            if !entry.file_type().is_some_and(|t| t.is_file()) {
                return ignore::WalkState::Continue;
            }
            let Ok(relative) = entry.path().strip_prefix(repo) else {
                return ignore::WalkState::Continue;
            };
            let file = relative.to_string_lossy().replace('\\', "/");
            if globbed && !in_scope(&file, paths) {
                return ignore::WalkState::Continue;
            }
            let mut local = Vec::new();
            let _ = searcher.search_path(
                matcher,
                entry.path(),
                sinks::Lossy(|line, text| {
                    local.push(Hit {
                        file: file.clone(),
                        line: line as usize,
                        text: text.trim().chars().take(LINE_TEXT_CHARS).collect(),
                        def: None,
                    });
                    Ok(true)
                }),
            );
            if !local.is_empty() {
                found.lock().unwrap().extend(local);
            }
            ignore::WalkState::Continue
        })
    });
    Ok(found.into_inner().unwrap())
}

fn in_scope(path: &str, paths: &[String]) -> bool {
    paths.iter().any(|scope| {
        let scope = scope.trim_end_matches('/');
        match scope.contains(['*', '?', '[']) {
            true => globset::Glob::new(scope).is_ok_and(|g| g.compile_matcher().is_match(path)),
            false => path == scope || path.starts_with(&format!("{scope}/")),
        }
    })
}

struct Candidate {
    def: Def,
    kind: String,
    id: i64,
}

pub(super) fn attach_definitions(
    client: &DuckDbClient,
    git: &GitInfo,
    hits: &mut Vec<Hit>,
    alternatives: &[Term],
    kinds: &[String],
) -> Result<()> {
    let node = NodeHydrator::embedded("Definition")?;
    let mut files: Vec<&str> = hits.iter().map(|h| h.file.as_str()).collect();
    files.dedup();
    let mut by_file: HashMap<String, Vec<Candidate>> = HashMap::new();
    for chunk in files.chunks(DEF_FILES_PER_QUERY) {
        let batches = client.query_arrow_json(
            &format!(
                "SELECT {file} AS file_path, {name} AS name, {kind} AS kind, {id} AS id,
       CAST({start} AS BIGINT) AS def_start, CAST({end} AS BIGINT) AS def_end
FROM {table}
WHERE {project} = ?1 AND {commit} = ?2 AND {fqn} NOT LIKE '%@%' AND {file} IN ({list})",
                file = node.column("file_path")?,
                name = node.column("name")?,
                kind = node.column("definition_type")?,
                id = node.column("id")?,
                start = node.column("start_line")?,
                end = node.column("end_line")?,
                table = node.table(),
                project = node.column("project_id")?,
                commit = node.column("commit_sha")?,
                fqn = node.column("fqn")?,
                list = chunk
                    .iter()
                    .map(|f| sql_lit(f))
                    .collect::<Vec<_>>()
                    .join(", "),
            ),
            &[git.project_id.into(), git.commit_sha.clone().into()],
        )?;
        let (paths, names, kinds_col) = (
            string_column(&batches, "file_path"),
            string_column(&batches, "name"),
            string_column(&batches, "kind"),
        );
        let (ids, starts, ends) = (
            i64_column(&batches, "id"),
            i64_column(&batches, "def_start"),
            i64_column(&batches, "def_end"),
        );
        for i in 0..paths.len() {
            by_file
                .entry(paths[i].clone())
                .or_default()
                .push(Candidate {
                    def: Def {
                        name: names[i].clone(),
                        start: starts[i] as usize,
                        end: ends[i] as usize,
                        id: ids[i],
                    },
                    kind: kinds_col[i].to_lowercase(),
                    id: ids[i],
                });
        }
    }
    let wanted: Vec<String> = kinds.iter().map(|k| k.to_lowercase()).collect();
    hits.retain_mut(|hit| {
        let best = by_file.get(&hit.file).and_then(|defs| {
            defs.iter()
                .filter(|c| c.def.start <= hit.line && hit.line <= c.def.end)
                .min_by_key(|c| {
                    let named = c.def.start == hit.line
                        && alternatives.iter().any(|a| a.names(&c.def.name));
                    (
                        !named,
                        c.def.end <= c.def.start,
                        c.def.end - c.def.start,
                        c.id,
                    )
                })
        });
        let keep = wanted.is_empty() || best.is_some_and(|c| wanted.contains(&c.kind));
        hit.def = best.map(|c| c.def.clone());
        keep
    });
    Ok(())
}

pub(super) fn connections(client: &DuckDbClient, hits: &[Hit]) -> Result<Connections> {
    let mut ids: Vec<i64> = hits
        .iter()
        .filter_map(|h| h.def.as_ref().map(|d| d.id))
        .collect();
    ids.sort_unstable();
    ids.dedup();
    type Named = Vec<(bool, String)>;
    let mut raw: HashMap<i64, (Named, Named)> = HashMap::new();
    if ids.is_empty() {
        return Ok(Connections::new());
    }
    let list = ids.iter().map(i64::to_string).collect::<Vec<_>>().join(",");
    client.execute(
        &format!(
            "CREATE OR REPLACE TEMP TABLE grep_defs AS SELECT unnest([{list}]::BIGINT[]) AS id"
        ),
        &[],
    )?;
    {
        let callers = client.query_arrow_json(
            "SELECT DISTINCT e.target_id AS def, d.name AS name, d.file_path AS file
FROM gl_edge e JOIN gl_definition d ON d.id = e.source_id
WHERE e.relationship_kind = 'CALLS' AND e.source_kind = 'Definition'
  AND e.target_kind = 'Definition' AND e.target_id IN (SELECT id FROM grep_defs)
  AND e.source_id <> e.target_id",
            &[],
        )?;
        let callees = client.query_arrow_json(
            "WITH calls AS (
  SELECT e.source_id, e.target_id, e.target_kind FROM gl_edge e
  JOIN grep_defs g ON g.id = e.source_id
  WHERE e.relationship_kind = 'CALLS' AND e.source_id <> e.target_id
)
SELECT DISTINCT c.source_id AS def, d.name AS name, d.file_path AS file
FROM calls c JOIN gl_definition d ON d.id = c.target_id
WHERE c.target_kind = 'Definition'
UNION
SELECT DISTINCT c.source_id AS def,
       COALESCE(NULLIF(i.identifier_alias, ''), NULLIF(i.identifier_name, ''), i.import_path) AS name,
       i.file_path AS file
FROM calls c JOIN gl_imported_symbol i ON i.id = c.target_id
WHERE c.target_kind = 'ImportedSymbol'",
            &[],
        )?;
        for (batches, callers_side) in [(callers, true), (callees, false)] {
            let (defs, names, files) = (
                i64_column(&batches, "def"),
                string_column(&batches, "name"),
                string_column(&batches, "file"),
            );
            for ((def, name), file) in defs.into_iter().zip(names).zip(files) {
                if name.is_empty() {
                    continue;
                }
                let entry = raw.entry(def).or_default();
                match callers_side {
                    true => entry.0.push((is_test(&file), name)),
                    false => entry.1.push((is_test(&file), name)),
                }
            }
        }
    }
    let names = |mut list: Named| {
        list.sort();
        let mut seen = std::collections::HashSet::new();
        list.into_iter()
            .filter_map(|(_, name)| seen.insert(name.clone()).then_some(name))
            .collect::<Vec<_>>()
    };
    Ok(raw
        .into_iter()
        .map(|(def, (callers, callees))| (def, (names(callers), names(callees))))
        .collect())
}

fn connection_label(def: &Def, connections: &Connections) -> String {
    let Some((callers, callees)) = connections.get(&def.id) else {
        return String::new();
    };
    let side = |arrow: &str, names: &[String], noun: &str| match names.len() {
        0 => String::new(),
        n if n > CONNECTION_NAMES => format!("{arrow}{n} {noun} "),
        _ => format!("{arrow}{} ", names.join(",")),
    };
    format!(
        "{}{}",
        side("←", callers, "callers"),
        side("→", callees, "callees")
    )
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

fn assignment(raw: &str) -> Option<regex::Regex> {
    regex::Regex::new(&format!(
        r"(?i)^\s*(?:(?:const|let|var)\s+)?(?:[\w$]+\.)*{}\s*=[^=]",
        regex::escape(raw.trim())
    ))
    .ok()
}

fn assigns(hit: &Hit, alternatives: &[Term]) -> bool {
    alternatives
        .iter()
        .filter_map(|t| t.assign.as_ref())
        .any(|re| re.is_match(&hit.text))
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

fn code_row(hits: &[&Hit], connections: &Connections) -> String {
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
            let label = def.map(|d| {
                let span = match d.end > d.start {
                    true => format!("{}:{}-{} ", d.name, d.start, d.end),
                    false => format!("{} ", d.name),
                };
                span + &connection_label(d, connections)
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

pub(super) fn render(hits: &[Hit], alternatives: &[Term], connections: &Connections) -> String {
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
                format!("  {file}{SEP}{}", code_row(list, connections)),
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
    hits.iter()
        .filter(|h| is_code(&h.file) && !is_test(&h.file) && defining_rank(h, alternatives) < 2)
        .min_by_key(|h| defining_rank(h, alternatives))
        .map(|hit| format!("\n{}", source_block(repo, hit, "Source")))
        .unwrap_or_default()
}

/// When the query is one plain identifier that names a definition, that definition's source is
/// the answer: it is printed before the rows so `head` keeps it.
pub(super) fn lookup(
    repo: &std::path::Path,
    hits: &[Hit],
    alternatives: &[Term],
) -> Option<String> {
    let [term] = alternatives else {
        return None;
    };
    if term.regex.is_some()
        || !term
            .raw
            .chars()
            .all(|c| c.is_alphanumeric() || "_$-:.".contains(c))
    {
        return None;
    }
    let named: Vec<&Hit> = hits.iter().filter(|h| names(h, alternatives)).collect();
    if named.len() > LOOKUP_MAX_DEFINITIONS {
        return None;
    }
    let hit = named
        .iter()
        .copied()
        .min_by_key(|h| (!is_code(&h.file), is_test(&h.file)))?;
    let label = match named.len() {
        1 => format!("Definition {}", term.raw),
        n => format!(
            "Definition {} (1 of {n}; the others are in the rows below)",
            term.raw
        ),
    };
    Some(source_block(repo, hit, &label))
}

fn source_block(repo: &std::path::Path, hit: &Hit, label: &str) -> String {
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
    let mut out = format!("{label} — {}:{start}-{def_end}\n", hit.file);
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
                id: start as i64,
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
        let out = render(&hits, &terms, &Connections::new());
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
        let out = render(
            &hits,
            &[Term::parse("maintenanceMode")],
            &Connections::new(),
        );
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

    #[test]
    fn scan_reads_the_working_tree_like_ripgrep() {
        let repo = tempfile::tempdir().unwrap();
        let write = |path: &str, body: &str| {
            let full = repo.path().join(path);
            std::fs::create_dir_all(full.parent().unwrap()).unwrap();
            std::fs::write(full, body).unwrap();
        };
        write(".gitignore", "dist/\n");
        write("src/a.rs", "fn mark_in_sync() {}\nlet x = 1;\n");
        write("src/nested/b.ts", "markInSync();\n");
        write("dist/c.js", "markInSync();\n");
        write(".github/ci.yml", "run: mark-in-sync\n");
        let term = Term::parse("markInSync");
        let matcher = matcher(&[term]).unwrap();
        let files = |paths: &[&str]| {
            let paths: Vec<String> = paths.iter().map(|p| p.to_string()).collect();
            let mut found: Vec<String> = scan(repo.path(), &paths, &matcher)
                .unwrap()
                .into_iter()
                .map(|h| format!("{}:{}", h.file, h.line))
                .collect();
            found.sort();
            found
        };
        assert_eq!(
            files(&[]),
            [".github/ci.yml:1", "src/a.rs:1", "src/nested/b.ts:1"]
        );
        assert_eq!(files(&["src/nested"]), ["src/nested/b.ts:1"]);
        assert_eq!(
            files(&["src/**/*.ts", ".github"]),
            [".github/ci.yml:1", "src/nested/b.ts:1"]
        );
    }
}
