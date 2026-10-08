use std::collections::{BTreeSet, HashMap};

use std::path::Path;
use std::sync::Mutex;

use anyhow::{Context, Result};
use duckdb_client::search::NodeHydrator;
use duckdb_client::{DuckDbClient, i64_column, sql_lit, string_column};
use grep_matcher::Matcher;
use grep_regex::{RegexMatcher, RegexMatcherBuilder};
use grep_searcher::{BinaryDetection, Searcher, SearcherBuilder, Sink, SinkContext, SinkMatch};

use super::{Options, Output};

use crate::workspace::GitInfo;

const VARIANT_MIN: usize = 3;
const PREFERRED_VARIANTS: &[&str] = &["en", "en-GB", "en-US", "en_US", "en_GB", "default"];

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct Def {
    pub(super) name: String,
    pub(super) start: usize,
    pub(super) end: usize,
    pub(super) id: i64,
    pub(super) kind: String,
}

/// Callers and callees of each listed definition, by definition id.
pub(super) type Connections = HashMap<i64, (Vec<String>, Vec<String>)>;

const CONNECTION_NAMES: usize = 8;
/// Doc comments and attributes put a definition's name a few lines below its first line.
const DECLARATION_LINES: usize = 10;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct Hit {
    pub(super) file: String,
    pub(super) line: usize,
    pub(super) text: String,
    pub(super) def: Option<Def>,
    /// A `-A`/`-B`/`-C` line around a match rather than a match.
    pub(super) context: bool,
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

const DEF_FILES_PER_QUERY: usize = 500;

pub(super) fn matcher(alternatives: &[Term], options: &Options) -> Result<RegexMatcher> {
    let pattern = alternatives
        .iter()
        .map(|term| format!("(?:{})", term.pattern()))
        .collect::<Vec<_>>()
        .join("|");
    Ok(RegexMatcherBuilder::new()
        .case_insensitive(true)
        .word(options.word)
        .line_terminator(Some(b'\n'))
        .build(&pattern)?)
}

/// Collects one file's matches and `-A`/`-B`/`-C` context lines, stopping after `-m` matches.
struct Collect<'a> {
    file: &'a str,
    hits: Vec<Hit>,
    max_count: Option<u64>,
    matched: u64,
}

impl Collect<'_> {
    fn push(&mut self, line: Option<u64>, bytes: &[u8], context: bool) {
        self.hits.push(Hit {
            file: self.file.to_string(),
            line: line.unwrap_or(0) as usize,
            text: String::from_utf8_lossy(bytes)
                .trim_end_matches(['\n', '\r'])
                .to_string(),
            def: None,
            context,
        });
    }
}

impl Sink for Collect<'_> {
    type Error = std::io::Error;

    fn matched(&mut self, _: &Searcher, found: &SinkMatch<'_>) -> std::io::Result<bool> {
        self.push(found.line_number(), found.bytes(), false);
        self.matched += 1;
        Ok(self.max_count.is_none_or(|max| self.matched < max))
    }

    fn context(&mut self, _: &Searcher, around: &SinkContext<'_>) -> std::io::Result<bool> {
        self.push(around.line_number(), around.bytes(), true);
        Ok(true)
    }
}

pub(super) fn scan(
    repo: &Path,
    paths: &[String],
    matcher: &RegexMatcher,
    options: &Options,
) -> Result<Vec<Hit>> {
    let scope = Scope::new(repo, paths)?;
    let scope = &scope;
    let roots = scope.roots(repo);
    let mut walk = ignore::WalkBuilder::new(&roots[0]);
    for root in &roots[1..] {
        walk.add(root);
    }
    walk.hidden(false)
        .require_git(false)
        .filter_entry(|entry| entry.file_name() != ".git");
    if !options.globs.is_empty() {
        let mut globs = ignore::overrides::OverrideBuilder::new(repo);
        for glob in &options.globs {
            globs
                .add(glob)
                .with_context(|| format!("invalid glob {glob:?}"))?;
        }
        walk.overrides(globs.build()?);
    }
    if !options.types.is_empty() || !options.types_not.is_empty() {
        let mut types = ignore::types::TypesBuilder::new();
        types.add_defaults();
        for name in &options.types {
            types.select(name);
        }
        for name in &options.types_not {
            types.negate(name);
        }
        walk.types(types.build()?);
    }
    let found = Mutex::new(Vec::new());
    walk.build_parallel().run(|| {
        let mut searcher = SearcherBuilder::new()
            .binary_detection(BinaryDetection::quit(0))
            .line_number(true)
            .invert_match(options.invert)
            .before_context(options.before)
            .after_context(options.after)
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
            if !scope.contains(&file) {
                return ignore::WalkState::Continue;
            }
            let mut sink = Collect {
                file: &file,
                hits: Vec::new(),
                max_count: options.max_count,
                matched: 0,
            };
            let _ = searcher.search_path(matcher, entry.path(), &mut sink);
            if sink.matched > 0 {
                found.lock().unwrap().extend(sink.hits);
            }
            ignore::WalkState::Continue
        })
    });
    Ok(found.into_inner().unwrap())
}

/// `--path` scopes compiled once for the walk. Paths that exist are literal, even with glob
/// characters such as `app/[slug]`; the rest are globs over files and directories.
struct Scope {
    globs: Option<globset::GlobSet>,
    prefixes: Vec<String>,
}

impl Scope {
    fn new(repo: &Path, paths: &[String]) -> Result<Self> {
        let root = dunce::canonicalize(repo)?;
        let mut globs = globset::GlobSetBuilder::new();
        let (mut globbed, mut prefixes, mut missing, mut outside) =
            (false, Vec::new(), Vec::new(), Vec::new());
        for path in paths.iter().map(|p| p.trim_end_matches('/')) {
            match dunce::canonicalize(repo.join(path)) {
                Ok(full) if full.starts_with(&root) => prefixes.push(path.to_string()),
                Ok(_) => outside.push(path),
                Err(_) if path.contains(['*', '?', '[']) => {
                    for pattern in [path.to_string(), format!("{path}/**")] {
                        globs.add(
                            globset::Glob::new(&pattern)
                                .with_context(|| format!("invalid path glob {path:?}"))?,
                        );
                    }
                    globbed = true;
                }
                Err(_) => missing.push(path),
            }
        }
        anyhow::ensure!(
            outside.is_empty(),
            "paths outside the repository: {}",
            outside.join(", ")
        );
        anyhow::ensure!(
            missing.is_empty(),
            "no such path in the repository: {}",
            missing.join(", ")
        );
        Ok(Self {
            globs: globbed.then(|| globs.build()).transpose()?,
            prefixes,
        })
    }

    fn roots(&self, repo: &Path) -> Vec<std::path::PathBuf> {
        if self.globs.is_some() || self.prefixes.is_empty() {
            return vec![repo.to_path_buf()];
        }
        let mut sorted = self.prefixes.clone();
        sorted.sort();
        let mut kept: Vec<String> = Vec::new();
        for path in sorted {
            if !kept.iter().any(|outer| within(&path, outer)) {
                kept.push(path);
            }
        }
        kept.iter().map(|path| repo.join(path)).collect()
    }

    fn contains(&self, path: &str) -> bool {
        self.globs.as_ref().is_none_or(|globs| {
            globs.is_match(path) || self.prefixes.iter().any(|scope| within(path, scope))
        })
    }
}

fn within(path: &str, scope: &str) -> bool {
    path.strip_prefix(scope)
        .is_some_and(|rest| rest.is_empty() || rest.starts_with('/'))
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
    edited: &BTreeSet<String>,
) -> Result<()> {
    let node = NodeHydrator::embedded("Definition")?;
    let mut files: Vec<&str> = hits
        .iter()
        .map(|h| h.file.as_str())
        .filter(|file| !edited.contains(*file))
        .collect();
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
                        kind: kinds_col[i].clone(),
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
        let keep =
            hit.context || wanted.is_empty() || best.is_some_and(|c| wanted.contains(&c.kind));
        hit.def = best.map(|c| c.def.clone());
        keep
    });
    let matched: std::collections::HashSet<String> = hits
        .iter()
        .filter(|hit| !hit.context)
        .map(|hit| hit.file.clone())
        .collect();
    hits.retain(|hit| matched.contains(&hit.file));
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
    hit.def.as_ref().is_some_and(|def| {
        (def.start..=def.start + DECLARATION_LINES).contains(&hit.line)
            && alternatives.iter().any(|a| a.names(&def.name))
    })
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

/// Graph context rides on rg's context-line form (`path-N-`), so every line keeps its path.
const NOTE: &str = "» ";

type Row<'a> = (String, Vec<&'a Hit>);

/// Files in reading order: defining code first, then other code, tests, and text files, each
/// group by match count.
fn ranked<'a>(hits: &'a [Hit], alternatives: &[Term], collapse: bool) -> Vec<Row<'a>> {
    let mut files: Vec<Row<'a>> = Vec::new();
    for hit in hits {
        match files.last_mut() {
            Some((file, list)) if *file == hit.file => list.push(hit),
            _ => files.push((hit.file.clone(), vec![hit])),
        }
    }
    let (code, text): (Vec<_>, Vec<_>) = files.into_iter().partition(|(file, _)| is_code(file));
    let text = match collapse {
        true => collapse_variants(text).0,
        false => text,
    };
    let matches = |list: &[&Hit]| list.iter().filter(|h| !h.context).count();
    let mut rows: Vec<(u8, usize, Row<'a>)> = code
        .into_iter()
        .map(|(file, list)| {
            let best = list
                .iter()
                .filter(|h| !h.context)
                .map(|h| defining_rank(h, alternatives))
                .min()
                .unwrap_or(2);
            let class = if is_test(&file) { 3 } else { best };
            (class, matches(&list), (file, list))
        })
        .chain(text.into_iter().map(|(file, list)| {
            let class = if is_test(&file) { 5 } else { 4 };
            (class, matches(&list), (file, list))
        }))
        .collect();
    rows.sort_by(|a, b| a.0.cmp(&b.0).then(b.1.cmp(&a.1)));
    rows.into_iter().map(|(_, _, row)| row).collect()
}

fn definition_note(def: &Def, connections: &Connections) -> String {
    let span = match def.end > def.start {
        true => format!("{} {}:{}-{}", def.kind, def.name, def.start, def.end),
        false => format!("{} {}", def.kind, def.name),
    };
    format!("{span} {}", connection_label(def, connections))
        .trim_end()
        .to_string()
}

pub(super) fn render(
    header: &str,
    hits: &[Hit],
    alternatives: &[Term],
    connections: &Connections,
    edited: &BTreeSet<String>,
    options: &Options,
) -> Result<String> {
    let rows = ranked(hits, alternatives, options.output == Output::Lines);
    let mut out = String::new();
    let matched = |list: &[&Hit]| list.iter().filter(|h| !h.context).count();
    match options.output {
        Output::Quiet => return Ok(out),
        Output::Files => {
            for (file, _) in &rows {
                out.push_str(&format!("{file}\n"));
            }
            return Ok(out);
        }
        Output::Count => {
            for (file, list) in &rows {
                out.push_str(&format!("{file}:{}\n", matched(list)));
            }
            return Ok(out);
        }
        Output::Lines => {}
    }
    let shown: Vec<&Hit> = rows
        .iter()
        .flat_map(|(_, list)| list.iter().copied())
        .filter(|h| !h.context)
        .collect();
    let per_term = match alternatives.len() > 1 && !shown.is_empty() && !options.invert {
        true => {
            let counts = alternatives
                .iter()
                .map(|term| {
                    let only = matcher(std::slice::from_ref(term), options)?;
                    let lines = shown
                        .iter()
                        .filter(|h| only.is_match(h.text.as_bytes()).unwrap_or(false))
                        .count();
                    Ok(format!("{} {lines}", term.raw))
                })
                .collect::<Result<Vec<_>>>()?;
            format!(" ({})", counts.join(", "))
        }
        false => String::new(),
    };
    out.push_str(&format!(
        "{header}: {} lines in {} files{per_term}\n",
        shown.len(),
        rows.len()
    ));
    let separated = options.before > 0 || options.after > 0;
    let mut previous: Option<(&str, usize)> = None;
    for (file, list) in &rows {
        let mut current: Option<&Def> = None;
        for (index, hit) in list.iter().enumerate() {
            if separated && previous.is_some_and(|(f, l)| f != hit.file || hit.line != l + 1) {
                out.push_str("--\n");
            }
            previous = Some((&hit.file, hit.line));
            if index == 0 && edited.contains(&hit.file) {
                out.push_str(&format!(
                    "{file}-{}-{NOTE}edited since index; definitions not shown\n",
                    hit.line
                ));
            }
            if let Some(def) = hit.def.as_ref().filter(|def| current != Some(*def)) {
                out.push_str(&format!(
                    "{file}-{}-{NOTE}{}\n",
                    def.start,
                    definition_note(def, connections)
                ));
            }
            current = hit.def.as_ref();
            let text = match options.max_columns.is_some_and(|max| hit.text.len() > max) {
                true => "[Omitted long line]",
                false => hit.text.as_str(),
            };
            let sep = if hit.context { '-' } else { ':' };
            out.push_str(&format!("{file}{sep}{}{sep}{text}\n", hit.line));
        }
    }
    Ok(out)
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
                kind: "Function".into(),
            }),
            context: false,
        }
    }

    fn show(hits: &[Hit], terms: &[Term], options: &Options) -> String {
        render(
            "grep",
            hits,
            terms,
            &Connections::new(),
            &BTreeSet::new(),
            options,
        )
        .unwrap()
    }

    #[test]
    fn lines_print_like_rg_with_definitions_noted_and_defining_files_first() {
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
                "    if (!meta.config.maintenanceMode) {",
                Some(("default", 9, 41)),
            ),
            hit(
                "test/controllers.js",
                1203,
                "meta.config.maintenanceMode = 1;",
                Some(("describe", 1201, 1230)),
            ),
        ];
        for line in 26..=27 {
            hits.push(hit(
                "src/routes/feeds.js",
                line,
                "app.get('/x', middleware.maintenanceMode, y);",
                Some(("default", 25, 38)),
            ));
        }
        hits.sort_by(|a, b| a.file.cmp(&b.file).then(a.line.cmp(&b.line)));
        let out = show(&hits, &terms, &Options::default());
        assert_eq!(
            out,
            "grep: 6 lines in 4 files (maintenanceMode 6, nosuchxyz 0)
src/middleware/maintenance.js-9-» Function default:9-41
src/middleware/maintenance.js:10:middleware.maintenanceMode = helpers.try(
src/middleware/maintenance.js:11:    if (!meta.config.maintenanceMode) {
src/routes/feeds.js-25-» Function default:25-38
src/routes/feeds.js:26:app.get('/x', middleware.maintenanceMode, y);
src/routes/feeds.js:27:app.get('/x', middleware.maintenanceMode, y);
test/controllers.js-1201-» Function describe:1201-1230
test/controllers.js:1203:meta.config.maintenanceMode = 1;
install/data/defaults.json:130:\"maintenanceMode\": 0,
"
        );
        let files = Options {
            output: Output::Files,
            ..Options::default()
        };
        assert_eq!(
            show(&hits, &terms, &files),
            "src/middleware/maintenance.js\nsrc/routes/feeds.js\ntest/controllers.js\ninstall/data/defaults.json\n"
        );
        let count = Options {
            output: Output::Count,
            ..Options::default()
        };
        assert!(
            show(&hits, &terms, &count)
                .starts_with("src/middleware/maintenance.js:2\nsrc/routes/feeds.js:2\n")
        );
        let quiet = Options {
            output: Output::Quiet,
            ..Options::default()
        };
        assert_eq!(show(&hits, &terms, &quiet), "");
    }

    #[test]
    fn context_lines_use_dashes_and_groups_are_separated() {
        let mut around = hit("src/a.rs", 2, "let x = 1;", Some(("run", 1, 9)));
        around.context = true;
        let hits = vec![
            around,
            hit("src/a.rs", 3, "go();", Some(("run", 1, 9))),
            hit("src/a.rs", 8, "go();", Some(("run", 1, 9))),
        ];
        let options = Options {
            before: 1,
            ..Options::default()
        };
        assert_eq!(
            show(&hits, &[Term::parse("go")], &options),
            "grep: 2 lines in 1 files
src/a.rs-1-» Function run:1-9
src/a.rs-2-let x = 1;
src/a.rs:3:go();
--
src/a.rs:8:go();
"
        );
    }

    #[test]
    fn edited_code_files_are_noted_without_definitions() {
        let hits = vec![
            hit("src/a.rs", 3, "fn go() {}", None),
            hit("src/b.rs", 4, "go();", Some(("run", 1, 9))),
        ];
        let edited = BTreeSet::from(["src/a.rs".to_string()]);
        let out = render(
            "grep",
            &hits,
            &[Term::parse("go")],
            &Connections::new(),
            &edited,
            &Options::default(),
        )
        .unwrap();
        assert!(
            out.contains(
                "src/a.rs-3-» edited since index; definitions not shown\nsrc/a.rs:3:fn go() {}\n"
            ),
            "{out}"
        );
        assert!(
            out.contains("src/b.rs-1-» Function run:1-9\nsrc/b.rs:4:go();\n"),
            "{out}"
        );
    }

    #[test]
    fn alternatives_are_counted_and_long_lines_print_whole_unless_capped() {
        let long = format!("{} go()", "x".repeat(500));
        let hits = vec![
            hit("src/a.rs", 3, "fn go() { stop() }", None),
            hit("src/a.rs", 4, &long, None),
        ];
        let terms = [Term::parse("go"), Term::parse("stop"), Term::parse("nope")];
        let out = show(&hits, &terms, &Options::default());
        assert!(
            out.starts_with("grep: 2 lines in 1 files (go 2, stop 1, nope 0)\n"),
            "{out}"
        );
        assert!(out.contains(&format!("src/a.rs:4:{long}\n")), "{out}");
        let capped = Options {
            max_columns: Some(100),
            ..Options::default()
        };
        let out = show(&hits, &terms, &capped);
        assert!(out.contains("src/a.rs:4:[Omitted long line]\n"), "{out}");
        assert!(
            show(&hits, &terms[..1], &Options::default()).starts_with("grep: 2 lines in 1 files\n")
        );
        assert_eq!(
            show(&[], &terms, &Options::default()),
            "grep: 0 lines in 0 files\n"
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
        let out = show(
            &hits,
            &[Term::parse("maintenanceMode")],
            &Options::default(),
        );
        assert_eq!(
            out,
            "grep: 1 lines in 1 files\npublic/language/{en-GB,+3}/advanced.json:2:\"maintenance-mode\": \"x\"\n"
        );
    }

    fn finds(term: &str, text: &str, options: &Options) -> bool {
        matcher(&[Term::parse(term)], options)
            .unwrap()
            .is_match(text.as_bytes())
            .unwrap()
    }

    #[test]
    fn regex_terms_match_like_ripgrep_and_bad_patterns_fall_back_to_literals() {
        let plain = Options::default();
        assert!(finds(r"\bCveDetail\b", "x := CveDetail{}", &plain));
        assert!(!finds(r"\bCveDetail\b", "CveDetails", &plain));
        assert!(finds(r"type .* struct\{\}", "type key struct{}", &plain));
        assert!(finds(r"route\(", ".route(\"/x\")", &plain));
        assert!(finds("^rand", "rand = \"0.10\"", &plain));
        assert!(!finds("^rand", "x.rand = 1", &plain));
        assert!(finds("mark_in_sync", "markInSync()", &plain));
        assert!(Term::parse("on.*Login").names("onLogin"));
        let literal = |raw: &str, text: &str| {
            matcher(&[Term::parse(raw).literal()], &plain)
                .unwrap()
                .is_match(text.as_bytes())
                .unwrap()
        };
        assert!(literal("route(", ".route(\"/x\")"));
        assert!(literal("????", "x = '????'"));
        let word = Options {
            word: true,
            ..Options::default()
        };
        assert!(finds("detail", "a detail here", &word));
        assert!(!finds("detail", "CveDetails", &word));
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
        let options = Options::default();
        let matcher = matcher(&[term], &options).unwrap();
        let files = |paths: &[&str]| {
            let paths: Vec<String> = paths.iter().map(|p| p.to_string()).collect();
            let mut found: Vec<String> = scan(repo.path(), &paths, &matcher, &options)
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
        assert_eq!(
            files(&["src", "src/nested"]),
            ["src/a.rs:1", "src/nested/b.ts:1"]
        );
        assert_eq!(files(&["s*/nested"]), ["src/nested/b.ts:1"]);
        write("app/[slug]/page.tsx", "markInSync();\n");
        assert_eq!(files(&["app/[slug]"]), ["app/[slug]/page.tsx:1"]);
        for bad in ["nope", "src/[*.ts", ".."] {
            let paths = vec![bad.to_string()];
            assert!(
                scan(repo.path(), &paths, &matcher, &options).is_err(),
                "{bad}"
            );
        }
    }

    #[test]
    fn scan_honors_rg_globs_types_context_inversion_and_max_count() {
        let repo = tempfile::tempdir().unwrap();
        let write = |path: &str, body: &str| {
            let full = repo.path().join(path);
            std::fs::create_dir_all(full.parent().unwrap()).unwrap();
            std::fs::write(full, body).unwrap();
        };
        write("src/a.rs", "one\ngo();\ntwo\ngo();\n");
        write("src/b.py", "go()\n");
        write("vendor/c.rs", "go();\n");
        let term = Term::parse("go");
        let run = |options: Options| {
            let found = matcher(std::slice::from_ref(&term), &options).unwrap();
            let mut lines: Vec<String> = scan(repo.path(), &[], &found, &options)
                .unwrap()
                .into_iter()
                .map(|h| format!("{}{}{}", h.file, if h.context { '-' } else { ':' }, h.line))
                .collect();
            lines.sort();
            lines
        };
        let with = |globs: &[&str]| Options {
            globs: globs.iter().map(|g| g.to_string()).collect(),
            ..Options::default()
        };
        assert_eq!(run(with(&["*.py"])), ["src/b.py:1"]);
        assert_eq!(
            run(with(&["!vendor/"])),
            ["src/a.rs:2", "src/a.rs:4", "src/b.py:1"]
        );
        assert_eq!(
            run(Options {
                types: vec!["rust".into()],
                ..Options::default()
            }),
            ["src/a.rs:2", "src/a.rs:4", "vendor/c.rs:1"]
        );
        assert_eq!(
            run(Options {
                before: 1,
                max_count: Some(1),
                globs: vec!["src/a.rs".into()],
                ..Options::default()
            }),
            ["src/a.rs-1", "src/a.rs:2"]
        );
        assert_eq!(
            run(Options {
                invert: true,
                globs: vec!["src/a.rs".into()],
                ..Options::default()
            }),
            ["src/a.rs:1", "src/a.rs:3"]
        );
    }
}
