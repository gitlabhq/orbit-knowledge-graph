use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::fmt::Write as _;
use std::path::{Path, PathBuf};
use std::process::Command;

use anyhow::{Context, Result};
use duckdb_client::search::excluded_path_predicate;
use duckdb_client::{DuckDbClient, i64_column, sql_lit, string_column};
use serde_json::{Value, json};

use crate::commands::setup::spec;
use crate::workspace;

const ENTRYPOINT_NAMES: &[&str] = &["main", "init", "new", "default", "drop", "fmt", "from"];
const ENTRYPOINT_PREFIXES: &[&str] = &["Test", "Benchmark", "Example", "Fuzz"];
const CONTAINER_KINDS: &[&str] = &["Impl", "Module", "Namespace"];
const TEST_MODULE_REGEX: &str = r"(^|::|\.)(tests?|[a-z_]+_tests)(::|\.)";
const CALLER_DEPTH: i64 = 3;
const GATE_FAILED_EXIT_CODE: i32 = 3;
const REPO_LOCAL_GIT_ENV: &[&str] = &[
    "GIT_DIR",
    "GIT_WORK_TREE",
    "GIT_INDEX_FILE",
    "GIT_OBJECT_DIRECTORY",
    "GIT_ALTERNATE_OBJECT_DIRECTORIES",
    "GIT_COMMON_DIR",
    "GIT_NAMESPACE",
    "GIT_PREFIX",
];
const MAX_SCAN_BYTES: u64 = 2 * 1024 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, clap::ValueEnum)]
pub(crate) enum Rule {
    Cycles,
    Hotspots,
    Dead,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, clap::ValueEnum)]
pub(crate) enum Format {
    #[default]
    Text,
    Json,
}

pub(crate) struct Options {
    pub rules: Vec<Rule>,
    pub repo: Option<PathBuf>,
    pub db: Option<PathBuf>,
    pub diff: Option<String>,
    pub gate: bool,
    pub limit: usize,
    pub depth: Option<usize>,
    pub format: Format,
}

pub(crate) fn run(options: Options) -> Result<()> {
    anyhow::ensure!(
        spec::nightly_enabled(),
        "`{} quality` is a nightly feature; set ORBIT_NIGHTLY=1 to try it",
        spec::launcher()
    );
    let rules: BTreeSet<Rule> = if options.rules.is_empty() {
        [Rule::Cycles, Rule::Hotspots, Rule::Dead].into()
    } else {
        options.rules.iter().copied().collect()
    };
    let workspace::IndexedRepo { git, client } =
        workspace::open_indexed(options.repo.clone(), options.db.clone())?;
    let graph = Graph {
        client: &client,
        params: [git.project_id.into(), git.commit_sha.clone().into()],
    };

    let hunks = match &options.diff {
        Some(base) => Some(diff_ranges(&git.repo_path, base)?),
        None => None,
    };
    let changed = match &hunks {
        Some(hunks) => Some(changed_definitions(&graph, hunks)?),
        None => None,
    };
    let changed_ids: HashSet<i64> = changed
        .iter()
        .flatten()
        .map(|definition| definition.id)
        .collect();
    let base = match (&options.diff, options.gate) {
        (Some(base), true) => Some(BaseGraph::index(&git.repo_path, base)?),
        _ => None,
    };

    let cycles = match rules.contains(&Rule::Cycles) || options.gate {
        true => Some(find_cycles(&graph, options.depth)?),
        false => None,
    };
    let hotspots = match rules.contains(&Rule::Hotspots) {
        true => Some(hotspots(&graph, &git.repo_path, options.limit)?),
        false => None,
    };
    let dead = match rules.contains(&Rule::Dead) || options.gate {
        true => Some(dead_functions(&graph, &git.repo_path)?),
        false => None,
    };
    let blast = match &changed {
        Some(changed) => Some(blast_radius(&graph, changed)?),
        None => None,
    };

    let violations = match &base {
        Some(base) => {
            let base_reader = base.open()?;
            let base_graph = Graph {
                client: &base_reader,
                params: [base.project_id.into(), base.commit_sha.clone().into()],
            };
            let renames = Renames::between(&git.repo_path, &base.commit_sha, &git.commit_sha)?;
            let base_edges: HashSet<(String, String)> = dependencies(&base_graph, options.depth)?
                .edges
                .into_keys()
                .map(|(from, to)| (renames.module(&from), renames.module(&to)))
                .collect();
            let base_dead: HashSet<(String, String)> = dead_functions(&base_graph, &base.checkout)?
                .into_iter()
                .map(|function| (renames.file(&function.file), function.name))
                .collect();
            gate_violations(
                cycles.as_deref().unwrap_or_default(),
                &base_edges,
                dead.as_deref().unwrap_or_default(),
                &base_dead,
                &changed_ids,
            )
        }
        None => Vec::new(),
    };
    let report = Report {
        repo: git.repo_path.display().to_string(),
        commit: git.short_sha().to_string(),
        base: options.diff.clone(),
        limit: options.limit,
        cycles: cycles.filter(|_| rules.contains(&Rule::Cycles)),
        hotspots,
        dead: dead.filter(|_| rules.contains(&Rule::Dead)),
        blast,
        violations: options.gate.then_some(violations),
    };
    match options.format {
        Format::Text => print!("{}", report.text()),
        Format::Json => println!("{}", serde_json::to_string_pretty(&report.json())?),
    }
    let failed = report.violations.as_ref().is_some_and(|v| !v.is_empty());
    drop(base);
    if failed {
        std::process::exit(GATE_FAILED_EXIT_CODE);
    }
    Ok(())
}

struct Graph<'a> {
    client: &'a DuckDbClient,
    params: [Value; 2],
}

const RUST_CRATE_MACRO: &str = "CREATE OR REPLACE TEMP MACRO rust_crate(path) AS
  regexp_replace(path, '(^|/)(src|tests|benches|examples)/.*$', '')";

impl Graph<'_> {
    fn query(&self, sql: &str) -> Result<Vec<arrow::array::RecordBatch>> {
        self.client
            .execute(RUST_CRATE_MACRO, &[])
            .context("failed to define the rust_crate macro")?;
        self.client
            .query_arrow_json(sql, &self.params)
            .with_context(|| format!("quality query failed: {}", sql.lines().next().unwrap_or("")))
    }
}

fn test_code(path: &str, fqn: &str) -> String {
    format!(
        "(COALESCE({}, FALSE) OR regexp_matches({fqn}, {}))",
        excluded_path_predicate(path),
        sql_lit(TEST_MODULE_REGEX)
    )
}

const DEFINITIONS_CTE: &str = "d AS (
  SELECT id, fqn, name, definition_type, file_path, start_line, end_line
  FROM gl_definition WHERE project_id = ?1 AND commit_sha = ?2
)";

const CALLS_CTE: &str = "imp AS (
  SELECT i.id AS imp_id, i.file_path, i.start_line, e.target_id
  FROM gl_edge e
  JOIN gl_imported_symbol i ON i.id = e.source_id AND i.project_id = ?1 AND i.commit_sha = ?2
  JOIN d t ON t.id = e.target_id
  WHERE e.relationship_kind = 'IMPORTS'
    AND (i.file_path NOT LIKE '%.rs'
      OR (rust_crate(i.file_path) = rust_crate(t.file_path) AND (
        i.import_path NOT LIKE '%::%'
        OR split_part(i.import_path, '::', 1) IN ('crate', 'self', 'super')
        OR position('::' || split_part(i.import_path, '::', 1) || '::' IN '::' || t.fqn) > 0))
      OR (rust_crate(i.file_path) <> rust_crate(t.file_path)
        AND split_part(i.import_path, '::', 1) = split_part(t.fqn, '::', 1)))
),
file_deps AS (
  SELECT DISTINCT imp.file_path, split_part(t.fqn, '::', 1) AS r FROM imp JOIN d t ON t.id = imp.target_id
  UNION
  SELECT DISTINCT i.file_path, split_part(i.import_path, '::', 1)
  FROM gl_imported_symbol i
  WHERE i.project_id = ?1 AND i.commit_sha = ?2 AND i.file_path LIKE '%.rs'
),
c AS (
  SELECT e.source_id, e.target_id FROM gl_edge e
  JOIN d s ON s.id = e.source_id JOIN d t ON t.id = e.target_id
  WHERE e.relationship_kind = 'CALLS' AND e.source_kind = 'Definition' AND e.target_kind = 'Definition'
    AND (s.file_path NOT LIKE '%.rs'
      OR rust_crate(s.file_path) = rust_crate(t.file_path)
      OR EXISTS (SELECT 1 FROM file_deps f WHERE f.file_path = s.file_path AND f.r = split_part(t.fqn, '::', 1)))
  UNION
  SELECT e.source_id, imp.target_id FROM gl_edge e JOIN imp ON imp.imp_id = e.target_id
  WHERE e.relationship_kind = 'CALLS' AND e.source_kind = 'Definition' AND e.target_kind = 'ImportedSymbol'
)";

fn module_of(path: &str, depth: Option<usize>) -> String {
    let dir = match path.rsplit_once('/') {
        Some((dir, _)) => dir,
        None => ".",
    };
    match depth {
        Some(depth) if dir != "." => dir.split('/').take(depth).collect::<Vec<_>>().join("/"),
        _ => dir.to_string(),
    }
}

#[derive(Debug, Clone)]
struct ModuleEdge {
    from: String,
    to: String,
    refs: usize,
    example: String,
}

#[derive(Debug, Clone)]
struct Cycle {
    modules: Vec<String>,
    edges: Vec<ModuleEdge>,
}

struct Dependencies {
    edges: BTreeMap<(String, String), ModuleEdge>,
    cycles: Vec<Cycle>,
}

fn find_cycles(graph: &Graph, depth: Option<usize>) -> Result<Vec<Cycle>> {
    Ok(dependencies(graph, depth)?.cycles)
}

fn dependencies(graph: &Graph, depth: Option<usize>) -> Result<Dependencies> {
    let sql = format!(
        "WITH {DEFINITIONS_CTE}, {CALLS_CTE},
dep AS (
  SELECT s.file_path AS sf, s.fqn AS sname, s.start_line AS sl, t.file_path AS tf, t.fqn AS tname
  FROM c JOIN d s ON s.id = c.source_id JOIN d t ON t.id = c.target_id
  UNION ALL
  SELECT imp.file_path, '', imp.start_line, t.file_path, t.fqn
  FROM imp JOIN d t ON t.id = imp.target_id
)
SELECT sf, sname, CAST(sl AS BIGINT) AS sl, tf, tname
FROM dep WHERE sf <> tf AND NOT {} AND NOT {}
ORDER BY sf, sl, tf, tname",
        test_code("sf", "sname"),
        test_code("tf", "tname")
    );
    let batches = graph.query(&sql)?;
    let sfs = string_column(&batches, "sf");
    let snames = string_column(&batches, "sname");
    let sls = i64_column(&batches, "sl");
    let tfs = string_column(&batches, "tf");
    let tnames = string_column(&batches, "tname");

    let mut edges: BTreeMap<(String, String), ModuleEdge> = BTreeMap::new();
    for i in 0..sfs.len() {
        let from = module_of(&sfs[i], depth);
        let to = module_of(&tfs[i], depth);
        if from == to {
            continue;
        }
        let edge = edges
            .entry((from.clone(), to.clone()))
            .or_insert_with(|| ModuleEdge {
                from,
                to,
                refs: 0,
                example: match snames[i].as_str() {
                    "" => format!("imports {}  ({}:{})", tnames[i], sfs[i], sls[i]),
                    source => format!("{source} -> {}  ({}:{})", tnames[i], sfs[i], sls[i]),
                },
            });
        edge.refs += 1;
    }

    let nodes: Vec<String> = edges
        .keys()
        .flat_map(|(from, to)| [from.clone(), to.clone()])
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect();
    let index: HashMap<&str, usize> = nodes
        .iter()
        .enumerate()
        .map(|(i, name)| (name.as_str(), i))
        .collect();
    let mut adjacency = vec![Vec::new(); nodes.len()];
    for (from, to) in edges.keys() {
        adjacency[index[from.as_str()]].push(index[to.as_str()]);
    }

    let mut cycles: Vec<Cycle> = strongly_connected(&adjacency)
        .into_iter()
        .filter(|component| component.len() > 1)
        .map(|component| {
            let members: BTreeSet<&str> = component.iter().map(|&i| nodes[i].as_str()).collect();
            let mut inner: Vec<ModuleEdge> = edges
                .values()
                .filter(|edge| {
                    members.contains(edge.from.as_str()) && members.contains(edge.to.as_str())
                })
                .cloned()
                .collect();
            inner.sort_by(|a, b| a.refs.cmp(&b.refs).then(a.from.cmp(&b.from)));
            Cycle {
                modules: members.into_iter().map(String::from).collect(),
                edges: inner,
            }
        })
        .collect();
    cycles.sort_by(|a, b| {
        b.modules
            .len()
            .cmp(&a.modules.len())
            .then(a.modules.cmp(&b.modules))
    });
    Ok(Dependencies { edges, cycles })
}

fn strongly_connected(adjacency: &[Vec<usize>]) -> Vec<Vec<usize>> {
    let n = adjacency.len();
    let mut index: Vec<Option<usize>> = vec![None; n];
    let mut low = vec![0; n];
    let mut on_stack = vec![false; n];
    let mut stack = Vec::new();
    let mut next = 0;
    let mut components = Vec::new();
    for root in 0..n {
        if index[root].is_some() {
            continue;
        }
        index[root] = Some(next);
        low[root] = next;
        next += 1;
        stack.push(root);
        on_stack[root] = true;
        let mut work: Vec<(usize, usize)> = vec![(root, 0)];
        while let Some(&(node, child)) = work.last() {
            if let Some(&target) = adjacency[node].get(child) {
                work.last_mut().expect("work is not empty").1 += 1;
                match index[target] {
                    None => {
                        index[target] = Some(next);
                        low[target] = next;
                        next += 1;
                        stack.push(target);
                        on_stack[target] = true;
                        work.push((target, 0));
                    }
                    Some(seen) if on_stack[target] => low[node] = low[node].min(seen),
                    Some(_) => {}
                }
                continue;
            }
            work.pop();
            if let Some(&(parent, _)) = work.last() {
                low[parent] = low[parent].min(low[node]);
            }
            if Some(low[node]) == index[node] {
                let mut component = Vec::new();
                while let Some(member) = stack.pop() {
                    on_stack[member] = false;
                    component.push(member);
                    if member == node {
                        break;
                    }
                }
                components.push(component);
            }
        }
    }
    components
}

#[derive(Debug)]
struct Hotspot {
    fqn: String,
    kind: String,
    loc: String,
    callers: i64,
    files: i64,
    churn: Option<usize>,
}

fn hotspots(graph: &Graph, repo: &Path, limit: usize) -> Result<Vec<Hotspot>> {
    let sql = format!(
        "WITH {DEFINITIONS_CTE}, {CALLS_CTE}
SELECT t.fqn, t.definition_type AS kind, t.file_path AS file, CAST(t.start_line AS BIGINT) AS line,
       CAST(count(DISTINCT s.id) AS BIGINT) AS callers, CAST(count(DISTINCT s.file_path) AS BIGINT) AS files
FROM c JOIN d s ON s.id = c.source_id JOIN d t ON t.id = c.target_id
WHERE s.file_path <> t.file_path AND NOT {} AND NOT {}
GROUP BY t.id, t.fqn, t.definition_type, t.file_path, t.start_line
ORDER BY files DESC, callers DESC, t.fqn
LIMIT {limit}",
        test_code("s.file_path", "s.fqn"),
        test_code("t.file_path", "t.fqn")
    );
    let batches = graph.query(&sql)?;
    let churn = file_churn(repo);
    let fqns = string_column(&batches, "fqn");
    let kinds = string_column(&batches, "kind");
    let files = string_column(&batches, "file");
    let lines = i64_column(&batches, "line");
    let callers = i64_column(&batches, "callers");
    let caller_files = i64_column(&batches, "files");
    Ok((0..fqns.len())
        .map(|i| Hotspot {
            fqn: fqns[i].clone(),
            kind: kinds[i].clone(),
            loc: format!("{}:{}", files[i], lines[i]),
            callers: callers[i],
            files: caller_files[i],
            churn: churn
                .as_ref()
                .map(|churn| churn.get(&files[i]).copied().unwrap_or(0)),
        })
        .collect())
}

fn file_churn(repo: &Path) -> Option<HashMap<String, usize>> {
    let commits = git(repo, &["rev-list", "--count", "--since=90.days", "HEAD"]).ok()?;
    if commits.trim().parse::<usize>().unwrap_or(0) < 2 {
        return None;
    }
    let output = git(
        repo,
        &["log", "--since=90.days", "--format=", "--name-only"],
    )
    .ok()?;
    let mut churn = HashMap::new();
    for line in output.lines().filter(|line| !line.is_empty()) {
        *churn.entry(line.to_string()).or_insert(0) += 1;
    }
    Some(churn)
}

#[derive(Debug, Clone)]
struct Dead {
    id: i64,
    fqn: String,
    name: String,
    file: String,
    loc: String,
}

fn is_entrypoint(name: &str) -> bool {
    ENTRYPOINT_NAMES.contains(&name)
        || name.starts_with("test_")
        || ENTRYPOINT_PREFIXES.iter().any(|prefix| {
            name.strip_prefix(prefix)
                .is_some_and(|rest| rest.is_empty() || !rest.starts_with(char::is_lowercase))
        })
}

fn dead_functions(graph: &Graph, repo: &Path) -> Result<Vec<Dead>> {
    let commit = graph.params[1].as_str().unwrap_or("HEAD").to_string();
    let sql = format!(
        "WITH {DEFINITIONS_CTE}, {CALLS_CTE},
referenced AS (
  SELECT target_id AS id FROM c
  UNION SELECT target_id FROM gl_edge
  WHERE target_kind = 'Definition' AND relationship_kind IN ('CALLS', 'IMPORTS', 'EXTENDS')
),
names AS (SELECT name, CAST(count(*) AS BIGINT) AS defs FROM d GROUP BY name),
owner AS (
  SELECT e.target_id AS id, min(p.start_line) AS impl_line FROM gl_edge e
  JOIN d p ON p.id = e.source_id AND p.definition_type = 'Impl'
  WHERE e.relationship_kind = 'DEFINES' AND e.source_kind = 'Definition'
  GROUP BY e.target_id
)
SELECT d.id, d.fqn, d.name, d.file_path AS file, CAST(d.start_line AS BIGINT) AS line, n.defs,
       CAST(COALESCE(o.impl_line, 0) AS BIGINT) AS impl_line
FROM d JOIN names n ON n.name = d.name LEFT JOIN owner o ON o.id = d.id
WHERE d.definition_type = 'Function'
  AND d.id NOT IN (SELECT id FROM referenced)
  AND NOT {}
ORDER BY d.file_path, d.start_line",
        test_code("d.file_path", "d.fqn")
    );
    let batches = graph.query(&sql)?;
    let ids = i64_column(&batches, "id");
    let fqns = string_column(&batches, "fqn");
    let names = string_column(&batches, "name");
    let files = string_column(&batches, "file");
    let lines = i64_column(&batches, "line");
    let defs = i64_column(&batches, "defs");
    let impl_lines = i64_column(&batches, "impl_line");
    let candidates: Vec<usize> = (0..ids.len())
        .filter(|&i| !is_entrypoint(&names[i]))
        .collect();
    let wanted: HashSet<&str> = candidates.iter().map(|&i| names[i].as_str()).collect();
    let committed = commit_files(repo, &commit)?;
    let mentions = count_identifiers(&committed, &wanted);
    let mut sources: HashMap<&str, Option<Vec<String>>> = HashMap::new();
    Ok(candidates
        .into_iter()
        .filter(|&i| mentions.get(names[i].as_str()).copied().unwrap_or(0) <= defs[i] as usize)
        .filter(|&i| {
            let source = sources.entry(files[i].as_str()).or_insert_with(|| {
                committed
                    .get(&files[i])
                    .map(|text| text.lines().map(String::from).collect())
            });
            !source.as_deref().is_some_and(|source| {
                has_test_attribute(source, lines_index(lines[i]))
                    || (impl_lines[i] > 0
                        && source
                            .get(lines_index(impl_lines[i]))
                            .is_some_and(|line| is_trait_impl(line)))
            })
        })
        .map(|i| Dead {
            id: ids[i],
            fqn: fqns[i].clone(),
            name: names[i].clone(),
            file: files[i].clone(),
            loc: format!("{}:{}", files[i], lines[i]),
        })
        .collect())
}

fn lines_index(start_line: i64) -> usize {
    usize::try_from(start_line).unwrap_or(1).saturating_sub(1)
}

fn is_trait_impl(line: &str) -> bool {
    let line = line.trim_start();
    line.starts_with("impl") && line.split_whitespace().any(|word| word == "for")
}

fn has_test_attribute(lines: &[String], start: usize) -> bool {
    lines[..start.min(lines.len())]
        .iter()
        .rev()
        .take(4)
        .map(|line| line.trim())
        .take_while(|line| {
            line.starts_with("#[") || line.starts_with("@") || line.starts_with("//")
        })
        .any(|line| {
            line.starts_with("#[") && line.contains("test")
                || line.starts_with("@pytest")
                || line == "@Test"
        })
}

fn commit_files(repo: &Path, commit: &str) -> Result<HashMap<String, String>> {
    let tree = git(
        repo,
        &["ls-tree", "-r", "-z", "-l", "--end-of-options", commit],
    )?;
    let mut paths: Vec<String> = Vec::new();
    let mut request = String::new();
    for entry in tree.split('\0').filter(|entry| !entry.is_empty()) {
        let Some((meta, path)) = entry.split_once('\t') else {
            continue;
        };
        let mut fields = meta.split_whitespace();
        let (Some(_), Some("blob"), Some(object), Some(size)) =
            (fields.next(), fields.next(), fields.next(), fields.next())
        else {
            continue;
        };
        if size
            .parse::<u64>()
            .map_or(true, |size| size > MAX_SCAN_BYTES)
        {
            continue;
        }
        paths.push(path.to_string());
        request.push_str(object);
        request.push('\n');
    }
    let mut child = without_repo_git_env(&mut Command::new("git"))
        .args(["cat-file", "--batch"])
        .current_dir(repo)
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .spawn()
        .context("failed to run git cat-file")?;
    let mut stdin = child.stdin.take().context("git cat-file has no stdin")?;
    let writer = std::thread::spawn(move || {
        use std::io::Write as _;
        stdin.write_all(request.as_bytes())
    });
    let mut output = Vec::new();
    {
        use std::io::Read as _;
        child
            .stdout
            .take()
            .context("git cat-file has no stdout")?
            .read_to_end(&mut output)?;
    }
    writer
        .join()
        .map_err(|_| anyhow::anyhow!("git cat-file writer panicked"))??;
    anyhow::ensure!(child.wait()?.success(), "git cat-file failed");
    let mut files = HashMap::new();
    let mut rest = output.as_slice();
    for path in paths {
        let Some(newline) = rest.iter().position(|&byte| byte == b'\n') else {
            break;
        };
        let header = String::from_utf8_lossy(&rest[..newline]).into_owned();
        rest = &rest[newline + 1..];
        let Some(size) = header
            .split_whitespace()
            .nth(2)
            .and_then(|size| size.parse::<usize>().ok())
        else {
            continue;
        };
        let body = &rest[..size.min(rest.len())];
        rest = &rest[(size + 1).min(rest.len())..];
        if let Ok(text) = std::str::from_utf8(body) {
            files.insert(path, text.to_string());
        }
    }
    Ok(files)
}

fn count_identifiers(
    files: &HashMap<String, String>,
    wanted: &HashSet<&str>,
) -> HashMap<String, usize> {
    let mut counts: HashMap<String, usize> = HashMap::new();
    if wanted.is_empty() {
        return counts;
    }
    for text in files.values() {
        for token in text.split(|c: char| !(c.is_alphanumeric() || c == '_')) {
            if wanted.contains(token) {
                *counts.entry(token.to_string()).or_insert(0) += 1;
            }
        }
    }
    counts
}

#[derive(Debug, Clone)]
struct ChangedDefinition {
    id: i64,
    fqn: String,
    kind: String,
    loc: String,
}

fn resolve_commit(repo: &Path, base: &str) -> Result<String> {
    anyhow::ensure!(!base.starts_with('-'), "--diff takes a commit, not {base}");
    git(
        repo,
        &[
            "rev-parse",
            "--verify",
            "--quiet",
            "--end-of-options",
            &format!("{base}^{{commit}}"),
        ],
    )
    .ok()
    .map(|sha| sha.trim().to_string())
    .filter(|sha| !sha.is_empty())
    .with_context(|| format!("{base} is not a commit here; is it fetched?"))
}

fn diff_ranges(repo: &Path, base: &str) -> Result<BTreeMap<String, Vec<(i64, i64)>>> {
    let range = format!("{}...HEAD", resolve_commit(repo, base)?);
    let diff = git(
        repo,
        &[
            "diff",
            "-U0",
            "--no-color",
            "--no-ext-diff",
            "--end-of-options",
            &range,
        ],
    )
    .with_context(|| format!("failed to diff {base}...HEAD"))?;
    Ok(parse_diff(&diff))
}

fn parse_diff(diff: &str) -> BTreeMap<String, Vec<(i64, i64)>> {
    let mut ranges: BTreeMap<String, Vec<(i64, i64)>> = BTreeMap::new();
    let mut file: Option<String> = None;
    for line in diff.lines() {
        if let Some(path) = line.strip_prefix("+++ ") {
            file = path.strip_prefix("b/").map(String::from);
            continue;
        }
        let (Some(file), Some(hunk)) = (&file, line.strip_prefix("@@ ")) else {
            continue;
        };
        let Some(added) = hunk.split_whitespace().find(|part| part.starts_with('+')) else {
            continue;
        };
        let mut parts = added[1..].splitn(2, ',');
        let start: i64 = parts.next().and_then(|s| s.parse().ok()).unwrap_or(0);
        let count: i64 = parts.next().and_then(|s| s.parse().ok()).unwrap_or(1);
        let range = match count {
            0 => (start.max(1), start.max(1)),
            count => (start, start + count - 1),
        };
        ranges.entry(file.clone()).or_default().push(range);
    }
    ranges
}

fn changed_definitions(
    graph: &Graph,
    ranges: &BTreeMap<String, Vec<(i64, i64)>>,
) -> Result<Vec<ChangedDefinition>> {
    if ranges.is_empty() {
        return Ok(Vec::new());
    }
    let values = ranges
        .iter()
        .flat_map(|(file, spans)| {
            spans
                .iter()
                .map(move |(start, end)| format!("({}, {start}, {end})", sql_lit(file)))
        })
        .collect::<Vec<_>>()
        .join(", ");
    let containers = CONTAINER_KINDS
        .iter()
        .map(|kind| sql_lit(kind))
        .collect::<Vec<_>>()
        .join(", ");
    let sql = format!(
        "WITH {DEFINITIONS_CTE}, hunks(file, hs, he) AS (VALUES {values}),
hit AS (
  SELECT DISTINCT h.file, h.hs, h.he, d.* FROM hunks h
  JOIN d ON d.file_path = h.file AND d.start_line <= h.he AND d.end_line >= h.hs
  WHERE d.definition_type NOT IN ({containers})
)
SELECT DISTINCT o.id, o.fqn, o.definition_type AS kind, o.file_path AS file, CAST(o.start_line AS BIGINT) AS line
FROM hit o
WHERE NOT EXISTS (
  SELECT 1 FROM hit i
  WHERE i.file = o.file AND i.hs = o.hs AND i.id <> o.id
    AND i.start_line >= o.start_line AND i.end_line <= o.end_line
    AND (i.start_line <> o.start_line OR i.end_line <> o.end_line)
)
ORDER BY file, line"
    );
    let batches = graph.query(&sql)?;
    let ids = i64_column(&batches, "id");
    let fqns = string_column(&batches, "fqn");
    let kinds = string_column(&batches, "kind");
    let files = string_column(&batches, "file");
    let lines = i64_column(&batches, "line");
    Ok((0..ids.len())
        .map(|i| ChangedDefinition {
            id: ids[i],
            fqn: fqns[i].clone(),
            kind: kinds[i].clone(),
            loc: format!("{}:{}", files[i], lines[i]),
        })
        .collect())
}

#[derive(Debug)]
struct Blast {
    definition: ChangedDefinition,
    direct: i64,
    transitive: i64,
    files: i64,
    tests: i64,
}

fn blast_radius(graph: &Graph, changed: &[ChangedDefinition]) -> Result<Vec<Blast>> {
    if changed.is_empty() {
        return Ok(Vec::new());
    }
    let roots = changed
        .iter()
        .map(|definition| format!("({})", definition.id))
        .collect::<Vec<_>>()
        .join(", ");
    let sql = format!(
        "WITH RECURSIVE {DEFINITIONS_CTE}, {CALLS_CTE}, roots(root) AS (VALUES {roots}),
walk(root, id, depth) AS (
  SELECT r.root, c.source_id, 1 FROM roots r JOIN c ON c.target_id = r.root
  UNION
  SELECT w.root, c.source_id, w.depth + 1 FROM walk w JOIN c ON c.target_id = w.id
  WHERE w.depth < {CALLER_DEPTH}
),
callers AS (
  SELECT w.root, w.id, min(w.depth) AS depth FROM walk w WHERE w.id <> w.root GROUP BY w.root, w.id
)
SELECT r.root,
       CAST(count(DISTINCT x.id) FILTER (WHERE x.depth = 1 AND NOT {test}) AS BIGINT) AS direct,
       CAST(count(DISTINCT x.id) FILTER (WHERE NOT {test}) AS BIGINT) AS transitive,
       CAST(count(DISTINCT s.file_path) FILTER (WHERE NOT {test}) AS BIGINT) AS files,
       CAST(count(DISTINCT x.id) FILTER (WHERE {test}) AS BIGINT) AS tests
FROM roots r
LEFT JOIN callers x ON x.root = r.root
LEFT JOIN d s ON s.id = x.id
GROUP BY r.root",
        test = test_code("s.file_path", "s.fqn")
    );
    let batches = graph.query(&sql)?;
    let roots = i64_column(&batches, "root");
    let direct = i64_column(&batches, "direct");
    let transitive = i64_column(&batches, "transitive");
    let files = i64_column(&batches, "files");
    let tests = i64_column(&batches, "tests");
    let by_root: HashMap<i64, usize> = roots.iter().enumerate().map(|(i, &id)| (id, i)).collect();
    let mut blast: Vec<Blast> = changed
        .iter()
        .map(|definition| {
            let i = by_root.get(&definition.id).copied();
            let pick = |column: &[i64]| i.map_or(0, |i| column[i]);
            Blast {
                definition: definition.clone(),
                direct: pick(&direct),
                transitive: pick(&transitive),
                files: pick(&files),
                tests: pick(&tests),
            }
        })
        .collect();
    blast.sort_by(|a, b| {
        b.transitive
            .cmp(&a.transitive)
            .then(b.direct.cmp(&a.direct))
            .then(a.definition.loc.cmp(&b.definition.loc))
    });
    Ok(blast)
}

struct BaseGraph {
    repo: PathBuf,
    scratch: tempfile::TempDir,
    checkout: PathBuf,
    project_id: i64,
    commit_sha: String,
}

impl BaseGraph {
    fn index(repo: &Path, base: &str) -> Result<Self> {
        let commit_sha = resolve_commit(repo, base)?;
        let merge_base = git(
            repo,
            &["merge-base", "--end-of-options", &commit_sha, "HEAD"],
        )
        .with_context(|| format!("{base} and HEAD share no history"))?
        .trim()
        .to_string();
        let scratch = tempfile::Builder::new()
            .prefix("orbit-quality-")
            .tempdir()?;
        let checkout = scratch.path().join("base");
        git(
            repo,
            &[
                "worktree",
                "add",
                "--detach",
                "--quiet",
                &checkout.to_string_lossy(),
                &merge_base,
            ],
        )?;
        let mut base = Self {
            repo: repo.to_path_buf(),
            checkout: dunce::canonicalize(&checkout)?,
            project_id: 0,
            commit_sha: merge_base,
            scratch,
        };
        let status = without_repo_git_env(&mut Command::new(std::env::current_exe()?))
            .args(["workspace", "index", "--db"])
            .arg(base.db_path())
            .arg(&base.checkout)
            .env("ORBIT_DATA_DIR", base.scratch.path().join("data"))
            .env("ORBIT_TELEMETRY_ENABLED", "false")
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::inherit())
            .status()
            .context("failed to index the base commit")?;
        anyhow::ensure!(
            status.success(),
            "indexing the base commit {} failed",
            base.commit_sha
        );
        base.project_id = workspace::git_info(&base.checkout)?.project_id;
        Ok(base)
    }

    fn db_path(&self) -> PathBuf {
        self.scratch.path().join("base.duckdb")
    }

    fn open(&self) -> Result<crate::database::GraphReader> {
        crate::database::GraphReader::open(Some(self.db_path()))
    }
}

impl Drop for BaseGraph {
    fn drop(&mut self) {
        let checkout = self.checkout.to_string_lossy();
        if git(&self.repo, &["worktree", "remove", "--force", &checkout]).is_err() {
            let _ = std::fs::remove_dir_all(&self.checkout);
            let _ = git(&self.repo, &["worktree", "prune"]);
        }
    }
}

struct Renames {
    dirs: Vec<(String, String)>,
    files: HashMap<String, String>,
}

impl Renames {
    fn between(repo: &Path, base: &str, head: &str) -> Result<Self> {
        let output = git(
            repo,
            &[
                "diff",
                "-M",
                "--name-status",
                "-z",
                "--no-ext-diff",
                "--end-of-options",
                base,
                head,
            ],
        )?;
        Ok(Self::parse(&output))
    }

    fn parse(output: &str) -> Self {
        let mut moved: BTreeMap<(String, String), usize> = BTreeMap::new();
        let mut files = HashMap::new();
        let mut fields = output.split('\0').filter(|field| !field.is_empty());
        while let Some(status) = fields.next() {
            if status.starts_with('R') || status.starts_with('C') {
                let (Some(old), Some(new)) = (fields.next(), fields.next()) else {
                    break;
                };
                if status.starts_with('R') {
                    files.insert(old.to_string(), new.to_string());
                    let old_dir = module_of(old, None);
                    let new_dir = module_of(new, None);
                    if old_dir != new_dir {
                        *moved.entry((old_dir, new_dir)).or_insert(0) += 1;
                    }
                }
            } else {
                fields.next();
            }
        }
        let mut best: BTreeMap<String, (usize, String)> = BTreeMap::new();
        for ((old, new), count) in moved {
            let entry = best.entry(old).or_insert((0, String::new()));
            if count > entry.0 {
                *entry = (count, new);
            }
        }
        let mut dirs: Vec<(String, String)> =
            best.into_iter().map(|(old, (_, new))| (old, new)).collect();
        dirs.sort_by_key(|(old, _)| std::cmp::Reverse(old.len()));
        Self { dirs, files }
    }

    fn file(&self, path: &str) -> String {
        self.files
            .get(path)
            .cloned()
            .unwrap_or_else(|| path.to_string())
    }

    fn module(&self, module: &str) -> String {
        for (old, new) in &self.dirs {
            if module == old {
                return new.clone();
            }
            if let Some(rest) = module.strip_prefix(old.as_str())
                && rest.starts_with('/')
            {
                return format!("{new}{rest}");
            }
        }
        module.to_string()
    }
}

fn gate_violations(
    cycles: &[Cycle],
    base_edges: &HashSet<(String, String)>,
    dead: &[Dead],
    base_dead: &HashSet<(String, String)>,
    changed: &HashSet<i64>,
) -> Vec<String> {
    let mut violations = Vec::new();
    for cycle in cycles {
        for edge in &cycle.edges {
            if !base_edges.contains(&(edge.from.clone(), edge.to.clone())) {
                violations.push(format!(
                    "the change adds {} -> {}, which closes a cycle of {} modules: {}",
                    edge.from,
                    edge.to,
                    cycle.modules.len(),
                    edge.example
                ));
            }
        }
    }
    for function in dead {
        if changed.contains(&function.id)
            && !base_dead.contains(&(function.file.clone(), function.name.clone()))
        {
            violations.push(format!(
                "the change adds {} ({}), which nothing references",
                function.fqn, function.loc
            ));
        }
    }
    violations
}

struct Report {
    repo: String,
    commit: String,
    base: Option<String>,
    limit: usize,
    cycles: Option<Vec<Cycle>>,
    hotspots: Option<Vec<Hotspot>>,
    dead: Option<Vec<Dead>>,
    blast: Option<Vec<Blast>>,
    violations: Option<Vec<String>>,
}

impl Report {
    fn text(&self) -> String {
        let mut out = String::new();
        let _ = writeln!(
            out,
            "Orbit quality report for {} at {}",
            self.repo, self.commit
        );
        if let Some(cycles) = &self.cycles {
            let _ = writeln!(
                out,
                "\nDependency cycles between directories: {}",
                cycles.len()
            );
            for (n, cycle) in cycles.iter().take(self.limit).enumerate() {
                let _ = writeln!(
                    out,
                    "  {}. {} modules: {}",
                    n + 1,
                    cycle.modules.len(),
                    cycle.modules.join(", ")
                );
                for edge in cycle.edges.iter().take(self.limit) {
                    let _ = writeln!(
                        out,
                        "     {} -> {}  ({} refs)  e.g. {}",
                        edge.from, edge.to, edge.refs, edge.example
                    );
                }
                if cycle.edges.len() > self.limit {
                    let _ = writeln!(out, "     … {} more edges", cycle.edges.len() - self.limit);
                }
            }
            if cycles.len() > self.limit {
                let _ = writeln!(out, "  … {} more cycles", cycles.len() - self.limit);
            }
            if !cycles.is_empty() {
                let _ = writeln!(
                    out,
                    "  Edges are listed fewest references first: those are the cheapest to cut."
                );
            }
        }
        if let Some(hotspots) = &self.hotspots {
            let churn = hotspots.iter().any(|spot| spot.churn.is_some());
            let _ = writeln!(out, "\nCoupling hotspots (most callers from other files):");
            for spot in hotspots {
                let churn = match spot.churn {
                    Some(commits) => format!(", {commits} commits in 90 days"),
                    None => String::new(),
                };
                let _ = writeln!(
                    out,
                    "  {:>4} files  {:>5} callers  {}  [{}]  {}{churn}",
                    spot.files, spot.callers, spot.fqn, spot.kind, spot.loc
                );
            }
            if !churn && !hotspots.is_empty() {
                let _ = writeln!(
                    out,
                    "  (no churn: the checkout has under 2 commits in the last 90 days)"
                );
            }
        }
        if let Some(dead) = &self.dead {
            let _ = writeln!(
                out,
                "\nUnreferenced functions: {} (no edge in the graph and the name appears nowhere else)",
                dead.len()
            );
            for function in dead.iter().take(self.limit) {
                let _ = writeln!(out, "  {}  {}", function.fqn, function.loc);
            }
            if dead.len() > self.limit {
                let _ = writeln!(
                    out,
                    "  … {} more; pass --limit to see them",
                    dead.len() - self.limit
                );
            }
        }
        if let Some(blast) = &self.blast {
            let base = self.base.as_deref().unwrap_or("");
            let _ = writeln!(
                out,
                "\nBlast radius of definitions changed since {base} ({}), callers up to {CALLER_DEPTH} hops:",
                blast.len()
            );
            for item in blast.iter().take(self.limit) {
                let tested = match item.tests {
                    0 => "no test reaches it".to_string(),
                    n => format!("{n} test callers"),
                };
                let _ = writeln!(
                    out,
                    "  {:>4} direct  {:>5} total  {:>4} files  {}  [{}]  {}  ({tested})",
                    item.direct,
                    item.transitive,
                    item.files,
                    item.definition.fqn,
                    item.definition.kind,
                    item.definition.loc
                );
            }
            if blast.len() > self.limit {
                let _ = writeln!(out, "  … {} more", blast.len() - self.limit);
            }
        }
        if let Some(violations) = &self.violations {
            if violations.is_empty() {
                let _ = writeln!(out, "\nGate: passed");
            } else {
                let _ = writeln!(
                    out,
                    "\nGate: failed ({} findings in changed code)",
                    violations.len()
                );
                for violation in violations {
                    let _ = writeln!(out, "  {violation}");
                }
            }
        }
        out
    }

    fn json(&self) -> Value {
        json!({
            "repo": self.repo,
            "commit": self.commit,
            "base": self.base,
            "cycles": self.cycles.as_ref().map(|cycles| cycles.iter().map(|cycle| json!({
                "modules": cycle.modules,
                "edges": cycle.edges.iter().map(|edge| json!({
                    "from": edge.from, "to": edge.to, "refs": edge.refs, "example": edge.example,
                })).collect::<Vec<_>>(),
            })).collect::<Vec<_>>()),
            "hotspots": self.hotspots.as_ref().map(|spots| spots.iter().map(|spot| json!({
                "fqn": spot.fqn, "kind": spot.kind, "location": spot.loc,
                "callers": spot.callers, "caller_files": spot.files, "churn_90d": spot.churn,
            })).collect::<Vec<_>>()),
            "dead": self.dead.as_ref().map(|dead| dead.iter().map(|function| json!({
                "id": function.id, "fqn": function.fqn, "location": function.loc,
            })).collect::<Vec<_>>()),
            "blast_radius": self.blast.as_ref().map(|blast| blast.iter().map(|item| json!({
                "id": item.definition.id, "fqn": item.definition.fqn, "kind": item.definition.kind,
                "location": item.definition.loc, "direct_callers": item.direct,
                "transitive_callers": item.transitive, "caller_files": item.files,
                "test_callers": item.tests,
            })).collect::<Vec<_>>()),
            "gate": self.violations.as_ref().map(|violations| json!({
                "passed": violations.is_empty(), "violations": violations,
            })),
        })
    }
}

fn without_repo_git_env(command: &mut Command) -> &mut Command {
    for name in REPO_LOCAL_GIT_ENV {
        command.env_remove(name);
    }
    command
}

fn git(repo: &Path, args: &[&str]) -> Result<String> {
    let output = without_repo_git_env(&mut Command::new("git"))
        .args(["-c", "core.hooksPath=/dev/null"])
        .args(args)
        .current_dir(repo)
        .output()
        .with_context(|| format!("failed to run git {}", args.join(" ")))?;
    anyhow::ensure!(
        output.status.success(),
        "git {} failed: {}",
        args.join(" "),
        String::from_utf8_lossy(&output.stderr).trim()
    );
    Ok(String::from_utf8_lossy(&output.stdout).into_owned())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cyclic(adjacency: &[Vec<usize>]) -> Vec<Vec<usize>> {
        let mut components: Vec<Vec<usize>> = strongly_connected(adjacency)
            .into_iter()
            .map(|mut component| {
                component.sort();
                component
            })
            .filter(|component| component.len() > 1)
            .collect();
        components.sort();
        components
    }

    #[test]
    fn tarjan_finds_cycles_and_skips_dags() {
        let adjacency = vec![vec![1], vec![2], vec![0, 3], vec![4], vec![]];
        assert_eq!(cyclic(&adjacency), vec![vec![0, 1, 2]]);
    }

    #[test]
    fn tarjan_handles_self_loops_and_two_cycles() {
        let adjacency = vec![vec![0, 1], vec![0], vec![3], vec![2]];
        assert_eq!(cyclic(&adjacency), vec![vec![0, 1], vec![2, 3]]);
    }

    #[test]
    fn module_of_truncates_to_depth() {
        assert_eq!(module_of("crates/a/src/b/c.rs", None), "crates/a/src/b");
        assert_eq!(module_of("crates/a/src/b/c.rs", Some(2)), "crates/a");
        assert_eq!(module_of("main.go", Some(2)), ".");
        assert_eq!(module_of("main.go", None), ".");
    }

    #[test]
    fn parse_diff_reads_added_ranges_and_skips_deleted_files() {
        let diff = "diff --git a/x.rs b/x.rs\n--- a/x.rs\n+++ b/x.rs\n@@ -3,0 +4,2 @@ fn a\n+x\n+y\n@@ -10,2 +12,0 @@\n-z\ndiff --git a/gone.rs b/gone.rs\n--- a/gone.rs\n+++ /dev/null\n@@ -1,3 +0,0 @@\n";
        let ranges = parse_diff(diff);
        assert_eq!(ranges.len(), 1);
        assert_eq!(ranges["x.rs"], vec![(4, 5), (12, 12)]);
    }

    #[test]
    fn entrypoints_are_not_dead() {
        assert!(is_entrypoint("main"));
        assert!(is_entrypoint("TestParse"));
        assert!(is_entrypoint("Test"));
        assert!(is_entrypoint("test_parse"));
        assert!(!is_entrypoint("parse"));
        assert!(!is_entrypoint("Tester"));
    }

    #[test]
    fn test_attributes_above_a_function_mark_it_as_a_test() {
        let lines: Vec<String> = [
            "#[tokio::test(flavor = \"multi_thread\")]",
            "async fn a() {}",
            "",
            "#[inline]",
            "fn b() {}",
            "#[test]",
            "}",
            "fn c() {}",
        ]
        .map(String::from)
        .to_vec();
        assert!(has_test_attribute(&lines, 1));
        assert!(!has_test_attribute(&lines, 4));
        assert!(!has_test_attribute(&lines, 7));
        assert!(!has_test_attribute(&lines, 0));
    }

    fn cycle(edges: &[(&str, &str)]) -> Cycle {
        Cycle {
            modules: edges
                .iter()
                .flat_map(|(a, b)| [a.to_string(), b.to_string()])
                .collect(),
            edges: edges
                .iter()
                .map(|(from, to)| ModuleEdge {
                    from: from.to_string(),
                    to: to.to_string(),
                    refs: 1,
                    example: String::new(),
                })
                .collect(),
        }
    }

    fn dead(id: i64, name: &str) -> Dead {
        Dead {
            id,
            fqn: format!("m::{name}"),
            name: name.to_string(),
            file: "m.rs".to_string(),
            loc: String::new(),
        }
    }

    fn edges(cycles: &[Cycle], extra: &[(&str, &str)]) -> HashSet<(String, String)> {
        cycles
            .iter()
            .flat_map(|cycle| cycle.edges.iter().cloned())
            .chain(cycle(extra).edges)
            .map(|edge| (edge.from, edge.to))
            .collect()
    }

    #[test]
    fn gate_reports_only_what_the_base_did_not_have() {
        let base = [cycle(&[("a", "b"), ("b", "a")])];
        let base_edges = edges(&base, &[("c", "d")]);
        let head = [
            cycle(&[("a", "b"), ("b", "a")]),
            cycle(&[("c", "d"), ("d", "c")]),
        ];
        let base_dead: HashSet<(String, String)> = [("m.rs".to_string(), "old".to_string())].into();
        let head_dead = [dead(1, "old"), dead(2, "new"), dead(3, "untouched")];
        let changed: HashSet<i64> = [1, 2].into();
        let violations = gate_violations(&head, &base_edges, &head_dead, &base_dead, &changed);
        assert_eq!(violations.len(), 2, "{violations:#?}");
        assert!(violations[0].contains("d -> c"));
        assert!(violations[1].contains("adds m::new"));
        let same = edges(&base, &[]);
        assert!(gate_violations(&base, &same, &head_dead[..1], &base_dead, &changed).is_empty());
    }

    #[test]
    fn renames_map_base_modules_to_their_new_directory() {
        let output = "R100\0src/b/y.rs\0src/d/y.rs\0R090\0src/b/mod.rs\0src/d/mod.rs\0M\0src/lib.rs\0R100\0src/a/x.rs\0src/a/z.rs\0";
        let renames = Renames::parse(output);
        assert_eq!(renames.module("src/b"), "src/d");
        assert_eq!(renames.module("src/b/inner"), "src/d/inner");
        assert_eq!(renames.module("src/bb"), "src/bb");
        assert_eq!(renames.module("src/a"), "src/a");
        assert_eq!(renames.file("src/b/y.rs"), "src/d/y.rs");
        assert_eq!(renames.file("src/lib.rs"), "src/lib.rs");
    }

    #[test]
    fn trait_impls_are_told_apart_from_inherent_impls() {
        assert!(is_trait_impl("impl clap::ValueEnum for DdlTarget {"));
        assert!(is_trait_impl("    impl<T: Clone> From<T> for Wrapper<T> {"));
        assert!(!is_trait_impl("impl NatsBroker {"));
        assert!(!is_trait_impl("impl Formatter {"));
    }

    #[test]
    fn test_modules_match_rust_and_dotted_names() {
        let re = regex::Regex::new(TEST_MODULE_REGEX).unwrap();
        assert!(re.is_match("crate::tests::helper"));
        assert!(re.is_match("crate::resolver::alias_tests::case"));
        assert!(re.is_match("pkg.test.helper"));
        assert!(!re.is_match("crate::attestation::verify"));
        assert!(!re.is_match("crate::contests::run"));
    }
}
