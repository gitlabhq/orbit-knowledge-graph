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

mod layers;

const GATE_FAILED_EXIT_CODE: i32 = 3;
const MAX_SCAN_BYTES: u64 = 2 * 1024 * 1024;
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, clap::ValueEnum)]
pub(crate) enum Rule {
    Cycles,
    Layers,
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
        [Rule::Cycles, Rule::Layers, Rule::Hotspots, Rule::Dead].into()
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
    let changed_ids: HashSet<i64> = changed.unwrap_or_default();
    let base = match (&options.diff, options.gate) {
        (Some(base), true) => Some(BaseGraph::index(&git.repo_path, base)?),
        _ => None,
    };

    let layer_rules = layers::load(&git.repo_path)?;
    if let Some(config) = &layer_rules {
        for pattern in &config.test_symbols {
            regex::Regex::new(pattern)
                .with_context(|| format!("invalid test_symbols pattern `{pattern}`"))?;
        }
        let _ = TEST_CODE.set(TestCode {
            paths: config.test_paths.clone(),
            symbols: config.test_symbols.clone(),
        });
    }
    anyhow::ensure!(
        layer_rules.is_some() || !(options.rules.contains(&Rule::Layers)),
        "the layers rule needs {} in the repository; see `{} quality --help`",
        layers::CONFIG_PATH,
        spec::launcher()
    );
    let has_layers = layer_rules.as_ref().is_some_and(|c| !c.layers.is_empty());
    let wants_deps = rules.contains(&Rule::Cycles) || options.gate || has_layers;
    let deps = match wants_deps {
        true => Some(dependencies(&graph, options.depth)?),
        false => None,
    };
    let layer_violations = match (&layer_rules, &deps) {
        (Some(config), Some(deps)) if rules.contains(&Rule::Layers) || options.gate => {
            Some(layers::check(&config.layers, file_edges(&deps.files)))
        }
        _ => None,
    };
    let symbol_uses = match &layer_rules {
        Some(config)
            if !config.forbid.is_empty() && (rules.contains(&Rule::Layers) || options.gate) =>
        {
            let uses = used_symbols(&graph)?;
            Some(layers::check_symbols(
                &config.forbid,
                uses.iter().map(|(f, s, l)| (f.as_str(), s.as_str(), *l)),
            ))
        }
        _ => None,
    };
    let cycles = deps.as_ref().map(|deps| deps.cycles.clone());
    let hotspots = match rules.contains(&Rule::Hotspots) {
        true => Some(hotspots(&graph, &git.repo_path, options.limit)?),
        false => None,
    };
    let no_config = layers::Config::default();
    let config = layer_rules.as_ref().unwrap_or(&no_config);
    let dead = match (rules.contains(&Rule::Dead), options.gate) {
        (true, _) => Some(dead_functions(
            &graph,
            &git.repo_path,
            &config.dead,
            DeadScope::All,
        )?),
        (false, true) => Some(dead_functions(
            &graph,
            &git.repo_path,
            &config.dead,
            DeadScope::Ids(&changed_ids),
        )?),
        (false, false) => None,
    };

    let violations = match (&base, &deps) {
        (Some(base), Some(_)) => {
            let reader = base.open()?;
            let base_graph = Graph {
                client: &reader,
                params: [base.project_id.into(), base.commit_sha.clone().into()],
            };
            let renames = Renames::between(&git.repo_path, &base.commit_sha, &git.commit_sha)?;
            let base_files: HashSet<(String, String)> = dependencies(&base_graph, options.depth)?
                .files
                .into_keys()
                .map(|(from, to)| (renames.file(&from), renames.file(&to)))
                .collect();
            let base_modules: HashSet<(String, String)> = base_files
                .iter()
                .map(|(from, to)| (module_of(from, options.depth), module_of(to, options.depth)))
                .filter(|(from, to)| from != to)
                .collect();
            let base_uses: HashSet<(String, String, String)> = match symbol_uses {
                Some(_) => layers::check_symbols(
                    &config.forbid,
                    used_symbols(&base_graph)?
                        .iter()
                        .map(|(f, s, l)| (f.as_str(), s.as_str(), *l)),
                )
                .into_iter()
                .map(|u| (u.rule, renames.file(&u.file), u.matched))
                .collect(),
                None => HashSet::new(),
            };
            let head_dead = dead.as_deref().unwrap_or_default();
            let names: HashSet<String> = head_dead.iter().map(|f| f.name.clone()).collect();
            let base_dead: HashSet<(String, String)> = match names.is_empty() {
                true => HashSet::new(),
                false => dead_functions(
                    &base_graph,
                    &git.repo_path,
                    &config.dead,
                    DeadScope::Names(&names),
                )?
                .into_iter()
                .map(|f| (renames.file(&f.file), f.name))
                .collect(),
            };
            let mut found = gate_violations(
                cycles.as_deref().unwrap_or_default(),
                &base_modules,
                head_dead,
                &changed_ids,
                &base_dead,
            );
            found.extend(
                layer_violations
                    .as_deref()
                    .unwrap_or_default()
                    .iter()
                    .filter(|v| !base_files.contains(&(v.from.clone(), v.to.clone())))
                    .map(|v| {
                        format!(
                            "the change adds {} -> {}, which breaks layer `{}`: {}",
                            v.from, v.to, v.rule, v.example
                        )
                    }),
            );
            found.extend(
                symbol_uses
                    .as_deref()
                    .unwrap_or_default()
                    .iter()
                    .filter(|u| {
                        !base_uses.contains(&(u.rule.clone(), u.file.clone(), u.matched.clone()))
                    })
                    .map(|u| {
                        format!(
                            "the change uses {} in {}:{}, which breaks `{}`",
                            u.symbol, u.file, u.line, u.rule
                        )
                    }),
            );
            found
        }
        _ => Vec::new(),
    };
    let report = Report {
        repo: git.repo_path.display().to_string(),
        commit: git.short_sha().to_string(),
        base: options.diff.clone(),
        limit: options.limit,
        cycles: cycles.filter(|_| rules.contains(&Rule::Cycles)),
        layers: layer_violations.filter(|_| rules.contains(&Rule::Layers)),
        layers_configured: layer_rules.as_ref().map(layers::Config::rule_count),
        symbol_uses: symbol_uses.filter(|_| rules.contains(&Rule::Layers)),
        hotspots,
        dead: dead.filter(|_| rules.contains(&Rule::Dead)),
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

impl Graph<'_> {
    fn query(&self, sql: &str) -> Result<Vec<arrow::array::RecordBatch>> {
        self.client
            .query_arrow_json(sql, &self.params)
            .with_context(|| format!("quality query failed: {}", sql.lines().next().unwrap_or("")))
    }
}

/// Test and vendored code: the shared search corpus policy by path, plus the
/// repository's `test_paths` globs and `test_symbols` patterns on the fqn.
fn test_code(path: &str, fqn: &str) -> String {
    let Some(TestCode { paths, symbols }) = TEST_CODE.get() else {
        return format!("COALESCE({}, FALSE)", excluded_path_predicate(path));
    };
    let paths = paths
        .iter()
        .map(|glob| format!(" OR {path} GLOB {}", sql_lit(glob)));
    let symbols = symbols
        .iter()
        .map(|re| format!(" OR regexp_matches({fqn}, {})", sql_lit(re)));
    format!(
        "(COALESCE({}, FALSE){})",
        excluded_path_predicate(path),
        paths.chain(symbols).collect::<String>()
    )
}

struct TestCode {
    paths: Vec<String>,
    symbols: Vec<String>,
}

static TEST_CODE: std::sync::OnceLock<TestCode> = std::sync::OnceLock::new();

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
),
c AS (
  SELECT e.source_id, e.target_id FROM gl_edge e
  JOIN d s ON s.id = e.source_id JOIN d t ON t.id = e.target_id
  WHERE e.relationship_kind IN ('CALLS', 'EXTENDS') AND e.source_kind = 'Definition' AND e.target_kind = 'Definition'
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
    files: BTreeMap<(String, String), FileEdge>,
    cycles: Vec<Cycle>,
}

#[derive(Debug, Clone)]
struct FileEdge {
    refs: usize,
    example: String,
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
  WHERE NOT EXISTS (
    SELECT 1 FROM d m
    WHERE m.file_path = imp.file_path
      AND imp.start_line BETWEEN m.start_line AND m.end_line
      AND {in_test}
  )
)
SELECT sf, sname, CAST(sl AS BIGINT) AS sl, tf, tname
FROM dep WHERE sf <> tf AND NOT {} AND NOT {}
ORDER BY sf, sl, tf, tname",
        test_code("sf", "sname"),
        test_code("tf", "tname"),
        in_test = test_code("m.file_path", "m.fqn")
    );
    let batches = graph.query(&sql)?;
    let sfs = string_column(&batches, "sf");
    let snames = string_column(&batches, "sname");
    let sls = i64_column(&batches, "sl");
    let tfs = string_column(&batches, "tf");
    let tnames = string_column(&batches, "tname");

    let mut edges: BTreeMap<(String, String), ModuleEdge> = BTreeMap::new();
    let mut files: BTreeMap<(String, String), FileEdge> = BTreeMap::new();
    for i in 0..sfs.len() {
        let example = || match snames[i].as_str() {
            "" => format!("imports {}  ({}:{})", tnames[i], sfs[i], sls[i]),
            source => format!("{source} -> {}  ({}:{})", tnames[i], sfs[i], sls[i]),
        };
        files
            .entry((sfs[i].clone(), tfs[i].clone()))
            .or_insert_with(|| FileEdge {
                refs: 0,
                example: example(),
            });
        let file_edge = files
            .get_mut(&(sfs[i].clone(), tfs[i].clone()))
            .expect("just inserted");
        file_edge.refs += 1;
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
                example: example(),
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
    Ok(Dependencies { files, cycles })
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

enum DeadScope<'a> {
    All,
    Ids(&'a HashSet<i64>),
    Names(&'a HashSet<String>),
}

/// Functions nothing refers to. A function counts as referenced when the
/// graph has a call, import, or extension edge to it, or when its name
/// appears anywhere else in the commit's tracked files. Only `Function`
/// definitions are checked: a call through a receiver or an interface often
/// has no edge to its method. Test code (by path) and the repository's
/// `dead.ignore` names and paths are left out.
fn dead_functions(
    graph: &Graph,
    repo: &Path,
    config: &layers::DeadConfig,
    scope: DeadScope,
) -> Result<Vec<Dead>> {
    let filter = match scope {
        DeadScope::All => String::new(),
        DeadScope::Ids(ids) if ids.is_empty() => return Ok(Vec::new()),
        DeadScope::Ids(ids) => format!(
            "AND d.id IN ({})",
            ids.iter()
                .map(i64::to_string)
                .collect::<Vec<_>>()
                .join(", ")
        ),
        DeadScope::Names(names) if names.is_empty() => return Ok(Vec::new()),
        DeadScope::Names(names) => format!(
            "AND d.name IN ({})",
            names
                .iter()
                .map(|n| sql_lit(n))
                .collect::<Vec<_>>()
                .join(", ")
        ),
    };
    let commit = graph.params[1].as_str().unwrap_or("HEAD").to_string();
    let sql = format!(
        "WITH {DEFINITIONS_CTE}, {CALLS_CTE},
referenced AS (
  SELECT target_id AS id FROM c
  UNION SELECT target_id FROM gl_edge
  WHERE target_kind = 'Definition' AND relationship_kind IN ('CALLS', 'IMPORTS', 'EXTENDS')
),
names AS (SELECT name, CAST(count(*) AS BIGINT) AS defs FROM d GROUP BY name)
SELECT d.id, d.fqn, d.name, d.file_path AS file, CAST(d.start_line AS BIGINT) AS line, n.defs
FROM d JOIN names n ON n.name = d.name
WHERE d.definition_type = 'Function'
  AND d.id NOT IN (SELECT id FROM referenced)
  {filter}
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
    let candidates: Vec<usize> = (0..ids.len())
        .filter(|&i| !config.ignores(&names[i], &files[i]))
        .collect();
    if candidates.is_empty() {
        return Ok(Vec::new());
    }
    let wanted: HashSet<&str> = candidates.iter().map(|&i| names[i].as_str()).collect();
    let mentions = count_identifiers(&commit_files(repo, &commit)?, &wanted);
    Ok(candidates
        .into_iter()
        .filter(|&i| mentions.get(names[i].as_str()).copied().unwrap_or(0) <= defs[i] as usize)
        .map(|i| Dead {
            id: ids[i],
            fqn: fqns[i].clone(),
            name: names[i].clone(),
            file: files[i].clone(),
            loc: format!("{}:{}", files[i], lines[i]),
        })
        .collect())
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
) -> Result<HashSet<i64>> {
    if ranges.is_empty() {
        return Ok(HashSet::new());
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
    let sql = format!(
        "WITH {DEFINITIONS_CTE}, hunks(file, hs, he) AS (VALUES {values}),
hit AS (
  SELECT DISTINCT h.file, h.hs, h.he, d.* FROM hunks h
  JOIN d ON d.file_path = h.file AND d.start_line <= h.he AND d.end_line >= h.hs
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
    Ok(i64_column(&batches, "id").into_iter().collect())
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

/// Files git saw renamed between the base and HEAD, old path to new.
struct Renames {
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
        let mut files = HashMap::new();
        let mut fields = output.split('\0').filter(|field| !field.is_empty());
        while let Some(status) = fields.next() {
            if status.starts_with('R') || status.starts_with('C') {
                let (Some(old), Some(new)) = (fields.next(), fields.next()) else {
                    break;
                };
                if status.starts_with('R') {
                    files.insert(old.to_string(), new.to_string());
                }
            } else {
                fields.next();
            }
        }
        Self { files }
    }

    fn file(&self, path: &str) -> String {
        self.files
            .get(path)
            .cloned()
            .unwrap_or_else(|| path.to_string())
    }
}

/// Symbols each file uses, from the graph: the paths it imports, and the
/// definitions its code calls or extends wherever the graph resolved them.
fn used_symbols(graph: &Graph) -> Result<Vec<(String, String, i64)>> {
    let sql = format!(
        "WITH {DEFINITIONS_CTE}
SELECT i.file_path AS file, i.import_path AS symbol, CAST(min(i.start_line) AS BIGINT) AS line
FROM gl_imported_symbol i
WHERE i.project_id = ?1 AND i.commit_sha = ?2 AND i.import_path <> ''
GROUP BY i.file_path, i.import_path
UNION ALL
SELECT s.file_path, t.fqn, CAST(min(s.start_line) AS BIGINT)
FROM gl_edge e JOIN d s ON s.id = e.source_id JOIN d t ON t.id = e.target_id
WHERE e.relationship_kind IN ('CALLS', 'EXTENDS') AND e.source_kind = 'Definition' AND e.target_kind = 'Definition'
  AND s.file_path <> t.file_path
GROUP BY s.file_path, t.fqn
ORDER BY file, line"
    );
    let batches = graph.query(&sql)?;
    let files = string_column(&batches, "file");
    let symbols = string_column(&batches, "symbol");
    let lines = i64_column(&batches, "line");
    Ok((0..files.len())
        .map(|i| (files[i].clone(), symbols[i].clone(), lines[i]))
        .collect())
}

fn file_edges(
    files: &BTreeMap<(String, String), FileEdge>,
) -> impl Iterator<Item = (&str, &str, usize, &str)> {
    files
        .iter()
        .map(|((from, to), edge)| (from.as_str(), to.as_str(), edge.refs, edge.example.as_str()))
}

fn gate_violations(
    cycles: &[Cycle],
    base_modules: &HashSet<(String, String)>,
    dead: &[Dead],
    changed: &HashSet<i64>,
    base_dead: &HashSet<(String, String)>,
) -> Vec<String> {
    let mut violations = Vec::new();
    for cycle in cycles {
        for edge in &cycle.edges {
            if !base_modules.contains(&(edge.from.clone(), edge.to.clone())) {
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
    layers: Option<Vec<layers::Violation>>,
    layers_configured: Option<usize>,
    symbol_uses: Option<Vec<layers::SymbolUse>>,
    hotspots: Option<Vec<Hotspot>>,
    dead: Option<Vec<Dead>>,
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
        if let Some(found) = &self.layers {
            let rules = self.layers_configured.unwrap_or(0);
            let _ = writeln!(
                out,
                "\nLayer violations ({rules} rules in {}): {}",
                layers::CONFIG_PATH,
                found.len()
            );
            let mut by_rule: BTreeMap<&str, Vec<&layers::Violation>> = BTreeMap::new();
            for v in found {
                by_rule.entry(v.rule.as_str()).or_default().push(v);
            }
            for (rule, list) in by_rule {
                let _ = writeln!(out, "  {rule}: {}", list.len());
                for v in list.iter().take(self.limit) {
                    let _ = writeln!(
                        out,
                        "     {} -> {}  ({} refs)  e.g. {}",
                        v.from, v.to, v.refs, v.example
                    );
                }
                if list.len() > self.limit {
                    let _ = writeln!(out, "     … {} more", list.len() - self.limit);
                }
            }
        }
        if let Some(found) = &self.symbol_uses {
            let _ = writeln!(out, "\nForbidden symbols: {}", found.len());
            let mut by_rule: BTreeMap<&str, Vec<&layers::SymbolUse>> = BTreeMap::new();
            for u in found {
                by_rule.entry(u.rule.as_str()).or_default().push(u);
            }
            for (rule, list) in by_rule {
                let _ = writeln!(out, "  {rule}: {} files", list.len());
                for u in list.iter().take(self.limit) {
                    let _ = writeln!(out, "     {}:{}  {}", u.file, u.line, u.symbol);
                }
                if list.len() > self.limit {
                    let _ = writeln!(out, "     … {} more", list.len() - self.limit);
                }
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
            "layers": self.layers.as_ref().map(|found| found.iter().map(|v| json!({
                "rule": v.rule, "from": v.from, "to": v.to, "refs": v.refs, "example": v.example,
            })).collect::<Vec<_>>()),
            "forbidden_symbols": self.symbol_uses.as_ref().map(|found| found.iter().map(|u| json!({
                "rule": u.rule, "file": u.file, "line": u.line, "symbol": u.symbol,
            })).collect::<Vec<_>>()),
            "hotspots": self.hotspots.as_ref().map(|spots| spots.iter().map(|spot| json!({
                "fqn": spot.fqn, "kind": spot.kind, "location": spot.loc,
                "callers": spot.callers, "caller_files": spot.files, "churn_90d": spot.churn,
            })).collect::<Vec<_>>()),
            "dead": self.dead.as_ref().map(|dead| dead.iter().map(|function| json!({
                "id": function.id, "fqn": function.fqn, "location": function.loc,
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

    #[test]
    fn gate_reports_only_what_the_base_did_not_have() {
        let head = [
            cycle(&[("a", "b"), ("b", "a")]),
            cycle(&[("c", "d"), ("d", "c")]),
        ];
        let base_modules: HashSet<(String, String)> = [("a", "b"), ("b", "a"), ("c", "d")]
            .iter()
            .map(|(a, b)| (a.to_string(), b.to_string()))
            .collect();
        let head_dead = [dead(1, "old"), dead(2, "new"), dead(3, "untouched")];
        let changed: HashSet<i64> = [1, 2].into();
        let base_dead: HashSet<(String, String)> = [("m.rs".to_string(), "old".to_string())].into();
        let violations = gate_violations(&head, &base_modules, &head_dead, &changed, &base_dead);
        assert_eq!(violations.len(), 2, "{violations:#?}");
        assert!(violations[0].contains("d -> c"));
        assert!(violations[1].contains("adds m::new"));
    }

    #[test]
    fn renames_map_old_paths_to_new_ones() {
        let renames =
            Renames::parse("R100\0src/b/y.rs\0src/d/y.rs\0M\0src/lib.rs\0C100\0a.rs\0b.rs\0");
        assert_eq!(renames.file("src/b/y.rs"), "src/d/y.rs");
        assert_eq!(renames.file("src/lib.rs"), "src/lib.rs");
        assert_eq!(renames.file("a.rs"), "a.rs");
    }
}
