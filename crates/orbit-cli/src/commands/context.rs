use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Write as _;

use anyhow::{Context, Result};
use duckdb_client::search::{NodeHydrator, NodeValue};
use duckdb_client::{i64_column, sql_lit, string_column};

use crate::commands::{definition, relations};
use crate::workspace;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SourceRange {
    pub(crate) id: i64,
    pub(crate) fqn: String,
    pub(crate) kind: String,
    pub(crate) file: String,
    pub(crate) start: usize,
    pub(crate) end: usize,
}

pub(crate) fn source_range(node: &NodeValue) -> Result<SourceRange> {
    let string = |property| {
        node.properties
            .get(property)
            .and_then(serde_json::Value::as_str)
            .map(str::to_string)
            .context("node has an invalid source property")
    };
    let line = |property| {
        node.properties
            .get(property)
            .and_then(serde_json::Value::as_i64)
            .and_then(|value| usize::try_from(value).ok())
            .context("node has an invalid source property")
    };
    Ok(SourceRange {
        id: node.id,
        fqn: string("fqn")?,
        kind: string("definition_type")?,
        file: string("file_path")?,
        start: line("start_line")?,
        end: line("end_line")?,
    })
}

pub(crate) async fn run(args: crate::ContextArgs) -> Result<()> {
    let ontology = ontology::Ontology::load_embedded()?;
    let nodes: Vec<&str> = ontology.node_names().collect();
    let (mut local, mut remote) = (Vec::new(), Vec::new());
    let dir = args.repo.clone().unwrap_or_else(|| ".".into());
    let mut client = None;
    let mut origin = None;
    let mut repo: Option<String> = None;
    let (mut targets, mut sdlc_targets, mut code_targets) = (Vec::new(), Vec::new(), Vec::new());
    let mut current_ci = false;
    let mut joined: Vec<String> = Vec::new();
    for target in &args.target {
        let range = target.split_once('-').is_some_and(|(from, to)| {
            !from.is_empty()
                && !to.is_empty()
                && from.bytes().chain(to.bytes()).all(|b| b.is_ascii_digit())
        });
        match joined.last_mut() {
            Some(path) if range && !path.contains(':') && path.contains('.') => {
                path.push(':');
                path.push_str(target);
            }
            _ => joined.push(target.clone()),
        }
    }
    for target in &joined {
        if is_current_ci(target) {
            current_ci = true;
            continue;
        }
        match qualify(target) {
            Some(Qualified::Repo(path)) => repo = Some(path),
            Some(Qualified::Target(target)) => targets.push(target),
            Some(Qualified::Code(code, value)) => code_targets.push((code, value)),
            Some(Qualified::Sdlc(target)) => sdlc_targets.push(target),
            None => targets.push(target.clone()),
        }
    }
    let has_branch = sdlc_targets
        .iter()
        .any(|target| matches!(target, crate::remote::sdlc::Target::Branch(_)));
    if args.target.is_empty() || (current_ci && !has_branch) {
        sdlc_targets.push(crate::remote::sdlc::Target::Branch(
            crate::remote::sdlc::current_branch(&dir)?,
        ));
    }
    let mut sdlc_notes = String::new();
    let other_project = repo.as_ref().is_some_and(|repo| {
        crate::remote::origin::project(&dir)
            .is_none_or(|(_, origin)| origin.trim_end_matches('/') != repo.trim_end_matches('/'))
    });
    let origin_project = crate::remote::origin::project(&dir).map(|(_, project)| project);
    let origin_group = origin_project
        .as_deref()
        .map(|project| project.split('/').next().unwrap_or_default().to_string());
    let mut remote_code: Vec<(Option<String>, crate::remote::code::Code)> = Vec::new();
    for (code, value) in code_targets {
        match other_project {
            true => remote_code.push((None, code)),
            false => targets.push(value),
        }
    }
    let mut kept = Vec::new();
    for target in targets {
        if !looks_like_path(&target) || exists_locally(&dir, &target) {
            kept.push(target);
            continue;
        }
        if other_project {
            remote_code.push((None, crate::remote::code::Code::parse("file", &target)));
            continue;
        }
        let in_group = origin_group
            .as_deref()
            .is_some_and(|group| target.starts_with(&format!("{group}/")));
        if in_group {
            gitlab_project(&dir, &repo, &mut origin, &mut client, &target)?;
            let found_client = client.as_ref().expect("gitlab_project sets the client");
            let file = crate::remote::code::Code::parse("file", &target);
            let crate::remote::code::Code::File { path, lines } = file else {
                unreachable!("file targets parse as files")
            };
            if let Some((project, path)) =
                crate::remote::code::split_project_path(found_client, &path).await
            {
                remote_code.push((
                    Some(project),
                    crate::remote::code::Code::File { path, lines },
                ));
                continue;
            }
        }
        kept.push(target);
    }
    let targets = kept;
    for (project, code) in remote_code {
        let what = code.to_string();
        let project = match project {
            Some(project) => project,
            None => gitlab_project(&dir, &repo, &mut origin, &mut client, &what)?,
        };
        let found_client = client.as_ref().expect("gitlab_project sets the client");
        match crate::remote::code::read(found_client, &project, &code).await {
            Ok(text) => sdlc_notes.push_str(&text),
            Err(error) => {
                let message = crate::remote::grep::first_line(&error.message);
                sdlc_notes.push_str(&format!("{what} in {project}: {message}\n"));
            }
        }
    }
    for target in &sdlc_targets {
        let project = gitlab_project(&dir, &repo, &mut origin, &mut client, &target.to_string())?;
        let found_client = client.as_ref().expect("gitlab_project sets the client");
        match crate::remote::sdlc::read(found_client, &project, target).await {
            Ok(crate::remote::sdlc::Found::MergeRequest { note, reference }) => {
                sdlc_notes.push_str(&note);
                if !remote.contains(&reference) {
                    remote.push(reference);
                }
            }
            Ok(crate::remote::sdlc::Found::Text(text)) => sdlc_notes.push_str(&text),
            Err(error) => {
                let message = crate::remote::grep::first_line(&error.message);
                sdlc_notes.push_str(&format!("{target}: {message}\n"));
            }
        }
    }
    for target in &targets {
        let token = match route(target, &nodes) {
            Route::Local(target) => {
                local.push(target);
                continue;
            }
            Route::Remote(token) => token,
            Route::Relative(reference) => {
                let project = gitlab_project(&dir, &repo, &mut origin, &mut client, &reference)?;
                format!("{project}{reference}")
            }
        };
        if !remote.contains(&token) {
            remote.push(token);
        }
    }
    let anchored_from_origin = !other_project
        && remote.iter().all(|token| {
            origin_project.as_deref().is_none_or(|origin| {
                !token.contains(['!', '#'])
                    || token.starts_with(&format!("{origin}!"))
                    || token.starts_with(&format!("{origin}#"))
            })
        });
    let pending = match remote.is_empty() {
        true => None,
        false => {
            let client = match client {
                Some(client) => client,
                None => crate::remote::client::OrbitClient::from_env_in(&dir)?,
            };
            Some(tokio::spawn(async move {
                crate::remote::context::fetch(client, &remote).await
            }))
        }
    };
    let requested = args.repo.clone();
    let groups = match local.is_empty() {
        true => Vec::new(),
        false => by_checkout(args.repo, args.db.clone(), local),
    };
    let mut printed = !groups.is_empty();
    for (index, (repo, targets)) in groups.into_iter().enumerate() {
        if index > 0 {
            println!();
        }
        if let Some(repo) = repo
            .as_ref()
            .filter(|repo| requested.as_ref() != Some(repo))
        {
            println!("repo: {}", repo.display());
        }
        print!("{}", run_local(repo, args.db.clone(), &targets)?);
    }
    if !sdlc_notes.is_empty() {
        if printed {
            println!();
        }
        print!("{sdlc_notes}");
        printed = false;
    }
    let mut gitlab_text = sdlc_notes.clone();
    let Some(pending) = pending else {
        if anchored_from_origin && !gitlab_text.is_empty() {
            print_anchored(requested.clone(), args.db.clone(), &gitlab_text);
        }
        return Ok(());
    };
    let bytes = pending.await??;
    let text = String::from_utf8_lossy(&bytes);
    let jobs_shown = sdlc_notes.contains("failed_jobs[")
        || [&sdlc_notes, text.as_ref()]
            .iter()
            .any(|found| found.lines().any(|line| line.starts_with("- job:")));
    let bytes = crate::remote::context::tidy(&text, jobs_shown).into_bytes();
    gitlab_text.push_str(&String::from_utf8_lossy(&bytes));
    let home = repo.or_else(|| crate::remote::origin::project(&dir).map(|(_, project)| project));
    let mut bytes = match home {
        Some(home) => {
            crate::remote::context::shorten(&String::from_utf8_lossy(&bytes), &home).into_bytes()
        }
        None => bytes,
    };
    if !bytes.ends_with(b"\n") {
        bytes.push(b'\n');
    }
    if printed {
        println!();
    }
    crate::remote::write_stdout_raw(&bytes)?;
    if anchored_from_origin {
        print_anchored(requested.clone(), args.db.clone(), &gitlab_text);
    }
    Ok(())
}

/// Appends the code that GitLab text points at, from the local graph, so one `context` call on an
/// issue, MR, branch, or job also returns the definitions it names.
fn print_anchored(repo: Option<std::path::PathBuf>, db: Option<std::path::PathBuf>, text: &str) {
    let Some(workspace::IndexedRepo { git, client }) = workspace::open_indexed_if_present(repo, db)
    else {
        return;
    };
    let Ok(files) = super::anchors::indexed_files(&client, &git) else {
        return;
    };
    let launcher = crate::commands::setup::spec::launcher();
    let hint = format!("`{launcher} ");
    let text: String = text
        .lines()
        .filter(|line| !line.contains(&hint))
        .map(|line| format!("{line}\n"))
        .collect();
    let found = super::anchors::extract(&text, &git.repo_path, &files);
    if let Ok(code) = super::anchors::render(&client, &git, &found)
        && !code.trim().is_empty()
    {
        println!(
            "\ncode this refers to (read one with `{launcher} context <path>:<lines>`):\n{}",
            code.trim_end()
        );
    }
}

#[derive(Debug, PartialEq, Eq)]
enum Route {
    Local(String),
    Remote(String),
    Relative(String),
}

const LOCAL_NODES: &[&str] = &["Definition", "File"];

#[derive(Debug, PartialEq, Eq)]
enum Qualified {
    Repo(String),
    Target(String),
    Sdlc(crate::remote::sdlc::Target),
    /// Code in another project when `repo:` names one; the original value otherwise.
    Code(crate::remote::code::Code, String),
}

fn qualify(target: &str) -> Option<Qualified> {
    use crate::remote::sdlc::Target;
    let (name, value) = target.split_once(':')?;
    if value.is_empty() || value.starts_with(':') {
        return None;
    }
    let number = value.parse::<i64>().ok().filter(|id| *id > 0);
    let hex = value.len() >= 7 && value.chars().all(|c| c.is_ascii_hexdigit());
    let qualified = match (name.to_ascii_lowercase().as_str(), number) {
        ("mr" | "mrs" | "pr", Some(iid)) => Qualified::Target(format!("!{iid}")),
        ("mr" | "mrs" | "pr", None) if value.contains('!') => Qualified::Target(value.into()),
        ("mr" | "mrs" | "pr", None) => Qualified::Sdlc(Target::Branch(
            value.strip_prefix("branch:").unwrap_or(value).into(),
        )),
        ("issue" | "issues", Some(iid)) => Qualified::Target(format!("#{iid}")),
        ("issue" | "issues", None) if value.contains('#') => Qualified::Target(value.into()),
        ("branch", _) => Qualified::Sdlc(Target::Branch(value.into())),
        ("pipeline", Some(id)) => Qualified::Sdlc(Target::Pipeline(id)),
        ("job", Some(id)) => Qualified::Sdlc(Target::Job(id)),
        ("commit", _) if hex => Qualified::Sdlc(Target::Commit(value.into())),
        (kind @ ("symbol" | "def" | "file" | "path"), _) => {
            Qualified::Code(crate::remote::code::Code::parse(kind, value), value.into())
        }
        ("repo" | "in", _) => Qualified::Repo(value.trim_end_matches('/').into()),
        _ => return None,
    };
    Some(qualified)
}

/// Whether a target names a file: it has a directory or an extension and is not a GitLab reference.
fn looks_like_path(target: &str) -> bool {
    let path = strip_range(target);
    !path.contains(['!', '#', '[', ' '])
        && (path.contains('/')
            || std::path::Path::new(path)
                .extension()
                .and_then(|ext| ext.to_str())
                .is_some_and(|ext| {
                    ext.len() <= 6
                        && ext
                            .chars()
                            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit())
                }))
}

fn strip_range(target: &str) -> &str {
    match target.rsplit_once(':') {
        Some((path, range)) if range.chars().all(|c| c.is_ascii_digit() || c == '-') => path,
        _ => target,
    }
}

fn exists_locally(dir: &std::path::Path, target: &str) -> bool {
    let path = strip_range(target);
    dir.join(path).exists()
        || crate::workspace::git_toplevel(dir).is_ok_and(|root| root.join(path).exists())
}

/// Bare `pipeline`, `ci`, or `pipeline:latest`: the current branch's MR and CI, which is what agents
/// mean by them.
fn is_current_ci(target: &str) -> bool {
    matches!(
        target.to_ascii_lowercase().as_str(),
        "pipeline"
            | "pipelines"
            | "ci"
            | "job"
            | "jobs"
            | "pipeline:latest"
            | "pipeline:last"
            | "pipeline:current"
            | "pipeline:head"
    )
}

fn gitlab_project(
    dir: &std::path::Path,
    repo: &Option<String>,
    origin: &mut Option<Option<(String, String)>>,
    client: &mut Option<crate::remote::client::OrbitClient>,
    what: &str,
) -> Result<String> {
    let found_client = match client {
        Some(client) => client,
        None => client.insert(crate::remote::client::OrbitClient::from_env_in(dir)?),
    };
    let api_host = found_client.get_host()?;
    if let Some(repo) = repo {
        return Ok(crate::remote::token::namespace(repo, &api_host));
    }
    let (host, project) = origin
        .get_or_insert_with(|| crate::remote::origin::project(dir))
        .clone()
        .with_context(|| {
            format!("{what} needs a GitLab `origin` remote; add repo:group/project")
        })?;
    anyhow::ensure!(
        host == api_host,
        "{what} names {project} on {host}, but Orbit uses {api_host}; add repo:group/project"
    );
    Ok(project)
}

fn route(target: &str, nodes: &[&str]) -> Route {
    use crate::remote::token;
    if let Some(token) = token::entity_reference(target) {
        return Route::Remote(token);
    }
    if token::is_relative_reference(target) {
        return Route::Relative(target.to_string());
    }
    let Some(typed) = token::typed(target) else {
        return Route::Local(target.to_string());
    };
    let named = |name: &&str| typed.names(name);
    if let Some(local) = LOCAL_NODES.iter().copied().find(named) {
        return Route::Local(format!("{local}:{}", typed.id));
    }
    match ["Issue"].iter().chain(nodes).copied().find(named) {
        Some(node) => Route::Remote(format!("{node}[{}]", typed.id)),
        None => Route::Local(target.to_string()),
    }
}

fn by_checkout(
    repo: Option<std::path::PathBuf>,
    db: Option<std::path::PathBuf>,
    targets: Vec<String>,
) -> Vec<(Option<std::path::PathBuf>, Vec<String>)> {
    let typed = |target: &str| {
        crate::remote::token::typed(target)
            .filter(|typed| LOCAL_NODES.iter().any(|node| typed.names(node)))
            .map(|typed| (typed.node.to_string(), typed.id))
    };
    let ids: Vec<(String, i64)> = targets.iter().filter_map(|target| typed(target)).collect();
    let owners = match repo.is_some() || ids.is_empty() {
        true => BTreeMap::new(),
        false => id_checkouts(db, &ids).unwrap_or_default(),
    };
    let here = workspace::git_toplevel(std::path::Path::new("."))
        .ok()
        .and_then(|root| dunce::canonicalize(root).ok());
    let mut groups: Vec<(Option<std::path::PathBuf>, Vec<String>)> = vec![(repo, Vec::new())];
    for target in targets {
        let owner = typed(&target)
            .and_then(|id| owners.get(&id))
            .filter(|repos| !here.as_ref().is_some_and(|here| repos.contains(here)))
            .and_then(|repos| match repos.as_slice() {
                [only] => Some(only.clone()),
                _ => None,
            });
        match owner {
            Some(owner) => match groups
                .iter_mut()
                .find(|(repo, _)| repo.as_ref() == Some(&owner))
            {
                Some((_, group)) => group.push(target),
                None => groups.push((Some(owner), vec![target])),
            },
            None => groups[0].1.push(target),
        }
    }
    groups.retain(|(_, targets)| !targets.is_empty());
    groups
}

fn id_checkouts(
    db: Option<std::path::PathBuf>,
    ids: &[(String, i64)],
) -> Result<BTreeMap<(String, i64), Vec<std::path::PathBuf>>> {
    let client = crate::database::GraphReader::open(db)?;
    let list = |node: &str| {
        let ids: Vec<String> = ids
            .iter()
            .filter(|(kind, _)| kind == node)
            .map(|(_, id)| id.to_string())
            .collect();
        match ids.is_empty() {
            true => "NULL".to_string(),
            false => ids.join(", "),
        }
    };
    let batches = client.query_arrow_json(
        &format!(
            "SELECT 'Definition' AS node, d.id, m.repo_path FROM gl_definition d
             JOIN _orbit_manifest m ON d.project_id = m.project_id AND d.commit_sha = m.commit_sha
             WHERE d.id IN ({definitions})
             UNION ALL
             SELECT 'File' AS node, f.id, m.repo_path FROM gl_file f
             JOIN _orbit_manifest m ON f.project_id = m.project_id AND f.commit_sha = m.commit_sha
             WHERE f.id IN ({files})",
            definitions = list("Definition"),
            files = list("File"),
        ),
        &[],
    )?;
    let mut owners: BTreeMap<(String, i64), Vec<std::path::PathBuf>> = BTreeMap::new();
    for ((node, id), repo) in string_column(&batches, "node")
        .into_iter()
        .zip(i64_column(&batches, "id"))
        .zip(string_column(&batches, "repo_path"))
    {
        owners
            .entry((node, id))
            .or_default()
            .push(std::path::PathBuf::from(repo));
    }
    Ok(owners)
}

fn run_local(
    repo: Option<std::path::PathBuf>,
    db: Option<std::path::PathBuf>,
    targets: &[String],
) -> Result<String> {
    use crate::commands::index::incremental::{Freshness, refresh};
    let (out, git, shown) = render_local(repo.clone(), db.clone(), targets)?;
    let mut files: Vec<&str> = shown.iter().map(String::as_str).collect();
    files.sort_unstable();
    files.dedup();
    let edited = match refresh(&git.repo_path, db.clone(), &files) {
        Ok(Freshness::Fresh) => Default::default(),
        Ok(Freshness::Refreshed) => return Ok(render_local(repo, db, targets)?.0),
        Ok(Freshness::Stale(changed)) => changed.into_iter().collect(),
        _ => workspace::edited_since_index(&git.repo_path, &files),
    };
    Ok(format!("{}{out}", edited_note(&git, edited)))
}

fn render_local(
    repo: Option<std::path::PathBuf>,
    db: Option<std::path::PathBuf>,
    targets: &[String],
) -> Result<(String, workspace::GitInfo, Vec<String>)> {
    let workspace::IndexedRepo { git, client } = workspace::open_indexed(repo, db)?;
    let hydrator = NodeHydrator::embedded("Definition")?;
    let targets = resolve_targets(&client, &git, &hydrator, targets)?;
    let mut out = missing_note(&targets.missing);
    for dir in &targets.dirs {
        render_dir(&mut out, &client, &git, dir)?;
    }
    for (path, start, end) in &targets.ranges {
        let (start, end) = snap_to_definitions(&client, &git, path, *start, *end)?;
        render_range(&mut out, &git, path, start, end)?;
        let (lines, calling) = callers(&client, &git, path, start, end)?;
        out.push_str(&lines);
        out.push_str(&tests_for(&client, &git, path, calling)?);
    }
    let mut shown: Vec<String> = targets
        .ranges
        .iter()
        .map(|(path, ..)| path.clone())
        .collect();
    if targets.files.is_empty() && targets.file_ids.is_empty() && targets.ids.is_empty() {
        return Ok((out, git, shown));
    }
    shown.extend(targets.files.iter().cloned());
    let (paths, file_ids, ids) = (targets.files, targets.file_ids, targets.ids);
    let files = resolve_files(&client, &git, &paths, &file_ids)?;
    let mut nodes = definition::resolve_ids(&client, &git, &hydrator, &ids)?;
    let mut defs = nodes.iter().map(source_range).collect::<Result<Vec<_>>>()?;
    defs.sort_by(|a, b| {
        a.file
            .cmp(&b.file)
            .then(a.start.cmp(&b.start))
            .then(b.end.cmp(&a.end))
            .then(a.fqn.cmp(&b.fqn))
    });
    let paths = files
        .iter()
        .map(|file| repo_relative(&git.repo_path, file.properties["path"].as_str().unwrap()))
        .collect::<Result<Vec<_>>>()?;
    let members = definitions_in_files(&client, &git, &hydrator, &paths)?
        .iter()
        .map(source_range)
        .collect::<Result<Vec<_>>>()?;
    for (file, file_defs) in outline(&defs) {
        let path = repo_relative(&git.repo_path, &file)?;
        shown.push(path.clone());
        let content = std::fs::read_to_string(git.repo_path.join(path))
            .with_context(|| format!("failed to read {file}"))?;
        let lines: Vec<&str> = content.lines().collect();
        if !out.is_empty() {
            out.push('\n');
        }
        render(&mut out, &file_defs, &lines, false)?;
    }
    nodes.splice(0..0, files);
    if !out.is_empty() {
        out.push('\n');
    }
    out.push_str(&relations::render(
        &client, &git, &hydrator, &nodes, &members,
    )?);
    Ok((out, git, shown))
}

/// Warns that the graph describes the indexed commit when a shown file has changed since.
fn edited_note(git: &workspace::GitInfo, edited: std::collections::BTreeSet<String>) -> String {
    match edited.is_empty() {
        true => String::new(),
        false => format!(
            "note: edited since index {}: {}. Definitions, line spans, and connections come from that commit.\n\n",
            git.short_sha(),
            edited.into_iter().collect::<Vec<_>>().join(", ")
        ),
    }
}

#[derive(Default)]
struct Targets {
    files: Vec<String>,
    file_ids: Vec<i64>,
    ids: Vec<i64>,
    ranges: Vec<(String, usize, usize)>,
    dirs: Vec<String>,
    missing: Vec<anyhow::Error>,
}

pub(crate) fn split_line_range(target: &str) -> Option<(&str, usize, usize)> {
    let (path, suffix) = target.rsplit_once(':')?;
    let (start, end) = match suffix.split_once('-') {
        Some((start, end)) => (start.parse().ok()?, end.parse().ok()?),
        None => {
            let line = suffix.parse().ok()?;
            (line, line)
        }
    };
    (!path.is_empty() && start >= 1 && start <= end).then_some((path, start, end))
}

fn resolve_targets(
    client: &duckdb_client::DuckDbClient,
    git: &workspace::GitInfo,
    hydrator: &NodeHydrator,
    targets: &[String],
) -> Result<Targets> {
    let mut resolved = Targets::default();
    let Targets {
        files,
        file_ids,
        ids,
        ranges,
        dirs,
        missing,
    } = &mut resolved;
    let mut seen = BTreeSet::new();
    for target in targets.iter().filter(|target| seen.insert(*target)) {
        if let Some((path, start, end)) = split_line_range(target)
            && let Ok(path) = repo_relative(&git.repo_path, path)
        {
            ranges.push((path, start, end));
        } else if let Ok(dir) = repo_relative_dir(&git.repo_path, target) {
            if !dirs.contains(&dir) {
                dirs.push(dir);
            }
        } else if let Some((kind, id)) = target.split_once(':')
            && matches!(kind, "Definition" | "File")
            && !id.starts_with(':')
        {
            let id = id
                .parse::<i64>()
                .ok()
                .filter(|id| *id > 0)
                .with_context(|| format!("{target:?} is not a valid {kind}:<id>"))?;
            let ids = if kind == "File" {
                &mut *file_ids
            } else {
                &mut *ids
            };
            if !ids.contains(&id) {
                ids.push(id);
            }
        } else {
            match repo_relative(&git.repo_path, target) {
                Ok(path) => match std::fs::read_to_string(git.repo_path.join(&path))
                    .map(|content| content.lines().count())
                {
                    Ok(lines) if lines > 0 => ranges.push((path, 1, lines)),
                    _ => {
                        if !files.contains(&path) {
                            files.push(path);
                        }
                    }
                },
                Err(error) => {
                    let mut matches = hydrator.query(
                        client,
                        &[
                            ("project_id", git.project_id.into()),
                            ("commit_sha", git.commit_sha.clone().into()),
                            ("fqn", target.clone().into()),
                        ],
                        None,
                    )?;
                    matches.sort_by_key(|node| node.id);
                    let mut found: Vec<i64> = matches.into_iter().map(|node| node.id).collect();
                    if found.is_empty() {
                        found = named(client, git, target)?;
                    }
                    if found.is_empty() {
                        missing.push(error);
                        continue;
                    }
                    for id in found {
                        if !ids.contains(&id) {
                            ids.push(id);
                        }
                    }
                }
            }
        }
    }
    let nothing = files.is_empty()
        && file_ids.is_empty()
        && ids.is_empty()
        && ranges.is_empty()
        && dirs.is_empty();
    if nothing && !missing.is_empty() {
        return Err(missing.remove(0));
    }
    Ok(resolved)
}

fn missing_note(missing: &[anyhow::Error]) -> String {
    missing.iter().map(|error| format!("» {error}\n")).collect()
}

const NAMED_MAX: usize = 5;

/// Definitions whose FQN ends with `target`, so a bare name or `Class.method` finds
/// `pkg.Class.method` and `Class::method` alike.
fn named(
    client: &duckdb_client::DuckDbClient,
    git: &workspace::GitInfo,
    target: &str,
) -> Result<Vec<i64>> {
    let normalize = "regexp_replace(?3, '::|#|/', '.', 'g')";
    let batches = client.query_arrow_json(
        &format!(
            "SELECT id, fqn FROM gl_definition WHERE project_id = ?1 AND commit_sha = ?2
             AND (regexp_replace(fqn, '::|#|/', '.', 'g') = {normalize}
                  OR ends_with(regexp_replace(fqn, '::|#|/', '.', 'g'), '.' || {normalize}))
             ORDER BY id"
        ),
        &[
            git.project_id.into(),
            git.commit_sha.clone().into(),
            target.to_string().into(),
        ],
    )?;
    let (ids, fqns) = (
        duckdb_client::i64_column(&batches, "id"),
        duckdb_client::string_column(&batches, "fqn"),
    );
    anyhow::ensure!(
        ids.len() <= NAMED_MAX,
        "{target} names {} definitions; pass one of these FQNs: {}",
        ids.len(),
        fqns.iter().take(10).cloned().collect::<Vec<_>>().join(", ")
    );
    Ok(ids)
}

fn repo_relative_dir(repo_path: &std::path::Path, path: &str) -> Result<String> {
    let canonical = dunce::canonicalize(repo_path.join(path))
        .or_else(|_| dunce::canonicalize(std::env::current_dir()?.join(path)))
        .with_context(|| format!("{path} does not exist"))?;
    anyhow::ensure!(canonical.is_dir(), "{path} is not a directory");
    let relative = canonical.strip_prefix(repo_path).with_context(|| {
        format!(
            "{path} is outside the indexed repository {}",
            repo_path.display()
        )
    })?;
    Ok(relative.to_string_lossy().replace('\\', "/"))
}

const DIR_FILE_LIMIT: usize = 50;

fn render_dir(
    out: &mut String,
    client: &duckdb_client::DuckDbClient,
    git: &workspace::GitInfo,
    dir: &str,
) -> Result<()> {
    let files = NodeHydrator::embedded("File")?;
    let defs = NodeHydrator::embedded("Definition")?;
    let prefix = if dir.is_empty() {
        String::new()
    } else {
        format!("{dir}/")
    };
    let batches = client.query_arrow_json(
        &format!(
            "SELECT f.{fid} AS id, f.{fpath} AS path, f.{lang} AS language,
                    CAST(count(d.{did}) AS BIGINT) AS definitions
             FROM {ftable} f
             LEFT JOIN {dtable} d ON d.{dproject} = f.{fproject} AND d.{dcommit} = f.{fcommit}
                  AND d.{dpath} = f.{fpath} AND d.{dfqn} NOT LIKE '%@%'
             WHERE f.{fproject} = ?1 AND f.{fcommit} = ?2 AND starts_with(f.{fpath}, ?3)
             GROUP BY ALL ORDER BY path",
            fid = files.column("id")?,
            fpath = files.column("path")?,
            lang = files.column("language")?,
            ftable = files.table(),
            fproject = files.column("project_id")?,
            fcommit = files.column("commit_sha")?,
            did = defs.column("id")?,
            dtable = defs.table(),
            dproject = defs.column("project_id")?,
            dcommit = defs.column("commit_sha")?,
            dpath = defs.column("file_path")?,
            dfqn = defs.column("fqn")?,
        ),
        &[
            git.project_id.into(),
            git.commit_sha.clone().into(),
            prefix.clone().into(),
        ],
    )?;
    let ids = i64_column(&batches, "id");
    let paths = string_column(&batches, "path");
    let languages = string_column(&batches, "language");
    let counts = i64_column(&batches, "definitions");
    if !out.is_empty() {
        out.push('\n');
    }
    let shown = if dir.is_empty() { "." } else { dir };
    if ids.is_empty() {
        writeln!(out, "Dir:  {shown}  (no indexed files)")?;
        return Ok(());
    }
    writeln!(
        out,
        "Dir:  {shown}  ({} files, {} definitions)",
        ids.len(),
        counts.iter().sum::<i64>()
    )?;
    for index in 0..ids.len().min(DIR_FILE_LIMIT) {
        writeln!(
            out,
            "  File:{}  {}  [{}]  {} definitions",
            ids[index],
            paths[index],
            languages.get(index).map(String::as_str).unwrap_or(""),
            counts[index]
        )?;
    }
    if ids.len() > DIR_FILE_LIMIT {
        writeln!(
            out,
            "  … {} more; name a subdirectory or file",
            ids.len() - DIR_FILE_LIMIT
        )?;
    }
    Ok(())
}

fn range_params(
    git: &workspace::GitInfo,
    path: &str,
    start: usize,
    end: usize,
) -> [serde_json::Value; 5] {
    [
        git.project_id.into(),
        git.commit_sha.clone().into(),
        path.to_string().into(),
        (start as i64).into(),
        (end as i64).into(),
    ]
}

fn snap_to_definitions(
    client: &duckdb_client::DuckDbClient,
    git: &workspace::GitInfo,
    path: &str,
    start: usize,
    end: usize,
) -> Result<(usize, usize)> {
    let batches = client.query_arrow_json(
        "SELECT start_line, end_line FROM gl_definition
         WHERE project_id = ?1 AND commit_sha = ?2 AND file_path = ?3
         AND start_line <= ?5 AND end_line >= ?4",
        &range_params(git, path, start, end),
    )?;
    let spans: Vec<(usize, usize)> = i64_column(&batches, "start_line")
        .into_iter()
        .zip(i64_column(&batches, "end_line"))
        .map(|(from, to)| (from as usize, to as usize))
        .collect();
    let snapped_start = spans
        .iter()
        .filter(|&&(from, to)| from < start && start <= to && to < end)
        .map(|&(from, _)| from)
        .max()
        .unwrap_or(start);
    let snapped_end = spans
        .iter()
        .filter(|&&(from, to)| start < from && from <= end && end < to)
        .map(|&(_, to)| to)
        .min()
        .unwrap_or(end);
    Ok((snapped_start, snapped_end))
}

fn render_range(
    out: &mut String,
    git: &workspace::GitInfo,
    path: &str,
    start: usize,
    end: usize,
) -> Result<()> {
    let content = std::fs::read_to_string(git.repo_path.join(path))
        .with_context(|| format!("failed to read {path}"))?;
    let lines: Vec<&str> = content.lines().collect();
    anyhow::ensure!(
        start <= lines.len(),
        "{path} has {} lines; range starts at {start}",
        lines.len()
    );
    let end = end.min(lines.len());
    if !out.is_empty() {
        out.push('\n');
    }
    writeln!(out, "{path}:{start}-{end}")?;
    write_lines(out, &lines, start, end)?;
    Ok(())
}

type Caller = (String, String, i64);

const CALLERS_SHOWN: usize = 5;
const CALLER_LINES: usize = 12;

/// One line per definition in a line range naming the indexed calls into it from outside the
/// range, so reading code also shows who uses it. A range inside one definition reports that
/// definition. Test callers are counted rather than named. The graph misses calls it cannot
/// resolve, such as through interfaces, so a definition without indexed callers gets no line.
fn callers(
    client: &duckdb_client::DuckDbClient,
    git: &workspace::GitInfo,
    path: &str,
    start: usize,
    end: usize,
) -> Result<(String, Vec<String>)> {
    let test = duckdb_client::search::excluded_path_predicate("c.file_path");
    let batches = client.query_arrow_json(
        &format!(
            "WITH defs AS (
  SELECT id, name, start_line FROM gl_definition
  WHERE project_id = ?1 AND commit_sha = ?2 AND file_path = ?3
    AND definition_type NOT IN ('Variable', 'Field', 'Property')
    AND ((start_line >= ?4 AND end_line <= ?5)
      OR (start_line = (SELECT max(start_line) FROM gl_definition
                        WHERE project_id = ?1 AND commit_sha = ?2 AND file_path = ?3
                          AND definition_type NOT IN ('Variable', 'Field', 'Property')
                          AND start_line <= ?4 AND end_line >= ?5)
          AND NOT EXISTS (SELECT 1 FROM gl_definition
                          WHERE project_id = ?1 AND commit_sha = ?2 AND file_path = ?3
                            AND definition_type NOT IN ('Variable', 'Field', 'Property')
                            AND start_line >= ?4 AND end_line <= ?5)
          AND start_line <= ?4 AND end_line >= ?5))
)
SELECT d.id AS target, d.name AS target_name, d.start_line AS target_start,
       COALESCE(c.name, '') AS caller, COALESCE(c.file_path, '') AS caller_path,
       COALESCE(c.start_line, 0) AS caller_line, COALESCE({test}, FALSE) AS test
FROM defs d
LEFT JOIN gl_edge e ON e.target_id = d.id AND e.relationship_kind = 'CALLS'
LEFT JOIN gl_definition c ON c.id = e.source_id AND c.project_id = ?1 AND c.commit_sha = ?2
  AND NOT (c.file_path = ?3 AND c.start_line >= ?4 AND c.end_line <= ?5)
ORDER BY d.start_line, d.id, test, caller_path, caller_line"
        ),
        &range_params(git, path, start, end),
    )?;
    let targets = i64_column(&batches, "target");
    let names = string_column(&batches, "target_name");
    let callers = string_column(&batches, "caller");
    let paths = string_column(&batches, "caller_path");
    let lines = i64_column(&batches, "caller_line");
    let tests = duckdb_client::bool_column(&batches, "test");
    let mut defs: Vec<(i64, String, Vec<Caller>, usize)> = Vec::new();
    let mut test_files: Vec<String> = Vec::new();
    for i in 0..targets.len() {
        if defs.last().is_none_or(|(id, ..)| *id != targets[i]) {
            defs.push((targets[i], names[i].clone(), Vec::new(), 0));
        }
        let def = defs.last_mut().expect("pushed above");
        match (callers[i].is_empty(), tests[i]) {
            (true, _) => {}
            (false, true) => {
                def.3 += 1;
                if !test_files.contains(&paths[i]) {
                    test_files.push(paths[i].clone());
                }
            }
            (false, false) => {
                let caller = (callers[i].clone(), paths[i].clone(), lines[i]);
                if !def.2.contains(&caller) {
                    def.2.push(caller);
                }
            }
        }
    }
    let mut out = String::new();
    let mut shown = 0;
    let mut more = 0;
    for (_, name, found, tests) in &defs {
        if found.is_empty() && *tests == 0 {
            continue;
        }
        if shown == CALLER_LINES {
            more += 1;
            continue;
        }
        shown += 1;
        let mut previous = "";
        let mut named: Vec<String> = found
            .iter()
            .take(CALLERS_SHOWN)
            .map(|(caller, file, line)| {
                let at = match file == previous {
                    true => format!(":{line}"),
                    false => format!("{file}:{line}"),
                };
                previous = file;
                format!("{caller} ({at})")
            })
            .collect();
        if found.len() > CALLERS_SHOWN {
            named.push(format!("+{} more", found.len() - CALLERS_SHOWN));
        }
        if *tests > 0 {
            named.push(format!("{tests} in tests"));
        }
        let _ = writeln!(out, "» indexed callers of {name}: {}", named.join(", "));
    }
    if more > 0 {
        let _ = writeln!(out, "» +{more} more definitions with callers");
    }
    Ok((out, test_files))
}

const TESTS_SHOWN: usize = 3;

/// Test file names that conventionally cover `path`: `x_test.go`, `x_spec.rb`, `test_x.py`,
/// `x.test.ts`, `XTest.java`, and the like.
fn test_names(path: &str) -> Vec<String> {
    let name = path.rsplit('/').next().unwrap_or(path);
    let Some((stem, ext)) = name.rsplit_once('.') else {
        return Vec::new();
    };
    if stem.is_empty() {
        return Vec::new();
    }
    let mut names = vec![
        format!("{stem}_test.{ext}"),
        format!("{stem}_spec.{ext}"),
        format!("test_{stem}.{ext}"),
        format!("{stem}_tests.{ext}"),
        format!("{stem}.test.{ext}"),
        format!("{stem}.spec.{ext}"),
        format!("{stem}Test.{ext}"),
        format!("{stem}Tests.{ext}"),
    ];
    for other in match ext {
        "ts" | "tsx" | "js" | "jsx" | "mts" | "vue" => &["ts", "tsx", "js"][..],
        _ => &[],
    } {
        names.push(format!("{stem}.test.{other}"));
        names.push(format!("{stem}.spec.{other}"));
    }
    names.sort();
    names.dedup();
    names
}

/// Directories two paths share, so `spec/lib/a/x_spec.rb` ranks above `spec/b/x_spec.rb` for
/// `lib/a/x.rb`.
fn shared_dirs(a: &str, b: &str) -> usize {
    let dirs = |path: &str| -> BTreeSet<String> {
        let mut parts: Vec<&str> = path.split('/').collect();
        parts.pop();
        parts.into_iter().map(str::to_string).collect()
    };
    dirs(a).intersection(&dirs(b)).count()
}

/// Indexed test files for `path`: files named after it, then those that call its definitions,
/// nearest first.
fn tests_for(
    client: &duckdb_client::DuckDbClient,
    git: &workspace::GitInfo,
    path: &str,
    calling: Vec<String>,
) -> Result<String> {
    let names = test_names(path);
    let mut named = Vec::new();
    if !names.is_empty() {
        let hydrator = NodeHydrator::embedded("File")?;
        let batches = client.query_arrow_json(
            &format!(
                "SELECT {path} AS path FROM {table}
                 WHERE {project} = ?1 AND {commit} = ?2
                   AND regexp_extract({path}, '[^/]+$') IN (SELECT unnest(?3::VARCHAR[]))",
                path = hydrator.column("path")?,
                table = hydrator.table(),
                project = hydrator.column("project_id")?,
                commit = hydrator.column("commit_sha")?,
            ),
            &[
                git.project_id.into(),
                git.commit_sha.clone().into(),
                names.into(),
            ],
        )?;
        named = string_column(&batches, "path")
            .into_iter()
            .filter_map(|found| repo_relative(&git.repo_path, &found).ok())
            .filter(|found| found != path)
            .collect();
        named.sort_by_key(|found| (std::cmp::Reverse(shared_dirs(path, found)), found.clone()));
    }
    let mut calling = calling;
    calling.sort_by_key(|found| (std::cmp::Reverse(shared_dirs(path, found)), found.clone()));
    let mut files = named;
    for found in calling {
        if !files.contains(&found) {
            files.push(found);
        }
    }
    files.retain(|file| file != path);
    if files.is_empty() {
        return Ok(String::new());
    }
    let mut shown: Vec<String> = files.iter().take(TESTS_SHOWN).cloned().collect();
    if files.len() > TESTS_SHOWN {
        shown.push(format!("+{} more", files.len() - TESTS_SHOWN));
    }
    Ok(format!("» tests for {path}: {}\n", shown.join(", ")))
}

fn repo_relative(repo_path: &std::path::Path, path: &str) -> Result<String> {
    let canonical = dunce::canonicalize(repo_path.join(path))
        .or_else(|_| dunce::canonicalize(std::env::current_dir()?.join(path)))
        .with_context(|| format!("{path} does not exist"))?;
    anyhow::ensure!(canonical.is_file(), "{path} is not a file");
    let relative = canonical.strip_prefix(repo_path).with_context(|| {
        format!(
            "{path} is outside the indexed repository {}",
            repo_path.display()
        )
    })?;
    Ok(relative.to_string_lossy().replace('\\', "/"))
}

fn resolve_files(
    client: &duckdb_client::DuckDbClient,
    git: &workspace::GitInfo,
    paths: &[String],
    ids: &[i64],
) -> Result<Vec<NodeValue>> {
    if paths.is_empty() && ids.is_empty() {
        return Ok(Vec::new());
    }
    let hydrator = NodeHydrator::embedded("File")?;
    let selected = client.query_arrow_json(
        &format!(
            "SELECT {id} AS id, {reason} AS reason FROM {table}
             WHERE {project} = ?1 AND {commit} = ?2
               AND ({id} IN (SELECT unnest(?3::BIGINT[])) OR {path} IN (SELECT unnest(?4::VARCHAR[])))",
            id = hydrator.column("id")?,
            reason = hydrator.column("reason")?,
            table = hydrator.table(),
            project = hydrator.column("project_id")?,
            commit = hydrator.column("commit_sha")?,
            path = hydrator.column("path")?,
        ),
        &[git.project_id.into(), git.commit_sha.clone().into(), ids.into(), paths.into()],
    )?;
    let selected_ids = i64_column(&selected, "id");
    let reasons: BTreeMap<_, _> = selected_ids
        .iter()
        .copied()
        .zip(string_column(&selected, "reason"))
        .collect();
    let mut nodes = hydrator.query(
        client,
        &[
            ("project_id", git.project_id.into()),
            ("commit_sha", git.commit_sha.clone().into()),
        ],
        Some(&selected_ids),
    )?;
    for id in ids {
        anyhow::ensure!(
            selected_ids.contains(id),
            "no indexed File:{id} for commit {}",
            git.commit_sha
        );
    }
    for path in paths {
        anyhow::ensure!(
            nodes.iter().any(|node| node.properties["path"] == *path),
            "no indexed File for {path:?} for commit {}",
            git.commit_sha
        );
    }
    for node in &mut nodes {
        node.properties
            .insert("reason".into(), reasons[&node.id].clone().into());
    }
    nodes.sort_by(|a, b| {
        a.properties["path"]
            .as_str()
            .cmp(&b.properties["path"].as_str())
    });
    Ok(nodes)
}

fn definitions_in_files(
    client: &duckdb_client::DuckDbClient,
    git: &workspace::GitInfo,
    hydrator: &NodeHydrator,
    paths: &[String],
) -> Result<Vec<NodeValue>> {
    if paths.is_empty() {
        return Ok(Vec::new());
    }
    let selected = client.query_arrow_json(
        &format!(
            "SELECT {id} AS id FROM {table} WHERE {project} = ?1 AND {commit} = ?2
         AND {path} IN ({paths}) ORDER BY {path}, {start}, {id}",
            id = hydrator.column("id")?,
            table = hydrator.table(),
            project = hydrator.column("project_id")?,
            commit = hydrator.column("commit_sha")?,
            path = hydrator.column("file_path")?,
            start = hydrator.column("start_line")?,
            paths = paths
                .iter()
                .map(|path| sql_lit(path))
                .collect::<Vec<_>>()
                .join(", "),
        ),
        &[git.project_id.into(), git.commit_sha.clone().into()],
    )?;
    let mut nodes = definition::resolve_ids(client, git, hydrator, &i64_column(&selected, "id"))?;
    nodes.retain(|node| {
        node.properties["definition_type"] != "Variable"
            || !node.properties["fqn"]
                .as_str()
                .is_some_and(|fqn| fqn.contains('@'))
    });
    Ok(nodes)
}

pub(crate) fn outline(defs: &[SourceRange]) -> BTreeMap<String, Vec<SourceRange>> {
    let mut by_file: BTreeMap<String, Vec<SourceRange>> = BTreeMap::new();
    for def in defs {
        let entry = by_file.entry(def.file.clone()).or_default();
        if entry
            .last()
            .is_some_and(|outer| def.start >= outer.start && def.end <= outer.end)
        {
            continue;
        }
        entry.push(def.clone());
    }
    by_file
}

pub(crate) fn render(
    out: &mut String,
    defs: &[SourceRange],
    lines: &[&str],
    include_gaps: bool,
) -> std::fmt::Result {
    let mut blocks: Vec<(Option<&SourceRange>, usize, usize)> = Vec::new();
    let mut cursor = 1;
    let push_gap =
        |blocks: &mut Vec<(Option<&SourceRange>, usize, usize)>, start: usize, end: usize| {
            if !include_gaps || start > end {
                return;
            }
            let blank = lines
                .get(start - 1..end.min(lines.len()))
                .is_none_or(|gap| gap.iter().all(|l| l.trim().is_empty()));
            if !blank {
                blocks.push((None, start, end));
            }
        };
    for def in defs {
        if def.start > cursor {
            push_gap(&mut blocks, cursor, def.start - 1);
        }
        let start = def.start.max(cursor);
        if start <= def.end {
            blocks.push((Some(def), start, def.end));
            cursor = def.end + 1;
        }
    }
    if cursor <= lines.len() {
        push_gap(&mut blocks, cursor, lines.len());
    }
    let mut prev_single_line = false;
    for (i, (def, start, end)) in blocks.into_iter().enumerate() {
        let single_line = start == end;
        if i > 0 && !(single_line && prev_single_line) {
            out.push('\n');
        }
        prev_single_line = single_line;
        if let Some(def) = def {
            let loc = if include_gaps {
                format!("L{}-{}", def.start, def.end)
            } else {
                format!("{}:{}-{}", def.file, def.start, def.end)
            };
            writeln!(out, "{}  [{}]  {loc}", def.fqn, def.kind)?;
        }
        write_lines(out, lines, start, end)?;
    }
    Ok(())
}

fn write_lines(out: &mut String, lines: &[&str], start: usize, end: usize) -> std::fmt::Result {
    for n in start..=end.min(lines.len()) {
        writeln!(out, "{n}|{}", lines[n - 1])?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_names_follow_each_language_convention() {
        assert!(
            test_names("lib/a/emoji_converter.rb").contains(&"emoji_converter_spec.rb".to_string())
        );
        assert!(test_names("src/cursor.ts").contains(&"cursor.test.ts".to_string()));
    }

    #[test]
    fn qualified_targets_use_what_gitlab_shows() {
        use crate::remote::sdlc::Target;
        let target = |value: &str| Some(Qualified::Target(value.to_string()));
        assert_eq!(qualify("mr:42"), target("!42"));
        assert_eq!(qualify("MR:gitlab-org/cli!42"), target("gitlab-org/cli!42"));
        assert_eq!(qualify("issue:7"), target("#7"));
        assert_eq!(
            qualify("issue:gitlab-org/cli#7"),
            target("gitlab-org/cli#7")
        );
        let code = |kind: &str, value: &str| {
            Some(Qualified::Code(
                crate::remote::code::Code::parse(kind, value),
                value.to_string(),
            ))
        };
        assert_eq!(qualify("symbol:Foo::bar"), code("symbol", "Foo::bar"));
        assert_eq!(
            qualify("file:src/a.rs:10-40"),
            code("file", "src/a.rs:10-40")
        );
        assert_eq!(
            qualify("mr:fix/docker-platform"),
            Some(Qualified::Sdlc(Target::Branch(
                "fix/docker-platform".into()
            )))
        );
        assert_eq!(
            qualify("mr:branch:260456-fix"),
            Some(Qualified::Sdlc(Target::Branch("260456-fix".into())))
        );
        assert_eq!(
            qualify("branch:aa/fix-ci"),
            Some(Qualified::Sdlc(Target::Branch("aa/fix-ci".into())))
        );
        assert_eq!(
            qualify("pipeline:7"),
            Some(Qualified::Sdlc(Target::Pipeline(7)))
        );
        assert_eq!(qualify("job:9"), Some(Qualified::Sdlc(Target::Job(9))));
        assert_eq!(
            qualify("commit:0123abc"),
            Some(Qualified::Sdlc(Target::Commit("0123abc".into())))
        );
        assert_eq!(
            qualify("repo:gitlab-org/cli/"),
            Some(Qualified::Repo("gitlab-org/cli".into()))
        );
        for literal in [
            "src/a.rs:10-40",
            "Definition:7",
            "MergeRequest:42",
            "commit:main",
        ] {
            assert_eq!(qualify(literal), None, "{literal}");
        }
    }

    fn def(fqn: &str, kind: &str, start: usize, end: usize) -> SourceRange {
        SourceRange {
            id: 1,
            fqn: fqn.to_string(),
            kind: kind.to_string(),
            file: "src/lib.rs".to_string(),
            start,
            end,
        }
    }

    #[test]
    fn line_ranges_split_only_valid_suffixes() {
        assert_eq!(split_line_range("src/a.py:3-9"), Some(("src/a.py", 3, 9)));
        assert_eq!(split_line_range("src/a.py:7"), Some(("src/a.py", 7, 7)));
        for invalid in ["src/a.py", "src/a.py:9-3", "src/a.py:0-3", ":3-4"] {
            assert_eq!(split_line_range(invalid), None, "{invalid}");
        }
    }

    #[test]
    fn outline_drops_definitions_nested_in_a_wider_span() {
        let defs = vec![
            def("m::Config", "Struct", 3, 6),
            def("m::Config::name", "Field", 4, 4),
            def("m::Config::size", "Field", 5, 5),
            def("m::Config::new", "AssociatedFunction", 9, 11),
            def("m::run", "Function", 13, 15),
        ];
        let grouped = outline(&defs);
        let kept: Vec<&str> = grouped["src/lib.rs"]
            .iter()
            .map(|d| d.fqn.as_str())
            .collect();
        assert_eq!(kept, vec!["m::Config", "m::Config::new", "m::run"]);
    }

    #[test]
    fn outline_keeps_files_apart() {
        let mut other = def("o::x", "Function", 1, 2);
        other.file = "src/other.rs".to_string();
        let defs = vec![def("m::run", "Function", 1, 2), other];
        let grouped = outline(&defs);
        assert_eq!(grouped.len(), 2);
        assert_eq!(grouped["src/other.rs"][0].fqn, "o::x");
    }

    #[test]
    fn render_runs_consecutive_one_line_definitions_together() {
        let lines = vec!["pub mod a;", "pub mod b;", "", "fn run() {", "}"];
        let defs = vec![
            def("m::a", "Module", 1, 1),
            def("m::b", "Module", 2, 2),
            def("m::run", "Function", 4, 5),
        ];
        let mut out = String::new();
        render(&mut out, &defs, &lines, true).unwrap();
        assert_eq!(
            out,
            "m::a  [Module]  L1-1\n1|pub mod a;\n\
             m::b  [Module]  L2-2\n2|pub mod b;\n\n\
             m::run  [Function]  L4-5\n4|fn run() {\n5|}\n"
        );
    }

    #[test]
    fn render_survives_a_file_that_shrank_since_indexing() {
        let lines = vec!["a", "b", "c"];
        let defs = vec![
            def("m::a", "Function", 1, 10),
            def("m::b", "Function", 20, 25),
        ];
        let mut out = String::new();
        render(&mut out, &defs, &lines, true).unwrap();
        assert!(out.contains("1|a\n2|b\n3|c\n"));
        assert!(out.contains("m::b  [Function]  L20-25\n"));
    }

    #[test]
    fn render_does_not_repeat_partially_overlapping_lines() {
        let lines = vec!["a", "b", "c"];
        let defs = vec![
            def("m::first", "Function", 1, 2),
            def("m::second", "Function", 2, 3),
        ];
        let mut out = String::new();
        render(&mut out, &defs, &lines, false).unwrap();
        assert_eq!(out.matches("2|b").count(), 1, "{out}");
        assert!(out.contains("m::second  [Function]  src/lib.rs:2-3\n3|c"));
    }

    #[test]
    fn render_without_gaps_prints_only_definition_bodies() {
        let lines = vec!["use a;", "", "fn one() {", "}", "", "fn two() {", "}"];
        let defs = vec![
            def("m::one", "Function", 3, 4),
            def("m::two", "Function", 6, 7),
        ];
        let mut out = String::new();
        render(&mut out, &defs, &lines, false).unwrap();
        assert_eq!(
            out,
            "m::one  [Function]  src/lib.rs:3-4\n3|fn one() {\n4|}\n\n\
             m::two  [Function]  src/lib.rs:6-7\n6|fn two() {\n7|}\n"
        );
    }

    #[test]
    fn render_with_gaps_prints_non_blank_gaps_between_definitions() {
        let lines = vec![
            "use a;",
            "",
            "fn one() {",
            "}",
            "",
            "fn two() {",
            "}",
            "// tail",
        ];
        let defs = vec![
            def("m::one", "Function", 3, 4),
            def("m::two", "Function", 6, 7),
        ];
        let mut out = String::new();
        render(&mut out, &defs, &lines, true).unwrap();
        let numbered: Vec<usize> = out
            .lines()
            .filter_map(|l| l.split_once('|').and_then(|(n, _)| n.trim().parse().ok()))
            .collect();
        assert_eq!(
            numbered,
            vec![1, 2, 3, 4, 6, 7, 8],
            "blank-only gaps are skipped"
        );
        assert!(out.starts_with("1|use a;\n2|\n\nm::one  [Function]"));
        assert!(out.ends_with("7|}\n\n8|// tail\n"));
    }
}
