use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Write as _;

use anyhow::{Context, Result};
use duckdb_client::search::{NodeHydrator, NodeValue};
use duckdb_client::{i64_column, sql_lit, string_column};

use crate::commands::{definition, relations, setup::spec};
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

pub(crate) fn run(target: crate::ContextArgs) -> Result<()> {
    let workspace::IndexedRepo { git, client } = workspace::open_indexed(target.repo, target.db)?;
    let hydrator = NodeHydrator::embedded("Definition")?;
    let (paths, file_ids, ids) = resolve_targets(&client, &git, &hydrator, &target.target)?;
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
    let mut out = String::new();
    for (file, file_defs) in outline(&defs) {
        let path = repo_relative(&git.repo_path, &file)?;
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
    print!("{out}");
    Ok(())
}

pub(crate) const INLINE_BODY_LINES: usize = 120;

pub(crate) fn render_bodies(
    client: &duckdb_client::DuckDbClient,
    git: &workspace::GitInfo,
    nodes: &[NodeValue],
) -> Result<String> {
    let defs = nodes.iter().map(source_range).collect::<Result<Vec<_>>>()?;
    let hydrator = NodeHydrator::embedded("Definition")?;
    let mut files = BTreeMap::new();
    let mut shown = BTreeSet::new();
    let mut remaining = INLINE_BODY_LINES;
    let mut out = String::new();
    for def in &defs {
        let content = match files.entry(def.file.clone()) {
            std::collections::btree_map::Entry::Vacant(entry) => entry.insert(
                std::fs::read_to_string(
                    git.repo_path
                        .join(repo_relative(&git.repo_path, &def.file)?),
                )
                .with_context(|| format!("failed to read {}", def.file))?,
            ),
            std::collections::btree_map::Entry::Occupied(entry) => entry.into_mut(),
        };
        let lines: Vec<&str> = content.lines().collect();
        if !out.is_empty() {
            out.push('\n');
        }
        let uncovered: Vec<_> = (def.start..=def.end.min(lines.len()))
            .filter(|line| !shown.contains(&(def.file.clone(), *line)))
            .collect();
        if uncovered.len() <= remaining && def.end.saturating_sub(def.start) < INLINE_BODY_LINES {
            writeln!(
                out,
                "Definition:{}  {}  [{}]  {}:{}-{}",
                def.id, def.fqn, def.kind, def.file, def.start, def.end
            )?;
            if uncovered.len() < def.end.saturating_sub(def.start) + 1 {
                writeln!(out, "Overlapping source already shown above.")?;
            }
            remaining -= uncovered.len();
            for line in uncovered {
                write_lines(&mut out, &lines, line, line)?;
                shown.insert((def.file.clone(), line));
            }
        } else {
            let members =
                definitions_in_files(client, git, &hydrator, std::slice::from_ref(&def.file))?;
            let members = members
                .iter()
                .map(source_range)
                .collect::<Result<Vec<_>>>()?;
            render_outline(&mut out, std::slice::from_ref(def), &members)?;
            writeln!(
                out,
                "Body omitted; run `{} context Definition:{}` for complete source and relationships.",
                spec::launcher(),
                def.id
            )?;
        }
    }
    Ok(out)
}

fn resolve_targets(
    client: &duckdb_client::DuckDbClient,
    git: &workspace::GitInfo,
    hydrator: &NodeHydrator,
    targets: &[String],
) -> Result<(Vec<String>, Vec<i64>, Vec<i64>)> {
    let mut files = Vec::new();
    let mut file_ids = Vec::new();
    let mut ids = Vec::new();
    let mut seen = BTreeSet::new();
    for target in targets.iter().filter(|target| seen.insert(*target)) {
        if let Some((kind, id)) = target.split_once(':')
            && matches!(kind, "Definition" | "File")
            && !id.starts_with(':')
        {
            let id = id
                .parse::<i64>()
                .ok()
                .filter(|id| *id > 0)
                .with_context(|| format!("{target:?} is not a valid {kind}:<id>"))?;
            let ids = if kind == "File" {
                &mut file_ids
            } else {
                &mut ids
            };
            if !ids.contains(&id) {
                ids.push(id);
            }
        } else {
            match repo_relative(&git.repo_path, target) {
                Ok(path) => {
                    if !files.contains(&path) {
                        files.push(path);
                    }
                }
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
                    if matches.is_empty() {
                        return Err(error);
                    }
                    matches.sort_by_key(|node| node.id);
                    ids.extend(matches.into_iter().map(|node| node.id));
                }
            }
        }
    }
    Ok((files, file_ids, ids))
}

fn repo_relative(repo_path: &std::path::Path, path: &str) -> Result<String> {
    let canonical = dunce::canonicalize(repo_path.join(path))
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

pub(crate) fn render_outline(
    out: &mut String,
    defs: &[SourceRange],
    members: &[SourceRange],
) -> std::fmt::Result {
    for (i, def) in defs.iter().enumerate() {
        if i > 0 && !members.is_empty() {
            out.push('\n');
        }
        writeln!(
            out,
            "Definition:{}  {}  [{}]  {}:{}-{}",
            def.id, def.fqn, def.kind, def.file, def.start, def.end
        )?;
        let mut nested: Vec<&SourceRange> = members
            .iter()
            .filter(|m| m != &def && belongs_to(def, m))
            .collect();
        nested.sort_by(|a, b| a.start.cmp(&b.start).then(b.end.cmp(&a.end)));
        let mut covered_until = 0;
        for member in nested {
            if member.start <= covered_until {
                continue;
            }
            covered_until = member.end;
            writeln!(
                out,
                "  Definition:{}  {}  [{}]  L{}-{}",
                member.id, member.fqn, member.kind, member.start, member.end
            )?;
        }
    }
    Ok(())
}

fn belongs_to(def: &SourceRange, member: &SourceRange) -> bool {
    let by_range = member.start >= def.start && member.end <= def.end;
    let by_name = member
        .fqn
        .strip_prefix(&def.fqn)
        .is_some_and(|rest| rest.starts_with([':', '.', '#']));
    by_range || by_name
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
            writeln!(
                out,
                "Definition:{}  {}  [{}]  {loc}",
                def.id, def.fqn, def.kind
            )?;
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
    fn inline_bodies_preserve_rank_order_across_files() {
        let repo = tempfile::tempdir().unwrap();
        std::fs::write(repo.path().join("z.rs"), "fn first() {}\n").unwrap();
        std::fs::write(repo.path().join("a.rs"), "fn second() {}\n").unwrap();
        let node = |id, fqn: &str, file: &str| NodeValue {
            entity_type: "Definition".into(),
            id,
            properties: serde_json::from_value(serde_json::json!({
                "fqn": fqn,
                "definition_type": "Function",
                "file_path": file,
                "start_line": 1,
                "end_line": 1
            }))
            .unwrap(),
        };
        let client = duckdb_client::DuckDbClient::open(&repo.path().join("graph.duckdb")).unwrap();
        let git = workspace::GitInfo {
            repo_path: dunce::canonicalize(repo.path()).unwrap(),
            project_id: 1,
            branch: "main".into(),
            commit_sha: "current".into(),
            parent_repo_path: repo.path().to_path_buf(),
        };
        let out = render_bodies(
            &client,
            &git,
            &[node(1, "z::first", "z.rs"), node(2, "a::second", "a.rs")],
        )
        .unwrap();
        assert!(out.find("z::first").unwrap() < out.find("a::second").unwrap());
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
            "Definition:1  m::a  [Module]  L1-1\n1|pub mod a;\n\
             Definition:1  m::b  [Module]  L2-2\n2|pub mod b;\n\n\
             Definition:1  m::run  [Function]  L4-5\n4|fn run() {\n5|}\n"
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
        assert!(out.contains("Definition:1  m::b  [Function]  L20-25\n"));
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
        assert!(out.contains("Definition:1  m::second  [Function]  src/lib.rs:2-3\n3|c"));
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
            "Definition:1  m::one  [Function]  src/lib.rs:3-4\n3|fn one() {\n4|}\n\n\
             Definition:1  m::two  [Function]  src/lib.rs:6-7\n6|fn two() {\n7|}\n"
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
        assert!(out.starts_with("1|use a;\n2|\n\nDefinition:1  m::one  [Function]"));
        assert!(out.ends_with("7|}\n\n8|// tail\n"));
    }
}
