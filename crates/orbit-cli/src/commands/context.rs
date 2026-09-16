use std::collections::BTreeMap;
use std::fmt::Write as _;

use anyhow::{Context, Result};
use duckdb_client::search::{NodeHydrator, NodeValue};

use crate::commands::{definition, relations, setup::spec};
use crate::workspace;

const SIGNATURE_LINES: usize = 3;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SourceRange {
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
        fqn: string("fqn")?,
        kind: string("definition_type")?,
        file: string("file_path")?,
        start: line("start_line")?,
        end: line("end_line")?,
    })
}

pub(crate) async fn run(target: crate::ContextArgs, targets: Targets) -> Result<()> {
    if let Some(local) = targets.local {
        run_local(&target, local)?;
        if !targets.remote_refs.is_empty() {
            println!("\n--- Remote context ---");
        }
    }
    if !targets.remote_refs.is_empty() {
        crate::remote::context::run(targets.remote_refs, target.response_format).await?;
    }
    Ok(())
}

fn run_local(target: &crate::ContextArgs, targets: LocalTargets) -> Result<()> {
    let workspace::IndexedRepo { git, client } =
        workspace::open_indexed(target.repo.clone(), target.db.clone())?;
    let (file, ids) = match targets {
        LocalTargets::File(path) => (Some(repo_relative(&git.repo_path, &path)?), Vec::new()),
        LocalTargets::Definitions(ids) => (None, ids),
    };
    let file_mode = file.is_some();
    let hydrator = NodeHydrator::embedded("Definition")?;
    let nodes = if let Some(path) = file.as_deref() {
        let nodes = definitions_in_file(&client, &git, &hydrator, path)?;
        if nodes.is_empty() {
            let launcher = spec::launcher();
            anyhow::bail!(
                "no indexed definitions in {path:?} for commit {} — run `{launcher} grep` or index the checkout",
                git.commit_sha
            );
        }
        nodes
    } else {
        definition::resolve_ids(&client, &git, &hydrator, &ids)?
    };
    let mut defs = nodes.iter().map(source_range).collect::<Result<Vec<_>>>()?;
    defs.sort_by(|a, b| {
        a.file
            .cmp(&b.file)
            .then(a.start.cmp(&b.start))
            .then(b.end.cmp(&a.end))
            .then(a.fqn.cmp(&b.fqn))
    });
    defs.dedup();

    let mut out = String::new();
    for (file, file_defs) in outline(&defs) {
        let content = std::fs::read_to_string(git.repo_path.join(&file))
            .with_context(|| format!("failed to read {file}"))?;
        let lines: Vec<&str> = content.lines().collect();
        if !out.is_empty() {
            out.push('\n');
        }
        if file_mode {
            writeln!(
                out,
                "{file}  ({} definitions, {} lines)",
                file_defs.len(),
                lines.len()
            )?;
        }
        render(&mut out, &file_defs, &lines, file_mode)?;
    }
    print!("{out}");
    if !file_mode {
        println!();
        relations::print(&client, &git, &hydrator, &nodes, target.tests)?;
    }
    Ok(())
}

pub(crate) const INLINE_BODY_LINES: usize = 120;

pub(crate) fn render_bodies(
    client: &duckdb_client::DuckDbClient,
    git: &workspace::GitInfo,
    nodes: &[NodeValue],
) -> Result<String> {
    let defs = nodes.iter().map(source_range).collect::<Result<Vec<_>>>()?;
    let hydrator = defs
        .iter()
        .any(|def| def.end.saturating_sub(def.start) >= INLINE_BODY_LINES)
        .then(|| NodeHydrator::embedded("Definition"))
        .transpose()?;
    let mut files = BTreeMap::new();
    let mut out = String::new();
    for def in &defs {
        let content = match files.entry(def.file.clone()) {
            std::collections::btree_map::Entry::Vacant(entry) => entry.insert(
                std::fs::read_to_string(git.repo_path.join(&def.file))
                    .with_context(|| format!("failed to read {}", def.file))?,
            ),
            std::collections::btree_map::Entry::Occupied(entry) => entry.into_mut(),
        };
        let lines: Vec<&str> = content.lines().collect();
        if !out.is_empty() {
            out.push('\n');
        }
        if def.end.saturating_sub(def.start) < INLINE_BODY_LINES {
            render(&mut out, std::slice::from_ref(def), &lines, false)?;
        } else {
            let hydrator = hydrator
                .as_ref()
                .context("definition hydrator unavailable")?;
            let members = definitions_in_file(client, git, hydrator, &def.file)?;
            let members = members
                .iter()
                .map(source_range)
                .collect::<Result<Vec<_>>>()?;
            render_outline(&mut out, std::slice::from_ref(def), &members, &lines)?;
        }
    }
    Ok(out)
}

#[derive(Debug, PartialEq)]
pub(crate) struct Targets {
    local: Option<LocalTargets>,
    pub(crate) remote_refs: Vec<String>,
}

#[derive(Debug, PartialEq)]
enum LocalTargets {
    Definitions(Vec<i64>),
    File(String),
}

pub(crate) fn classify(args: &crate::ContextArgs) -> Result<Targets> {
    let ontology = ontology::Ontology::load_embedded()?;
    let mut ids = Vec::new();
    let mut refs = Vec::new();
    let mut files = Vec::new();
    for target in &args.target {
        let delimiter = target.find([':', '[', ']']);
        let path_prefix = target
            .find(['/', '\\'])
            .is_some_and(|separator| delimiter.is_none_or(|delimiter| separator < delimiter));
        let windows_absolute = target
            .as_bytes()
            .first()
            .is_some_and(u8::is_ascii_alphabetic)
            && target.as_bytes().get(1) == Some(&b':')
            && matches!(target.as_bytes().get(2), Some(b'/' | b'\\'));
        if path_prefix || windows_absolute {
            files.push(target.clone());
            continue;
        }
        let Some(delimiter) = delimiter else {
            files.push(target.clone());
            continue;
        };
        let (kind, suffix) = target.split_at(delimiter);
        let canonical = if kind == "Issue" { "WorkItem" } else { kind };
        ontology.get_node(canonical).with_context(|| {
            format!("unsupported context type {kind:?}; expected an ontology node name or Issue")
        })?;
        let id = suffix
            .strip_prefix(':')
            .or_else(|| suffix.strip_prefix('[').and_then(|id| id.strip_suffix(']')));
        let id = id
            .filter(|id| !id.is_empty() && id.bytes().all(|byte| byte.is_ascii_digit()))
            .and_then(|id| id.parse::<i64>().ok())
            .with_context(|| format!("invalid context reference {target:?}; expected {kind}:<id> or {kind}[<id>] with a non-negative 64-bit database ID"))?;
        if canonical == "Definition" {
            ids.push(id);
        } else {
            refs.push(format!("{canonical}[{id}]"));
        }
    }
    anyhow::ensure!(
        files.is_empty() || (files.len() == 1 && ids.is_empty()),
        "a file path is accepted only as the sole local target"
    );
    let local = if let Some(file) = files.pop() {
        anyhow::ensure!(!args.tests, "--tests requires Definition:<id> targets");
        Some(LocalTargets::File(file))
    } else if !ids.is_empty() {
        Some(LocalTargets::Definitions(ids))
    } else {
        anyhow::ensure!(
            !args.tests && args.repo.is_none() && args.db.is_none(),
            "--tests, --repo, and --db are local-only options; remove them for remote-only entity context (use an explicit path for a local file)"
        );
        None
    };
    anyhow::ensure!(
        !refs.is_empty() || args.response_format.is_none(),
        "--response-format is only supported for remote entity context"
    );
    Ok(Targets {
        local,
        remote_refs: refs,
    })
}

fn repo_relative(repo_path: &std::path::Path, path: &str) -> Result<String> {
    let canonical = dunce::canonicalize(repo_path.join(path))
        .with_context(|| format!("{path} does not exist"))?;
    let relative = canonical.strip_prefix(repo_path).with_context(|| {
        format!(
            "{path} is outside the indexed repository {}",
            repo_path.display()
        )
    })?;
    Ok(relative.to_string_lossy().replace('\\', "/"))
}

fn definitions_in_file(
    client: &duckdb_client::DuckDbClient,
    git: &workspace::GitInfo,
    hydrator: &NodeHydrator,
    path: &str,
) -> Result<Vec<NodeValue>> {
    let mut nodes = hydrator.query(
        client,
        &[
            ("project_id", git.project_id.into()),
            ("commit_sha", git.commit_sha.clone().into()),
            ("file_path", path.into()),
        ],
        None,
    )?;
    nodes.retain(|node| {
        node.properties["fqn"]
            .as_str()
            .is_some_and(|fqn| !fqn.contains('@'))
    });
    Ok(nodes)
}

pub(crate) fn render_outline(
    out: &mut String,
    defs: &[SourceRange],
    members: &[SourceRange],
    lines: &[&str],
) -> std::fmt::Result {
    for (i, def) in defs.iter().enumerate() {
        if i > 0 {
            out.push('\n');
        }
        writeln!(
            out,
            "{}  [{}]  {}:{}-{}",
            def.fqn, def.kind, def.file, def.start, def.end
        )?;
        write_signature(out, lines, def.start, def.end)?;
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
                "  {}  [{}]  L{}-{}",
                member.fqn, member.kind, member.start, member.end
            )?;
            write_signature(out, lines, member.start, member.end)?;
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

fn write_signature(out: &mut String, lines: &[&str], start: usize, end: usize) -> std::fmt::Result {
    let last = end.min(start + SIGNATURE_LINES - 1).min(lines.len());
    for n in start..=last {
        let line = lines[n - 1];
        writeln!(out, "{n}|{line}")?;
        if line.trim_end().ends_with(['{', ':', ';']) {
            return Ok(());
        }
    }
    if last < end {
        writeln!(out, "{}|…", last + 1)?;
    }
    Ok(())
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

    fn def(fqn: &str, kind: &str, start: usize, end: usize) -> SourceRange {
        SourceRange {
            fqn: fqn.to_string(),
            kind: kind.to_string(),
            file: "src/lib.rs".to_string(),
            start,
            end,
        }
    }

    #[test]
    fn targets_classify_definition_references_or_one_file() {
        let classify_targets = |targets: &[&str]| {
            classify(&crate::ContextArgs {
                target: targets.iter().map(|target| (*target).into()).collect(),
                response_format: None,
                tests: false,
                repo: None,
                db: None,
            })
        };
        assert_eq!(
            classify_targets(&["src/lib.rs"]).unwrap().local,
            Some(LocalTargets::File("src/lib.rs".into()))
        );
        let definitions = classify_targets(&["Definition:7", "Definition[9]"]).unwrap();
        assert_eq!(
            definitions.local,
            Some(LocalTargets::Definitions(vec![7, 9]))
        );
        assert!(definitions.remote_refs.is_empty());
        assert!(classify_targets(&["Type::method"]).is_err());
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
            repo_path: repo.path().to_path_buf(),
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
