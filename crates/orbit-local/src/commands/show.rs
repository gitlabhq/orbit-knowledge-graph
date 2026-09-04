use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::path::PathBuf;

use anyhow::{Context, Result};
use duckdb_client::search::kind_scope;

use crate::commands::{fqn, setup::spec};
use crate::workspace;

pub(crate) struct Target {
    pub fqns: Vec<String>,
    pub file: Option<String>,
    pub kinds: Vec<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct Def {
    pub fqn: String,
    pub kind: String,
    pub file: String,
    pub start: usize,
    pub end: usize,
}

pub(crate) fn run(target: Target, repo: Option<PathBuf>, db: Option<PathBuf>) -> Result<()> {
    let file = target.file.as_deref().map(|p| p.trim_end_matches('/'));
    let file_mode = target.fqns.is_empty();
    let workspace::IndexedRepo { git, client } = workspace::open_indexed(repo, db)?;
    let resolved = match (target.fqns.as_slice(), file) {
        ([], None) => anyhow::bail!("pass one or more fqns or globs, or --file <path>"),
        ([], Some(path)) => {
            let batches = client.query_arrow_json(
                &format!(
                    "SELECT id, fqn, definition_type, file_path, start_line, end_line
                     FROM gl_definition
                     WHERE project_id = ?1 AND commit_sha = ?2 AND file_path = ?3
                       AND fqn NOT LIKE '%@%'
                     {}
                     ORDER BY start_line, end_line DESC, fqn",
                    kind_scope("definition_type", &target.kinds)
                ),
                &[
                    git.project_id.into(),
                    git.commit_sha.clone().into(),
                    path.into(),
                ],
            )?;
            let resolved = fqn::defs_from(&batches);
            if resolved.is_empty() {
                let launcher = spec::launcher();
                anyhow::bail!(
                    "no indexed definitions in {path:?}{} for commit {} — pass a repo-relative \
                     path as printed by `{launcher} grep`, and make sure the commit is indexed \
                     (`{launcher} index <path>`)",
                    fqn::kind_suffix(&target.kinds),
                    git.commit_sha
                );
            }
            resolved
        }
        (names, file) => names
            .iter()
            .map(|name| fqn::resolve(&client, &git, name, file, &target.kinds))
            .collect::<Result<Vec<_>>>()?
            .into_iter()
            .flatten()
            .collect(),
    };
    let mut defs: Vec<Def> = resolved
        .into_iter()
        .map(|d| Def {
            fqn: d.fqn,
            kind: d.kind,
            file: d.file,
            start: usize::try_from(d.start).unwrap_or(1),
            end: usize::try_from(d.end).unwrap_or(0),
        })
        .collect();
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
    Ok(())
}

pub(crate) fn outline(defs: &[Def]) -> BTreeMap<String, Vec<Def>> {
    let mut by_file: BTreeMap<String, Vec<Def>> = BTreeMap::new();
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
    defs: &[Def],
    lines: &[&str],
    include_gaps: bool,
) -> std::fmt::Result {
    let mut blocks: Vec<(Option<&Def>, usize, usize)> = Vec::new();
    let mut cursor = 1;
    let push_gap = |blocks: &mut Vec<(Option<&Def>, usize, usize)>, start: usize, end: usize| {
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
        blocks.push((Some(def), def.start, def.end));
        cursor = cursor.max(def.end + 1);
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

    fn def(fqn: &str, kind: &str, start: usize, end: usize) -> Def {
        Def {
            fqn: fqn.to_string(),
            kind: kind.to_string(),
            file: "src/lib.rs".to_string(),
            start,
            end,
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
