use std::collections::BTreeMap;
use std::fmt::Write as _;

use anyhow::{Context, Result};
use duckdb_client::search::kind_scope;

use crate::commands::{
    fqn::{self, Def},
    setup::spec,
};
use crate::workspace;

const SIGNATURE_LINES: usize = 3;

pub(crate) fn run(target: crate::ContextArgs) -> Result<()> {
    let file_mode = target.fqn.is_empty();
    let workspace::IndexedRepo { git, client } = workspace::open_indexed(target.repo, target.db)?;
    let kinds = crate::kind_names(target.kind);
    let file = target
        .file
        .as_deref()
        .map(|p| repo_relative(&git.repo_path, p))
        .transpose()?;
    let file = file.as_deref();
    let mut defs = match (target.fqn.as_slice(), file) {
        ([], None) => anyhow::bail!("pass one or more fqns or globs, or --file <path>"),
        ([], Some(path)) => {
            let resolved = definitions_in_file(&client, &git, path, &kinds)?;
            if resolved.is_empty() {
                let launcher = spec::launcher();
                anyhow::bail!(
                    "no indexed definitions in {path:?}{} for commit {} — pass a repo-relative \
                     path as printed by `{launcher} grep`, and make sure the commit is indexed \
                     (`{launcher} index <path>`)",
                    fqn::kind_suffix(&kinds),
                    git.commit_sha
                );
            }
            resolved
        }
        (names, file) => {
            let mut defs = Vec::new();
            for name in names {
                defs.extend(fqn::resolve(&client, &git, name, file, &kinds)?);
            }
            defs
        }
    };
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
        if target.outline {
            let members = definitions_in_file(&client, &git, &file, &[])?;
            render_outline(&mut out, &file_defs, &members, &lines)?;
        } else {
            render(&mut out, &file_defs, &lines, file_mode)?;
        }
    }
    print!("{out}");
    Ok(())
}

pub(crate) const INLINE_BODY_LINES: usize = 120;

pub(crate) fn render_bodies(
    client: &duckdb_client::DuckDbClient,
    git: &workspace::GitInfo,
    defs: &[Def],
) -> Result<String> {
    let mut out = String::new();
    for (file, file_defs) in outline(defs) {
        let content = std::fs::read_to_string(git.repo_path.join(&file))
            .with_context(|| format!("failed to read {file}"))?;
        let lines: Vec<&str> = content.lines().collect();
        let (short, long): (Vec<Def>, Vec<Def>) = file_defs
            .into_iter()
            .partition(|d| d.end.saturating_sub(d.start) < INLINE_BODY_LINES);
        if !out.is_empty() {
            out.push('\n');
        }
        render(&mut out, &short, &lines, false)?;
        if !long.is_empty() {
            let members = definitions_in_file(client, git, &file, &[])?;
            if !short.is_empty() {
                out.push('\n');
            }
            render_outline(&mut out, &long, &members, &lines)?;
        }
    }
    Ok(out)
}

fn repo_relative(repo_path: &std::path::Path, path: &str) -> Result<String> {
    let trimmed = path.trim_end_matches('/');
    if !std::path::Path::new(trimmed).is_absolute() {
        return Ok(trimmed.trim_start_matches("./").to_string());
    }
    let canonical =
        dunce::canonicalize(trimmed).with_context(|| format!("{trimmed} does not exist"))?;
    let relative = canonical.strip_prefix(repo_path).with_context(|| {
        format!(
            "{trimmed} is outside the indexed repository {}",
            repo_path.display()
        )
    })?;
    Ok(relative.to_string_lossy().replace('\\', "/"))
}

fn definitions_in_file(
    client: &duckdb_client::DuckDbClient,
    git: &workspace::GitInfo,
    path: &str,
    kinds: &[String],
) -> Result<Vec<Def>> {
    let batches = client.query_arrow_json(
        &format!(
            "SELECT id, fqn, definition_type, file_path, start_line, end_line
             FROM gl_definition
             WHERE project_id = ?1 AND commit_sha = ?2 AND file_path = ?3
               AND fqn NOT LIKE '%@%'
             {}
             ORDER BY start_line, end_line DESC, fqn",
            kind_scope("definition_type", kinds)
        ),
        &[
            git.project_id.into(),
            git.commit_sha.clone().into(),
            path.into(),
        ],
    )?;
    Ok(fqn::defs_from(&batches))
}

pub(crate) fn render_outline(
    out: &mut String,
    defs: &[Def],
    members: &[Def],
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
        let mut nested: Vec<&Def> = members
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

fn belongs_to(def: &Def, member: &Def) -> bool {
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
            id: 0,
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
