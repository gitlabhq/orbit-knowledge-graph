use std::collections::{BTreeSet, HashSet};
use std::path::Path;

use anyhow::Result;
use grep_matcher::Matcher;

use super::{Options, Output};

use super::defs::{Connections, Def, connection_label};
use super::rank::{is_code, is_test, names, ranked};
use super::scan::Hit;
use super::term::{Term, matcher};

/// Graph context rides on rg's context-line form (`N-`), marked so it reads as a label.
const NOTE: &str = "» ";
/// A query that names a definition gets that definition's source first, so `head` keeps it.
const LOOKUP_MAX_DEFINITIONS: usize = 3;
const LOOKUP_LINES: usize = 80;
/// Matched lines are cut here, as rg does with `-M`, unless `-M` sets another limit.
const LINE_CHARS: usize = 160;
const EDITED_MARK: &str = " (edited since index)";

fn definition_note(def: &Def, connections: &Connections) -> String {
    let span = match def.end > def.start {
        true => format!("{} {}:{}-{}", def.kind, def.name, def.start, def.end),
        false => format!("{} {}", def.kind, def.name),
    };
    format!("{span} {}", connection_label(def, connections))
        .trim_end()
        .to_string()
}

fn defined(hits: &[&Hit]) -> String {
    let mut labels: Vec<String> = Vec::new();
    for def in hits.iter().filter_map(|h| h.def.as_ref()) {
        let label = format!("{} {}", def.kind, def.name);
        if !labels.contains(&label) {
            labels.push(label);
        }
    }
    match labels.is_empty() {
        true => String::new(),
        false => format!(" │ {}", labels.join(", ")),
    }
}

pub(super) fn render(
    header: &str,
    hits: &[Hit],
    alternatives: &[Term],
    connections: &Connections,
    edited: &BTreeSet<String>,
    options: &Options,
    lookup: Option<&str>,
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
        Output::FileRows => {
            for (file, list) in &rows {
                out.push_str(&format!("{file}:{}{}\n", matched(list), defined(list)));
            }
            return Ok(out);
        }
        Output::Directories => {
            let mut dirs: std::collections::BTreeMap<&str, (usize, usize)> = Default::default();
            for (file, list) in &rows {
                let dir = file.rsplit_once('/').map_or(".", |(dir, _)| dir);
                let entry = dirs.entry(dir).or_default();
                *entry = (entry.0 + 1, entry.1 + matched(list));
            }
            for (dir, (files, lines)) in dirs {
                out.push_str(&format!("{dir}/ {files} files, {lines} lines\n"));
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
    if let Some(body) = lookup {
        out.push_str(body);
        out.push('\n');
    }
    // rg's heading layout: each file's path once, then `line:text` for matches, `line-text` for
    // -A/-B/-C context, and `line-» Kind name:start-end ←callers →callees` before each enclosing
    // definition, with `--` between separate groups and a blank line between files.
    for (index, (file, list)) in rows.iter().enumerate() {
        if index > 0 {
            out.push('\n');
        }
        let mark = if edited.contains(file) {
            EDITED_MARK
        } else {
            ""
        };
        out.push_str(&format!("{file}{mark}\n"));
        let mut current: Option<&Def> = None;
        let mut previous: Option<usize> = None;
        for hit in list {
            if previous.is_some_and(|line| hit.line > line + 1)
                && (options.before > 0 || options.after > 0)
            {
                out.push_str("--\n");
            }
            previous = Some(hit.line);
            if let Some(def) = hit.def.as_ref().filter(|def| current != Some(*def)) {
                out.push_str(&format!(
                    "{}-{NOTE}{}\n",
                    def.start,
                    definition_note(def, connections)
                ));
            }
            current = hit.def.as_ref();
            let text = match options.max_columns {
                Some(max) if hit.text.len() > max => "[Omitted long line]".to_string(),
                None if hit.text.chars().count() > LINE_CHARS => {
                    format!("{}…", hit.text.chars().take(LINE_CHARS).collect::<String>())
                }
                _ => hit.text.clone(),
            };
            let sep = if hit.context { '-' } else { ':' };
            out.push_str(&format!("{}{sep}{text}\n", hit.line));
        }
    }
    Ok(out)
}

/// The source of the one definition a single plain query term names, as rg context lines
/// under its `»` line. None for patterns, OR queries, or names with many definitions.
/// The source of a definition a plain query term names (at most a few such definitions), printed
/// before the rows so `head` keeps it. None with `-A`/`-B`/`-C`, `-v`, or `-l`/`-c`/`-q`.
pub(super) fn lookup(
    repo: &Path,
    hits: &[Hit],
    alternatives: &[Term],
    connections: &Connections,
    options: &Options,
) -> Option<String> {
    if options.invert || options.output != Output::Lines || options.before > 0 || options.after > 0
    {
        return None;
    }
    alternatives.iter().find_map(|term| {
        if term.is_regex() {
            return None;
        }
        let only = std::slice::from_ref(term);
        let mut seen = HashSet::new();
        let named: Vec<&Hit> = hits
            .iter()
            .filter(|h| !h.context && names(h, only))
            .filter(|h| h.def.as_ref().is_some_and(|d| seen.insert(d.id)))
            .collect();
        if named.len() > LOOKUP_MAX_DEFINITIONS {
            return None;
        }
        let hit = named
            .iter()
            .min_by_key(|h| (!is_code(&h.file), is_test(&h.file)))?;
        let def = hit.def.as_ref()?;
        let content = std::fs::read_to_string(repo.join(&hit.file)).ok()?;
        let lines: Vec<&str> = content.lines().collect();
        let end = def.end.min(lines.len()).min(def.start + LOOKUP_LINES - 1);
        let others = match named.len() {
            1 => String::new(),
            n => format!(" (1 of {n}; the others are in the rows below)"),
        };
        let mut out = format!(
            "{} {} — {}:{}-{}{others} {}",
            def.kind,
            def.name,
            hit.file,
            def.start,
            def.end,
            connection_label(def, connections)
        )
        .trim_end()
        .to_string();
        out.push('\n');
        for number in def.start..=end {
            out.push_str(&format!("  {number}|{}\n", lines.get(number - 1)?));
        }
        if def.end > end {
            out.push_str(&format!(
                "  rest: {} context {}:{}-{}\n",
                crate::commands::setup::spec::launcher(),
                hit.file,
                end + 1,
                def.end
            ));
        }
        Some(out)
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn hit(file: &str, line: usize, text: &str, def: Option<(&str, usize, usize)>) -> Hit {
        let def = def.map(|(name, start, end)| Def {
            name: name.into(),
            start,
            end,
            id: start as i64,
            kind: "Function".into(),
        });
        let (file, text) = (file.into(), text.into());
        Hit {
            file,
            line,
            text,
            def,
            context: false,
        }
    }

    fn show(hits: &[Hit], terms: &[Term], output: Output, edited: &[&str]) -> String {
        let edited = edited.iter().map(|f| f.to_string()).collect();
        let options = Options {
            output,
            ..Options::default()
        };
        render(
            "grep",
            hits,
            terms,
            &Connections::new(),
            &edited,
            &options,
            None,
        )
        .unwrap()
    }

    #[test]
    fn rows_group_lines_under_definitions_with_defining_files_first() {
        let hits = [
            hit("data.json", 1, "\"go\": 0,", None),
            hit("src/a.rs", 3, "go();", Some(("run", 1, 9))),
            hit("src/a.rs", 4, "go();", Some(("run", 1, 9))),
            hit("src/b.rs", 2, "fn go() {}", None),
        ];
        let terms = [Term::parse("go"), Term::parse("nope")];
        assert_eq!(
            show(&hits, &terms, Output::Lines, &["src/b.rs"]),
            "grep: 4 lines in 3 files (go 4, nope 0)
src/a.rs
1-» Function run:1-9
3:go();
4:go();

src/b.rs (edited since index)
2:fn go() {}

data.json
1:\"go\": 0,
"
        );
        assert_eq!(
            show(&hits, &terms, Output::Count, &[]),
            "src/a.rs:2\nsrc/b.rs:1\ndata.json:1\n"
        );
    }
}
