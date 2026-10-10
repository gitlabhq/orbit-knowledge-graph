use std::collections::{BTreeSet, HashSet};
use std::path::Path;

use anyhow::Result;
use grep_matcher::Matcher;

use super::{Options, Output};

use super::defs::{Connections, Def, connection_label};
use super::rank::{is_code, is_test, names, ranked};
use super::scan::Hit;
use super::term::{Term, matcher};

/// Graph context rides on rg's context-line form (`path-N-`), so every line keeps its path.
const NOTE: &str = "» ";
/// A query that names a definition gets that definition's source first, so `head` keeps it.
const LOOKUP_MAX_DEFINITIONS: usize = 3;
const LOOKUP_LINES: usize = 80;
/// Rows print each file once: the first few matching lines of a definition in full, the rest as
/// line numbers.
const SEP: &str = " │ ";
const FULL_LINES: usize = 3;
const LINE_CHARS: usize = 160;
const EDITED_MARK: &str = " (edited since index)";

fn located(lines: &[&Hit], max_columns: Option<usize>) -> String {
    let mut parts: Vec<String> = lines
        .iter()
        .take(FULL_LINES)
        .map(|h| match max_columns {
            Some(max) if h.text.len() > max => format!(":{} [Omitted long line]", h.line),
            None if h.text.chars().count() > LINE_CHARS => format!(
                ":{} {}…",
                h.line,
                h.text.chars().take(LINE_CHARS).collect::<String>()
            ),
            _ => format!(":{} {}", h.line, h.text),
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

fn code_row(hits: &[&Hit], connections: &Connections, max_columns: Option<usize>) -> String {
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
            let label = def.map(|d| format!("{} ", definition_note(d, connections)));
            format!(
                "{}{}",
                label.unwrap_or_default(),
                located(list, max_columns)
            )
        })
        .collect::<Vec<_>>()
        .join(SEP)
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
    if options.before == 0 && options.after == 0 {
        for (file, list) in &rows {
            let mark = if edited.contains(file) {
                EDITED_MARK
            } else {
                ""
            };
            let row = match is_code(file) {
                true => code_row(list, connections, options.max_columns),
                false => located(list, options.max_columns),
            };
            out.push_str(&format!("  {file}{mark}{SEP}{row}\n"));
        }
        return Ok(out);
    }
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
  src/a.rs │ Function run:1-9 :3 go(); │ :4 go();
  src/b.rs (edited since index) │ :2 fn go() {}
  data.json │ :1 \"go\": 0,
"
        );
        assert_eq!(
            show(&hits, &terms, Output::Count, &[]),
            "src/a.rs:2\nsrc/b.rs:1\ndata.json:1\n"
        );
    }

    #[test]
    fn a_named_definition_prints_first_and_long_ones_are_capped() {
        let repo = tempfile::tempdir().unwrap();
        let body: String = (1..=100).map(|n| format!("line {n}\n")).collect();
        std::fs::write(repo.path().join("a.rs"), body).unwrap();
        let look = |hits: &[Hit], term: &str| {
            let terms = [Term::parse(term)];
            lookup(
                repo.path(),
                hits,
                &terms,
                &Connections::new(),
                &Options::default(),
            )
        };
        let short = [hit("a.rs", 2, "fn go() {", Some(("go", 2, 4)))];
        assert_eq!(
            look(&short, "go").unwrap(),
            "Function go — a.rs:2-4\n  2|line 2\n  3|line 3\n  4|line 4\n"
        );
        let long = [hit("a.rs", 1, "fn go() {", Some(("go", 1, 100)))];
        assert!(
            look(&long, "go")
                .unwrap()
                .ends_with("  rest: orbit context a.rs:81-100\n")
        );
        assert!(look(&short, r"go\(").is_none());
    }
}
