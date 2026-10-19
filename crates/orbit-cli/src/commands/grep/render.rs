use std::collections::{BTreeSet, HashSet};
use std::path::Path;

use anyhow::Result;
use grep_matcher::Matcher;

use super::{Options, Output};

use super::defs::Def;
use super::rank::{is_code, is_test, names, ranked};
use super::scan::Hit;
use super::term::{Term, matcher};

/// Graph context rides on rg's context-line form (`N-`), marked so it reads as a label.
const NOTE: &str = "» ";
/// A query that names a definition gets that definition's source first, so `head` keeps it.
const LOOKUP_MAX_DEFINITIONS: usize = 3;
const LOOKUP_LINES: usize = 80;
const EDITED_MARK: &str = " (edited since index)";
const FILE_INDEX_MIN: usize = 20;
const FILE_INDEX_DIRECTORIES: usize = 30;
/// Marks code matches that no indexed definition encloses, such as Rust `#[cfg(test)]`
/// modules, so they do not read as part of the definition above.
const OUTSIDE: &str = "outside indexed definitions";

#[derive(PartialEq)]
enum Scope<'a> {
    Start,
    Def(&'a Def),
    Imports(usize),
    Outside,
}

fn definition_note(def: &Def) -> String {
    match def.end > def.start {
        true => format!("{} {}:{}-{}", def.kind, def.name, def.start, def.end),
        false => format!("{} {}", def.kind, def.name),
    }
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
    if rows.len() > FILE_INDEX_MIN {
        out.push_str(&file_index(&rows, matched));
    }
    // rg's heading layout: each file's path once, then `line:text` for matches, `line-text` for
    // -A/-B/-C context, and `line-» Kind name:start-end ←callers →callees` before each enclosing
    // definition, with `--` between separate groups and a blank line between files. Import lines
    // get `line-» imports ...`, and code outside any indexed definition gets an `outside` line.
    // Lines print in full: `head` truncates, and `-M` replaces lines wider than its limit.
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
        let code = is_code(file);
        let mut scope = Scope::Start;
        let mut previous: Option<usize> = None;
        for hit in list {
            let adjacent = previous.is_some_and(|line| hit.line == line + 1);
            if previous.is_some_and(|line| hit.line > line + 1)
                && (options.before > 0 || options.after > 0)
            {
                out.push_str("--\n");
            }
            previous = Some(hit.line);
            let next = match (&hit.def, hit.imports.is_empty()) {
                (Some(def), _) => Scope::Def(def),
                (None, false) => Scope::Imports(hit.line),
                (None, true) if scope == Scope::Start && !code => Scope::Start,
                (None, true) => Scope::Outside,
            };
            if next != scope {
                match &next {
                    Scope::Def(def) => {
                        out.push_str(&format!("{}-{NOTE}{}\n", def.start, definition_note(def)))
                    }
                    Scope::Imports(_) => out.push_str(&format!(
                        "{}-{NOTE}imports {}\n",
                        hit.line,
                        hit.imports.join(", ")
                    )),
                    Scope::Outside if !(matches!(scope, Scope::Imports(_)) && adjacent) => {
                        out.push_str(&format!("{}-{NOTE}{OUTSIDE}\n", hit.line))
                    }
                    _ => {}
                }
            }
            scope = next;
            let text = match options.max_columns.is_some_and(|max| hit.text.len() > max) {
                true => "[Omitted long line]",
                false => hit.text.as_str(),
            };
            let sep = if hit.context { '-' } else { ':' };
            out.push_str(&format!("{}{sep}{text}\n", hit.line));
        }
    }
    Ok(out)
}

fn file_index(rows: &[(String, Vec<&Hit>)], matched: impl Fn(&[&Hit]) -> usize) -> String {
    let mut dirs: Vec<(&str, Vec<String>)> = Vec::new();
    for (file, list) in rows {
        let (dir, name) = file.rsplit_once('/').unwrap_or(("", file.as_str()));
        let entry = format!("{name} {}", matched(list));
        match dirs.iter_mut().find(|(d, _)| *d == dir) {
            Some((_, names)) => names.push(entry),
            None => dirs.push((dir, vec![entry])),
        }
    }
    let mut out = format!(
        "{NOTE}{} files by directory, matching lines per file:\n",
        rows.len()
    );
    let more = dirs.len().saturating_sub(FILE_INDEX_DIRECTORIES);
    let rest: usize = dirs
        .iter()
        .skip(FILE_INDEX_DIRECTORIES)
        .map(|(_, n)| n.len())
        .sum();
    for (dir, names) in dirs.into_iter().take(FILE_INDEX_DIRECTORIES) {
        let dir = match dir.is_empty() {
            true => ".".to_string(),
            false => format!("{dir}/"),
        };
        out.push_str(&format!("{NOTE}  {dir} {}\n", names.join(", ")));
    }
    if more > 0 {
        out.push_str(&format!(
            "{NOTE}  +{more} more directories with {rest} files\n"
        ));
    }
    out
}

/// The source of the one definition a single plain query term names, as rg context lines
/// under its `»` line. None for patterns, OR queries, or names with many definitions.
/// The source of a definition a plain query term names (at most a few such definitions), printed
/// before the rows so `head` keeps it. None with `-A`/`-B`/`-C`, `-v`, or `-l`/`-c`/`-q`.
pub(super) fn lookup(
    repo: &Path,
    hits: &[Hit],
    alternatives: &[Term],
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
            "{} {} — {}:{}-{}{others}",
            def.kind, def.name, hit.file, def.start, def.end
        );
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
            imports: Vec::new(),
            context: false,
        }
    }

    fn render_with(hits: &[Hit], terms: &[Term], options: &Options, edited: &[&str]) -> String {
        let edited = edited.iter().map(|f| f.to_string()).collect();
        render("grep", hits, terms, &edited, options, None).unwrap()
    }

    fn show(hits: &[Hit], terms: &[Term], output: Output, edited: &[&str]) -> String {
        let options = Options {
            output,
            ..Options::default()
        };
        render_with(hits, terms, &options, edited)
    }

    fn import(file: &str, line: usize, text: &str, label: &str) -> Hit {
        let mut hit = hit(file, line, text, None);
        hit.imports = vec![label.into()];
        hit
    }

    #[test]
    fn matches_print_under_file_headings_with_graph_lines() {
        let hits = [
            hit("data.json", 1, "\"go\": 0,", None),
            import("src/a.rs", 1, "use go::{", "go (go::go) → src/go.rs:4"),
            hit("src/a.rs", 2, "    go,", None),
            hit("src/a.rs", 3, "go();", Some(("run", 3, 9))),
            hit("src/a.rs", 4, "go();", Some(("run", 3, 9))),
            hit("src/a.rs", 12, "go();", Some(("walk", 11, 14))),
            hit("src/a.rs", 20, "go();", None),
            import("src/a.rs", 30, "use go::stop;", "stop (go::stop)"),
            hit("src/a.rs", 40, "go();", None),
            hit("src/b.rs", 2, "fn go() {}", None),
        ];
        let terms = [Term::parse("go"), Term::parse("nope")];
        assert_eq!(
            show(&hits, &terms, Output::Lines, &["src/b.rs"]),
            "grep: 10 lines in 3 files (go 10, nope 0)
src/a.rs
1-» imports go (go::go) → src/go.rs:4
1:use go::{
2:    go,
3-» Function run:3-9
3:go();
4:go();
11-» Function walk:11-14
12:go();
20-» outside indexed definitions
20:go();
30-» imports stop (go::stop)
30:use go::stop;
40-» outside indexed definitions
40:go();

src/b.rs (edited since index)
2-» outside indexed definitions
2:fn go() {}

data.json
1:\"go\": 0,
"
        );
        assert_eq!(
            show(&hits, &terms, Output::Count, &[]),
            "src/a.rs:8\nsrc/b.rs:1\ndata.json:1\n"
        );
    }

    #[test]
    fn many_matching_files_get_an_index_by_directory_before_the_matches() {
        let hits: Vec<Hit> = (0..45)
            .map(|i| hit(&format!("dir{}/f{i}.rs", i % 35), 1, "go();", None))
            .collect();
        let out = show(&hits, &[Term::parse("go")], Output::Lines, &[]);
        let lines: Vec<&str> = out.lines().collect();
        assert_eq!(
            lines[1],
            "» 45 files by directory, matching lines per file:"
        );
        assert!(lines[2].starts_with("»   dir"));
        assert_eq!(lines[32], "»   +5 more directories with 5 files");
        assert!(!lines[33].starts_with("» "));
        let few: Vec<Hit> = (0..20)
            .map(|i| hit(&format!("f{i}.rs"), 1, "go();", None))
            .collect();
        assert!(
            !show(&few, &[Term::parse("go")], Output::Lines, &[]).contains("files by directory")
        );
    }

    #[test]
    fn long_lines_print_in_full_unless_max_columns_is_set() {
        let long = "x".repeat(400);
        let hits = [hit("data.txt", 1, &long, None)];
        let terms = [Term::parse("x")];
        assert!(show(&hits, &terms, Output::Lines, &[]).ends_with(&format!("1:{long}\n")));
        let options = Options {
            max_columns: Some(100),
            ..Options::default()
        };
        assert!(render_with(&hits, &terms, &options, &[]).ends_with("1:[Omitted long line]\n"));
    }
}
