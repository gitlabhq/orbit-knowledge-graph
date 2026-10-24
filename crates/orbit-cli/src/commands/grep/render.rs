use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::path::Path;

use anyhow::Result;
use grep_matcher::Matcher;

use super::{Options, Output};

use super::defs::Def;
use super::rank::{is_code, is_test, names, ranked};
use super::scan::Hit;
use super::term::{Term, matcher};

const NOTE: &str = "» ";
/// A query that names a definition gets that definition's source first, so `head` keeps it.
const LOOKUP_MAX_DEFINITIONS: usize = 3;
const LOOKUP_LINES: usize = 80;
const EDITED_MARK: &str = " (edited since index)";
const FILE_INDEX_MIN: usize = 20;
const FILE_INDEX_DIRECTORIES: usize = 30;
const OUTSIDE: &str = "outside indexed definitions";
const STALE: &str = "edited since index; definitions not shown";

#[derive(PartialEq)]
enum Scope<'a> {
    Start,
    Def(&'a Def),
    Imports(usize),
    Outside,
    Stale,
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
    for (index, (file, list)) in rows.iter().enumerate() {
        if index > 0 {
            out.push('\n');
        }
        let stale = edited.contains(file);
        let mark = if stale { EDITED_MARK } else { "" };
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
                _ if stale => Scope::Stale,
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
                    Scope::Stale => out.push_str(&format!("{}-{NOTE}{STALE}\n", hit.line)),
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
    let mut dirs: Vec<(&str, Vec<(&str, usize)>)> = Vec::new();
    for (file, list) in rows {
        let (dir, name) = file.rsplit_once('/').unwrap_or(("", file.as_str()));
        match dirs.iter_mut().find(|(d, _)| *d == dir) {
            Some((_, names)) => names.push((name, matched(list))),
            None => dirs.push((dir, vec![(name, matched(list))])),
        }
    }
    let mut out = format!(
        "{NOTE}{} files by directory, matching lines per file:\n",
        rows.len()
    );
    let lines = index_lines(&dirs);
    let more = lines.len().saturating_sub(FILE_INDEX_DIRECTORIES);
    let rest: usize = lines
        .iter()
        .skip(FILE_INDEX_DIRECTORIES)
        .map(|(_, files)| files)
        .sum();
    for (line, _) in lines.into_iter().take(FILE_INDEX_DIRECTORIES) {
        out.push_str(&format!("{NOTE}  {line}\n"));
    }
    if more > 0 {
        out.push_str(&format!(
            "{NOTE}  +{more} more directories with {rest} files\n"
        ));
    }
    out
}

const SIBLING_MIN: usize = 3;

fn index_lines(dirs: &[(&str, Vec<(&str, usize)>)]) -> Vec<(String, usize)> {
    let parent = |dir: &str| {
        dir.rsplit_once('/')
            .map_or("", |(parent, _)| parent)
            .to_string()
    };
    let mut siblings: BTreeMap<(String, &str), Vec<usize>> = BTreeMap::new();
    for (dir, names) in dirs {
        if let [(name, count)] = names.as_slice() {
            siblings
                .entry((parent(dir), *name))
                .or_default()
                .push(*count);
        }
    }
    let mut shared = BTreeSet::new();
    let mut lines = Vec::new();
    for (dir, names) in dirs {
        if let [(name, _)] = names.as_slice() {
            let key = (parent(dir), *name);
            let counts = &siblings[&key];
            if counts.len() >= SIBLING_MIN {
                if shared.insert(key.clone()) {
                    let dir = match key.0.is_empty() {
                        true => "*/".to_string(),
                        false => format!("{}/*/", key.0),
                    };
                    let total: usize = counts.iter().sum();
                    lines.push((
                        format!(
                            "{dir} {name} in {} directories, {total} lines",
                            counts.len()
                        ),
                        counts.len(),
                    ));
                }
                continue;
            }
        }
        let dir = match dir.is_empty() {
            true => ".".to_string(),
            false => format!("{dir}/"),
        };
        let files: Vec<String> = names
            .iter()
            .map(|(name, count)| format!("{name} {count}"))
            .collect();
        lines.push((format!("{dir} {}", files.join(", ")), names.len()));
    }
    lines
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

    #[test]
    fn sibling_directories_with_the_same_lone_file_share_a_line() {
        let dirs: Vec<(&str, Vec<(&str, usize)>)> = ["a/x", "a/y", "a/z"]
            .into_iter()
            .map(|dir| (dir, vec![("c.ts", 1)]))
            .collect();
        assert_eq!(
            index_lines(&dirs),
            vec![("a/*/ c.ts in 3 directories, 3 lines".to_string(), 3)]
        );
    }

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
2-» edited since index; definitions not shown
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
}
