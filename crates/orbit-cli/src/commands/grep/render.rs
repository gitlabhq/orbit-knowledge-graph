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
const LOOKUP_LINES: usize = 40;

fn definition_note(def: &Def, connections: &Connections) -> String {
    let span = match def.end > def.start {
        true => format!("{} {}:{}-{}", def.kind, def.name, def.start, def.end),
        false => format!("{} {}", def.kind, def.name),
    };
    format!("{span} {}", connection_label(def, connections))
        .trim_end()
        .to_string()
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
        out.push_str("--\n");
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
pub(super) fn lookup(
    repo: &Path,
    hits: &[Hit],
    alternatives: &[Term],
    connections: &Connections,
    options: &Options,
) -> Option<String> {
    let [term] = alternatives else {
        return None;
    };
    if term.is_regex() || options.invert || options.output != Output::Lines {
        return None;
    }
    let mut seen = HashSet::new();
    let named: Vec<&Hit> = hits
        .iter()
        .filter(|h| !h.context && names(h, alternatives))
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
    let file = &hit.file;
    let mut out = format!(
        "{file}-{}-{NOTE}{}\n",
        def.start,
        definition_note(def, connections)
    );
    for number in def.start..=end {
        out.push_str(&format!("{file}-{number}-{}\n", lines.get(number - 1)?));
    }
    if def.end > end {
        out.push_str(&format!(
            "{file}-{}-{NOTE}{} more lines: {} context {file}:{}-{}\n",
            end + 1,
            def.end - end,
            crate::commands::setup::spec::launcher(),
            end + 1,
            def.end
        ));
    }
    Some(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn hit(file: &str, line: usize, text: &str, def: Option<(&str, usize, usize)>) -> Hit {
        Hit {
            file: file.into(),
            line,
            text: text.into(),
            def: def.map(|(name, start, end)| Def {
                name: name.into(),
                start,
                end,
                id: start as i64,
                kind: "Function".into(),
            }),
            context: false,
        }
    }

    fn show(hits: &[Hit], terms: &[Term], options: &Options) -> String {
        render(
            "grep",
            hits,
            terms,
            &Connections::new(),
            &BTreeSet::new(),
            options,
            None,
        )
        .unwrap()
    }

    #[test]
    fn lines_print_like_rg_with_definitions_noted_and_defining_files_first() {
        let terms = vec![Term::parse("maintenanceMode"), Term::parse("nosuchxyz")];
        let mut hits = vec![
            hit(
                "install/data/defaults.json",
                130,
                "\"maintenanceMode\": 0,",
                None,
            ),
            hit(
                "src/middleware/maintenance.js",
                10,
                "middleware.maintenanceMode = helpers.try(",
                Some(("default", 9, 41)),
            ),
            hit(
                "src/middleware/maintenance.js",
                11,
                "    if (!meta.config.maintenanceMode) {",
                Some(("default", 9, 41)),
            ),
            hit(
                "test/controllers.js",
                1203,
                "meta.config.maintenanceMode = 1;",
                Some(("describe", 1201, 1230)),
            ),
        ];
        for line in 26..=27 {
            hits.push(hit(
                "src/routes/feeds.js",
                line,
                "app.get('/x', middleware.maintenanceMode, y);",
                Some(("default", 25, 38)),
            ));
        }
        hits.sort_by(|a, b| a.file.cmp(&b.file).then(a.line.cmp(&b.line)));
        let out = show(&hits, &terms, &Options::default());
        assert_eq!(
            out,
            "grep: 6 lines in 4 files (maintenanceMode 6, nosuchxyz 0)
src/middleware/maintenance.js-9-» Function default:9-41
src/middleware/maintenance.js:10:middleware.maintenanceMode = helpers.try(
src/middleware/maintenance.js:11:    if (!meta.config.maintenanceMode) {
src/routes/feeds.js-25-» Function default:25-38
src/routes/feeds.js:26:app.get('/x', middleware.maintenanceMode, y);
src/routes/feeds.js:27:app.get('/x', middleware.maintenanceMode, y);
test/controllers.js-1201-» Function describe:1201-1230
test/controllers.js:1203:meta.config.maintenanceMode = 1;
install/data/defaults.json:130:\"maintenanceMode\": 0,
"
        );
        let files = Options {
            output: Output::Files,
            ..Options::default()
        };
        assert_eq!(
            show(&hits, &terms, &files),
            "src/middleware/maintenance.js\nsrc/routes/feeds.js\ntest/controllers.js\ninstall/data/defaults.json\n"
        );
        let count = Options {
            output: Output::Count,
            ..Options::default()
        };
        assert!(
            show(&hits, &terms, &count)
                .starts_with("src/middleware/maintenance.js:2\nsrc/routes/feeds.js:2\n")
        );
        let quiet = Options {
            output: Output::Quiet,
            ..Options::default()
        };
        assert_eq!(show(&hits, &terms, &quiet), "");
    }

    #[test]
    fn context_lines_use_dashes_and_groups_are_separated() {
        let mut around = hit("src/a.rs", 2, "let x = 1;", Some(("run", 1, 9)));
        around.context = true;
        let hits = vec![
            around,
            hit("src/a.rs", 3, "go();", Some(("run", 1, 9))),
            hit("src/a.rs", 8, "go();", Some(("run", 1, 9))),
        ];
        let options = Options {
            before: 1,
            ..Options::default()
        };
        assert_eq!(
            show(&hits, &[Term::parse("go")], &options),
            "grep: 2 lines in 1 files
src/a.rs-1-» Function run:1-9
src/a.rs-2-let x = 1;
src/a.rs:3:go();
--
src/a.rs:8:go();
"
        );
    }

    #[test]
    fn edited_code_files_are_noted_without_definitions() {
        let hits = vec![
            hit("src/a.rs", 3, "fn go() {}", None),
            hit("src/b.rs", 4, "go();", Some(("run", 1, 9))),
        ];
        let edited = BTreeSet::from(["src/a.rs".to_string()]);
        let out = render(
            "grep",
            &hits,
            &[Term::parse("go")],
            &Connections::new(),
            &edited,
            &Options::default(),
            None,
        )
        .unwrap();
        assert!(
            out.contains(
                "src/a.rs-3-» edited since index; definitions not shown\nsrc/a.rs:3:fn go() {}\n"
            ),
            "{out}"
        );
        assert!(
            out.contains("src/b.rs-1-» Function run:1-9\nsrc/b.rs:4:go();\n"),
            "{out}"
        );
    }

    #[test]
    fn alternatives_are_counted_and_long_lines_print_whole_unless_capped() {
        let long = format!("{} go()", "x".repeat(500));
        let hits = vec![
            hit("src/a.rs", 3, "fn go() { stop() }", None),
            hit("src/a.rs", 4, &long, None),
        ];
        let terms = [Term::parse("go"), Term::parse("stop"), Term::parse("nope")];
        let out = show(&hits, &terms, &Options::default());
        assert!(
            out.starts_with("grep: 2 lines in 1 files (go 2, stop 1, nope 0)\n"),
            "{out}"
        );
        assert!(out.contains(&format!("src/a.rs:4:{long}\n")), "{out}");
        let capped = Options {
            max_columns: Some(100),
            ..Options::default()
        };
        let out = show(&hits, &terms, &capped);
        assert!(out.contains("src/a.rs:4:[Omitted long line]\n"), "{out}");
        assert!(
            show(&hits, &terms[..1], &Options::default()).starts_with("grep: 2 lines in 1 files\n")
        );
        assert_eq!(
            show(&[], &terms, &Options::default()),
            "grep: 0 lines in 0 files\n"
        );
    }

    #[test]
    fn locale_copies_collapse_into_one_row() {
        let hits: Vec<Hit> = ["ar", "de", "en-GB", "fr"]
            .iter()
            .map(|l| {
                hit(
                    &format!("public/language/{l}/advanced.json"),
                    2,
                    "\"maintenance-mode\": \"x\"",
                    None,
                )
            })
            .collect();
        let out = show(
            &hits,
            &[Term::parse("maintenanceMode")],
            &Options::default(),
        );
        assert_eq!(
            out,
            "grep: 1 lines in 1 files\npublic/language/{en-GB,+3}/advanced.json:2:\"maintenance-mode\": \"x\"\n"
        );
    }

    #[test]
    fn a_single_name_prints_its_definition_first_and_long_ones_are_capped() {
        let repo = tempfile::tempdir().unwrap();
        let body: String = (1..=60).map(|n| format!("line {n}\n")).collect();
        std::fs::write(repo.path().join("a.rs"), body).unwrap();
        let plain = Options::default();
        let short = [hit("a.rs", 2, "fn go() {", Some(("go", 2, 4)))];
        let out = lookup(
            repo.path(),
            &short,
            &[Term::parse("go")],
            &Connections::new(),
            &plain,
        )
        .unwrap();
        assert_eq!(
            out,
            "a.rs-2-» Function go:2-4\na.rs-2-line 2\na.rs-3-line 3\na.rs-4-line 4\n"
        );
        let long = [hit("a.rs", 1, "fn go() {", Some(("go", 1, 60)))];
        let out = lookup(
            repo.path(),
            &long,
            &[Term::parse("go")],
            &Connections::new(),
            &plain,
        )
        .unwrap();
        assert!(
            out.ends_with("a.rs-41-» 20 more lines: orbit context a.rs:41-60\n"),
            "{out}"
        );
        let either = [Term::parse("go"), Term::parse("stop")];
        assert!(lookup(repo.path(), &short, &either, &Connections::new(), &plain).is_none());
    }
}
