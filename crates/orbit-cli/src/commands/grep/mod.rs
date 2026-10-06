mod local;
mod text;

use std::collections::{HashMap, HashSet};
use std::io::Write;
use std::path::PathBuf;

use anyhow::Result;
use duckdb_client::search::NodeValue;
use orbit_search::{RecallFilter, query_alternatives};

use crate::commands::context;
use local::LocalBackend;

pub(crate) fn run(
    query: Option<String>,
    repo: Option<PathBuf>,
    db: Option<PathBuf>,
    limit: usize,
    paths: Vec<String>,
    filter: RecallFilter,
) -> Result<()> {
    let launcher = crate::commands::setup::spec::launcher();
    if let Some(query) = &query
        && query_alternatives(query).is_err()
    {
        anyhow::bail!(
            "no usable search terms in query: {query:?} — to list every definition in a \
             file or directory instead, run `{launcher} grep --path <path>`; for a file's \
             definition map and connections, `{launcher} context <path>`"
        );
    }

    let backend = LocalBackend::open(repo, db, &paths)?;

    let mut out = std::io::stdout().lock();
    let Some(query) = query else {
        return report_outline(&mut out, &backend, &paths, &filter, launcher);
    };
    if !paths.is_empty() {
        writeln!(out, "path: {}", paths.join(" "))?;
    }
    if !filter.kinds.is_empty() {
        writeln!(out, "kind: {}", filter.kinds.join(" "))?;
    }

    writeln!(out, "grep {:?} @ {}", query, backend.header())?;
    let (outcome, nodes) = backend.grep(&query, limit, &filter)?;
    report_exact_query_note(&mut out, &outcome)?;
    let text_hits: text::Mentions = match filter.is_empty() {
        true => text::mentions(
            backend.search().client(),
            backend.git(),
            &outcome.alternatives,
            &paths,
        )
        .unwrap_or_default(),
        false => text::Mentions::default(),
    };
    if !text_hits.unmatched.is_empty() {
        writeln!(out, "No matches: {}", text_hits.unmatched.join(" | "))?;
    }

    if nodes.is_empty() && !text_hits.files.is_empty() {
        writeln!(out, "No definitions match.")?;
        write!(out, "{}", text::render(&text_hits, &HashSet::new()))?;
        return Ok(());
    }
    if nodes.is_empty() {
        if paths.is_empty() && filter.is_empty() {
            writeln!(
                out,
                "No definitions match. Try a symbol name, or `{launcher} context <path>`."
            )?;
        } else {
            writeln!(
                out,
                "No definitions match in scope. Drop --path/--kind, or try a symbol name."
            )?;
        }
        return Ok(());
    }

    let sources = body_sources(&backend.git().repo_path, &outcome, &nodes);
    let shown = report_results(&mut out, &outcome, &nodes, &sources)?;
    write!(out, "{}", text::render(&text_hits, &shown))?;
    if let Some(top) = nodes
        .iter()
        .zip(&outcome.matches)
        .find(|(_, hit)| hit.exact_name)
        .map(|(node, _)| (node, None))
        .or_else(|| {
            nodes
                .iter()
                .zip(&outcome.matches)
                .find(|(_, hit)| assigns(&hit.body_text, &outcome.alternatives))
                .map(|(node, hit)| (node, hit.body_offset))
        })
    {
        write!(
            out,
            "{}",
            top_source(&backend.git().repo_path, top.0, top.1)?
        )?;
    }
    Ok(())
}

const TOP_SOURCE_LINES: usize = 25;

fn assigns(line: &str, alternatives: &[String]) -> bool {
    alternatives.iter().any(|term| {
        regex::Regex::new(&format!(r"(?i)\b{}\s*=[^=]", regex::escape(term.trim())))
            .is_ok_and(|re| re.is_match(line))
    })
}

fn top_source(repo: &std::path::Path, node: &NodeValue, offset: Option<usize>) -> Result<String> {
    let range = context::source_range(node)?;
    let Ok(content) = std::fs::read_to_string(repo.join(&range.file)) else {
        return Ok(String::new());
    };
    let lines: Vec<&str> = content.lines().collect();
    let start = offset.map_or(range.start, |offset| range.start + offset - 1);
    if start > lines.len() {
        return Ok(String::new());
    }
    let end = range.end.min(lines.len()).min(start + TOP_SOURCE_LINES - 1);
    let mut out = format!(
        "\nSource — {}  {}:{start}-{}\n",
        range.fqn, range.file, range.end
    );
    for number in start..=end {
        out.push_str(&format!("  {number}|{}\n", lines[number - 1]));
    }
    if range.end > end {
        out.push_str(&format!(
            "  … {} more lines: {} context {}:{}-{}\n",
            range.end - end,
            crate::commands::setup::spec::launcher(),
            range.file,
            end + 1,
            range.end
        ));
    }
    Ok(out)
}

fn report_outline(
    out: &mut impl Write,
    backend: &LocalBackend,
    paths: &[String],
    filter: &RecallFilter,
    launcher: &str,
) -> Result<()> {
    writeln!(out, "outline {} @ {}", paths.join(" "), backend.header())?;
    if !filter.kinds.is_empty() {
        writeln!(out, "kind: {}", filter.kinds.join(" "))?;
    }
    let rows = backend.search().list_corpus(filter)?;
    if rows.is_empty() {
        writeln!(
            out,
            "\nNo indexed definitions under that path. Paths are repo-relative, as printed by `{launcher} grep`."
        )?;
        return Ok(());
    }
    writeln!(out, "\nDefinitions ({}):", rows.len())?;
    for node in &rows {
        report_definition(out, node)?;
    }
    Ok(())
}

fn body_sources(
    repo: &std::path::Path,
    outcome: &orbit_search::GrepOutcome,
    nodes: &[NodeValue],
) -> HashMap<String, Vec<String>> {
    let mut sources = HashMap::new();
    for (node, hit) in nodes.iter().zip(&outcome.matches) {
        if hit.exact_name || hit.name_match {
            continue;
        }
        let Ok(range) = context::source_range(node) else {
            continue;
        };
        if sources.contains_key(&range.file) {
            continue;
        }
        if let Ok(content) = std::fs::read_to_string(repo.join(&range.file)) {
            sources.insert(range.file, content.lines().map(str::to_string).collect());
        }
    }
    sources
}

fn compact(text: &str) -> String {
    text.chars()
        .filter(|c| !(c.is_whitespace() || *c == '_' || *c == '-'))
        .flat_map(char::to_lowercase)
        .collect()
}

fn matching_lines(
    lines: &[String],
    start: usize,
    end: usize,
    alternatives: &[String],
) -> Vec<usize> {
    let terms: Vec<String> = alternatives
        .iter()
        .map(|a| compact(a))
        .filter(|t| !t.is_empty())
        .collect();
    (start..=end.min(lines.len()))
        .filter(|number| {
            let line = compact(&lines[number - 1]);
            terms.iter().any(|term| line.contains(term.as_str()))
        })
        .collect()
}

fn snippet_around(text: &str, alternatives: &[String]) -> String {
    let text = text.split_whitespace().collect::<Vec<_>>().join(" ");
    let lower = text.to_lowercase();
    let hit = alternatives
        .iter()
        .filter_map(|a| lower.find(&a.to_lowercase()))
        .min()
        .unwrap_or(0);
    let chars: Vec<char> = text.chars().collect();
    let at = text[..hit.min(text.len())].chars().count();
    let from = at.saturating_sub(SNIPPET_BEFORE);
    let to = (from + SNIPPET_CHARS).min(chars.len());
    let mut out: String = chars[from..to].iter().collect();
    if from > 0 {
        out.insert(0, '…');
    }
    if to < chars.len() {
        out.push('…');
    }
    out
}

fn line_runs(lines: &[usize]) -> String {
    let mut parts = Vec::new();
    let mut index = 0;
    while index < lines.len() {
        let mut end = index;
        while end + 1 < lines.len() && lines[end + 1] == lines[end] + 1 {
            end += 1;
        }
        match end > index {
            true => parts.push(format!(":{}-{}", lines[index], lines[end])),
            false => parts.push(format!(":{}", lines[index])),
        }
        index = end + 1;
    }
    parts.join(" ")
}

fn packed_matches(lines: &[String], numbers: &[usize], alternatives: &[String]) -> String {
    let mut out = String::from("      ");
    let mut shown = 0;
    for number in numbers {
        let part = format!(
            ":{number} {}",
            snippet_around(&lines[number - 1], alternatives)
        );
        if shown > 0 && out.chars().count() + part.chars().count() + 2 > PACKED_LINE_CHARS {
            break;
        }
        if shown > 0 {
            out.push_str("  ");
        }
        out.push_str(&part);
        shown += 1;
    }
    if shown < numbers.len() {
        out.push_str(&format!(
            "  +{} {}",
            numbers.len() - shown,
            line_runs(&numbers[shown..])
        ));
    }
    out
}

fn report_results(
    out: &mut impl Write,
    outcome: &orbit_search::GrepOutcome,
    nodes: &[NodeValue],
    sources: &HashMap<String, Vec<String>>,
) -> Result<HashSet<(String, usize)>> {
    let mut shown: HashSet<(String, usize)> = HashSet::new();
    let ranges = nodes
        .iter()
        .map(context::source_range)
        .collect::<Result<Vec<_>>>()?;
    let parent_of = |index: usize| {
        let hit = &outcome.matches[index];
        (!hit.exact_name && hit.name_match)
            .then(|| {
                ranges
                    .iter()
                    .zip(&outcome.matches)
                    .position(|(parent, parent_hit)| {
                        parent_hit.exact_name && is_member(&ranges[index].fqn, &parent.fqn)
                    })
            })
            .flatten()
    };
    let mut members: HashMap<usize, usize> = HashMap::new();
    for index in 0..ranges.len() {
        if let Some(parent) = parent_of(index) {
            *members.entry(parent).or_default() += 1;
        }
    }
    for (index, (node, hit)) in nodes.iter().zip(&outcome.matches).enumerate() {
        if parent_of(index).is_some() {
            continue;
        }
        let range = context::source_range(node)?;
        let label = if hit.exact_name {
            "exact-name"
        } else if hit.name_match {
            "name/path"
        } else {
            "body-only"
        };
        let body = hit
            .body_offset
            .filter(|_| !hit.exact_name && !hit.name_match)
            .map(|offset| {
                let text: String = hit.body_text.chars().take(BODY_PREVIEW_CHARS).collect();
                (range.start + offset - 1, text)
            });
        let mentions = match (&body, members.get(&index)) {
            (Some(_), _) => format!(" \u{d7}{}", hit.mentions),
            (None, Some(count)) => format!("  +{count} members"),
            (None, None) => String::new(),
        };
        writeln!(
            out,
            "  {}  [{}]  {}:{}-{}  {label}{mentions}",
            range.fqn, range.kind, range.file, range.start, range.end
        )?;
        let numbers = sources
            .get(&range.file)
            .filter(|_| body.is_some())
            .map(|lines| {
                (
                    lines,
                    matching_lines(lines, range.start, range.end, &outcome.alternatives),
                )
            })
            .filter(|(_, numbers)| !numbers.is_empty());
        match (numbers, body) {
            (Some((lines, numbers)), _) => {
                let fresh: Vec<usize> = numbers
                    .into_iter()
                    .filter(|n| shown.insert((range.file.clone(), *n)))
                    .collect();
                if !fresh.is_empty() {
                    writeln!(
                        out,
                        "{}",
                        packed_matches(lines, &fresh, &outcome.alternatives)
                    )?;
                }
            }
            (None, Some((line, text))) => {
                if shown.insert((range.file.clone(), line)) {
                    writeln!(out, "      {line}| {text}")?;
                }
            }
            (None, None) => {}
        }
    }
    let hidden = outcome.total.saturating_sub(outcome.matches.len());
    if hidden >= BROAD_HIDDEN_HITS {
        writeln!(
            out,
            "  … {hidden} more; broad query. Add --path/--kind or a specific name."
        )?;
    } else if hidden > 0 {
        writeln!(out, "  … {hidden} more; add --path/--kind or --limit.")?;
    }
    Ok(shown)
}

fn is_member(fqn: &str, parent: &str) -> bool {
    fqn.strip_prefix(parent)
        .is_some_and(|rest| rest.starts_with(['.', ':', '#', '/']))
}

fn report_definition(out: &mut impl Write, node: &NodeValue) -> Result<()> {
    let range = context::source_range(node)?;
    writeln!(
        out,
        "  {}  [{}]  {}:{}-{}",
        range.fqn, range.kind, range.file, range.start, range.end
    )?;
    Ok(())
}

const BROAD_HIDDEN_HITS: usize = 100;
const BODY_PREVIEW_CHARS: usize = 100;
const PACKED_LINE_CHARS: usize = 240;
const SNIPPET_CHARS: usize = 64;
const SNIPPET_BEFORE: usize = 20;

fn report_exact_query_note(
    out: &mut impl Write,
    outcome: &orbit_search::GrepOutcome,
) -> std::io::Result<()> {
    let exact: HashSet<_> = outcome
        .exact_alternatives
        .iter()
        .map(|alternative| alternative.to_lowercase())
        .collect();
    let alternatives: Vec<_> = outcome
        .alternatives
        .iter()
        .map(String::as_str)
        .filter(|alternative| !alternative.chars().any(char::is_whitespace))
        .collect();
    let matched: Vec<_> = alternatives
        .iter()
        .copied()
        .filter(|alternative| exact.contains(&alternative.to_lowercase()))
        .collect();
    if !matched.is_empty() {
        writeln!(out, "exact: {}", matched.join(" | "))?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn outcome() -> orbit_search::GrepOutcome {
        orbit_search::GrepOutcome {
            alternatives: Vec::new(),
            exact_alternatives: Vec::new(),
            matches: Vec::new(),
            total: 0,
        }
    }

    #[test]
    fn results_print_definition_identity_and_range() {
        let mut result = outcome();
        result.matches.push(orbit_search::GrepMatch {
            id: 481,
            score: 1.0,
            exact_name: true,
            name_match: true,
            body_offset: None,
            body_text: String::new(),
            mentions: 0,
        });
        result.total = 1;
        let node = NodeValue {
            entity_type: "Definition".to_string(),
            id: 481,
            properties: serde_json::from_value(serde_json::json!({
                "fqn": "Repo::commit_hook",
                "definition_type": "Method",
                "file_path": "crates/repo/src/lib.rs",
                "start_line": 42,
                "end_line": 57
            }))
            .unwrap(),
        };
        let mut buf = Vec::new();
        report_results(&mut buf, &result, &[node], &HashMap::new()).unwrap();
        assert_eq!(
            String::from_utf8(buf).unwrap(),
            "  Repo::commit_hook  [Method]  crates/repo/src/lib.rs:42-57  exact-name\n"
        );
    }

    #[test]
    fn body_only_results_print_the_first_matching_line() {
        let mut result = outcome();
        result.matches.push(orbit_search::GrepMatch {
            id: 7,
            score: 1.0,
            exact_name: false,
            name_match: false,
            body_offset: Some(3),
            body_text: "x".repeat(140),
            mentions: 2,
        });
        result.total = 1;
        let node = NodeValue {
            entity_type: "Definition".to_string(),
            id: 7,
            properties: serde_json::from_value(serde_json::json!({
                "fqn": "Repo::run",
                "definition_type": "Method",
                "file_path": "src/lib.rs",
                "start_line": 10,
                "end_line": 20
            }))
            .unwrap(),
        };
        let mut buf = Vec::new();
        report_results(&mut buf, &result, &[node], &HashMap::new()).unwrap();
        assert_eq!(
            String::from_utf8(buf).unwrap(),
            format!(
                "  Repo::run  [Method]  src/lib.rs:10-20  body-only ×2\n      12| {}\n",
                "x".repeat(100)
            )
        );
    }

    #[test]
    fn body_hits_pack_every_matching_line_and_skip_lines_already_shown() {
        let mut result = outcome();
        result.alternatives = vec!["port".to_string()];
        for id in [1, 2] {
            result.matches.push(orbit_search::GrepMatch {
                id,
                score: 1.0,
                exact_name: false,
                name_match: false,
                body_offset: Some(2),
                body_text: "port_a();".into(),
                mentions: 3,
            });
        }
        let node = |id: i64, start: i64, end: i64| NodeValue {
            entity_type: "Definition".to_string(),
            id,
            properties: serde_json::from_value(serde_json::json!({
                "fqn": format!("m::f{id}"),
                "definition_type": "Function",
                "file_path": "src/a.rs",
                "start_line": start,
                "end_line": end
            }))
            .unwrap(),
        };
        let lines: Vec<String> = [
            "fn f() {",
            "    port_a();",
            "    other();",
            "    port_b();",
            "    port_c();",
            "}",
        ]
        .iter()
        .map(|l| l.to_string())
        .collect();
        let sources = HashMap::from([("src/a.rs".to_string(), lines)]);
        let mut buf = Vec::new();
        report_results(&mut buf, &result, &[node(1, 1, 6), node(2, 4, 5)], &sources).unwrap();
        assert_eq!(
            String::from_utf8(buf).unwrap(),
            "  m::f1  [Function]  src/a.rs:1-6  body-only ×3\n      :2 port_a();  :4 port_b();  :5 port_c();\n  m::f2  [Function]  src/a.rs:4-5  body-only ×3\n"
        );
    }

    #[test]
    fn packed_lines_stay_under_the_width_and_count_the_rest() {
        let lines: Vec<String> = (1..=40)
            .map(|n| format!("call_port_{n}(argument_number_{n}, other_value)"))
            .collect();
        let numbers: Vec<usize> = (1..=40).collect();
        let packed = packed_matches(&lines, &numbers, &["port".to_string()]);
        assert!(packed.chars().count() <= PACKED_LINE_CHARS + 20, "{packed}");
        assert!(packed.contains(" +"), "{packed}");
        assert!(packed.ends_with("-40"), "{packed}");
        assert_eq!(line_runs(&[3, 4, 5, 9]), ":3-5 :9");
    }

    #[test]
    fn truncated_results_report_how_many_were_hidden() {
        let mut o = outcome();
        o.total = 42;
        let mut buf = Vec::new();
        report_results(&mut buf, &o, &[], &HashMap::new()).unwrap();
        let text = String::from_utf8(buf).unwrap();
        assert!(text.contains("42 more; add"), "{text}");

        o.total = 0;
        let mut buf = Vec::new();
        report_results(&mut buf, &o, &[], &HashMap::new()).unwrap();
        assert!(!String::from_utf8(buf).unwrap().contains(" more"));
    }

    #[test]
    fn exact_query_note_distinguishes_or_alternatives() {
        let mut result = outcome();
        result.alternatives = vec![
            "present".into(),
            "missing".into(),
            "natural language".into(),
        ];
        result.exact_alternatives = vec!["present".to_string()];
        let mut buf = Vec::new();
        report_exact_query_note(&mut buf, &result).unwrap();
        assert_eq!(String::from_utf8(buf).unwrap(), "exact: present\n");
    }
}
