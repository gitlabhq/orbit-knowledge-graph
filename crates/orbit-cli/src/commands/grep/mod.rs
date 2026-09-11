mod local;

use std::fmt::Write;
use std::path::{Path, PathBuf};

use anyhow::Result;
use duckdb_client::search::DuckDbSearch;
use orbit_search::{RecallFilter, SearchVocab, content_words};

use crate::commands::{context, fqn::Def, shell_quote};

fn build_vocab<S: orbit_search::grep::GrepSource>(source: &S) -> Result<SearchVocab, S::Error> {
    use strum::IntoEnumIterator;
    let parts: Vec<(String, String)> = code_graph::v2::types::EdgeKind::iter()
        .flat_map(|kind| {
            let name = kind.as_ref().to_string();
            SearchVocab::kind_name_parts(kind.as_ref())
                .map(|part| (part.to_string(), name.clone()))
                .collect::<Vec<_>>()
        })
        .collect();
    let words: Vec<String> = parts.iter().map(|(part, _)| part.clone()).collect();
    let stems = source.stem(&words)?;
    Ok(SearchVocab::new(
        stems
            .into_iter()
            .zip(parts.into_iter().map(|(_, kind)| kind)),
    ))
}

const RESULT_LIMIT: usize = 10;
const BODY_LIMIT: usize = 5;
const OUTPUT_CHARS: usize = 24_000;
const OUTPUT_OMITTED: &str =
    "\nOutput budget reached; narrow with --path/--kind or use context on a listed FQN.\n";

struct Output {
    text: String,
    remaining: usize,
    omitted: bool,
}

impl Output {
    fn new() -> Self {
        Self {
            text: String::new(),
            remaining: OUTPUT_CHARS - OUTPUT_OMITTED.chars().count(),
            omitted: false,
        }
    }

    fn push(&mut self, text: &str) -> bool {
        let chars = text.chars().count();
        if chars > self.remaining {
            self.omitted = true;
            return false;
        }
        self.text.push_str(text);
        self.remaining -= chars;
        true
    }

    fn finish(mut self) -> String {
        if self.omitted {
            self.text.push_str(OUTPUT_OMITTED);
        }
        self.text
    }
}

pub(crate) fn run(
    queries: Vec<String>,
    repo: Option<PathBuf>,
    db: Option<PathBuf>,
    paths: Vec<String>,
    filter: RecallFilter,
) -> Result<()> {
    let launcher = crate::commands::setup::spec::launcher();
    if let Some(query) = queries.iter().find(|q| content_words(q).is_empty()) {
        anyhow::bail!(
            "no usable search terms in query: {query:?} — to list every definition in a \
             file or directory instead, run `{launcher} grep --path <path>`; to print a whole \
             file, `{launcher} context --file <path>`"
        );
    }

    let context_command = context_command(launcher, repo.as_deref(), db.as_deref());
    let (git, search) = local::open(repo, db, &paths)?;

    let mut out = Output::new();
    if queries.is_empty() {
        report_outline(
            &mut out,
            &search,
            git.short_sha(),
            &paths,
            &filter,
            launcher,
        )?;
        print!("{}", out.finish());
        return Ok(());
    }
    if !paths.is_empty() {
        out.push(&format!("path: {}\n", paths.join(" ")));
    }
    if !filter.kinds.is_empty() {
        out.push(&format!("kind: {}\n", filter.kinds.join(" ")));
    }

    let vocab = build_vocab(&search)?;
    let mut defs = Vec::new();
    for (i, query) in queries.iter().enumerate() {
        let mut header = format!("grep {:?} @ {}\n", query, git.short_sha());
        if i > 0 {
            header.insert(0, '\n');
        }
        if !out.push(&header) {
            break;
        }
        let mut outcome = search.grep(query, RESULT_LIMIT, &vocab, &filter)?;
        outcome
            .matches
            .sort_by_key(|candidate| !exact_match(&candidate.row, query));
        let typed: Vec<String> = query.split_whitespace().map(str::to_lowercase).collect();
        if outcome.terms != typed {
            out.push(&format!("terms: {}\n", outcome.terms.join(" ")));
        }

        if outcome.matches.is_empty() {
            if paths.is_empty() && filter.is_empty() {
                out.push("\nNo definitions match those terms.\n");
            } else {
                out.push("\nNo definitions match those terms within that scope; drop --path/--kind to widen.\n");
            }
            out.push(
                "Rephrase and retry once — use synonyms or identifier fragments \
                 from the code (e.g. \"throttle\" → \"rate limit\"). If the retry \
                 also misses, fall back to text grep.\n",
            );
            continue;
        }

        for candidate in &outcome.matches {
            let def = def_from(&candidate.row);
            if defs.len() < BODY_LIMIT && !defs.contains(&def) {
                defs.push(def);
            }
        }
        let mut report = String::new();
        report_results(&mut report, &outcome, Some(&context_command))?;
        if !out.push(&report) {
            break;
        }
    }
    if !defs.is_empty() {
        let bodies = context::render_bodies(
            search.client(),
            &git,
            &defs,
            out.remaining,
            &context_command,
        )?;
        if bodies.is_empty() {
            out.omitted = true;
        }
        out.push(&bodies);
    }
    print!("{}", out.finish());
    Ok(())
}

fn exact_match(row: &orbit_search::CorpusRow, query: &str) -> bool {
    row.fqn.eq_ignore_ascii_case(query.trim())
        || row
            .fqn
            .rsplit([':', '.', '#', '/'])
            .next()
            .is_some_and(|name| name.eq_ignore_ascii_case(query.trim()))
}

fn def_from(row: &orbit_search::CorpusRow) -> Def {
    let (file, start) = row.loc.rsplit_once(':').unwrap_or((&row.loc, "1"));
    Def {
        id: row.id,
        fqn: row.fqn.clone(),
        kind: row.kind.clone(),
        file: file.to_string(),
        start: start.parse().unwrap_or(1),
        end: usize::try_from(row.end_line).unwrap_or(0),
    }
}

fn report_outline(
    out: &mut Output,
    search: &DuckDbSearch,
    header: &str,
    paths: &[String],
    filter: &RecallFilter,
    launcher: &str,
) -> Result<()> {
    out.push(&format!("outline {} @ {header}\n", paths.join(" ")));
    if !filter.kinds.is_empty() {
        out.push(&format!("kind: {}\n", filter.kinds.join(" ")));
    }
    let rows = search.list_corpus(filter)?;
    if rows.is_empty() {
        out.push(&format!("\nNo indexed definitions under that path. Paths are repo-relative, as printed by `{launcher} grep`.\n"));
        return Ok(());
    }
    out.push(&format!("\nDefinitions ({}):\n", rows.len()));
    for r in &rows {
        if !out.push(&format!("  {}  [{}]  {}\n", r.fqn, r.kind, r.loc)) {
            break;
        }
    }
    Ok(())
}

fn context_command(launcher: &str, repo: Option<&Path>, db: Option<&Path>) -> String {
    let mut command = format!("{launcher} context");
    for (flag, path) in [("--repo", repo), ("--db", db)] {
        if let Some(path) = path {
            command.push_str(&format!(" {flag}={}", shell_quote(&path.to_string_lossy())));
        }
    }
    command
}

fn report_results(
    out: &mut impl Write,
    outcome: &orbit_search::GrepOutcome,
    context_command: Option<&str>,
) -> std::fmt::Result {
    if let Some(command) = context_command.filter(|_| !outcome.matches.is_empty()) {
        write!(out, "Candidate context: {command} --")?;
        for candidate in outcome.matches.iter().take(BODY_LIMIT) {
            write!(out, " {}", shell_quote(&candidate.row.fqn))?;
        }
        writeln!(out)?;
    }
    report_confidence(out, outcome)?;
    writeln!(out, "\nNodes:")?;
    for m in &outcome.matches {
        writeln!(out, "  {}  [{}]  {}", m.row.fqn, m.row.kind, m.row.loc)?;
    }
    Ok(())
}

const COMPOUND_TERM_HINT: usize = 5;

fn report_confidence(
    out: &mut impl Write,
    outcome: &orbit_search::GrepOutcome,
) -> std::fmt::Result {
    if outcome.terms.len() >= COMPOUND_TERM_HINT {
        writeln!(
            out,
            "note: {} search terms — matches may cover different parts of the query.",
            outcome.terms.len()
        )?;
    }
    if outcome.weak {
        writeln!(
            out,
            "note: weak matches — symbol names do not closely match enough of the query."
        )?;
    }
    if !outcome.unmatched_terms.is_empty() {
        writeln!(
            out,
            "note: no matches for: {} — results reflect only the matched terms. \
             If needed, retry once with a synonym or identifier fragment.",
            outcome.unmatched_terms.join(", ")
        )?;
    }
    if !outcome.unmatched_terms.is_empty() && !outcome.term_anchors.is_empty() {
        let anchors: Vec<String> = outcome
            .term_anchors
            .iter()
            .map(|(term, fqn)| format!("{term} → {fqn}"))
            .collect();
        writeln!(out, "note: matched-term candidates: {}", anchors.join(", "))?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn outcome(unmatched: Vec<&str>, weak: bool) -> orbit_search::GrepOutcome {
        orbit_search::GrepOutcome {
            terms: Vec::new(),
            matches: Vec::new(),
            weak,
            unmatched_terms: unmatched.into_iter().map(String::from).collect(),
            term_anchors: Vec::new(),
        }
    }

    #[test]
    fn partial_anchor_note_lists_unmatched_terms_with_a_retry_instruction() {
        let mut buf = String::new();
        report_confidence(&mut buf, &outcome(vec!["throttle", "dlq"], false)).unwrap();
        let text = buf;
        assert!(text.contains("no matches for: throttle, dlq"), "{text}");
        assert!(
            text.contains("retry once with a synonym or identifier"),
            "{text}"
        );
        assert!(!text.contains("weak matches"), "{text}");
    }

    #[test]
    fn weak_and_unmatched_notes_stack() {
        let mut buf = String::new();
        report_confidence(&mut buf, &outcome(vec!["throttle"], true)).unwrap();
        let text = buf;
        assert!(text.contains("weak matches"), "{text}");
        assert!(text.contains("symbol names do not closely match"), "{text}");
        assert!(!text.contains("no term anchors"), "{text}");
        assert!(text.contains("no matches for: throttle"), "{text}");
    }

    #[test]
    fn candidate_context_is_bounded_ordered_and_before_advice() {
        let mut o = outcome(vec!["unknown"], true);
        o.matches = [
            ("crate::Type::field", "Field"),
            ("crate::module", "Module"),
            ("crate::it's_a_function", "Function"),
            ("crate::other", "Function"),
        ]
        .into_iter()
        .map(|(fqn, kind)| orbit_search::grep::GrepMatch {
            row: orbit_search::CorpusRow {
                id: 1,
                fqn: fqn.into(),
                kind: kind.into(),
                loc: "src/lib.rs:1".into(),
                end_line: 1,
                degree: 0,
                grams: 0,
            },
            score: 0.0,
        })
        .collect();
        let mut buf = String::new();
        report_results(&mut buf, &o, Some("orbit context")).unwrap();
        let text = buf;
        assert_eq!(
            text.lines().next().unwrap(),
            "Candidate context: orbit context -- 'crate::Type::field' 'crate::module' 'crate::it'\\''s_a_function' 'crate::other'"
        );
        let nodes: Vec<_> = text.lines().filter(|line| line.starts_with("  ")).collect();
        assert_eq!(nodes.len(), o.matches.len());
        for (line, candidate) in nodes.iter().zip(&o.matches) {
            assert!(
                line.starts_with(&format!("  {}  [", candidate.row.fqn)),
                "{line}"
            );
        }

        let mut buf = String::new();
        report_results(&mut buf, &o, None).unwrap();
        assert!(!buf.contains("Candidate context:"));
        o.matches.clear();
        let mut buf = String::new();
        report_results(&mut buf, &o, Some("orbit context")).unwrap();
        assert!(!buf.contains("Candidate context:"));
    }

    #[test]
    fn context_command_keeps_only_explicit_scope_and_quotes_it() {
        assert_eq!(context_command("orbit", None, None), "orbit context");
        assert_eq!(
            context_command(
                "glab orbit local",
                Some(Path::new("my repo's checkout")),
                Some(Path::new("-graph $db.duckdb")),
            ),
            "glab orbit local context --repo='my repo'\\''s checkout' --db='-graph $db.duckdb'"
        );
    }

    #[test]
    fn confident_full_anchor_prints_no_notes() {
        let mut buf = String::new();
        report_confidence(&mut buf, &outcome(Vec::new(), false)).unwrap();
        assert!(buf.is_empty());
    }
}
