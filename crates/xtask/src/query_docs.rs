//! Regenerates the auto-derivable tables in the query language reference docs
//! from the ontology.
//!
//! Currently this owns the **text-indexed properties** table: the set of
//! `(entity, property)` pairs that accept the `token_match`, `all_tokens`, and
//! `any_tokens` operators. That set is the columns carrying a `text(...)`
//! storage index in the ontology YAML — the same signal the compiler's
//! token-operator validation enforces — so the table is fully derivable and
//! must never be hand-edited.
//!
//! The same table appears in two committed mirrors, both updated here:
//! - `docs/source/remote/queries/query-language.md` (published reference)
//! - `skills/orbit/references/query_language.md` (agent skill copy, kept in
//!   sync by the `orbit-skill-docs-sync` hook)
//!
//! The generated table lives between HTML marker comments in each doc:
//!
//! ```text
//! <!-- BEGIN GENERATED: text-indexed-properties -->
//! ...generated table...
//! <!-- END GENERATED: text-indexed-properties -->
//! ```
//!
//! Everything outside the markers stays hand-authored. With `--check`, each
//! committed file is compared against a fresh render and the command fails on
//! drift, mirroring the `metrics-catalog` and `dashboards` CI gates.

use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, anyhow, bail};
use ontology::Ontology;

/// Docs that embed the generated table, relative to the workspace root.
const DEFAULT_DOCS: &[&str] = &[
    "docs/source/remote/queries/query-language.md",
    "skills/orbit/references/query_language.md",
];

const BEGIN_MARKER: &str = "<!-- BEGIN GENERATED: text-indexed-properties -->";
const END_MARKER: &str = "<!-- END GENERATED: text-indexed-properties -->";

/// Sanity floor so an accidental ontology-loading regression (or an empty
/// embedded ontology) fails the check rather than silently wiping the table.
const MIN_ENTITIES: usize = 10;

pub fn run(doc: Option<PathBuf>, check: bool) -> Result<()> {
    let ontology = Ontology::load_embedded().context("failed to load embedded ontology")?;
    let table = render_table(&ontology)?;

    let paths: Vec<PathBuf> = match doc {
        Some(p) => vec![p],
        None => DEFAULT_DOCS.iter().map(PathBuf::from).collect(),
    };

    let mut stale = Vec::new();
    for path in &paths {
        if process_doc(path, &table, check)? {
            stale.push(path.display().to_string());
        }
    }

    if check {
        if stale.is_empty() {
            println!("text-indexed properties table is up to date in all docs");
            return Ok(());
        }
        eprintln!(
            "text-indexed properties table is stale in: {}. Run `mise run docs:query-language` and commit.",
            stale.join(", ")
        );
        return Err(anyhow!("query-language text-indexed table stale"));
    }
    Ok(())
}

/// Updates one doc in place (or, with `check`, reports drift without writing).
/// Returns `true` when `check` is set and the doc is stale.
fn process_doc(path: &Path, table: &str, check: bool) -> Result<bool> {
    let current =
        fs::read_to_string(path).with_context(|| format!("reading {}", path.display()))?;
    let updated = replace_marked_region(&current, table)?;

    if check {
        if current != updated {
            eprintln!("stale: {}", path.display());
            print_diff(&current, &updated);
            return Ok(true);
        }
        return Ok(false);
    }

    if current != updated {
        fs::write(path, &updated).with_context(|| format!("writing {}", path.display()))?;
        println!(
            "updated text-indexed properties table in {}",
            path.display()
        );
    }
    Ok(false)
}

/// Renders the markdown table body (without the surrounding marker comments).
fn render_table(ontology: &Ontology) -> Result<String> {
    // Node-only by design: the validator's token-op gate keys off
    // `text_index_tokenizer`, which is itself node-only, and no edge YAML
    // carries a `text(...)` index. If an edge ever gains one, extend both this
    // iteration and the validator lookup together so the docs can't omit it.
    let mut rows: Vec<(&str, Vec<&str>)> = ontology
        .nodes()
        .filter_map(|node| {
            let columns = ontology.text_indexed_columns(&node.name);
            if columns.is_empty() {
                None
            } else {
                Some((node.name.as_str(), columns))
            }
        })
        .collect();
    rows.sort_by(|a, b| a.0.cmp(b.0));

    if rows.len() < MIN_ENTITIES {
        bail!(
            "only {} entities carry a text index (minimum {}); did the ontology fail to load?",
            rows.len(),
            MIN_ENTITIES
        );
    }

    let mut out = String::new();
    out.push_str("| Entity | Text-indexed properties |\n");
    out.push_str("|--------|------------------------|\n");
    for (entity, columns) in rows {
        let props = columns
            .iter()
            .map(|c| format!("`{c}`"))
            .collect::<Vec<_>>()
            .join(", ");
        out.push_str(&format!("| `{entity}` | {props} |\n"));
    }
    Ok(out)
}

/// Replaces the content between the BEGIN/END markers with `table`, leaving the
/// markers (and a blank line of padding) in place. Fails if either marker is
/// missing or duplicated so a botched edit can't silently disable the gate. A
/// duplicate marker pair is the dangerous case: `find()` would only ever
/// rewrite the first region, so a stale second region would survive while
/// `--check` reports "up to date".
///
/// END is searched only after BEGIN's end position, so an END that precedes
/// BEGIN (or one embedded inside the region) can never be mistaken for the
/// closing marker — it falls through to the missing-marker error instead of
/// silently truncating the replacement.
fn replace_marked_region(doc: &str, table: &str) -> Result<String> {
    let begin_count = doc.matches(BEGIN_MARKER).count();
    let end_count = doc.matches(END_MARKER).count();
    if begin_count > 1 || end_count > 1 {
        bail!(
            "expected exactly one `{BEGIN_MARKER}` / `{END_MARKER}` pair, found {begin_count} begin and {end_count} end markers"
        );
    }

    let begin = doc
        .find(BEGIN_MARKER)
        .ok_or_else(|| anyhow!("missing `{BEGIN_MARKER}` marker in doc"))?;
    let after_begin = begin + BEGIN_MARKER.len();
    let end = doc[after_begin..]
        .find(END_MARKER)
        .map(|offset| after_begin + offset)
        .ok_or_else(|| anyhow!("missing `{END_MARKER}` marker after `{BEGIN_MARKER}` in doc"))?;

    let mut out = String::with_capacity(doc.len());
    out.push_str(&doc[..after_begin]);
    out.push('\n');
    out.push('\n');
    out.push_str(table);
    out.push('\n');
    out.push_str(&doc[end..]);
    Ok(out)
}

/// Prints a positional (line-index) comparison, not an LCS diff: when a row is
/// inserted or removed every following line reads as changed. That is adequate
/// here because the generated region is a sorted table — drift is almost always
/// an in-place edit or an added/removed adjacent row — and a real diff crate is
/// not worth the dependency for a CI hint. The authoritative output is the
/// "run `mise run docs:query-language`" instruction, not this preview.
fn print_diff(before: &str, after: &str) {
    let before_lines: Vec<&str> = before.lines().collect();
    let after_lines: Vec<&str> = after.lines().collect();
    let shown = before_lines.len().max(after_lines.len()).min(80);
    for i in 0..shown {
        let b = before_lines.get(i).copied().unwrap_or("");
        let a = after_lines.get(i).copied().unwrap_or("");
        if b != a {
            eprintln!("- {b}");
            eprintln!("+ {a}");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn render_table_is_deterministic_and_sorted() {
        let ontology = Ontology::load_embedded().unwrap();
        let first = render_table(&ontology).unwrap();
        let second = render_table(&ontology).unwrap();
        assert_eq!(first, second);

        let entities: Vec<&str> = first
            .lines()
            .filter_map(|l| l.strip_prefix("| `"))
            .filter_map(|l| l.split('`').next())
            .collect();
        assert!(entities.windows(2).all(|w| w[0] <= w[1]), "entities sorted");
        assert!(first.contains("| `MergeRequest` |"));
    }

    #[test]
    fn replace_marked_region_swaps_body_only() {
        let doc = format!("intro\n\n{BEGIN_MARKER}\n\nold table\n{END_MARKER}\n\noutro\n");
        let out = replace_marked_region(&doc, "new table\n").unwrap();
        assert!(out.contains("intro\n"));
        assert!(out.contains("outro\n"));
        assert!(out.contains("new table"));
        assert!(!out.contains("old table"));
        assert!(out.contains(BEGIN_MARKER));
        assert!(out.contains(END_MARKER));
    }

    #[test]
    fn replace_marked_region_is_idempotent() {
        let doc = format!("a\n\n{BEGIN_MARKER}\n\nbody\n{END_MARKER}\n\nz\n");
        let once = replace_marked_region(&doc, "body\n").unwrap();
        let twice = replace_marked_region(&once, "body\n").unwrap();
        assert_eq!(once, twice);
    }

    #[test]
    fn replace_marked_region_requires_markers() {
        assert!(replace_marked_region("no markers here", "x").is_err());
        let only_begin = format!("{BEGIN_MARKER}\nbody\n");
        assert!(replace_marked_region(&only_begin, "x").is_err());
    }

    #[test]
    fn replace_marked_region_rejects_duplicate_markers() {
        let two_begin =
            format!("{BEGIN_MARKER}\nbody\n{END_MARKER}\n\n{BEGIN_MARKER}\nstale\n{END_MARKER}\n");
        assert!(
            replace_marked_region(&two_begin, "x").is_err(),
            "a second marker pair must fail loudly, not be silently ignored"
        );

        let two_end = format!("{BEGIN_MARKER}\nbody\n{END_MARKER}\n{END_MARKER}\n");
        assert!(replace_marked_region(&two_end, "x").is_err());
    }

    #[test]
    fn replace_marked_region_rejects_end_before_begin() {
        let reversed = format!("{END_MARKER}\nbody\n{BEGIN_MARKER}\n");
        assert!(
            replace_marked_region(&reversed, "x").is_err(),
            "an END that precedes BEGIN must not be accepted as the closing marker"
        );
    }

    #[test]
    fn render_table_bails_below_min_entities() {
        let empty = Ontology::new();
        assert!(
            render_table(&empty).is_err(),
            "an ontology with no text-indexed entities must trip the MIN_ENTITIES floor"
        );
    }
}
