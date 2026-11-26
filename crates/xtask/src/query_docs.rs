use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, anyhow, bail};
use ontology::Ontology;

const DEFAULT_DOCS: &[&str] = &["docs/docs-gitlab-com/queries/query-language.md"];

const BEGIN_MARKER: &str = "<!-- BEGIN GENERATED: text-indexed-properties -->";
const END_MARKER: &str = "<!-- END GENERATED: text-indexed-properties -->";

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

fn render_table(ontology: &Ontology) -> Result<String> {
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
