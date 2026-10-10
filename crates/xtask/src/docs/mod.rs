mod tables;

use std::collections::BTreeMap;
use std::fs;

use anyhow::{Context, Result, anyhow, bail};
use ontology::Ontology;

const QUERY_LANGUAGE_DOC: &str = "docs/source/queries/query-language.md";
const SKILL_QUERY_LANGUAGE_DOC: &str = "skills/orbit/references/query_language.md";
const SCHEMA_DOC: &str = "docs/source/schema.md";

struct Region {
    name: &'static str,
    docs: &'static [&'static str],
    render: fn(&Ontology) -> Result<String>,
}

const REGIONS: &[Region] = &[
    Region {
        name: "text-indexed-properties",
        docs: &[QUERY_LANGUAGE_DOC, SKILL_QUERY_LANGUAGE_DOC],
        render: tables::text_indexed_properties,
    },
    Region {
        name: "schema-nodes",
        docs: &[SCHEMA_DOC],
        render: tables::schema_nodes,
    },
    Region {
        name: "schema-edges",
        docs: &[SCHEMA_DOC],
        render: tables::schema_edges,
    },
];

pub fn run(check: bool) -> Result<()> {
    let ontology = Ontology::load_embedded().context("failed to load embedded ontology")?;
    let stale = regenerate_regions(&ontology, check)?;

    if !stale.is_empty() {
        eprintln!(
            "generated docs regions are stale in: {}. Run `mise run docs:generate` and commit.",
            stale.join(", ")
        );
        bail!("generated docs regions stale");
    }
    if check {
        println!("generated docs regions are up to date");
    }
    Ok(())
}

fn regenerate_regions(ontology: &Ontology, check: bool) -> Result<Vec<&'static str>> {
    let mut regions_by_doc: BTreeMap<&str, Vec<(&str, String)>> = BTreeMap::new();
    for region in REGIONS {
        let body = (region.render)(ontology)
            .with_context(|| format!("rendering region `{}`", region.name))?;
        for doc in region.docs {
            regions_by_doc
                .entry(doc)
                .or_default()
                .push((region.name, body.clone()));
        }
    }

    let mut stale = Vec::new();
    for (path, regions) in regions_by_doc {
        let current = fs::read_to_string(path).with_context(|| format!("reading {path}"))?;
        let mut updated = current.clone();
        for (name, body) in &regions {
            updated = replace_marked_region(&updated, name, body)
                .with_context(|| format!("updating region `{name}` in {path}"))?;
        }
        if current == updated {
            continue;
        }
        if check {
            stale.push(path);
        } else {
            fs::write(path, &updated).with_context(|| format!("writing {path}"))?;
            println!("updated generated regions in {path}");
        }
    }
    Ok(stale)
}

fn replace_marked_region(doc: &str, name: &str, body: &str) -> Result<String> {
    let begin_marker = format!("<!-- BEGIN GENERATED: {name} -->");
    let end_marker = format!("<!-- END GENERATED: {name} -->");
    // A second marker pair would never be rewritten and would go stale silently.
    let begin_count = doc.matches(&begin_marker).count();
    let end_count = doc.matches(&end_marker).count();
    if begin_count > 1 || end_count > 1 {
        bail!(
            "expected exactly one `{begin_marker}` / `{end_marker}` pair, found {begin_count} begin and {end_count} end markers"
        );
    }

    let begin = doc
        .find(&begin_marker)
        .ok_or_else(|| anyhow!("missing `{begin_marker}` marker"))?;
    let after_begin = begin + begin_marker.len();
    let end = doc[after_begin..]
        .find(&end_marker)
        .map(|offset| after_begin + offset)
        .ok_or_else(|| anyhow!("missing `{end_marker}` marker after `{begin_marker}`"))?;

    let mut out = String::with_capacity(doc.len() + body.len());
    out.push_str(&doc[..after_begin]);
    out.push_str("\n\n");
    out.push_str(body);
    out.push('\n');
    out.push_str(&doc[end..]);
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    const BEGIN: &str = "<!-- BEGIN GENERATED: t -->";
    const END: &str = "<!-- END GENERATED: t -->";

    #[test]
    fn replace_marked_region_swaps_body_only() {
        let doc = format!("intro\n\n{BEGIN}\n\nold table\n{END}\n\noutro\n");
        let out = replace_marked_region(&doc, "t", "new table\n").unwrap();
        assert_eq!(
            out,
            format!("intro\n\n{BEGIN}\n\nnew table\n\n{END}\n\noutro\n")
        );
    }

    #[test]
    fn replace_marked_region_is_idempotent() {
        let doc = format!("a\n\n{BEGIN}\n\nbody\n{END}\n\nz\n");
        let once = replace_marked_region(&doc, "t", "body\n").unwrap();
        let twice = replace_marked_region(&once, "t", "body\n").unwrap();
        assert_eq!(once, twice);
    }

    #[test]
    fn replace_marked_region_fills_empty_pair() {
        let doc = format!("{BEGIN}\n{END}\n");
        let out = replace_marked_region(&doc, "t", "body\n").unwrap();
        assert_eq!(out, format!("{BEGIN}\n\nbody\n\n{END}\n"));
    }

    #[test]
    fn replace_marked_region_only_touches_named_region() {
        let doc = format!(
            "{BEGIN}\nkeep-t\n{END}\n<!-- BEGIN GENERATED: u -->\nold\n<!-- END GENERATED: u -->\n"
        );
        let out = replace_marked_region(&doc, "u", "new\n").unwrap();
        assert!(out.contains("keep-t"));
        assert!(out.contains("new"));
        assert!(!out.contains("old"));
    }

    #[test]
    fn replace_marked_region_requires_markers() {
        assert!(replace_marked_region("no markers here", "t", "x").is_err());
        assert!(replace_marked_region(&format!("{BEGIN}\nbody\n"), "t", "x").is_err());
    }

    #[test]
    fn replace_marked_region_rejects_duplicate_markers() {
        let two_begin = format!("{BEGIN}\nbody\n{END}\n\n{BEGIN}\nstale\n{END}\n");
        assert!(replace_marked_region(&two_begin, "t", "x").is_err());
        let two_end = format!("{BEGIN}\nbody\n{END}\n{END}\n");
        assert!(replace_marked_region(&two_end, "t", "x").is_err());
    }

    #[test]
    fn replace_marked_region_rejects_end_before_begin() {
        let reversed = format!("{END}\nbody\n{BEGIN}\n");
        assert!(replace_marked_region(&reversed, "t", "x").is_err());
    }
}
