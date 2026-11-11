//! Markdown parses with comrak through `orbit_utils::markdown`, the extractor
//! code-graph uses too, so both engines agree on sections, names and FQN
//! segments. The result is a small tree for `langs/markdown.yaml`:
//! `document` holds nested `section`s, each led by a `heading` (the name) and
//! a `section_key` (the FQN segment).

use orbit_utils::markdown::{LineIndex, sections};

use crate::intern::Lang;
use crate::tree::{Node, Tree};

pub fn parse(source: &str, lang: &Lang, label: &str) -> Tree {
    let lines = LineIndex::new(source);
    let node = |kind: &str, sym: u32, start: usize, end: usize| {
        let (start_row, start_col) = lines.position(start);
        let (end_row, end_col) = lines.end_position(source, end);
        Node {
            kind: lang.intern_kind(kind),
            named: true,
            sym,
            start: start as u32,
            end: end as u32,
            start_row: start_row as u32,
            start_col: start_col as u32,
            end_row: end_row as u32,
            end_col: end_col as u32,
            ..Default::default()
        }
    };
    let sections = sections(source);
    let mut tree = Tree::with_capacity(
        1 + 3 * sections.len(),
        node("document", lang.syms.intern(label), 0, source.len()),
    );
    tree.label = label.to_string();
    tree.source = std::sync::Arc::from(source);

    let mut ids = Vec::with_capacity(sections.len());
    for section in &sections {
        let parent = section.parent.map_or(tree.root, |index| ids[index]);
        let id = tree.append(parent, node("section", 0, section.start, section.end));
        let (start, end) = (section.start, section.heading_end);
        let name = lang.syms.intern(&section.name);
        let segment = lang.syms.intern(&section.segment);
        tree.append(id, node("heading", name, start, end));
        tree.append(id, node("section_key", segment, start, end));
        ids.push(id);
    }
    tree
}
