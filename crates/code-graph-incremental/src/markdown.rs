use orbit_utils::markdown::{Document, FRONT_MATTER_NAME, LineIndex};

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
    let Document {
        front_matter,
        sections,
    } = orbit_utils::markdown::parse(source);
    let mut tree = Tree::with_capacity(
        1 + 3 * (usize::from(front_matter.is_some()) + sections.len()),
        node("document", lang.syms.intern(label), 0, source.len()),
    );
    tree.label = label.to_string();
    tree.source = std::sync::Arc::from(source);

    if let Some(block) = front_matter {
        let (start, end) = (block.start, block.end);
        let root = tree.root;
        let id = tree.append(root, node("front_matter", 0, start, end));
        let name = lang.syms.intern(FRONT_MATTER_NAME);
        tree.append(id, node("name", name, start, end));
        tree.append(id, node("segment", name, start, end));
    }

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
