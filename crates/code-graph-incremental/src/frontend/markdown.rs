use orbit_utils::markdown::{self, FRONT_MATTER_FIELDS, FRONT_MATTER_NAME, LineIndex};

use crate::canonical::Canonical;
use crate::intern::Lang;
use crate::tree::{Node, Tree};

pub fn parse(source: &str, lang: &Lang, label: &str) -> Tree {
    let lines = LineIndex::new(source);
    let node = |kind: u16, sym: u32, start: usize, end: usize| {
        let (start_row, start_col) = lines.position(start);
        let (end_row, end_col) = lines.end_position(source, end);
        Node {
            kind,
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
    let document = markdown::parse(source);
    let sections = document.sections;
    let targets: Vec<_> = document
        .links
        .iter()
        .filter_map(|link| link.target(label))
        .collect();
    let front_matter_nodes = document.front_matter.as_ref().map_or(0, |block| {
        3 + block
            .values
            .iter()
            .filter(|value| !value.is_empty())
            .count()
    });
    let mut tree = Tree::with_capacity(
        1 + front_matter_nodes + 4 * sections.len() + 2 * targets.len(),
        node(
            lang.intern_kind("document"),
            lang.syms.intern(label),
            0,
            source.len(),
        ),
    );
    tree.label = label.to_string();
    tree.source = std::sync::Arc::from(source);

    if let Some(block) = document.front_matter {
        let (start, end) = (block.start, block.end);
        let root = tree.root;
        let id = tree.append(root, node(lang.intern_kind("front_matter"), 0, start, end));
        let name = lang.syms.intern(FRONT_MATTER_NAME);
        tree.append(id, node(lang.intern_kind("name"), name, start, end));
        tree.append(id, node(lang.intern_kind("segment"), name, start, end));
        for (field, value) in FRONT_MATTER_FIELDS.iter().zip(&block.values) {
            if !value.is_empty() {
                let kind = lang.intern_kind(&format!("__{}", field.column));
                tree.append(id, node(kind, lang.syms.intern(value), start, end));
            }
        }
    }

    let mut ids = Vec::with_capacity(sections.len());
    for section in &sections {
        let parent = section.parent.map_or(tree.root, |index| ids[index]);
        let id = tree.append(
            parent,
            node(lang.intern_kind("section"), 0, section.start, section.end),
        );
        let (start, end) = (section.start, section.heading_end);
        let name = lang.syms.intern(&section.name);
        let segment = lang.syms.intern(&section.segment);
        let anchor = lang.syms.intern(&section.anchor);
        tree.append(id, node(lang.intern_kind("heading"), name, start, end));
        tree.append(
            id,
            node(lang.intern_kind("section_key"), segment, start, end),
        );
        tree.append(id, node(Canonical::Anchor.into(), anchor, start, end));
        ids.push(id);
    }
    let end = source.len();
    for target in targets {
        let link = tree.append(
            tree.root,
            node(
                Canonical::Link.into(),
                lang.syms.intern(&target.path),
                end,
                end,
            ),
        );
        if let Some(anchor) = target.anchor {
            tree.append(
                link,
                node(
                    Canonical::Anchor.into(),
                    lang.syms.intern(&anchor),
                    end,
                    end,
                ),
            );
        }
    }
    tree
}
