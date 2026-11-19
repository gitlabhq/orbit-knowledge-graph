use orbit_utils::markdown::{self, AgentFile, FRONT_MATTER_FIELDS, FRONT_MATTER_NAME, LineIndex};
use rustc_hash::{FxHashMap, FxHashSet};

use crate::canonical::Canonical;
use crate::edge::{Edge, EdgeKind};
use crate::intern::Lang;
use crate::tree::{Compact, Node, Tree};

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
        .filter_map(|link| link.target(label).map(|target| (link.section, target)))
        .collect();
    let agent_file = AgentFile::classify(label);
    let front_matter_values = document.front_matter.as_ref().map_or(0, |block| {
        block
            .values
            .iter()
            .filter(|value| !value.is_empty())
            .count()
    });
    let front_matter_nodes = document
        .front_matter
        .as_ref()
        .map_or(0, |_| 3 + front_matter_values);
    let agent_file_nodes = agent_file.map_or(0, |_| 2 + front_matter_values);
    let mut tree = Tree::with_capacity(
        1 + agent_file_nodes + front_matter_nodes + 4 * sections.len() + 2 * targets.len(),
        node(
            lang.intern_kind("document"),
            lang.syms.intern(label),
            0,
            source.len(),
        ),
    );
    tree.label = label.to_string();
    tree.source = std::sync::Arc::from(source);

    let append_front_matter_values = |tree: &mut Tree, parent, start, end| {
        let Some(block) = &document.front_matter else {
            return;
        };
        for (field, value) in FRONT_MATTER_FIELDS.iter().zip(&block.values) {
            if !value.is_empty() {
                let kind = lang.intern_kind(&format!("__{}", field.column));
                tree.append(parent, node(kind, lang.syms.intern(value), start, end));
            }
        }
    };

    if let Some(agent_file) = agent_file {
        let (start, end) = (0, source.len());
        let kind = match agent_file {
            AgentFile::Instructions => "agent_instructions",
            AgentFile::Skill => "skill",
        };
        let root = tree.root;
        let id = tree.append(root, node(lang.intern_kind(kind), 0, start, end));
        let name = agent_file.name(label, document.front_matter.as_ref());
        let name = lang.syms.intern(name);
        tree.append(id, node(lang.intern_kind("name"), name, start, end));
        append_front_matter_values(&mut tree, id, start, end);
    }

    if let Some(block) = &document.front_matter {
        let (start, end) = (block.start, block.end);
        let root = tree.root;
        let id = tree.append(root, node(lang.intern_kind("front_matter"), 0, start, end));
        let name = lang.syms.intern(FRONT_MATTER_NAME);
        tree.append(id, node(lang.intern_kind("name"), name, start, end));
        tree.append(id, node(lang.intern_kind("segment"), name, start, end));
        append_front_matter_values(&mut tree, id, start, end);
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
    for (section, target) in targets {
        let (parent, end) = section.map_or((tree.root, source.len()), |index| {
            (ids[index], sections[index].end)
        });
        let link = tree.append(
            parent,
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

pub fn link_edges(trees: &[Tree<Compact>], lang: &Lang) -> Vec<Edge> {
    let links: Vec<_> = trees
        .iter()
        .enumerate()
        .flat_map(|(from_fi, tree)| {
            tree.root()
                .descendants()
                .filter(|node| node.is(Canonical::Link))
                .map(move |link| (from_fi, link))
        })
        .collect();
    if links.is_empty() {
        return Vec::new();
    }
    let tree_by_label: FxHashMap<&str, usize> = trees
        .iter()
        .enumerate()
        .map(|(fi, tree)| (tree.label.as_str(), fi))
        .collect();
    let mut sections_by_anchor: FxHashMap<usize, FxHashMap<u32, u32>> = FxHashMap::default();
    let mut seen = FxHashSet::default();
    let mut edges = Vec::new();
    for (from_fi, link) in links {
        let Some(&to_fi) = tree_by_label.get(lang.syms.resolve(link.sym())) else {
            continue;
        };
        let section = link
            .child_sym_of_kind(Canonical::Anchor.into())
            .and_then(|anchor| {
                sections_by_anchor
                    .entry(to_fi)
                    .or_insert_with(|| section_anchors(&trees[to_fi]))
                    .get(&anchor)
                    .copied()
            });
        let to_node = section.unwrap_or_else(|| trees[to_fi].root().index());
        let from_nodes = std::iter::once(trees[from_fi].root().index()).chain(
            link.parent()
                .filter(|parent| parent.is(Canonical::Def))
                .map(|parent| parent.index()),
        );
        for from_node in from_nodes {
            let edge = Edge::new(
                from_fi as u32,
                from_node,
                to_fi as u32,
                to_node,
                EdgeKind::Links,
            );
            if seen.insert(edge) {
                edges.push(edge);
            }
        }
    }
    edges
}

fn section_anchors(tree: &Tree<Compact>) -> FxHashMap<u32, u32> {
    let mut by_anchor = FxHashMap::default();
    for anchor in tree
        .root()
        .descendants()
        .filter(|node| node.is(Canonical::Anchor))
    {
        if let Some(section) = anchor.parent().filter(|parent| parent.is(Canonical::Def)) {
            by_anchor.entry(anchor.sym()).or_insert(section.index());
        }
    }
    by_anchor
}
