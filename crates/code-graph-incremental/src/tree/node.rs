use indextree::NodeId;

use super::{Mutable, Storage, Tree};
use crate::canonical;
use crate::intern::Lang;

#[derive(Clone, Copy, Debug, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
pub struct Tag {
    pub key: u32,
    pub val: u32,
}

#[derive(Clone, Copy, Debug, Default)]
pub struct Node {
    pub(crate) kind: u16,
    pub(crate) field: u16,
    pub(crate) sym: u32,
    pub(crate) start: u32,
    pub(crate) end: u32,
    pub(crate) start_row: u32,
    pub(crate) start_col: u32,
    pub(crate) end_row: u32,
    pub(crate) end_col: u32,
    pub(crate) synth: bool,
    pub(crate) named: bool,
}

impl<S: Storage<Node = Node>> Tree<S> {
    pub(crate) fn text_at<'a>(&'a self, id: u32, lang: &'a Lang) -> &'a str {
        let node = self.storage.node(id);
        if node.sym != 0 {
            return lang.syms.resolve(node.sym);
        }
        if node.synth || !node.named {
            return "";
        }
        self.source
            .get(node.start as usize..node.end as usize)
            .unwrap_or("")
    }

    pub(crate) fn sym_at(&self, id: u32, lang: &Lang) -> u32 {
        let node = self.storage.node(id);
        if node.sym != 0 {
            return node.sym;
        }
        match self.text_at(id, lang) {
            "" => 0,
            text => lang.syms.intern(text),
        }
    }
}

impl Tree {
    pub fn unparsed(lang: &Lang, path: &str, size: u64, reason: &str) -> Self {
        let mut tree = Self::new(Node {
            kind: canonical::Canonical::SourceFile.into(),
            named: true,
            sym: lang.syms.intern(path),
            end: size as u32,
            ..Default::default()
        });
        tree.label = path.to_string();
        if !reason.is_empty() {
            tree.set_tag(0, lang.syms.intern("reason"), lang.syms.intern(reason));
        }
        tree
    }

    pub fn text<'a>(&'a self, id: NodeId, lang: &'a Lang) -> &'a str {
        self.text_at(Self::to_raw(id), lang)
    }
    pub fn sym_of(&self, id: NodeId, lang: &Lang) -> u32 {
        self.sym_at(Self::to_raw(id), lang)
    }
    pub(crate) fn to_id(&self, raw: u32) -> NodeId {
        self.storage.id(raw)
    }
    pub(crate) fn to_raw(id: NodeId) -> u32 {
        Mutable::<Node>::index(id)
    }
    pub(crate) fn node(&self, id: NodeId) -> &Node {
        self.storage.0[id].get()
    }
    pub(crate) fn node_mut(&mut self, id: NodeId) -> &mut Node {
        self.storage.0[id].get_mut()
    }
    pub fn prune(&mut self) {
        for id in self.preorder().into_iter().skip(1) {
            if !canonical::is_canonical(self.node(id).kind) {
                let children: Vec<_> = id.children(&self.storage.0).collect();
                for child in children {
                    child.detach(&mut self.storage.0);
                    id.insert_before(child, &mut self.storage.0);
                }
                id.remove(&mut self.storage.0);
            } else {
                self.node_mut(id).field = 0;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn node(kind: u16, sym: u32) -> Node {
        Node {
            kind,
            sym,
            named: true,
            ..Default::default()
        }
    }

    #[test]
    fn compact_keeps_structure_and_tags_across_removed_nodes() {
        let mut tree = Tree::new(node(1, 10));
        let a = tree.append(tree.root, node(2, 20));
        let dropped = tree.append(tree.root, node(3, 30));
        let b = tree.append(tree.root, node(4, 40));
        let under_dropped = tree.append(dropped, node(5, 50));
        tree.set_tag(Tree::to_raw(a), 7, 70);
        tree.set_tag(Tree::to_raw(b), 8, 80);
        tree.set_tag(Tree::to_raw(under_dropped), 9, 90);
        under_dropped.remove_subtree(&mut tree.storage.0);
        dropped.remove(&mut tree.storage.0);
        tree.compact();
        let kids: Vec<_> = tree
            .root()
            .children()
            .map(|c| (c.kind(), c.sym(), c.index()))
            .collect();
        assert_eq!(kids, [(2, 20, 1), (4, 40, 2)]);
        assert_eq!(tree.storage.0.len(), 3);
        assert_eq!(tree.get_tag(1, 7), Some(70));
        assert_eq!(tree.get_tag(2, 8), Some(80));
        assert_eq!(tree.tags.len(), 2, "the removed node's tag is gone");
    }
}
