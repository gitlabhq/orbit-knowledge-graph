use indextree::{Arena, NodeId};

use super::{NONE, Node, Tree};

pub struct MutableTree {
    arena: Arena<Node>,
    pub(crate) root: NodeId,
    pub label: String,
}

impl MutableTree {
    pub fn with_capacity(capacity: usize, root: Node) -> Self {
        let mut arena = Arena::with_capacity(capacity);
        let root = arena.new_node(root);
        Self {
            arena,
            root,
            label: String::new(),
        }
    }

    pub fn append(&mut self, parent: NodeId, node: Node) -> NodeId {
        parent.append_value(node, &mut self.arena)
    }

    pub fn create(&mut self, node: Node, parent: Option<NodeId>) -> NodeId {
        match parent {
            Some(parent) => self.append(parent, node),
            None => self.arena.new_node(node),
        }
    }

    pub fn node(&self, id: NodeId) -> &Node {
        self.arena[id].get()
    }

    pub fn node_mut(&mut self, id: NodeId) -> &mut Node {
        self.arena[id].get_mut()
    }

    pub fn children(&self, id: NodeId) -> impl Iterator<Item = NodeId> + '_ {
        id.children(&self.arena)
    }

    pub fn descendants(&self, id: NodeId) -> impl Iterator<Item = NodeId> + '_ {
        id.descendants(&self.arena).skip(1)
    }

    pub fn nodes(&self) -> impl Iterator<Item = NodeId> + '_ {
        self.root.descendants(&self.arena)
    }

    pub fn ancestors(&self, id: NodeId) -> impl Iterator<Item = NodeId> + '_ {
        id.ancestors(&self.arena).skip(1)
    }

    pub fn child_by_field(&self, id: NodeId, field: u16) -> Option<NodeId> {
        self.children(id)
            .find(|&child| self.node(child).field == field)
    }

    pub fn postorder(&self) -> Vec<NodeId> {
        use indextree::NodeEdge;
        self.root
            .reverse_traverse(&self.arena)
            .filter_map(|edge| match edge {
                NodeEdge::Start(id) => Some(id),
                NodeEdge::End(_) => None,
            })
            .collect()
    }

    pub fn freeze(&self) -> Tree {
        let mut nodes = Vec::with_capacity(self.root.descendants(&self.arena).count());
        self.copy_into(self.root, NONE, &mut nodes);
        Tree::new(nodes, self.label.clone())
    }

    pub fn finish(&self, mut edges: Vec<super::Edge>) -> (Tree, Vec<super::Edge>) {
        let mut nodes = Vec::new();
        let mut remap = Vec::new();
        self.copy_canonical(self.root, NONE, &mut nodes, &mut remap);
        for edge in &mut edges {
            if let Some(&node) = remap.get(edge.from.node as usize)
                && node != NONE
            {
                edge.from.node = node;
            }
            if let Some(&node) = remap.get(edge.to.node as usize)
                && node != NONE
            {
                edge.to.node = node;
            }
        }
        (Tree::new(nodes, self.label.clone()), edges)
    }

    pub fn clone_subtree(&mut self, source: NodeId, parent: Option<NodeId>) -> NodeId {
        let children: smallvec::SmallVec<[NodeId; 8]> = self.children(source).collect();
        let copy = self.arena.new_node(*self.node(source));
        if let Some(parent) = parent {
            parent.append(copy, &mut self.arena);
        }
        for child in children {
            self.clone_subtree(child, Some(copy));
        }
        copy
    }

    pub fn replace(&mut self, target: NodeId, roots: Vec<NodeId>) {
        let field = self.node(target).field;
        if target == self.root && roots.len() != 1 {
            for root in roots {
                root.remove_subtree(&mut self.arena);
            }
            return;
        }
        if let Some(&root) = roots.first() {
            self.node_mut(root).field = field;
        }
        if target == self.root {
            self.root = roots[0];
        } else {
            for root in roots {
                target.insert_before(root, &mut self.arena);
            }
        }
        target.remove_subtree(&mut self.arena);
    }

    fn copy_into(&self, id: NodeId, parent: u32, out: &mut Vec<Node>) {
        let at = out.len() as u32;
        out.push(Node {
            parent,
            ..*self.node(id)
        });
        for child in self.children(id) {
            self.copy_into(child, at, out);
        }
        out[at as usize].size = out.len() as u32 - at;
    }

    fn copy_canonical(&self, id: NodeId, parent: u32, out: &mut Vec<Node>, remap: &mut Vec<u32>) {
        let keep = id == self.root || crate::canonical::is_canonical(self.node(id).kind);
        let old = remap.len();
        remap.push(if keep { out.len() as u32 } else { NONE });
        let next_parent = if keep {
            let at = out.len() as u32;
            out.push(Node {
                parent,
                field: 0,
                ..*self.node(id)
            });
            at
        } else {
            parent
        };
        for child in self.children(id) {
            self.copy_canonical(child, next_parent, out, remap);
        }
        if keep {
            let at = remap[old];
            out[at as usize].size = out.len() as u32 - at;
        }
    }
}
