use std::ops::ControlFlow;

use super::{Mutable, Storage, Tree};

pub enum Step<R> {
    Into,
    Over,
    Out(R),
}

pub struct Cursor<'a, S: Storage = Mutable> {
    trees: &'a [Tree<S>],
    tree: &'a Tree<S>,
    fi: u32,
    id: u32,
}

impl<S: Storage> Copy for Cursor<'_, S> {}
impl<S: Storage> Clone for Cursor<'_, S> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<'a, S: Storage> Cursor<'a, S> {
    pub fn new(trees: &'a [Tree<S>], fi: u32, id: u32) -> Self {
        Self {
            trees,
            tree: &trees[fi as usize],
            fi,
            id,
        }
    }
    fn tree(self) -> &'a Tree<S> {
        self.tree
    }
    pub fn node(self) -> &'a S::Node {
        self.tree().storage.node(self.id)
    }
    fn at(self, id: u32) -> Self {
        Self { id, ..self }
    }
    pub fn index(self) -> u32 {
        self.id
    }
    pub fn fi(self) -> u32 {
        self.fi
    }
    pub fn tag(self, key: u32) -> Option<u32> {
        self.tree().get_tag(self.id, key)
    }
    pub fn has_tag(self, key: u32) -> bool {
        self.tag(key).is_some()
    }
    pub fn size(self) -> u32 {
        self.descendants().count() as u32 + 1
    }
    pub fn parent(self) -> Option<Self> {
        self.tree().storage.parent(self.id).map(|id| self.at(id))
    }

    pub fn children(self) -> impl Iterator<Item = Self> + 'a {
        std::iter::successors(self.tree().storage.first_child(self.id), move |&id| {
            self.tree().storage.next_sibling(id)
        })
        .map(move |id| self.at(id))
    }

    pub fn children_rev(self) -> impl Iterator<Item = Self> + 'a {
        std::iter::successors(self.tree().storage.last_child(self.id), move |&id| {
            self.tree().storage.previous_sibling(id)
        })
        .map(move |id| self.at(id))
    }

    pub fn descendants(self) -> impl Iterator<Item = Self> + 'a {
        self.walk()
    }
    pub fn ancestors(self) -> impl Iterator<Item = Self> + 'a {
        std::iter::successors(self.parent(), |node| node.parent())
    }
    pub fn walk(self) -> Walk<'a, S> {
        Walk {
            root: self,
            next: self.children().next(),
            last: None,
        }
    }
    pub fn jump(self, fi: u32, id: u32) -> Self {
        if fi == self.fi {
            return Self { id, ..self };
        }
        Self {
            trees: self.trees,
            tree: &self.trees[fi as usize],
            fi,
            id,
        }
    }

    pub(crate) fn acquired(tree: &'a Tree<S>, fi: u32, id: u32) -> Self {
        Self {
            trees: &[],
            tree,
            fi,
            id,
        }
    }

    pub fn descend<R>(self, mut visitor: impl FnMut(Self) -> Step<R>) -> Option<R> {
        self.walk().run(|node, walk| match visitor(node) {
            Step::Out(value) => ControlFlow::Break(value),
            Step::Over => {
                walk.skip_subtree();
                ControlFlow::Continue(())
            }
            Step::Into => ControlFlow::Continue(()),
        })
    }

    pub fn for_each(self, mut visitor: impl FnMut(Self, &mut Walk<'a, S>)) {
        self.walk().run(|node, walk| {
            visitor(node, walk);
            ControlFlow::<()>::Continue(())
        });
    }

    pub fn fold_tree<A>(
        self,
        mut value: A,
        mut visitor: impl FnMut(&mut A, Self, &mut Walk<'a, S>),
    ) -> A {
        self.for_each(|node, walk| visitor(&mut value, node, walk));
        value
    }

    pub fn descendants_pruned(
        self,
        prune: impl Fn(Self) -> bool + 'a,
    ) -> impl Iterator<Item = Self> + 'a {
        let mut walk = self.walk();
        std::iter::from_fn(move || {
            let node = walk.next()?;
            if prune(node) {
                walk.skip_subtree();
            }
            Some(node)
        })
    }

    pub fn ascend<R>(self, mut visitor: impl FnMut(Self) -> Step<R>) -> Option<R> {
        self.ancestors().find_map(|node| match visitor(node) {
            Step::Out(value) => Some(value),
            Step::Into | Step::Over => None,
        })
    }

    pub fn any_desc(self, pred: impl Fn(Self) -> bool) -> bool {
        self.descendants().any(pred)
    }
    pub fn enclosing(self, pred: impl Fn(Self) -> bool) -> Option<Self> {
        self.ancestors().find(|&node| pred(node))
    }
}

pub struct Walk<'a, S: Storage = Mutable> {
    root: Cursor<'a, S>,
    next: Option<Cursor<'a, S>>,
    last: Option<Cursor<'a, S>>,
}

impl<'a, S: Storage> Iterator for Walk<'a, S> {
    type Item = Cursor<'a, S>;
    fn next(&mut self) -> Option<Self::Item> {
        let node = self.next?;
        self.next = node.children().next().or_else(|| self.after(node));
        self.last = Some(node);
        Some(node)
    }
}

impl<'a, S: Storage> Walk<'a, S> {
    fn after(&self, mut node: Cursor<'a, S>) -> Option<Cursor<'a, S>> {
        while node.id != self.root.id {
            if let Some(id) = node.tree().storage.next_sibling(node.id) {
                return Some(node.at(id));
            }
            node = node.parent()?;
        }
        None
    }

    pub fn skip_subtree(&mut self) {
        if let Some(node) = self.last.take() {
            self.next = self.after(node);
        }
    }

    pub fn run<B>(
        mut self,
        mut visitor: impl FnMut(Cursor<'a, S>, &mut Self) -> ControlFlow<B>,
    ) -> Option<B> {
        while let Some(node) = self.next() {
            if let ControlFlow::Break(value) = visitor(node, &mut self) {
                return Some(value);
            }
        }
        None
    }
}

impl<S: Storage> Tree<S> {
    pub fn cursor(&self, id: u32) -> Cursor<'_, S> {
        Cursor::new(std::slice::from_ref(self), 0, id)
    }
    pub fn root(&self) -> Cursor<'_, S> {
        self.cursor(S::index(self.root))
    }
    pub fn len(&self) -> u32 {
        self.root().size()
    }
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}
