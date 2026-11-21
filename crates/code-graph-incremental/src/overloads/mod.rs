mod java;

use std::cell::RefCell;
use std::rc::Rc;

use rustc_hash::{FxHashMap, FxHashSet};

use crate::canonical::Canonical as C;
use crate::intern::Lang;
use crate::resolver::CLASS_LIKE;
use crate::tags::ReservedTags;
use crate::tree::{Compact, Edge, EdgeKind, members_named};
use java::{ArgType, select_overloads};

type Tree = crate::tree::Tree<Compact>;
type Cursor<'a> = crate::tree::Cursor<'a, Compact>;
type NodeRef = (u32, u32);

type Site = (u32, u32, u32);

type Pool = Rc<[NodeRef]>;
type MemberIndex = FxHashMap<u32, Vec<NodeRef>>;

pub fn narrow_calls(trees: &[Tree], edges: &[Edge], lang: &Lang) -> Option<Vec<Edge>> {
    let narrower = Narrower {
        trees,
        lang,
        tags: ReservedTags::new(lang),
        pools: RefCell::default(),
        members: RefCell::default(),
        fields: RefCell::default(),
    };
    let mut sites: FxHashMap<Site, Vec<usize>> = FxHashMap::default();
    for (index, edge) in edges.iter().enumerate() {
        let target = narrower.cursor((edge.to_tree, edge.to_node));
        if edge.kind == EdgeKind::Calls
            && let Some(site) = edge.site
            && narrower.signature(target).is_some()
        {
            sites
                .entry((edge.from_tree, edge.from_node, site))
                .or_default()
                .push(index);
        }
    }
    if sites.is_empty() {
        return None;
    }
    let extends = extends_of(edges);
    let mut replaced: FxHashMap<usize, Vec<Edge>> = FxHashMap::default();
    let mut dropped: FxHashSet<usize> = FxHashSet::default();
    for ((from_tree, _, site), indices) in &sites {
        let call = narrower.cursor((*from_tree, *site));
        let targets: Vec<NodeRef> = indices
            .iter()
            .map(|&i| (edges[i].to_tree, edges[i].to_node))
            .collect();
        let Some(chosen) = narrower.choose(call, &targets, &extends) else {
            continue;
        };
        if chosen.len() == targets.len() && chosen.iter().all(|c| targets.contains(c)) {
            continue;
        }
        let template = edges[indices[0]];
        let narrowed = chosen.iter().map(|&(to_tree, to_node)| Edge {
            to_tree,
            to_node,
            ..template
        });
        replaced.insert(indices[0], narrowed.collect());
        dropped.extend(indices);
    }
    if replaced.is_empty() {
        return None;
    }
    let mut narrowed = Vec::with_capacity(edges.len());
    for (index, edge) in edges.iter().enumerate() {
        match replaced.remove(&index) {
            Some(edges) => narrowed.extend(edges),
            None if !dropped.contains(&index) => narrowed.push(*edge),
            None => {}
        }
    }
    Some(narrowed)
}

fn extends_of(edges: &[Edge]) -> FxHashMap<NodeRef, Vec<NodeRef>> {
    let mut extends: FxHashMap<NodeRef, Vec<NodeRef>> = FxHashMap::default();
    for edge in edges.iter().filter(|e| e.kind == EdgeKind::Extends) {
        extends.entry(edge.from()).or_default().push(edge.to());
    }
    extends
}

struct Narrower<'a> {
    trees: &'a [Tree],
    lang: &'a Lang,
    tags: ReservedTags,
    pools: RefCell<FxHashMap<(NodeRef, u32), Pool>>,
    members: RefCell<FxHashMap<NodeRef, Rc<MemberIndex>>>,
    fields: RefCell<FxHashMap<NodeRef, Rc<FxHashMap<u32, NodeRef>>>>,
}

impl<'a> Narrower<'a> {
    fn cursor(&self, (tree, node): NodeRef) -> Cursor<'a> {
        Cursor::new(self.trees, tree, node)
    }

    fn tag_text(&self, node: Cursor<'a>, key: u32) -> Option<&'a str> {
        let sym = node.tag(key).filter(|&sym| sym != 0)?;
        Some(self.lang.syms.resolve(sym))
    }

    fn signature(&self, method: Cursor<'a>) -> Option<&'a str> {
        self.tag_text(method, self.tags.signature)
    }

    fn choose(
        &self,
        call: Cursor<'a>,
        targets: &[NodeRef],
        extends: &FxHashMap<NodeRef, Vec<NodeRef>>,
    ) -> Option<Vec<NodeRef>> {
        let args = self.arguments(call)?;
        let mut chosen: Vec<NodeRef> = Vec::new();
        for &target in targets {
            let pool = self.overloads(self.cursor(target), extends);
            let signatures: Vec<&str> = pool
                .iter()
                .filter_map(|&def| self.signature(self.cursor(def)))
                .collect();
            for index in select_overloads(&signatures, &args) {
                if !chosen.contains(&pool[index]) {
                    chosen.push(pool[index]);
                }
            }
        }
        (!chosen.is_empty()).then_some(chosen)
    }

    fn overloads(&self, method: Cursor<'a>, extends: &FxHashMap<NodeRef, Vec<NodeRef>>) -> Pool {
        let own = || Rc::from(vec![(method.fi(), method.index())]);
        let Some(name) = method.child_sym(C::DefName) else {
            return own();
        };
        let Some(owner) = method.enclosing_def(CLASS_LIKE) else {
            return own();
        };
        let key = ((owner.fi(), owner.index()), name);
        if let Some(pool) = self.pools.borrow().get(&key) {
            return Rc::clone(pool);
        }
        let pool = self.build_pool(key.0, name, extends);
        self.pools.borrow_mut().insert(key, Rc::clone(&pool));
        pool
    }

    fn build_pool(
        &self,
        owner: NodeRef,
        name: u32,
        extends: &FxHashMap<NodeRef, Vec<NodeRef>>,
    ) -> Pool {
        let mut pool: Vec<NodeRef> = Vec::new();
        let mut seen_signatures: FxHashSet<&str> = FxHashSet::default();
        let mut seen: FxHashSet<NodeRef> = FxHashSet::default();
        let mut level = vec![owner];
        while !level.is_empty() {
            let mut next = Vec::new();
            for class in level {
                if !seen.insert(class) {
                    continue;
                }
                let members = self.members_of(class);
                let mut hidden_here = Vec::new();
                for &def in members.get(&name).into_iter().flatten() {
                    let Some(signature) = self.signature(self.cursor(def)) else {
                        continue;
                    };
                    if !seen_signatures.contains(signature) {
                        hidden_here.push(signature);
                        pool.push(def);
                    }
                }
                seen_signatures.extend(hidden_here);
                next.extend(extends.get(&class).into_iter().flatten().copied());
            }
            level = next;
        }
        pool.into()
    }

    fn members_of(&self, class: NodeRef) -> Rc<MemberIndex> {
        if let Some(index) = self.members.borrow().get(&class) {
            return Rc::clone(index);
        }
        let class_cursor = self.cursor(class);
        let mut index: FxHashMap<u32, Vec<NodeRef>> = FxHashMap::default();
        for (name, def) in members_named(class_cursor) {
            if self.cursor((class.0, def)).is_member_of(class_cursor) {
                index.entry(name).or_default().push((class.0, def));
            }
        }
        let index = Rc::new(index);
        self.members.borrow_mut().insert(class, Rc::clone(&index));
        index
    }

    fn fields_of(&self, class: Cursor<'a>) -> Rc<FxHashMap<u32, NodeRef>> {
        let key = (class.fi(), class.index());
        if let Some(index) = self.fields.borrow().get(&key) {
            return Rc::clone(index);
        }
        let mut index: FxHashMap<u32, NodeRef> = FxHashMap::default();
        for field in class
            .children()
            .filter(|child| child.is(C::Def) && child.has(C::FieldDef))
        {
            if let Some(name) = field.child_sym(C::DefName) {
                index.entry(name).or_insert((field.fi(), field.index()));
            }
        }
        let index = Rc::new(index);
        self.fields.borrow_mut().insert(key, Rc::clone(&index));
        index
    }

    fn arguments(&self, call: Cursor<'a>) -> Option<Vec<ArgType>> {
        let tokens = self.tag_text(call, self.tags.java_arg_types)?;
        let tokens = tokens.strip_prefix('(')?.strip_suffix(')')?;
        if tokens.is_empty() {
            return Some(Vec::new());
        }
        Some(
            tokens
                .split(',')
                .map(|token| self.argument(call, token))
                .collect(),
        )
    }

    fn argument(&self, call: Cursor<'a>, token: &str) -> ArgType {
        let named = |name: &str| ArgType::Named(name.to_string());
        if let Some(literal) = token.strip_prefix("#int:") {
            return named(if literal.ends_with(['l', 'L']) {
                "long"
            } else {
                "int"
            });
        }
        if let Some(literal) = token.strip_prefix("#fp:") {
            return named(if literal.ends_with(['f', 'F']) {
                "float"
            } else {
                "double"
            });
        }
        if token == "this" {
            let class = call.enclosing_def(CLASS_LIKE);
            return class
                .and_then(|class| class.child_sym(C::DefName))
                .map_or(ArgType::Unknown, |sym| named(self.lang.syms.resolve(sym)));
        }
        match token.split_at_checked(1) {
            Some(("~", name)) => self.local_type(call, name),
            Some(("^", name)) => self.field_type(call, name),
            _ if token == "?" => ArgType::Unknown,
            _ => named(token),
        }
    }

    fn declared(&self, node: Cursor<'a>) -> ArgType {
        match self.tag_text(node, self.tags.java_decl_type) {
            Some("var") | None => ArgType::Unknown,
            Some(ty) => ArgType::Named(ty.to_string()),
        }
    }

    fn local_type(&self, call: Cursor<'a>, name: &str) -> ArgType {
        let sym = self.lang.syms.lookup(name);
        if sym == 0 {
            return ArgType::Unknown;
        }
        let mut node = call;
        while let Some(parent) = node.parent() {
            let binding = parent
                .children()
                .take_while(|sibling| sibling.index() != node.index())
                .filter(|sibling| sibling.is(C::Binding) && sibling.sym() == sym)
                .last();
            if let Some(binding) = binding {
                return self.declared(binding);
            }
            node = parent;
        }
        ArgType::Unknown
    }

    fn field_type(&self, call: Cursor<'a>, name: &str) -> ArgType {
        let sym = self.lang.syms.lookup(name);
        let Some(class) = call.enclosing_def(CLASS_LIKE) else {
            return ArgType::Unknown;
        };
        match self.fields_of(class).get(&sym) {
            Some(&field) if sym != 0 => self.declared(self.cursor(field)),
            _ => ArgType::Unknown,
        }
    }
}
