mod java;

use std::cell::RefCell;
use std::rc::Rc;

use rustc_hash::{FxHashMap, FxHashSet};

use crate::canonical::Canonical as C;
use crate::intern::Lang;
use crate::resolver::CLASS_LIKE;
use crate::sentinel::{Killed, Sentinel};
use crate::tags::ReservedTags;
use crate::tree::{Compact, Edge, EdgeKind, members_named};
use java::{ArgType, Param, parse_signature, select_parsed};

type Tree = crate::tree::Tree<Compact>;
type Cursor<'a> = crate::tree::Cursor<'a, Compact>;
type NodeRef = (u32, u32);

type Site = (u32, u32, u32);

const MAX_FAN_OUT: usize = 32;

struct PoolData {
    defs: Vec<NodeRef>,
    parsed: Vec<Vec<Param>>,
}

type Pool = Rc<PoolData>;
type MemberIndex = FxHashMap<u32, Vec<NodeRef>>;

#[derive(Default)]
struct Scope {
    positions: FxHashMap<u32, usize>,
    bindings: FxHashMap<u32, Vec<(usize, u32)>>,
}

pub fn narrow_calls(
    trees: &[Tree],
    edges: &[Edge],
    lang: &Lang,
    max_file_edges: usize,
    run: &Sentinel,
) -> Result<Option<Vec<Edge>>, Killed> {
    let narrower = Narrower {
        trees,
        lang,
        tags: ReservedTags::new(lang),
        pools: RefCell::default(),
        members: RefCell::default(),
        fields: RefCell::default(),
        locals: RefCell::default(),
        scopes: RefCell::default(),
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
        return Ok(None);
    }
    let mut file_edges: FxHashMap<u32, usize> = FxHashMap::default();
    for edge in edges {
        *file_edges.entry(edge.from_tree).or_default() += 1;
    }
    let mut sites: Vec<_> = sites.into_iter().collect();
    sites.sort_unstable_by_key(|(site, _)| *site);
    let extends = extends_of(edges);
    let mut replaced: FxHashMap<usize, Vec<Edge>> = FxHashMap::default();
    let mut dropped: FxHashSet<usize> = FxHashSet::default();
    for (visited, ((from_tree, _, site), indices)) in sites.iter().enumerate() {
        if visited % 256 == 0 {
            run.check()?;
        }
        let call = narrower.cursor((*from_tree, *site));
        let targets: Vec<NodeRef> = indices
            .iter()
            .map(|&i| (edges[i].to_tree, edges[i].to_node))
            .collect();
        let Some(chosen) = narrower.choose(call, &targets, &extends) else {
            continue;
        };
        let existing: FxHashSet<NodeRef> = targets.iter().copied().collect();
        if chosen.len() == targets.len() && chosen.iter().all(|c| existing.contains(c)) {
            continue;
        }
        let file_total = file_edges.entry(*from_tree).or_default();
        let grown = (*file_total + chosen.len()).saturating_sub(targets.len());
        if max_file_edges != 0 && chosen.len() > targets.len() && grown > max_file_edges {
            continue;
        }
        *file_total = grown;
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
        return Ok(None);
    }
    let mut narrowed = Vec::with_capacity(edges.len());
    for (index, edge) in edges.iter().enumerate() {
        match replaced.remove(&index) {
            Some(edges) => narrowed.extend(edges),
            None if !dropped.contains(&index) => narrowed.push(*edge),
            None => {}
        }
    }
    Ok(Some(narrowed))
}

fn extends_of(edges: &[Edge]) -> FxHashMap<NodeRef, Vec<NodeRef>> {
    let mut extends: FxHashMap<NodeRef, Vec<NodeRef>> = FxHashMap::default();
    for edge in edges.iter().filter(|e| e.kind == EdgeKind::Extends) {
        extends.entry(edge.from()).or_default().push(edge.to());
    }
    extends
}

type Locals<'a> = FxHashSet<&'a str>;

struct Narrower<'a> {
    trees: &'a [Tree],
    lang: &'a Lang,
    tags: ReservedTags,
    pools: RefCell<FxHashMap<(NodeRef, u32), Pool>>,
    members: RefCell<FxHashMap<NodeRef, Rc<MemberIndex>>>,
    locals: RefCell<FxHashMap<NodeRef, Rc<Locals<'a>>>>,
    scopes: RefCell<FxHashMap<NodeRef, Rc<Scope>>>,
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
        let mut chosen_set: FxHashSet<NodeRef> = FxHashSet::default();
        for &target in targets {
            let pool = self.overloads(self.cursor(target), extends);
            let picked = select_parsed(&pool.parsed, &args);
            if picked.len() > MAX_FAN_OUT {
                return None;
            }
            for index in picked {
                if chosen_set.insert(pool.defs[index]) {
                    chosen.push(pool.defs[index]);
                }
            }
        }
        let unknown = args.iter().any(|arg| matches!(arg, ArgType::Unknown));
        if unknown && chosen.len() > MAX_FAN_OUT {
            return None;
        }
        (!chosen.is_empty()).then_some(chosen)
    }

    fn overloads(&self, method: Cursor<'a>, extends: &FxHashMap<NodeRef, Vec<NodeRef>>) -> Pool {
        let own = || {
            let signature = self.signature(method).unwrap_or_default();
            Rc::new(PoolData {
                defs: vec![(method.fi(), method.index())],
                parsed: vec![parse_signature(signature)],
            })
        };
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
        let parsed = pool
            .iter()
            .map(|&def| parse_signature(self.signature(self.cursor(def)).unwrap_or_default()))
            .collect();
        Rc::new(PoolData { defs: pool, parsed })
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
            return self
                .class_around(call)
                .and_then(|class| class.child_sym(C::DefName))
                .map_or(ArgType::Unknown, |sym| named(self.lang.syms.resolve(sym)));
        }
        match token.strip_prefix('~') {
            Some(name) => self.variable_type(call, name),
            None if token == "?" => ArgType::Unknown,
            None => named(token),
        }
    }

    fn variable_type(&self, call: Cursor<'a>, name: &str) -> ArgType {
        let Some(member) = call.nearby_ancestors().find(|node| node.is(C::Def)) else {
            return ArgType::Unknown;
        };
        let Some(locals) = self.locals_of(member) else {
            return ArgType::Unknown;
        };
        if locals.contains(name) {
            self.local_type(call, member, name)
        } else {
            self.field_type(call, name)
        }
    }

    fn locals_of(&self, member: Cursor<'a>) -> Option<Rc<Locals<'a>>> {
        let key = (member.fi(), member.index());
        if let Some(locals) = self.locals.borrow().get(&key) {
            return Some(Rc::clone(locals));
        }
        let names = self.tag_text(member, self.tags.java_locals)?;
        let locals = Rc::new(names.split_whitespace().collect::<Locals>());
        self.locals.borrow_mut().insert(key, Rc::clone(&locals));
        Some(locals)
    }

    fn scope_of(&self, node: Cursor<'a>) -> Rc<Scope> {
        let key = (node.fi(), node.index());
        if let Some(scope) = self.scopes.borrow().get(&key) {
            return Rc::clone(scope);
        }
        let mut scope = Scope::default();
        for (position, child) in node.children().enumerate() {
            scope.positions.insert(child.index(), position);
            if child.is(C::Binding) {
                scope
                    .bindings
                    .entry(child.sym())
                    .or_default()
                    .push((position, child.index()));
            }
        }
        let scope = Rc::new(scope);
        self.scopes.borrow_mut().insert(key, Rc::clone(&scope));
        scope
    }

    fn declared(&self, node: Cursor<'a>) -> ArgType {
        match self.tag_text(node, self.tags.java_decl_type) {
            Some("var") | None => ArgType::Unknown,
            Some(ty) => ArgType::Named(ty.to_string()),
        }
    }

    fn local_type(&self, call: Cursor<'a>, member: Cursor<'a>, name: &str) -> ArgType {
        let sym = self.lang.syms.lookup(name);
        if sym == 0 {
            return ArgType::Unknown;
        }
        let mut node = call;
        for parent in call.nearby_ancestors() {
            let scope = self.scope_of(parent);
            let before = scope.positions.get(&node.index()).copied().unwrap_or(0);
            let binding = scope.bindings.get(&sym).and_then(|bindings| {
                let earlier = bindings.partition_point(|&(position, _)| position < before);
                bindings[..earlier].last()
            });
            if let Some(&(_, binding)) = binding {
                return self.declared(self.cursor((parent.fi(), binding)));
            }
            if parent.index() == member.index() {
                break;
            }
            node = parent;
        }
        ArgType::Unknown
    }

    fn class_around(&self, call: Cursor<'a>) -> Option<Cursor<'a>> {
        call.nearby_ancestors().find(|node| {
            node.is(C::Def)
                && node
                    .children()
                    .any(|child| CLASS_LIKE.iter().any(|&kind| child.is(kind)))
        })
    }

    fn field_type(&self, call: Cursor<'a>, name: &str) -> ArgType {
        let sym = self.lang.syms.lookup(name);
        let Some(class) = self.class_around(call) else {
            return ArgType::Unknown;
        };
        match self.fields_of(class).get(&sym) {
            Some(&field) if sym != 0 => self.declared(self.cursor(field)),
            _ => ArgType::Unknown,
        }
    }
}
