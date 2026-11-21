mod java;

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

pub fn narrow_calls(trees: &[Tree], edges: &[Edge], lang: &Lang) -> Option<Vec<Edge>> {
    let narrower = Narrower {
        trees,
        lang,
        tags: ReservedTags::new(lang),
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

    fn overloads(
        &self,
        method: Cursor<'a>,
        extends: &FxHashMap<NodeRef, Vec<NodeRef>>,
    ) -> Vec<NodeRef> {
        let Some(name) = method.child_sym(C::DefName) else {
            return vec![(method.fi(), method.index())];
        };
        let Some(owner) = method.enclosing_def(CLASS_LIKE) else {
            return vec![(method.fi(), method.index())];
        };
        let mut pool: Vec<NodeRef> = Vec::new();
        let mut seen_signatures: FxHashSet<&str> = FxHashSet::default();
        let mut seen: FxHashSet<NodeRef> = FxHashSet::default();
        let mut level = vec![(owner.fi(), owner.index())];
        while !level.is_empty() {
            let mut next = Vec::new();
            for class in level {
                if !seen.insert(class) {
                    continue;
                }
                let class_cursor = self.cursor(class);
                let declared = members_named(class_cursor)
                    .filter(|&(member_name, _)| member_name == name)
                    .map(|(_, def)| self.cursor((class.0, def)))
                    .filter(|def| def.is_member_of(class_cursor))
                    .filter_map(|def| Some((def, self.signature(def)?)));
                let mut hidden_here = Vec::new();
                for (def, signature) in declared {
                    if !seen_signatures.contains(signature) {
                        hidden_here.push(signature);
                        pool.push((def.fi(), def.index()));
                    }
                }
                seen_signatures.extend(hidden_here);
                next.extend(extends.get(&class).into_iter().flatten().copied());
            }
            level = next;
        }
        pool
    }

    fn arguments(&self, call: Cursor<'a>) -> Option<Vec<ArgType>> {
        let tokens = self.tag_text(call, self.tags.arg_types)?;
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
        match token {
            "?" => ArgType::Unknown,
            _ => match token.strip_prefix('~') {
                Some(variable) => self.variable_type(call, variable),
                None => named(token),
            },
        }
    }

    fn variable_type(&self, call: Cursor<'a>, name: &str) -> ArgType {
        let sym = self.lang.syms.lookup(name);
        if sym == 0 {
            return ArgType::Unknown;
        }
        let declared = |node: Cursor<'a>| match self.tag_text(node, self.tags.decl_type) {
            Some("var") | None => ArgType::Unknown,
            Some(ty) => ArgType::Named(ty.to_string()),
        };
        let mut node = call;
        while let Some(parent) = node.parent() {
            let earlier: Vec<Cursor<'a>> = parent
                .children()
                .take_while(|sibling| sibling.index() != node.index())
                .collect();
            if let Some(binding) = earlier
                .iter()
                .rev()
                .find(|sibling| sibling.is(C::Binding) && sibling.sym() == sym)
            {
                return declared(*binding);
            }
            let field = parent
                .children()
                .filter(|child| child.is(C::Def) && child.has(C::FieldDef))
                .find(|child| child.child_sym(C::DefName) == Some(sym));
            if let Some(field) = field {
                return declared(field);
            }
            node = parent;
        }
        ArgType::Unknown
    }
}
