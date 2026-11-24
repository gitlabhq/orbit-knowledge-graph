pub mod graph;
mod names;
mod values;
mod walk;

use std::rc::Rc;

use rustc_hash::{FxHashMap, FxHashSet};
use smallvec::SmallVec;

use crate::constants::WILDCARD;
use crate::engine::{Cursor, Cx, Function, Module, Store, Val, Vals, of, yes};
use crate::modules::{function, node};
pub use graph::{BlockId, SsaEngine, Value};

pub struct Ssa;

#[derive(Clone, Copy, PartialEq, Eq, Hash)]
enum Key {
    Name(u32),
    Declaration(u32, u32),
    Field(u32, u32),
}

enum Frame {
    Scope {
        wildcards: usize,
        resume: Option<BlockId>,
    },
    Branch {
        pre: BlockId,
        sink: Option<u32>,
        exits: SmallVec<[BlockId; 4]>,
    },
    Loop(BlockId),
}

#[derive(Clone)]
enum Pending {
    Callee(Value, Option<Rc<Vec<u32>>>),
    Receiver(Value, Vec<Value>),
}

type Undo = Vec<(u32, Option<(u32, usize)>)>;

crate::roles!(Kinds: u16 {
    source_file, declaration, def, rhs, member, defname, binding, qualifier, alias, ssa_branch, import_type, call, ssa_arm, import, ssa_loop, ssa_hint, object, ivar, side_effect, scope, obj, name, lambda, impl_block, decorator, callee, args, trait_type, supertype, struct_type, source_path, interface, enum_type, dispatch, destructure, config_field, class, arr });
crate::roles!(Tags: u32 { scoped, hoisted, lifted, implicit_self, type_only, weak, outer });

impl Kinds {
    fn class_like(self) -> [u16; 6] {
        [
            self.class,
            self.struct_type,
            self.impl_block,
            self.interface,
            self.trait_type,
            self.enum_type,
        ]
    }
}

#[derive(Default)]
pub struct State {
    k: Kinds,
    t: Tags,
    graph: SsaEngine,
    cur: BlockId,
    frames: Vec<Frame>,
    scopes: Vec<(u32, Undo)>,
    visible: FxHashMap<u32, (u32, usize)>,
    statics: FxHashMap<u32, Value>,
    keys: FxHashMap<Key, u32>,
    fields: FxHashMap<u32, FxHashMap<u32, u32>>,
    loops: Vec<(BlockId, u32, u32)>,
    loop_ends: FxHashMap<u32, u32>,
    assigned: FxHashMap<u32, Vec<u32>>,
    wildcard: u32,
    wildcards: Rc<Vec<u32>>,
    pending: FxHashMap<u32, Pending>,
    bound: FxHashSet<u32>,
    walking: bool,
    walked: bool,
    /// What each reference saw when the walk passed it.
    seen: FxHashMap<u32, Vals>,
    /// References read before the walk entered their definition.
    read_outside: FxHashSet<u32>,
    /// A deferred call's values, once the walk has seen every assignment.
    late: FxHashMap<u32, Vals>,
    /// The wildcard imports in force at each call.
    wild_at: FxHashMap<u32, Rc<Vec<u32>>>,
    /// The binding each member-chain object stands for; valid only until
    /// the walk takes a step that is not a chain node.
    chains: FxHashMap<u32, u32>,
}

impl Module for Ssa {
    const NAME: &'static str = "ssa";
    type State = State;

    fn functions() -> &'static [Function<Self>] {
        FUNCTIONS
    }

    const KINDS: &'static [&'static str] = Kinds::NAMES;
    const TAGS: &'static [&'static str] = Tags::NAMES;

    fn start<'t, S: Store>(cx: &mut Cx<'_, 't, Self, S>, root: Cursor<'t, S>) {
        let wildcard = cx.lang().syms.intern(WILDCARD);
        let label = root.sym();
        let (k, t) = (Kinds::bound(cx.kinds()), Tags::bound(cx.tags()));
        let s = cx.write();
        s.k = k;
        s.t = t;
        s.cur = s.graph.add_block();
        s.graph.seal_block(s.cur);
        s.wildcard = wildcard;
        s.scopes = vec![(label, Vec::new())];
        for scope in crate::modules::top_level(root, k.source_file) {
            cx.predeclare(scope);
        }
    }

    fn finish<S: Store>(_: &mut Cx<'_, '_, Self, S>) {}
}

static FUNCTIONS: &[Function<Ssa>] = &[
    function!(required Flow, "value_of", [one]),
    function!(required Flow, "valued", [one]),
    function!(required Flow, "source", [one]),
    function!(Flow, "flow", [one], |cx, a| {
        let c = node(cx, a, 0);
        match cx.state().walking {
            true => cx.read(c),
            false => {
                cx.walked();
                cx.state().seen.get(&c.index()).cloned().unwrap_or_default()
            }
        }
    }),
    function!(Flow, "slot", [one], |cx, a| {
        cx.walked();
        let c = node(cx, a, 0);
        let k = cx.state().k;
        let slot = match c.is(k.binding) {
            true => cx.target(c),
            false => cx.field_slot(c),
        };
        slot.map_or_else(Vals::new, |slot| crate::engine::one(Val::Sym(slot)))
    }),
    function!(Flow, "pending", [one], |cx, a| {
        cx.walked();
        let call = node(cx, a, 0).index();
        yes(matches!(
            cx.state().pending.get(&call),
            Some(Pending::Callee(..))
        ))
    }),
    function!(Flow, "deferred", [one], |cx, a| {
        cx.walked();
        let call = node(cx, a, 0).index();
        cx.state().late.get(&call).cloned().unwrap_or_default()
    }),
    function!(Flow, "wildcards", [one], |cx, a| {
        cx.walked();
        let call = node(cx, a, 0).index();
        let found = cx.state().wild_at.get(&call).cloned().unwrap_or_default();
        of(found.iter().map(|&node| cx.loc(node)))
    }),
];

impl<'t, S: Store> Cx<'_, 't, Ssa, S> {
    fn vals(&self, values: &[Value]) -> Vals {
        values
            .iter()
            .filter_map(|value| match *value {
                Value::LocalDef(n) | Value::ImportRef(n) | Value::Call(n) => {
                    Some(Val::Node(self.loc(n)))
                }
                Value::Type(n) => Some(Val::Type(self.loc(n))),
                Value::Opaque => Some(Val::Opaque),
                _ => None,
            })
            .collect()
    }

    fn fork(&mut self, sink: Option<Cursor<'t, S>>) {
        let k = self.state().k;
        let s = self.write();
        let sink = match sink {
            Some(c) if c.is(k.binding) => Some(c.index()),
            Some(c) if c.is(k.ssa_branch) => s
                .frames
                .iter()
                .rev()
                .find_map(|f| match f {
                    Frame::Branch { sink, .. } => Some(*sink),
                    _ => None,
                })
                .flatten(),
            _ => None,
        };
        let pre = s.cur;
        s.cur = s.graph.add_sealed_successor(pre);
        s.frames.push(Frame::Branch {
            pre,
            sink,
            exits: SmallVec::new(),
        });
    }

    fn yields(&mut self, values: &[Val], from: Option<Cursor<'t, S>>) {
        let sink = self.state().frames.iter().rev().find_map(|f| match f {
            Frame::Branch { sink, .. } => Some(*sink),
            _ => None,
        });
        if let Some(Some(sink)) = sink {
            self.bind(self.at(sink), values, from);
        }
    }

    fn join(&mut self) {
        let s = self.write();
        let cur = s.cur;
        if let Some(Frame::Branch { pre, exits, .. }) = s.frames.last_mut() {
            exits.push(cur);
            let pre = *pre;
            s.cur = s.graph.add_sealed_successor(pre);
        }
    }

    fn loop_header(&mut self, c: Cursor<'t, S>) {
        if self.state().loop_ends.is_empty() {
            let (ends, assigned) = loop_spans(self.state().k, self.tree().root());
            let s = self.write();
            (s.loop_ends, s.assigned) = (ends, assigned);
        }
        let s = self.write();
        let end = s.loop_ends[&c.index()];
        s.loops.push((s.cur, c.index(), end));
        let (header, body) = s.graph.begin_loop(s.cur);
        s.cur = body;
        s.frames.push(Frame::Loop(header));
    }

    fn leave(&mut self) {
        let s = self.write();
        match s.frames.pop() {
            Some(Frame::Scope { wildcards, resume }) => {
                let (_, undo) = s.scopes.pop().unwrap_or_default();
                for (name, previous) in undo.into_iter().rev() {
                    match previous {
                        Some(previous) => s.visible.insert(name, previous),
                        None => s.visible.remove(&name),
                    };
                }
                if s.wildcards.len() > wildcards {
                    Rc::make_mut(&mut s.wildcards).truncate(wildcards);
                }
                if let Some(block) = resume {
                    s.cur = block;
                }
            }
            Some(Frame::Branch { pre, sink, exits }) => {
                s.cur = match exits.is_empty() {
                    true => pre,
                    false => {
                        let exits = exits.into_iter().chain(sink.is_none().then_some(pre));
                        s.graph.add_sealed_join(exits)
                    }
                };
            }
            Some(Frame::Loop(header)) => {
                s.loops.pop();
                s.cur = s.graph.finish_loop(header, s.cur);
            }
            None => {}
        }
    }

    fn defer(&mut self, call: Cursor<'t, S>) {
        let k = self.state().k;
        let Some(callee) = call.child(k.callee) else {
            return;
        };
        if let Some(value) = self.pending(callee) {
            let named = callee.sym_opt().is_some()
                && !callee.has(k.member)
                && !callee.has(k.ivar)
                && is_name_text(self.lang().syms.resolve(callee.sym()));
            let fallback = named.then(|| Rc::clone(&self.state().wildcards));
            let s = self.write();
            s.pending
                .insert(call.index(), Pending::Callee(value, fallback));
            return;
        }
        let Some(object) = callee.child(k.member).and_then(|m| m.child(k.object)) else {
            return;
        };
        let leaf = !self.state().loops.is_empty() && object.children().next().is_none();
        if let Some(phi) = leaf.then(|| self.read_phi(object.sym())).flatten() {
            let seen = self.lookup(object.sym());
            let s = self.write();
            s.pending.insert(call.index(), Pending::Receiver(phi, seen));
        }
    }

    fn deferred(&mut self, call: Cursor<'t, S>) -> Vals {
        let Some(pending) = self.state().pending.get(&call.index()).cloned() else {
            return Vals::new();
        };
        let found = match pending {
            Pending::Callee(value, fallback) => {
                let mut found = self.state().graph.resolve_value(&value);
                if found.is_empty() {
                    let wildcards = fallback.iter().flat_map(|w| w.iter());
                    found.extend(wildcards.map(|&w| Value::ImportRef(w)));
                }
                found
            }
            Pending::Receiver(value, seen) => {
                let mut found = self.state().graph.resolve_value(&value);
                found.retain(|value| !seen.contains(value));
                found
            }
        };
        self.vals(&found)
    }

    fn pending(&mut self, callee: Cursor<'t, S>) -> Option<Value> {
        if let Some(slot) = self.field_slot(callee) {
            return Some(self.read_value(slot));
        }
        callee.sym_opt().and_then(|sym| self.read_phi(sym))
    }
}

fn loop_spans<S: Store>(
    k: Kinds,
    root: Cursor<'_, S>,
) -> (FxHashMap<u32, u32>, FxHashMap<u32, Vec<u32>>) {
    let (mut ends, mut assigned) = (FxHashMap::default(), FxHashMap::<u32, Vec<u32>>::default());
    let mut stack = vec![(root, false)];
    let mut last = root.index();
    while let Some((node, leaving)) = stack.pop() {
        if leaving {
            ends.insert(node.index(), last + 1);
            continue;
        }
        last = node.index();
        if node.is(k.import) || node.is(k.import_type) {
            for name in std::iter::once(node).chain(node.descendants()) {
                assigned.entry(name.sym()).or_default().push(last);
            }
            continue;
        }
        let assigns = [k.binding, k.defname, k.alias, k.ssa_hint]
            .into_iter()
            .any(|kind| node.is(kind));
        if assigns || node.has(k.declaration) {
            assigned.entry(node.sym()).or_default().push(last);
        }
        if node.is(k.ssa_loop) {
            stack.push((node, true));
        }
        stack.extend(node.children_rev().map(|child| (child, false)));
    }
    (ends, assigned)
}

fn is_name_text(text: &str) -> bool {
    !text.is_empty()
        && !text
            .chars()
            .any(|c| c.is_whitespace() || "()[]{}<>*&,;'\"=+/%^|~".contains(c))
}

fn is_class<S: Store>(k: Kinds, c: Cursor<'_, S>) -> bool {
    let class_like = k.class_like();
    c.children()
        .any(|n| class_like.iter().any(|&kind| n.is(kind)))
}

/// The last named child that is not a side effect and stands for its parent.
fn tail_child<S: Store>(k: Kinds, node: Cursor<'_, S>) -> Option<Cursor<'_, S>> {
    node.children()
        .filter(|c| c.named() && !c.is(k.side_effect))
        .last()
        .filter(|c| !(node.labelled() && c.lifted()))
}
