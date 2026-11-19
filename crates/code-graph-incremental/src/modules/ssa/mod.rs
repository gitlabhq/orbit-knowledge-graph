pub mod graph;
mod names;
mod values;
mod walk;

use std::rc::Rc;

use rustc_hash::{FxHashMap, FxHashSet};
use smallvec::SmallVec;

use crate::canonical::Canonical as C;
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

#[derive(Default)]
pub struct State {
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
    /// A deferred call's values, once the walk has seen every assignment.
    late: FxHashMap<u32, Vals>,
    /// The wildcard imports in force at each call.
    wild_at: FxHashMap<u32, Rc<Vec<u32>>>,
}

impl Module for Ssa {
    const NAME: &'static str = "ssa";
    type State = State;

    fn functions() -> &'static [Function<Self>] {
        FUNCTIONS
    }

    fn start<'t, S: Store>(cx: &mut Cx<'_, 't, Self, S>, root: Cursor<'t, S>) {
        let wildcard = cx.lang().syms.intern(WILDCARD);
        let label = root.sym();
        let s = cx.write();
        s.cur = s.graph.add_block();
        s.graph.seal_block(s.cur);
        s.wildcard = wildcard;
        s.scopes = vec![(label, Vec::new())];
        for scope in crate::modules::top_level(root) {
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
    function!(Flow, "pending", [one], |cx, a| {
        cx.walked();
        let call = node(cx, a, 0).index();
        yes(cx.state().pending.contains_key(&call))
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
        let s = self.write();
        let sink = match sink {
            Some(c) if c.is(C::Binding) => Some(c.index()),
            Some(c) if c.is(C::SsaBranch) => s
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
            let (ends, assigned) = loop_spans(self.tree().root());
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
        let Some(callee) = call.child(C::Callee) else {
            return;
        };
        if let Some(value) = self.pending(callee) {
            let named = callee.sym_opt().is_some()
                && !callee.has(C::Member)
                && !callee.has(C::Ivar)
                && crate::tree::is_name_text(self.lang().syms.resolve(callee.sym()));
            let fallback = named.then(|| Rc::clone(&self.state().wildcards));
            let s = self.write();
            s.pending
                .insert(call.index(), Pending::Callee(value, fallback));
            return;
        }
        let Some(object) = callee.child(C::Member).and_then(|m| m.child(C::Object)) else {
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

fn loop_spans<S: Store>(root: Cursor<'_, S>) -> (FxHashMap<u32, u32>, FxHashMap<u32, Vec<u32>>) {
    let (mut ends, mut assigned) = (FxHashMap::default(), FxHashMap::<u32, Vec<u32>>::default());
    let mut stack = vec![(root, false)];
    let mut last = root.index();
    while let Some((node, leaving)) = stack.pop() {
        if leaving {
            ends.insert(node.index(), last + 1);
            continue;
        }
        last = node.index();
        if node.is(C::Import) || node.is(C::ImportType) {
            for name in std::iter::once(node).chain(node.descendants()) {
                assigned.entry(name.sym()).or_default().push(last);
            }
            continue;
        }
        let assigns = [C::Binding, C::DefName, C::Alias, C::SsaHint]
            .into_iter()
            .any(|kind| node.is(kind));
        if assigns || node.has(C::Declaration) {
            assigned.entry(node.sym()).or_default().push(last);
        }
        if node.is(C::SsaLoop) {
            stack.push((node, true));
        }
        stack.extend(node.children_rev().map(|child| (child, false)));
    }
    (ends, assigned)
}
