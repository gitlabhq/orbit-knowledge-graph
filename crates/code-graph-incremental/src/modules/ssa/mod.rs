pub mod engine;
mod names;
mod values;

use std::rc::Rc;

use rustc_hash::{FxHashMap, FxHashSet};
use smallvec::SmallVec;

use crate::canonical::Canonical as C;
use crate::constants::WILDCARD;
use crate::engine::{Cursor, Cx, Function, Loc, Module, Val, Vals, of, one, yes};
use crate::modules::{function, node};
pub use engine::{BlockId, SsaEngine, Value};

pub struct Ssa;

#[derive(Clone, Copy, PartialEq, Eq, Hash)]
enum Key {
    Name(u32),
    Declaration(u32, u32),
    Field(u32, u32),
}

#[derive(Clone)]
enum Deferred {
    Callee(Option<Rc<Vec<u32>>>),
    Receiver(Vec<Value>),
}

enum Frame {
    Scope {
        wildcards: usize,
        block: Option<BlockId>,
    },
    Branch {
        pre: BlockId,
        exhaustive: bool,
        exits: SmallVec<[BlockId; 4]>,
    },
    Loop(BlockId),
}

type Undo = Vec<(u32, Option<(u32, usize)>)>;

/// Flow state of one file: the SSA graph and its current block, the open
/// frames, the scopes from names to variables, field variables, the loops
/// being walked and the calls deferred until their loop closes.
#[derive(Default)]
pub struct State {
    graph: SsaEngine,
    entry: BlockId,
    cur: BlockId,
    frames: Vec<Frame>,
    scopes: Vec<(u32, Undo)>,
    visible: FxHashMap<u32, (u32, usize)>,
    statics: FxHashMap<u32, Value>,
    keys: FxHashMap<Key, u32>,
    fields: FxHashMap<u32, FxHashMap<u32, u32>>,
    loops: Vec<(BlockId, (u32, u32))>,
    loop_ends: FxHashMap<u32, u32>,
    assigned: FxHashMap<u32, Vec<u32>>,
    wildcard: u32,
    wildcards: Rc<Vec<u32>>,
    pending: FxHashMap<u32, (Value, Deferred)>,
    bound: FxHashSet<u32>,
}

fn to_vals<'v>(values: impl IntoIterator<Item = &'v Value>) -> Vals {
    let loc = |node| Loc { fi: 0, node };
    values
        .into_iter()
        .filter_map(|value| match *value {
            Value::LocalDef(n) | Value::ImportRef(n) | Value::Call(n) => Some(Val::Node(loc(n))),
            Value::Type(n) => Some(Val::Type(loc(n))),
            Value::Opaque => Some(Val::Opaque),
            _ => None,
        })
        .collect()
}

/// Where each name is assigned, by node position, and where each loop ends.
fn loop_spans(root: Cursor) -> (FxHashMap<u32, u32>, FxHashMap<u32, Vec<u32>>) {
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
        if [C::Binding, C::DefName, C::Alias, C::SsaHint]
            .into_iter()
            .any(|kind| node.is(kind))
            || node.has(C::Declaration)
        {
            assigned.entry(node.sym()).or_default().push(last);
        }
        if node.is(C::SsaLoop) {
            stack.push((node, true));
        }
        stack.extend(node.children_rev().map(|child| (child, false)));
    }
    (ends, assigned)
}

impl<'t> Cx<'_, 't, Ssa> {
    fn fork(&mut self, exhaustive: bool) {
        let s = self.write();
        let pre = s.cur;
        s.cur = s.graph.add_sealed_successor(pre);
        s.frames.push(Frame::Branch {
            pre,
            exhaustive,
            exits: SmallVec::new(),
        });
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

    fn loop_header(&mut self, c: Cursor<'t>) {
        let root = self.tree().root();
        let s = self.write();
        if s.loop_ends.is_empty() {
            (s.loop_ends, s.assigned) = loop_spans(root);
        }
        let span = (c.index(), s.loop_ends[&c.index()]);
        s.loops.push((s.cur, span));
        let (header, body) = s.graph.begin_loop(s.cur);
        s.cur = body;
        s.frames.push(Frame::Loop(header));
    }

    fn leave(&mut self) {
        match self.write().frames.pop() {
            Some(Frame::Scope { wildcards, block }) => {
                self.exit_scope();
                let s = self.write();
                if s.wildcards.len() > wildcards {
                    Rc::make_mut(&mut s.wildcards).truncate(wildcards);
                }
                if let Some(block) = block {
                    s.cur = block;
                }
            }
            Some(Frame::Branch {
                pre,
                exhaustive,
                exits,
            }) => {
                let s = self.write();
                s.cur = match exits.is_empty() {
                    true => pre,
                    false => {
                        let exits = exits.into_iter().chain((!exhaustive).then_some(pre));
                        s.graph.add_sealed_join(exits)
                    }
                };
            }
            Some(Frame::Loop(header)) => {
                let s = self.write();
                s.loops.pop();
                s.cur = s.graph.finish_loop(header, s.cur);
            }
            None => {}
        }
    }
}

impl Module for Ssa {
    const NAME: &'static str = "ssa";
    type State = State;

    fn functions() -> &'static [Function<Self>] {
        FUNCTIONS
    }

    fn start<'t>(cx: &mut Cx<'_, 't, Self>, root: Cursor<'t>) {
        let wildcard = cx.env().lang.syms.intern(WILDCARD);
        let label = root.tag(cx.tags().scoped).unwrap_or(root.sym());
        let s = cx.write();
        s.entry = s.graph.add_block();
        s.graph.seal_block(s.entry);
        s.cur = s.entry;
        s.wildcard = wildcard;
        s.scopes = vec![(label, Vec::new())];
        cx.predeclare(root);
    }

    fn finish(cx: &mut Cx<'_, '_, Self>) {
        let s = cx.write();
        s.graph.seal_remaining();
        s.graph.remove_redundant_phi_sccs();
    }
}

static FUNCTIONS: &[Function<Ssa>] = &[
    function!(Effect, "bind", [one, set, set], |cx, a| {
        let from = a[2].first().and_then(|v| v.loc()).map(|loc| cx.jump(loc));
        cx.bind(node(cx, a, 0), &a[1], from);
        yes(true)
    }),
    function!(Effect, "declare", [one], |cx, a| {
        let c = node(cx, a, 0);
        cx.declare(c, c.sym());
        yes(true)
    }),
    function!(Effect, "bind_static", [one], |cx, a| {
        let c = node(cx, a, 0);
        cx.declare_static(c, c.sym(), Value::Call(c.index()));
        yes(true)
    }),
    function!(Effect, "bind_import", [one], |cx, a| {
        cx.bind_import(node(cx, a, 0));
        yes(true)
    }),
    function!(Effect, "bind_inline", [one], |cx, a| {
        let c = node(cx, a, 0);
        for import in c
            .children()
            .filter(|i| i.is(C::Import) || i.is(C::ImportType))
        {
            cx.bind_import(import);
        }
        yes(true)
    }),
    function!(Effect, "enter_scope", [one], |cx, a| {
        cx.scope(node(cx, a, 0), None);
        yes(true)
    }),
    function!(Effect, "enter_def", [one], |cx, a| {
        cx.enter_def(node(cx, a, 0));
        yes(true)
    }),
    function!(Effect, "fork", [set], |cx, a| {
        cx.fork(!a[0].is_empty());
        yes(true)
    }),
    function!(Effect, "join", [], |cx, _| {
        cx.join();
        yes(true)
    }),
    function!(Effect, "loop_header", [one], |cx, a| {
        cx.loop_header(node(cx, a, 0));
        yes(true)
    }),
    function!(Effect, "leave", [], |cx, _| {
        cx.leave();
        yes(true)
    }),
    function!(Effect, "defer", [one], |cx, a| {
        cx.defer(node(cx, a, 0));
        yes(true)
    }),
    function!(Flow, "deferred_callee", [one], |cx, a| cx
        .deferred_callee(node(cx, a, 0))),
    function!(Flow, "deferred_receiver", [one], |cx, a| cx
        .deferred_receiver(node(cx, a, 0))),
    function!(Flow, "read", [one], |cx, a| cx.read(node(cx, a, 0))),
    function!(Flow, "slot", [one], |cx, a| {
        let c = node(cx, a, 0);
        let slot = match c.is(C::Binding) {
            true => cx.target(c),
            false => cx.field_slot(c),
        };
        slot.map_or_else(Vals::new, |slot| one(Val::Sym(slot)))
    }),
    function!(Flow, "phi", [one], |cx, a| {
        let callee = node(cx, a, 0);
        yes(cx.pending_value(callee).is_some())
    }),
    function!(Flow, "wildcards", [], |cx, _| {
        let fi = cx.fi();
        let wildcards = Rc::clone(&cx.state().wildcards);
        of(wildcards.iter().map(|&node| Loc { fi, node }))
    }),
];
