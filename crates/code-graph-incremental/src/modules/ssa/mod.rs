mod bind;
mod control;
mod deferred;
pub mod engine;
mod fields;

use std::rc::Rc;

use rustc_hash::{FxHashMap, FxHashSet};
use smallvec::SmallVec;

use crate::canonical::Canonical as C;
use crate::constants::WILDCARD;
use crate::engine::{Cursor, Cx, Function, Loc, Module, Val, Vals, of, yes};
use crate::modules::{function, node};
use crate::rules::LinkConfig;
pub use engine::{BlockId, SsaEngine, Value};

pub struct Ssa;

#[derive(Clone, Copy, PartialEq, Eq, Hash)]
enum BindingKey {
    Name(u32),
    Declaration(u32, u32),
    Field(u32, u32),
}

enum Deferred {
    Callee(Option<Rc<Vec<u32>>>),
    Receiver(u32, Vec<Value>),
}

struct Bound {
    binding: u32,
    lhs: u32,
    first_edge: usize,
    declaration: bool,
    branch: bool,
}

enum Frame {
    Scope {
        wildcards: usize,
        block: Option<BlockId>,
    },
    Binding(Option<Bound>),
    Branch {
        node: u32,
        lhs: Option<u32>,
        pre: Option<BlockId>,
        exits: SmallVec<[BlockId; 4]>,
    },
    Arm(u32),
    Loop(BlockId),
    Lambda(BlockId),
}

type Undo = Vec<(u32, Option<(u32, usize)>)>;

#[derive(Default)]
pub struct State {
    graph: SsaEngine,
    entry: BlockId,
    cur: BlockId,
    registered: FxHashSet<u32>,
    declared: Vec<u32>,
    wildcards: Rc<Vec<u32>>,
    frames: Vec<Frame>,
    loops: Vec<(BlockId, (u32, u32))>,
    loop_ends: FxHashMap<u32, u32>,
    assigned: FxHashMap<u32, Vec<u32>>,
    loop_wildcards: Vec<u32>,
    wildcard: u32,
    value_sink: FxHashMap<u32, u32>,
    pending: Vec<(Value, u32, u32, Deferred)>,
    fields: FxHashMap<u32, FxHashMap<u32, u32>>,
    scopes: Vec<(u32, Undo)>,
    visible: FxHashMap<u32, (u32, usize)>,
    statics: FxHashMap<u32, Value>,
    keys: FxHashMap<BindingKey, u32>,
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

fn loop_head(c: Cursor<'_>) -> usize {
    c.children()
        .position(|node| node.is(C::Binding) && node.has(C::Declaration) && !node.lifted())
        .unwrap_or(0)
}

impl<'t> Cx<'_, 't, Ssa> {
    fn link_config(&self) -> &'t LinkConfig {
        &self.env().rules_for(&self.tree().label).config.link
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
        cx.resolve_pending();
        let s = cx.write();
        s.cur = s.entry;
        cx.decorators();
    }
}

static FUNCTIONS: &[Function<Ssa>] = &[
    function!(Effect, "bind_import", [one], |cx, a| {
        cx.bind_import(node(cx, a, 0));
        yes(true)
    }),
    function!(Effect, "bind_imports", [one], |cx, a| {
        for import in node(cx, a, 0).children().filter(|c| c.is(C::Import)) {
            cx.bind_import(import);
        }
        yes(true)
    }),
    function!(Effect, "enter_def", [one], |cx, a| {
        cx.enter_def(node(cx, a, 0));
        yes(true)
    }),
    function!(Effect, "enter_scope", [one], |cx, a| {
        cx.scope(node(cx, a, 0), None);
        yes(true)
    }),
    function!(Effect, "leave", [], |cx, _| {
        cx.leave();
        yes(true)
    }),
    function!(Effect, "declare_static", [one], |cx, a| {
        let c = node(cx, a, 0);
        cx.declare_static(c, c.sym(), Value::Call(c.index()));
        yes(true)
    }),
    function!(Effect, "bind", [one, one], |cx, a| {
        let lhs = match a[1][0] {
            Val::Sym(lhs) => lhs,
            _ => 0,
        };
        cx.bind_binding(node(cx, a, 0), lhs);
        yes(true)
    }),
    function!(Effect, "destructure", [one], |cx, a| {
        cx.destructure(node(cx, a, 0));
        yes(true)
    }),
    function!(Effect, "fork", [one], |cx, a| {
        cx.fork(node(cx, a, 0));
        yes(true)
    }),
    function!(Effect, "arm", [one], |cx, a| {
        cx.arm(node(cx, a, 0));
        yes(true)
    }),
    function!(Effect, "fork_lambda", [], |cx, _| {
        let s = cx.write();
        s.frames.push(Frame::Lambda(s.cur));
        s.cur = s.graph.add_sealed_successor(s.cur);
        yes(true)
    }),
    function!(Effect, "loop_header", [one], |cx, a| {
        cx.begin_loop(node(cx, a, 0));
        yes(true)
    }),
    function!(Effect, "defer", [one], |cx, a| {
        cx.defer(node(cx, a, 0));
        yes(true)
    }),
    function!(Effect, "defer_receiver", [one], |cx, a| {
        cx.defer_receiver(node(cx, a, 0));
        yes(true)
    }),
    function!(Flow, "read", [one], |cx, a| {
        let found = cx.lookup(node(cx, a, 0).sym());
        to_vals(&found)
    }),
    function!(Flow, "read_static", [one], |cx, a| {
        let found = cx.lookup_static(node(cx, a, 0).sym());
        to_vals(&found)
    }),
    function!(Flow, "defined", [one], |cx, a| {
        let c = node(cx, a, 0);
        let binding = cx.binding(c.sym());
        let s = cx.state();
        match s.graph.is_defined(binding, s.cur) {
            true => of([c]),
            false => Vals::new(),
        }
    }),
    function!(Flow, "phi", [one], |cx, a| {
        let callee = node(cx, a, 0);
        yes(cx.pending_value(callee).is_some())
    }),
    function!(Flow, "field", [one], |cx, a| {
        let c = node(cx, a, 0);
        match cx.field_slot(c) {
            Some(_) => of([c]),
            None => Vals::new(),
        }
    }),
    function!(Flow, "wildcards", [], |cx, _| {
        let fi = cx.fi();
        let wildcards = Rc::clone(&cx.state().wildcards);
        of(wildcards.iter().map(|&node| Loc { fi, node }))
    }),
    function!(Flow, "target", [one], |cx, a| cx.target(node(cx, a, 0))),
    function!(Flow, "rhs_order", [one], |cx, a| cx
        .bind_children(node(cx, a, 0))),
    function!(Flow, "in_fork", [one], |cx, a| {
        let arm = node(cx, a, 0);
        let top = match cx.state().frames.last() {
            Some(Frame::Branch { node, .. }) => Some(*node),
            _ => None,
        };
        yes(arm.parent().map(|p| p.index()) == top)
    }),
    function!(Pure, "loop_head", [one], |cx, a| {
        let c = node(cx, a, 0);
        of(c.children().take(loop_head(c)))
    }),
    function!(Pure, "loop_body", [one], |cx, a| {
        let c = node(cx, a, 0);
        of(c.children().skip(loop_head(c)))
    }),
];
