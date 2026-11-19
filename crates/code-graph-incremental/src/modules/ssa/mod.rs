mod calls;
pub mod engine;
mod flow;

use std::cell::RefCell;
use std::rc::Rc;

use rustc_hash::{FxHashMap, FxHashSet};
use smallvec::SmallVec;

use crate::canonical::Canonical as C;
use crate::constants::WILDCARD;
use crate::engine::{Arg, Cursor, Cx, Function, Kind, Module, Tree, Val, Vals, yes};
use crate::env::Env;
use crate::modules::core::ops::Members;
use crate::rules::LinkConfig;
use crate::sentinel::{Killed, Sentinel};
use crate::tree::{CallResolution, Edge, EdgeKind};
pub use engine::{BlockId, SsaEngine, Value};

type Values = SmallVec<[Value; 2]>;

#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum BindingKey {
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
pub type Epoch = (u32, usize, usize, usize, usize);

#[derive(Default)]
pub struct State {
    graph: SsaEngine,
    cur: BlockId,
    registered: FxHashSet<u32>,
    declared: Vec<u32>,
    defs_by_name: FxHashMap<u32, Vec<u32>>,
    members: Option<Members>,
    ivars: RefCell<FxHashMap<u32, FxHashMap<u32, u32>>>,
    supertypes: FxHashMap<u32, Vec<u32>>,
    wildcards: Rc<Vec<u32>>,
    frames: Vec<Frame>,
    binds: usize,
    loops: Vec<(BlockId, (u32, u32))>,
    loop_ends: FxHashMap<u32, u32>,
    assigned: FxHashMap<u32, Vec<u32>>,
    loop_wildcards: Vec<u32>,
    wildcard: u32,
    value_sink: FxHashMap<u32, u32>,
    pending_calls: Vec<(Value, u32, u32, Deferred)>,
    shadowed_calls: FxHashSet<u32>,
    fields: FxHashMap<u32, FxHashMap<u32, u32>>,
    scopes: Vec<(u32, Undo)>,
    visible: FxHashMap<u32, (u32, usize)>,
    statics: FxHashMap<u32, Value>,
    keys: RefCell<FxHashMap<BindingKey, u32>>,
    pub(crate) site: Option<(u32, u32, u32)>,
}

impl State {
    pub fn epoch(&self, defs: usize) -> Epoch {
        let depth = self.scopes.len() + defs;
        (
            self.cur.0,
            self.graph.writes(),
            self.binds,
            depth,
            self.frames.len(),
        )
    }
}

pub(crate) fn to_vals<'v>(values: impl IntoIterator<Item = &'v Value>) -> Vals {
    let loc = |node| crate::engine::Loc { fi: 0, node };
    values
        .into_iter()
        .filter_map(|value| match *value {
            Value::LocalDef(n) | Value::ImportRef(n) | Value::Call(n) => Some(Val::Node(loc(n))),
            Value::Type(n) => Some(Val::Type(loc(n))),
            _ => None,
        })
        .collect()
}

impl<'t> Cx<'t> {
    pub(crate) fn value(&self, val: Val) -> Option<Value> {
        match val {
            Val::Type(loc) => Some(Value::Type(loc.node)),
            Val::Node(loc) => Some(match self.at(loc.node).kind() {
                k if k == C::Def => Value::LocalDef(loc.node),
                k if k == C::Name => Value::ImportRef(loc.node),
                _ => Value::Call(loc.node),
            }),
            _ => None,
        }
    }

    pub(crate) fn values(&self, vals: &[Val]) -> Values {
        vals.iter().filter_map(|&v| self.value(v)).collect()
    }

    fn link_config(&self) -> &'t LinkConfig {
        &self.env.rules_for(&self.tree().label).config.link
    }

    pub(crate) fn enclosing(&self) -> u32 {
        self.defs.last().copied().unwrap_or(0)
    }

    pub(crate) fn lookup_chain(&mut self, c: Cursor) -> Values {
        let found = self.call1(crate::fn_id!("core", "type"), Val::Node(c.into()));
        self.values(&found)
    }

    pub(crate) fn members_index(&self) -> &Members {
        self.ssa
            .members
            .as_ref()
            .expect("the link phase builds the member index")
    }
}

fn node<'t>(cx: &Cx<'t>, args: &[Vals], at: usize) -> Cursor<'t> {
    cx.jump(args[at][0].loc().expect("ssa functions take nodes"))
}

macro_rules! function {
    ($kind:ident, $name:literal, [$($arg:ident),*], $body:expr) => {
        Function {
            name: $name,
            sig: &[$(function!(@arg $arg)),*],
            kind: Kind::$kind,
            call: $body,
        }
    };
    (@arg one) => { Arg::One };
}
pub(crate) use function;

pub static MODULE: Module = Module {
    name: "ssa",
    functions: &[
        function!(Effect, "bind_import", [one], |cx, a| {
            cx.handle_import(node(cx, a, 0));
            yes(true)
        }),
        function!(Effect, "bind_imports", [one], |cx, a| {
            cx.handle_inline_imports(node(cx, a, 0));
            yes(true)
        }),
        function!(Effect, "shadow", [one], |cx, a| {
            cx.shadow(node(cx, a, 0));
            yes(true)
        }),
        function!(Flow, "pending", [one], |cx, a| {
            let callee = node(cx, a, 0);
            yes(cx.pending_value(callee).is_some())
        }),
        function!(Effect, "defer", [one], |cx, a| {
            cx.defer(node(cx, a, 0));
            yes(true)
        }),
        function!(Effect, "defer_receiver", [one], |cx, a| {
            cx.defer_receiver(node(cx, a, 0));
            yes(true)
        }),
        function!(Effect, "enter_def", [one], |cx, a| {
            cx.handle_def(node(cx, a, 0));
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
        function!(Flow, "target", [one], |cx, a| cx.target(node(cx, a, 0))),
        function!(Effect, "bind", [one, one], |cx, a| {
            let lhs = match a[1][0] {
                Val::Sym(lhs) => lhs,
                _ => 0,
            };
            cx.bind_binding(node(cx, a, 0), lhs);
            yes(true)
        }),
        function!(Flow, "rhs_order", [one], |cx, a| cx
            .bind_children(node(cx, a, 0))),
        function!(Effect, "destructure", [one], |cx, a| {
            cx.destructure(node(cx, a, 0));
            yes(true)
        }),
        function!(Effect, "fork", [one], |cx, a| {
            cx.branch(node(cx, a, 0));
            yes(true)
        }),
        function!(Flow, "in_fork", [one], |cx, a| {
            let arm = node(cx, a, 0);
            let top = match cx.ssa.frames.last() {
                Some(Frame::Branch { node, .. }) => Some(*node),
                _ => None,
            };
            yes(arm.parent().map(|p| p.index()) == top)
        }),
        function!(Effect, "arm", [one], |cx, a| {
            cx.arm(node(cx, a, 0));
            yes(true)
        }),
        function!(Effect, "fork_lambda", [], |cx, _| {
            cx.ssa.frames.push(Frame::Lambda(cx.ssa.cur));
            cx.ssa.cur = cx.ssa.graph.add_sealed_successor(cx.ssa.cur);
            yes(true)
        }),
        function!(Flow, "loop_head", [one], |cx, a| {
            let c = node(cx, a, 0);
            crate::engine::of(c.children().take(loop_head(c)))
        }),
        function!(Flow, "loop_body", [one], |cx, a| {
            let c = node(cx, a, 0);
            crate::engine::of(c.children().skip(loop_head(c)))
        }),
        function!(Effect, "loop_header", [one], |cx, a| {
            cx.begin_loop(node(cx, a, 0));
            yes(true)
        }),
    ],
};

fn loop_head(c: Cursor<'_>) -> usize {
    c.children()
        .position(|node| node.is(C::Binding) && node.has(C::Declaration) && !node.lifted())
        .unwrap_or(0)
}

pub fn link(tree: &Tree, env: &Env, run: &Sentinel) -> Result<Vec<Edge>, Killed> {
    let file = Sentinel::new("link", &tree.label, env.limits.file_link_ms).on_thread_cpu();
    let mut cx = Cx::new(std::slice::from_ref(tree), 0, env, run, file);
    let root = tree.root();
    let entry = cx.ssa.graph.add_block();
    cx.ssa.graph.seal_block(entry);
    cx.ssa.cur = entry;
    cx.ssa.members = Some(Members::new(1));
    cx.ssa.wildcard = env.lang.syms.intern(WILDCARD);
    cx.ssa.scopes = vec![(root.tag(cx.tags.scoped).unwrap_or(root.sym()), Vec::new())];
    cx.predeclare(root);
    env.pipeline.phase("link").flow(&mut cx, root);
    if let Some(killed) = cx.killed.take() {
        return Err(killed);
    }
    cx.ssa.graph.seal_remaining();
    cx.ssa.graph.remove_redundant_phi_sccs();
    cx.resolve_pending()?;
    cx.ssa.cur = entry;
    cx.decorators()?;
    Ok(cx.finish())
}

impl Cx<'_> {
    fn decorators(&mut self) -> Result<(), Killed> {
        for dn in self.ssa.declared.clone() {
            self.run.check().and_then(|()| self.file.check())?;
            for c in self.at(dn).children().filter(|c| c.is(C::Decorator)) {
                for target in self.lookup_chain(c) {
                    match target {
                        Value::LocalDef(target) => {
                            self.edges.push(Edge::local(dn, target, EdgeKind::Calls))
                        }
                        Value::ImportRef(target) => self.edges.push(Edge {
                            site: Some(c.index()),
                            ..Edge::local(dn, target, EdgeKind::Imports)
                        }),
                        _ => {}
                    }
                }
            }
        }
        Ok(())
    }

    fn finish(mut self) -> Vec<Edge> {
        let (tree, config, wildcard) = (self.tree(), self.link_config(), self.ssa.wildcard);
        let provided = |edge: &Edge| {
            let Some(name) = edge
                .site
                .and_then(|site| tree.cursor(site).child_sym(C::Callee))
            else {
                return false;
            };
            let target = tree.cursor(edge.to_node);
            let source = target
                .parent()
                .and_then(|import| import.child_sym(C::SourcePath));
            config.builtins.contains(&name)
                || source.is_some_and(|source| {
                    let providers = config.providers.iter();
                    providers
                        .into_iter()
                        .any(|(module, symbols)| *module == source && symbols.contains(&name))
                })
        };
        for edge in &mut self.edges {
            if edge.kind == EdgeKind::TypeFlow && self.ssa.shadowed_calls.contains(&edge.to_node) {
                edge.call_resolution = CallResolution::NonCallable;
            }
            let target = tree.cursor(edge.to_node);
            if edge.kind == EdgeKind::Imports
                && (target.sym() == wildcard || target.child_sym(C::SsaHint) == Some(wildcard))
                && provided(edge)
            {
                edge.call_resolution = CallResolution::Reference;
            }
        }
        let mut seen = FxHashSet::default();
        self.edges
            .retain(|e| seen.insert((e.from(), e.to(), e.kind, e.site, e.call_resolution)));
        self.edges
    }
}
