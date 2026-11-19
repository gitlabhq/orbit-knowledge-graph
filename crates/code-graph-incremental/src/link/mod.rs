mod calls;
mod flow;

use std::cell::RefCell;
use std::rc::Rc;

use rustc_hash::{FxHashMap, FxHashSet};
use smallvec::SmallVec;

use crate::canonical::Canonical as C;
use crate::constants::WILDCARD;
use crate::env::Env;
use crate::rules::{self, Chain, Guard, Host, Key, LinkConfig, Members, Memo, MemoMap, Types, Val};
use crate::sentinel::{Killed, Sentinel};
use crate::ssa::{BlockId, SsaEngine, Value};
use crate::tags::ReservedTags;
use crate::tree::{CallResolution, Compact, Edge, EdgeKind};

type Tree = crate::tree::Tree<Compact>;
type Cursor<'a> = crate::tree::Cursor<'a, Compact>;
type Values = SmallVec<[Value; 2]>;

#[derive(Clone, Copy, PartialEq, Eq, Hash, strum::EnumString, strum::EnumProperty)]
pub enum Fun {
    #[strum(serialize = "names.import", props(arity = "1"))]
    Import,
    #[strum(serialize = "link.call", props(arity = "1"))]
    Call,
    #[strum(serialize = "link.member", props(arity = "1"))]
    Member,
    #[strum(serialize = "types.chain", props(arity = "1", memo = "flow"))]
    Chain,
    #[strum(serialize = "ssa.def", props(arity = "1"))]
    Def,
    #[strum(serialize = "ssa.body", props(arity = "1"))]
    Body,
    #[strum(serialize = "ssa.scope", props(arity = "1"))]
    Scope,
    #[strum(serialize = "ssa.leave", props(arity = "1"))]
    Leave,
    #[strum(serialize = "ssa.qualified", props(arity = "1"))]
    Qualified,
    #[strum(serialize = "ssa.target", props(arity = "1"))]
    Target,
    #[strum(serialize = "ssa.bind", props(arity = "2"))]
    Bind,
    #[strum(serialize = "ssa.bind_children", props(arity = "1"))]
    BindChildren,
    #[strum(serialize = "ssa.destructure", props(arity = "1"))]
    Destructure,
    #[strum(serialize = "ssa.branch", props(arity = "1"))]
    Branch,
    #[strum(serialize = "ssa.arms_last", props(arity = "1"))]
    ArmsLast,
    #[strum(serialize = "ssa.in_branch", props(arity = "1"))]
    InBranch,
    #[strum(serialize = "ssa.arm", props(arity = "1"))]
    Arm,
    #[strum(serialize = "ssa.loop_children", props(arity = "1"))]
    LoopChildren,
    #[strum(serialize = "ssa.begin_loop", props(arity = "1"))]
    BeginLoop,
    #[strum(serialize = "ssa.lambda", props(arity = "1"))]
    Lambda,
}

#[derive(Clone, Copy, PartialEq, Eq, Hash)]
enum BindingKey {
    Name(u32),
    Declaration(u32, u32),
    Field(u32, u32),
}

#[derive(Clone, Copy)]
enum Wildcards {
    All,
    Callable,
}

enum Deferred {
    Callee(Option<Rc<Vec<u32>>>),
    Receiver(u32, u32, Vec<Value>),
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
type Epoch = (BlockId, usize, usize, usize, usize);

pub(crate) struct Fold<'t> {
    tree: &'t Tree,
    max_edges: usize,
    ssa: SsaEngine,
    cur: BlockId,
    registered: FxHashSet<u32>,
    defs: Vec<u32>,
    defs_by_name: FxHashMap<u32, Vec<u32>>,
    members: Members,
    ivars: RefCell<FxHashMap<u32, FxHashMap<u32, u32>>>,
    supertypes: FxHashMap<u32, Vec<u32>>,
    wildcards: Rc<Vec<u32>>,
    tags: ReservedTags,
    config: &'t LinkConfig,
    syms: &'t crate::intern::Interner,
    run: &'t Sentinel,
    file: Sentinel,
    killed: Option<Killed>,
    def_stack: Vec<u32>,
    guard: Guard<Fun>,
    memo: MemoMap<Fun>,
    memo_at: Epoch,
    obj_seen: FxHashSet<u32>,
    frames: Vec<Frame>,
    binds: usize,
    loops: Vec<(BlockId, (u32, u32))>,
    loop_ends: FxHashMap<u32, u32>,
    assigned: FxHashMap<u32, Vec<u32>>,
    loop_wildcards: Vec<u32>,
    wildcard: u32,
    edges: Vec<Edge>,
    value_sink: FxHashMap<u32, u32>,
    pending_calls: Vec<(Value, u32, u32, Deferred)>,
    shadowed_calls: FxHashSet<u32>,
    fields: FxHashMap<u32, FxHashMap<u32, u32>>,
    scopes: Vec<(u32, Undo)>,
    visible: FxHashMap<u32, (u32, usize)>,
    statics: FxHashMap<u32, Value>,
    keys: RefCell<FxHashMap<BindingKey, u32>>,
}

impl<'t> Fold<'t> {
    fn at(&self, node: u32) -> Cursor<'t> {
        self.tree.cursor(node)
    }

    fn enclosing(&self) -> u32 {
        self.def_stack.last().copied().unwrap_or(0)
    }

    fn check(&self) -> Result<(), Killed> {
        self.run.check().and_then(|()| self.file.check())?;
        match self.max_edges {
            0 => Ok(()),
            cap if self.edges.len() <= cap => Ok(()),
            cap => Err(Killed::limit(
                "edges",
                &self.tree.label,
                self.edges.len(),
                cap,
            )),
        }
    }

    fn epoch(&self) -> Epoch {
        let depth = self.scopes.len() + self.def_stack.len();
        (
            self.cur,
            self.ssa.writes(),
            self.binds,
            depth,
            self.frames.len(),
        )
    }

    fn lookup_chain(&mut self, c: Cursor) -> Values {
        match rules::call(self, Fun::Chain, Memo::Flow, &[Val::Node(c.into())]) {
            Val::Flow(values) => values,
            _ => Values::new(),
        }
    }
}

impl<'t> Types<'t> for Fold<'t> {
    fn parts(&mut self, owner: Cursor<'t>) -> rules::Parts<'t> {
        let dn = owner.index();
        let same_name = owner
            .child_sym(C::DefName)
            .and_then(|name| self.defs_by_name.get(&name))
            .into_iter()
            .flatten()
            .filter(|&&d| d != dn && (owner.has(C::ImplBlock) || self.at(d).has(C::ImplBlock)));
        std::iter::once(owner)
            .chain(same_name.map(|&d| self.at(d)))
            .collect()
    }

    fn supers(&mut self, part: Cursor<'t>) -> rules::Parts<'t> {
        let supers = self.supertypes.get(&part.index()).into_iter().flatten();
        supers.map(|&d| self.at(d)).collect()
    }

    fn members_index(&self) -> (&Members, &ReservedTags) {
        (&self.members, &self.tags)
    }
}

impl<'t> Chain<'t> for Fold<'t> {
    type Out = Values;

    fn early(&mut self, c: Cursor<'t>) -> Option<Values> {
        let call = c.value_child(C::Call)?;
        if call.has(C::Property) && self.chain_is_class(c.reference()) {
            return None;
        }
        Some(SmallVec::from_elem(Value::Call(call.index()), 1))
    }

    fn name(&mut self, c: Cursor<'t>) -> Values {
        if c.has(C::Qualifier) {
            return self.lookup_static(c.sym()).into_iter().collect();
        }
        let ivar = c.value_child(C::Ivar);
        let sym = ivar.map_or(c.sym(), |iv| iv.sym());
        let mut bound = match ivar {
            Some(_) => Vec::new(),
            None => self.lookup(sym),
        };
        if bound.is_empty() {
            let field = self
                .enclosing_class(self.enclosing())
                .and_then(|cls| self.ivar_type(cls, sym));
            bound.extend(field.map(|f| Value::Call(Self::producer_of(f))));
        }
        bound
            .into_iter()
            .map(|r| match self.member_for(r, c) {
                Value::LocalDef(d) => self.field_value(d).unwrap_or(Value::LocalDef(d)),
                r => r,
            })
            .collect()
    }

    fn member(&mut self, c: Cursor<'t>, member: Cursor<'t>, object: Cursor<'t>) -> Values {
        let mut targets = Values::new();
        for r in self.lookup_chain(object) {
            match r {
                Value::Type(node) => targets.push(Value::LocalDef(node)),
                Value::Call(n) if let Some(ty) = self.binding_type(n) => {
                    targets.extend(self.lookup_chain(ty));
                    targets.push(r);
                }
                _ => targets.push(r),
            }
        }
        let role = c.role();
        let mut found = Values::new();
        for r in targets {
            match r {
                Value::LocalDef(d) => {
                    let members = self.members(d, member.sym(), role);
                    found.extend(members.into_iter().map(Value::LocalDef));
                }
                Value::ImportRef(_) => found.push(r),
                _ => {}
            }
        }
        found
    }
}

impl Host for Fold<'_> {
    type F = Fun;

    fn call(&mut self, f: Fun, args: &[Val]) -> Val {
        let Some(node) = args[0].node() else {
            return Val::None;
        };
        let c = self.at(node.node);
        match f {
            Fun::Import => self.handle_import(c),
            Fun::Call => {
                self.handle_inline_imports(c);
                self.handle_call(c);
            }
            Fun::Member => self.handle_standalone_member(c),
            Fun::Chain => return Val::Flow(rules::chain(self, c)),
            Fun::Def => self.handle_def(c),
            Fun::Body => return self.body(c),
            Fun::Scope => self.scope(c, None),
            Fun::Leave => self.leave(),
            Fun::Qualified => {
                self.declare_static(c, c.sym(), Value::Call(c.index()));
            }
            Fun::Target => return self.target(c),
            Fun::Bind => self.bind_binding(c, args[1].sym()),
            Fun::BindChildren => return self.bind_children(c),
            Fun::Destructure => self.destructure(c),
            Fun::Branch => self.branch(c),
            Fun::ArmsLast => {
                let (arms, rest): (Vec<_>, Vec<_>) = c.children().partition(|a| a.is(C::SsaArm));
                return Val::of(rest.into_iter().chain(arms));
            }
            Fun::InBranch => {
                let top = match self.frames.last() {
                    Some(Frame::Branch { node, .. }) => Some(*node),
                    _ => None,
                };
                return Val::when(c.parent().map(|p| p.index()) == top);
            }
            Fun::Arm => self.arm(c),
            Fun::LoopChildren => return self.loop_children(c),
            Fun::BeginLoop => self.begin_loop(c),
            Fun::Lambda => {
                self.frames.push(Frame::Lambda(self.cur));
                self.cur = self.ssa.add_sealed_successor(self.cur);
            }
        }
        Val::Yes
    }

    fn memo_get(&mut self, key: &Key<Fun>) -> Option<Val> {
        let epoch = self.epoch();
        if self.memo_at != epoch {
            self.memo.clear();
            self.memo_at = epoch;
        }
        self.memo.get(key).cloned()
    }

    fn memo_put(&mut self, key: Key<Fun>, val: &Val) {
        if self.memo_at == self.epoch() {
            self.memo.insert(key, val.clone());
        }
    }

    fn guard(&mut self) -> &mut Guard<Fun> {
        &mut self.guard
    }

    fn defs(&mut self) -> &mut Vec<u32> {
        &mut self.def_stack
    }

    fn step(&mut self) -> bool {
        if self.killed.is_none()
            && let Err(killed) = self.check()
        {
            self.killed = Some(killed);
        }
        self.killed.is_none()
    }
}

pub fn link(tree: &Tree, env: &Env, run: &Sentinel) -> Result<Vec<Edge>, Killed> {
    let (lang, config) = (&env.lang, &env.rules_for(&tree.label).config.link);
    let tags = ReservedTags::new(lang);
    let root = tree.root();
    let mut ssa = SsaEngine::new();
    let entry = ssa.add_block();
    ssa.seal_block(entry);
    let mut f = Fold {
        tree,
        max_edges: env.limits.max_file_edges,
        ssa,
        cur: entry,
        registered: FxHashSet::default(),
        defs: Vec::new(),
        defs_by_name: FxHashMap::default(),
        members: Members::new(1),
        ivars: RefCell::default(),
        supertypes: FxHashMap::default(),
        wildcards: Rc::default(),
        tags,
        config,
        syms: &lang.syms,
        run,
        file: Sentinel::new("link", &tree.label, env.limits.file_link_ms).on_thread_cpu(),
        killed: None,
        def_stack: Vec::new(),
        guard: Guard::default(),
        memo: MemoMap::default(),
        memo_at: (entry, 0, 0, 0, 0),
        obj_seen: FxHashSet::default(),
        frames: Vec::new(),
        binds: 0,
        loops: Vec::new(),
        loop_ends: FxHashMap::default(),
        assigned: FxHashMap::default(),
        loop_wildcards: Vec::new(),
        wildcard: lang.syms.intern(WILDCARD),
        edges: Vec::new(),
        value_sink: FxHashMap::default(),
        pending_calls: Vec::new(),
        shadowed_calls: FxHashSet::default(),
        fields: FxHashMap::default(),
        scopes: vec![(root.tag(tags.scoped).unwrap_or(root.sym()), Vec::new())],
        visible: FxHashMap::default(),
        statics: FxHashMap::default(),
        keys: RefCell::default(),
    };
    f.predeclare(root);
    env.link_rules.walk(&mut f, lang, root);
    if let Some(killed) = f.killed.take() {
        return Err(killed);
    }
    f.ssa.seal_remaining();
    f.ssa.remove_redundant_phi_sccs();
    f.resolve_pending()?;
    f.cur = entry;
    f.decorators()?;
    Ok(f.finish())
}

impl Fold<'_> {
    fn decorators(&mut self) -> Result<(), Killed> {
        for dn in self.defs.clone() {
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
        let (tree, config, wildcard) = (self.tree, self.config, self.wildcard);
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
            if edge.kind == EdgeKind::TypeFlow && self.shadowed_calls.contains(&edge.to_node) {
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
