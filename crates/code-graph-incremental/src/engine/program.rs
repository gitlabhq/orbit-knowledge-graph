//! A compiled pipeline: phases of passes, each pass ordered cases whose
//! values are expressions, and the definitions those expressions call.

use std::collections::{BTreeMap, HashMap};

use smallvec::SmallVec;

use super::expr::Expr;
use super::lang::DataSpec;
use super::pattern::{self, Cap, Pat, Slots, Tpl};
use super::{Cursor, FnId, Loc, Run, Store, Tree, Val, Vals};
use crate::intern::Lang;

/// The budgets of a phase, by `Limits` field name: one per tree, and one for
/// the whole scope.
#[derive(serde::Deserialize, Default, Clone)]
#[serde(deny_unknown_fields)]
pub struct BudgetSpec {
    pub label: Option<String>,
    pub tree: Option<String>,
    pub scope: Option<String>,
    /// The reason a file this phase stops is reported with.
    pub skip: Option<String>,
}

/// A pattern anchored at some node kinds, for one language or all.
pub(super) struct Match {
    pub(super) src: Box<str>,
    pub(super) pat: Pat,
    pub(super) depth: u8,
    pub(super) kinds: Vec<u16>,
    pub(super) slots: Slots,
    pub(super) lang: Option<u8>,
    pub(super) edge: Option<(crate::tree::EdgeKind, Option<u8>)>,
}

impl Match {
    /// Whether the pattern matches at `c`, filling `caps` with its captures.
    pub(super) fn capture<S: Store>(
        &self,
        lang: &Lang,
        c: Cursor<'_, S>,
        caps: &mut Vec<Cap<S::Id>>,
        bound: &[(u16, u16)],
    ) -> bool {
        let n = self.slots.len();
        if caps.len() < n {
            caps.resize(n, SmallVec::new());
        }
        caps[..n].iter_mut().for_each(|cap| cap.clear());
        let tree = c.tree();
        let id = tree.storage.id(c.index());
        match self.depth {
            0 => pattern::matches(tree, lang, id, None, &self.pat, caps, bound),
            depth => (0..depth)
                .try_fold(c, |node, _| node.parent())
                .is_some_and(|top| {
                    let top = tree.storage.id(top.index());
                    pattern::matches(tree, lang, top, Some(id), &self.pat, caps, bound)
                }),
        }
    }
}

/// A case: its match (none for `_`, which takes any value), guard and value.
pub(super) type Case = (Option<Match>, Option<Expr>, Expr);

/// Ordered cases, each anchored at some node kinds or at any. `by_kind`
/// lists, per kind, the cases a node of that kind tries in order: the kind's
/// own merged with `any`, the cases for every kind. At a node, each group's
/// first case that holds fires, and a stopping case ends the node.
pub(super) struct Cases {
    pub(super) items: Vec<Case>,
    groups: Vec<u16>,
    pub(super) width: usize,
    pub(super) stops: Vec<bool>,
    pub(super) by_kind: HashMap<u16, Vec<u16>>,
    pub(super) any: Vec<u16>,
    /// Whether a case's pattern takes a kind from a parameter.
    pub(super) binds_kinds: bool,
}

impl Cases {
    /// Cases in their groups, in order; `stops` marks the stopping ones.
    pub(super) fn new(items: Vec<(u16, Case)>, stops: Vec<bool>) -> Self {
        let (groups, items): (Vec<u16>, Vec<Case>) = items.into_iter().unzip();
        let mut by_kind: HashMap<u16, Vec<u16>> = HashMap::new();
        for (at, (m, _, _)) in items.iter().enumerate() {
            for &kind in m.as_ref().map_or(&[0][..], |m| &m.kinds) {
                by_kind.entry(kind).or_default().push(at as u16);
            }
        }
        let any = by_kind.remove(&0).unwrap_or_default();
        for list in by_kind.values_mut() {
            list.extend(&any);
            list.sort_unstable();
            list.dedup();
        }
        let binds_kinds = items
            .iter()
            .any(|(m, _, _)| m.as_ref().is_some_and(|m| pattern::binds_kinds(&m.pat)));
        Self {
            binds_kinds,
            width: groups.iter().max().map_or(0, |&g| usize::from(g) + 1),
            stops,
            items,
            groups,
            by_kind,
            any,
        }
    }

    pub(super) fn group(&self, at: u16) -> u16 {
        self.groups[usize::from(at)]
    }

    /// Whether a node of `kind` has any case to try.
    pub(super) fn tries(&self, kind: u16) -> bool {
        !self.by_kind.get(&kind).unwrap_or(&self.any).is_empty()
    }

    pub(super) fn calls<'a>(&'a self, f: &mut dyn FnMut(Result<FnId, u16>, &'a [Expr])) {
        for (_, guard, value) in &self.items {
            if let Some(g) = guard {
                g.calls(f);
            }
            value.calls(f);
        }
    }
}

/// A tree a case builds once it fires, with its case's captures: in place
/// of the node, or appended under it. The `build` op that writes it gives the
/// tag values, then the template's texts. Tags go on the built node, or on
/// the first node of `tag_on`'s kind under it.
pub(super) struct Template {
    pub(super) case: u16,
    pub(super) tpl: Tpl<Expr>,
    pub(super) replace: bool,
    pub(super) unique: Option<(Pat, u16, usize)>,
    pub(super) tags: Vec<u32>,
    pub(super) tag_on: Option<u16>,
}

/// One walk over a tree: the cases fired at each node, in source order or
/// children first. A repeating pass reruns until it replaces nothing.
pub(super) struct Pass {
    pub(super) cases: Cases,
    pub(super) slots: usize,
    pub(super) templates: Vec<Template>,
    pub(super) post: bool,
    pub(super) repeat: bool,
    /// Every tag key the pass can set.
    pub(super) tag_keys: Vec<u32>,
}

/// A named expression; with cases, tried in order on each value of its first
/// argument, the first case whose pattern and guard match giving the value.
pub struct Def {
    pub(super) name: Box<str>,
    pub(super) params: usize,
    pub(super) body: DefImpl,
    pub(super) slots: usize,
}

pub(super) enum DefImpl {
    Expr(Expr),
    Cases(Cases),
}

impl Def {
    pub(super) fn key(&self, args: &[Vals]) -> SmallVec<[Val; 4]> {
        let mut key = SmallVec::new();
        for vals in args {
            key.push(Val::Sym(vals.len() as u32));
            key.extend_from_slice(vals);
        }
        key
    }

    pub(super) fn calls<'a>(&'a self, f: &mut dyn FnMut(Result<FnId, u16>, &'a [Expr])) {
        match &self.body {
            DefImpl::Expr(e) => e.calls(f),
            DefImpl::Cases(cases) => cases.calls(f),
        }
    }
}

/// A declared index compiled for one language, or for all.
pub(super) struct Indexer {
    pub(super) at: Match,
    pub(super) id: u32,
    pub(super) key: Expr,
}

/// A row shape: its table, its column names, and how its id is made. Values
/// arrive in column order, then the values only the id reads.
#[derive(Clone)]
pub struct RowShape {
    pub table: String,
    pub columns: Vec<String>,
    pub id: Option<(String, Vec<Part>)>,
    pub expect: Vec<(usize, String)>,
    pub follows: Option<usize>,
}

#[derive(Clone, Copy)]
pub enum Part {
    Column(usize),
    Value(usize),
}

/// A module's roles, resolved to ids.
#[derive(Clone, Default)]
pub struct Roles {
    pub kinds: Vec<u16>,
    pub tags: Vec<u32>,
}

pub struct Phase {
    pub name: String,
    pub label: &'static str,
    pub budget: BudgetSpec,
    pub walks: TreeSets,
    pub reads: TreeSets,
    pub(super) input: usize,
    /// Reruns over the family until its edges stop changing.
    pub until_stable: bool,
    pub(super) modules: Vec<u8>,
    pub(super) links: super::Links,
    pub(super) data: Vec<Vec<Vals>>,
    /// Every definition, compiled once for the pipeline and shared.
    pub(super) defs: std::sync::Arc<[Def]>,
    /// Per definition: the module states it reaches under this phase's binds.
    pub(super) def_deps: Vec<u64>,
    /// Per language: a redefined definition's id to the language's own.
    pub(super) overrides: std::sync::Arc<[HashMap<u16, u16>]>,
    pub(super) index_count: usize,
    pub(super) enclosing: Vec<u16>,
    pub(super) roles: Vec<Roles>,
    pub(super) indexers: std::sync::Arc<[Indexer]>,
    pub(super) passes: Vec<Pass>,
    /// Per language: its passes, in order.
    pub(super) by_lang: Vec<Vec<u16>>,
    /// Per language: the passes it runs inside its files' regions, before
    /// each region's own.
    pub(super) region_by_lang: Vec<Vec<u16>>,
}

#[derive(Clone, Copy, Default, PartialEq, Eq)]
pub struct TreeSets(pub u64);

impl TreeSets {
    pub fn has(self, set: usize) -> bool {
        self.0 & (1 << set) != 0
    }

    pub fn only(set: usize) -> Self {
        Self(1 << set)
    }

    pub fn iter(self) -> impl Iterator<Item = usize> {
        (0..64).filter(move |&set| self.has(set))
    }

    fn meets(self, other: TreeSets) -> bool {
        self.0 & other.0 != 0
    }
}

impl Phase {
    pub fn shared(&self) -> bool {
        self.walks.meets(self.reads)
    }

    pub fn per_file(&self) -> bool {
        self.walks == TreeSets::only(self.input) && self.reads == TreeSets::default()
    }

    pub(super) fn passes(&self, rules: usize) -> impl Iterator<Item = &Pass> {
        self.by_lang
            .get(rules)
            .into_iter()
            .flatten()
            .map(|&at| &self.passes[usize::from(at)])
    }

    pub(super) fn region_passes(&self, rules: usize) -> impl Iterator<Item = &Pass> {
        self.region_by_lang
            .get(rules)
            .into_iter()
            .flatten()
            .map(|&at| &self.passes[usize::from(at)])
    }

    /// Whether the phase builds trees, so it runs before trees are compact.
    pub fn builds(&self) -> bool {
        self.passes.iter().any(|p| !p.templates.is_empty())
    }

    pub fn has_rules(&self, rules: usize) -> bool {
        self.passes(rules).any(|p| !p.cases.items.is_empty())
    }
}

pub struct Pipeline {
    pub registry: super::Registry,
    pub phases: Vec<Phase>,
    pub rows: Vec<RowShape>,
    pub(super) settings: Vec<BTreeMap<String, DataSpec>>,
}

impl Pipeline {
    /// A language's setting as written: its own value, or the shared default.
    pub fn setting(&self, rules: usize, name: &str) -> Option<&DataSpec> {
        self.settings.get(rules)?.get(name)
    }

    pub fn split(&self) -> (&[Phase], &[Phase]) {
        let at = self
            .phases
            .iter()
            .position(|p| !p.per_file())
            .unwrap_or(self.phases.len());
        self.phases.split_at(at)
    }

    /// The skip reason a file gets when the phase with this budget label stops it.
    pub fn skip_reason(&self, label: &str) -> Option<&str> {
        self.phases
            .iter()
            .find(|p| p.label == label)
            .and_then(|p| p.budget.skip.as_deref())
    }
}

/// A case's captures, and the slot values the rules read them as.
pub(super) struct Scratch<S: Store> {
    caps: Vec<Cap<S::Id>>,
    pub(super) vals: Vec<Vals>,
}

impl<S: Store> Scratch<S> {
    /// Ready for `slots` captures, every one empty, keeping its buffers.
    pub(super) fn reset(&mut self, slots: usize) {
        if self.vals.len() < slots {
            self.vals.resize(slots, Vals::new());
        }
        self.vals.iter_mut().for_each(|v| v.clear());
    }

    pub(super) fn new(slots: usize) -> Self {
        Self {
            caps: Vec::new(),
            vals: vec![Vals::new(); slots],
        }
    }

    /// Whether `at` matches at `c`; slot 0 holds `c`, and each capture slot
    /// the captured nodes its kind test admits.
    pub(super) fn matches(
        &mut self,
        run: &Run<S>,
        at: &Match,
        c: Cursor<'_, S>,
        bound: &[(u16, u16)],
    ) -> bool {
        if at.lang.is_some_and(|l| usize::from(l) != run.input.rules)
            || !at.capture(run.lang(), c, &mut self.caps, bound)
        {
            return false;
        }
        let n = at.slots.len();
        if self.vals.len() < n {
            self.vals.resize(n, Vals::new());
        }
        let fi = c.fi();
        self.vals[0] = SmallVec::from_elem(Val::Node(c.into()), 1);
        for slot in 1..n {
            let filter = at.slots.filter(slot as u16);
            self.vals[slot] = self.caps[slot]
                .iter()
                .map(|&id| S::index(id))
                .filter(|&node| filter.admits(c.jump(fi, node).kind()))
                .map(|node| Val::Node(Loc { fi, node }))
                .collect();
        }
        true
    }
}

/// The nodes of a tree in source order, or children first.
pub(super) fn order<S: Store>(tree: &Tree<S>, post: bool) -> Vec<u32> {
    let root = tree.root();
    let mut all: Vec<u32> = std::iter::once(root.index())
        .chain(root.descendants().map(|c| c.index()))
        .collect();
    if post {
        all.reverse();
    }
    all
}
