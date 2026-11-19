//! Compiles the rule files into phases, and runs a phase's rules on trees.
//! A phase rewrites, emits edges, sets tags or emits rows; rules are
//! `pattern: value` pairs, and definitions are named expressions.

use std::cell::RefCell;
use std::collections::{BTreeMap, HashMap};

use serde_json::Value;
use smallvec::SmallVec;

use super::expr::{Expr, IndexSpec, Scope};
use super::lang::{DataSpec, DefBody, Pair, RuleFiles, StageSpec};
use super::pattern::{self, Cap, Pat, Slots, Tpl};
use indextree::NodeId;

use super::{Cursor, FnId, Kind, Loc, Run, Store, Tree, Val, Vals};
use crate::canonical::Canonical as C;
use crate::env::Env;
use crate::error::LoadError;
use crate::intern::Lang;
use crate::sentinel::{Killed, Sentinel};
use crate::tree::{CallResolution, Edge, EdgeKind, Mutable, Storage};

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct PhaseSpec {
    name: String,
    over: String,
    does: String,
    #[serde(default)]
    until: Option<String>,
    #[serde(default)]
    bind: BTreeMap<String, String>,
    #[serde(default)]
    budget: BudgetSpec,
}

/// The budgets of a phase, by `Limits` field name: one per tree, and one for
/// the whole scope.
#[derive(serde::Deserialize, Default, Clone)]
#[serde(deny_unknown_fields)]
pub struct BudgetSpec {
    pub label: Option<String>,
    pub tree: Option<String>,
    pub scope: Option<String>,
}

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct PipelineSpec {
    scopes: Vec<String>,
    phases: Vec<PhaseSpec>,
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum Walk {
    Flow,
    Each,
    Fixpoint,
}

/// What a phase's rules produce.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Does {
    Rewrite,
    Edges,
    Tags,
    Rows,
}

pub fn resolution(name: &str) -> Option<CallResolution> {
    match name {
        "reference" => Some(CallResolution::Reference),
        "callable" => Some(CallResolution::Callable),
        "non_callable" => Some(CallResolution::NonCallable),
        _ => None,
    }
}

struct Match {
    pat: Pat,
    depth: u8,
    kinds: Vec<u16>,
    slots: Slots,
    when: Option<Expr>,
    lang: Option<u8>,
}

/// One edge a pair emits: from the innermost enclosing scope unless it says
/// otherwise, at the site it names, if any.
struct Emit {
    kind: EdgeKind,
    from: Option<Expr>,
    to: Expr,
    site: Option<Expr>,
    resolution: CallResolution,
}

/// The rows one pair emits: its table, its column names, and how its id is
/// made. Values arrive in column order, then the extra id parts.
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

struct RowRule {
    shape: u16,
    exprs: Box<[Expr]>,
}

enum Out {
    Replace(Tpl<Expr>),
    Append(Vec<Tpl<Expr>>),
    Tag,
}

enum TagVal {
    Lit(u32),
    Expr(Expr),
}

/// A rewrite or tag pair: its template, and the tags it sets on the node it
/// builds or on the first node of `tag_on`'s kind under it.
struct Rewrite {
    at: Match,
    out: Out,
    tags: Vec<(u32, TagVal)>,
    tag_on: Option<u16>,
    unique: Option<(Pat, u16, usize)>,
}

/// `by_kind` lists, per node kind, the rules a node of that kind tries in
/// order: the kind's own rules merged with `any`, the rules for every kind.
struct Stage {
    rules: Vec<Rewrite>,
    by_kind: HashMap<u16, Vec<u16>>,
    any: Vec<u16>,
    repeat: bool,
}

/// A group's pairs, tried in order on each node; the first that matches
/// emits every edge its value names.
struct EdgeRules {
    rules: Vec<(Match, Vec<Emit>)>,
    by_kind: HashMap<u16, Vec<u16>>,
    any: Vec<u16>,
}

/// A declared index compiled for one language, or for all.
struct Indexer {
    at: Match,
    id: u32,
    key: Expr,
}

enum Body {
    Rewrite(Vec<Vec<Stage>>),
    Edges(Vec<EdgeRules>),
    Tags(Vec<Vec<Stage>>),
    Rows(Vec<Vec<(Match, Vec<RowRule>)>>),
}

/// A named expression; with cases, tried on its first argument in order,
/// the first match with a non-empty value wins.
pub struct Def {
    params: usize,
    body: DefImpl,
    pub(super) deps: u8,
    slots: usize,
}

enum DefImpl {
    Expr(Expr),
    Cases(Vec<(Match, Expr)>),
}

impl Def {
    pub(super) fn key<S: Store>(&self, _: &mut Run<S>, args: &[Vals]) -> SmallVec<[Val; 4]> {
        let mut key = SmallVec::new();
        for vals in args {
            key.push(Val::Sym(vals.len() as u32));
            key.extend_from_slice(vals);
        }
        key
    }

    pub(super) fn eval<S: Store>(&self, run: &mut Run<S>, args: &[Vals]) -> Vals {
        match &self.body {
            DefImpl::Expr(expr) => {
                let mut bound: Vec<Vals> = vec![Vals::new(); self.slots.max(self.params)];
                bound[..args.len()].clone_from_slice(args);
                expr.eval(run, &bound)
            }
            DefImpl::Cases(cases) => {
                let Some(head) = args[0].first().and_then(|v| v.loc()) else {
                    return Vals::new();
                };
                let c = Cursor::new(run.trees, head.fi, head.node);
                let mut w = Scratch::<S>::new(self.slots.max(1));
                for (at, value) in cases {
                    if !w.matches(run, at, c) {
                        continue;
                    }
                    w.vals[1..args.len()].clone_from_slice(&args[1..]);
                    if !w.holds(run, at) {
                        continue;
                    }
                    if matches!(value, Expr::Empty) {
                        return Vals::new();
                    }
                    let found = value.eval(run, &w.vals);
                    if !found.is_empty() {
                        return found;
                    }
                }
                Vals::new()
            }
        }
    }
}

pub struct Phase {
    pub name: String,
    pub label: &'static str,
    pub budget: BudgetSpec,
    pub scope: String,
    pub walk: Walk,
    pub(super) modules: Vec<u8>,
    pub(super) links: super::Links,
    pub(super) data: Vec<Vec<Vals>>,
    pub(super) defs: Vec<Def>,
    /// Per language: a redefined definition's id to the language's own.
    pub(super) overrides: Vec<HashMap<u16, u16>>,
    pub(super) index_count: usize,
    pub(super) enclosing: Vec<u16>,
    pub rows: Vec<RowShape>,
    indexers: Vec<Indexer>,
    body: Body,
    slots: usize,
}

pub struct Pipeline {
    pub phases: Vec<Phase>,
    settings: Vec<BTreeMap<String, DataSpec>>,
}

impl Pipeline {
    /// `languages` holds each language's rule file, by its index in the env.
    pub fn load(lang: &Lang, languages: &[Option<&'static str>]) -> Result<Self, LoadError> {
        let spec: PipelineSpec =
            orbit_utils::yaml::from_str(include_str!("../../config/pipeline.yaml"))?;
        let files = super::lang::load(languages)?;
        let names: HashMap<String, u16> = files
            .shared
            .data
            .keys()
            .enumerate()
            .map(|(at, name)| (name.clone(), at as u16))
            .collect();
        let values = data_values(&files, lang)?;
        let enclosing: Vec<u16> = spec.scopes.iter().map(|k| lang.intern_kind(k)).collect();
        let indexes = RefCell::new(Vec::new());
        let mut phases = spec
            .phases
            .iter()
            .map(|p| {
                let mut phase = Phase::load(p, &files, &names, lang, &indexes)?;
                phase.data = values.clone();
                phase.enclosing = enclosing.clone();
                Ok(phase)
            })
            .collect::<Result<Vec<_>, LoadError>>()?;
        let indexes = indexes.into_inner();
        for (p, phase) in spec.phases.iter().zip(&mut phases) {
            phase.index_count = indexes.len();
            phase.indexers = Phase::indexers(p, &files, &names, lang, &indexes)?;
        }
        let settings = files
            .langs
            .iter()
            .map(|file| {
                let mut merged = files.shared.data.clone();
                if let Some(file) = file {
                    merged.extend(file.data.clone());
                }
                merged
            })
            .collect();
        Ok(Self { phases, settings })
    }

    /// A language's setting as written: its own value, or the shared default.
    pub fn setting(&self, rules: usize, name: &str) -> Option<&DataSpec> {
        self.settings.get(rules)?.get(name)
    }

    pub fn phase(&self, name: &str) -> &Phase {
        self.phases
            .iter()
            .find(|p| p.name == name)
            .unwrap_or_else(|| panic!("no phase {name} in pipeline.yaml"))
    }
}

fn data_values(files: &RuleFiles, lang: &Lang) -> Result<Vec<Vec<Vals>>, LoadError> {
    files
        .langs
        .iter()
        .map(|file| {
            let own = file.as_ref().map(|f| &f.data);
            if let Some(name) = own
                .into_iter()
                .flat_map(BTreeMap::keys)
                .find(|n| !files.shared.data.contains_key(*n))
            {
                return Err(LoadError(format!(
                    "data {name} is not declared in the shared rules"
                )));
            }
            Ok(files
                .shared
                .data
                .iter()
                .map(|(name, shared)| {
                    let mut vals = Vals::new();
                    data_vals(
                        own.and_then(|d| d.get(name)).unwrap_or(shared),
                        lang,
                        &mut vals,
                    );
                    vals
                })
                .collect())
        })
        .collect()
}

fn data_vals(spec: &DataSpec, lang: &Lang, out: &mut Vals) {
    match spec {
        DataSpec::Bool(b) => out.push(Val::Bool(*b)),
        DataSpec::Num(n) => out.push(Val::Sym(*n)),
        DataSpec::Str(s) => out.push(match s.parse::<C>() {
            Ok(kind) => Val::Kind(kind as u16),
            Err(_) => Val::Sym(lang.syms.intern(s)),
        }),
        DataSpec::List(items) => items.iter().for_each(|i| data_vals(i, lang, out)),
    }
}

fn data_texts(spec: &DataSpec, out: &mut Vec<String>) {
    match spec {
        DataSpec::Bool(b) => out.push(b.to_string()),
        DataSpec::Num(n) => out.push(n.to_string()),
        DataSpec::Str(s) => out.push(s.clone()),
        DataSpec::List(items) => items.iter().for_each(|i| data_texts(i, out)),
    }
}

fn does_of(spec: &PhaseSpec) -> Result<(Does, Walk), LoadError> {
    let does = match spec.does.as_str() {
        "rewrite" => Does::Rewrite,
        "edges" => Does::Edges,
        "tags" => Does::Tags,
        "rows" => Does::Rows,
        other => {
            return Err(LoadError(format!(
                "phase {}: unknown does: {other}",
                spec.name
            )));
        }
    };
    let walk = match (does, spec.until.as_deref()) {
        (Does::Rewrite, _) => Walk::Fixpoint,
        (_, Some("stable")) => Walk::Fixpoint,
        (Does::Tags, None) => Walk::Each,
        (_, None) => Walk::Flow,
        (_, Some(other)) => {
            return Err(LoadError(format!(
                "phase {}: unknown until: {other}",
                spec.name
            )));
        }
    };
    let scope = match spec.over.as_str() {
        "tree" | "project" | "family" | "graph" => spec.over.as_str(),
        other => {
            return Err(LoadError(format!(
                "phase {}: unknown over: {other}",
                spec.name
            )));
        }
    };
    let _ = scope;
    Ok((does, walk))
}

struct Loader<'a> {
    modules: Vec<String>,
    links: &'a super::Links,
    names: &'a HashMap<String, u16>,
    defs: HashMap<String, (u16, usize)>,
    data: &'a BTreeMap<String, DataSpec>,
    lang_data: Vec<BTreeMap<String, DataSpec>>,
    lang: &'a Lang,
    indexes: &'a RefCell<Vec<IndexSpec>>,
    rows: RefCell<Vec<RowShape>>,
}

fn all_modules() -> Vec<String> {
    super::modules::ALL
        .iter()
        .map(|m| m.name.to_owned())
        .collect()
}

impl Phase {
    fn load(
        spec: &PhaseSpec,
        files: &RuleFiles,
        names: &HashMap<String, u16>,
        lang: &Lang,
        indexes: &RefCell<Vec<IndexSpec>>,
    ) -> Result<Self, LoadError> {
        let (does, walk) = does_of(spec)?;
        let (binds, def_binds) = binds(spec, &files.shared.defs)?;
        let links = super::Links::load(&binds, &def_binds)?;
        let shared_defs = &files.shared.defs;
        let mut defs: HashMap<String, (u16, usize)> = HashMap::new();
        for (at, d) in shared_defs.iter().enumerate() {
            if defs
                .insert(d.name.clone(), (at as u16, d.params.len()))
                .is_some()
            {
                return Err(LoadError(format!("{}: define {} twice", d.origin, d.name)));
            }
        }
        let loader = Loader {
            modules: all_modules(),
            links: &links,
            names,
            defs,
            data: &files.shared.data,
            lang_data: files
                .langs
                .iter()
                .map(|file| {
                    let mut merged = files.shared.data.clone();
                    if let Some(file) = file {
                        merged.extend(file.data.clone());
                    }
                    merged
                })
                .collect(),
            lang,
            indexes,
            rows: RefCell::new(Vec::new()),
        };
        let mut compiled: Vec<Def> = Vec::new();
        for d in shared_defs {
            compiled.push(loader.def(d, None, None)?);
        }
        let mut overrides = Vec::new();
        for (at, file) in files.langs.iter().enumerate() {
            let mut own = HashMap::new();
            for d in file.iter().flat_map(|f| &f.defs) {
                let &(shared, arity) = loader.defs.get(&d.name).ok_or_else(|| {
                    LoadError(format!(
                        "{}: define {} redefines nothing shared",
                        d.origin, d.name
                    ))
                })?;
                if arity != d.params.len() {
                    return Err(LoadError(format!(
                        "{}: define {} takes {arity} parameters",
                        d.origin, d.name
                    )));
                }
                compiled.push(loader.def(d, Some(at as u8), Some((shared, arity)))?);
                own.insert(shared, (compiled.len() - 1) as u16);
            }
            overrides.push(own);
        }
        let deps = loader.deps(&compiled, shared_defs, files);
        for (def, d) in compiled.iter_mut().zip(deps) {
            def.deps = d;
        }
        let stages_of = |rules: Option<&Vec<StageSpec>>| rules.cloned().unwrap_or_default();
        let shared = stages_of(files.shared.phases.get(&spec.name));
        let langs: Vec<Vec<StageSpec>> = files
            .langs
            .iter()
            .map(|f| stages_of(f.as_ref().and_then(|f| f.phases.get(&spec.name))))
            .collect();
        let body = match does {
            Does::Rewrite | Does::Tags => {
                let per_lang = langs
                    .iter()
                    .enumerate()
                    .map(|(at, own)| {
                        own.iter()
                            .chain(&shared)
                            .map(|s| loader.stage(s, at as u8, does == Does::Tags))
                            .collect::<Result<Vec<_>, _>>()
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                match does {
                    Does::Rewrite => Body::Rewrite(per_lang),
                    _ => Body::Tags(per_lang),
                }
            }
            Does::Edges => Body::Edges(loader.edges(&shared, &langs)?),
            Does::Rows => Body::Rows(loader.rows_of(&shared, &langs)?),
        };
        let slots = match &body {
            Body::Rewrite(s) | Body::Tags(s) => s
                .iter()
                .flatten()
                .flat_map(|s| &s.rules)
                .map(|r| r.at.slots.len())
                .max(),
            Body::Edges(groups) => groups
                .iter()
                .flat_map(|k| &k.rules)
                .map(|(m, _)| m.slots.len())
                .max(),
            Body::Rows(groups) => groups.iter().flatten().map(|(m, _)| m.slots.len()).max(),
        }
        .unwrap_or(1);
        let used = modules_reached(&body, &compiled);
        let bound: Vec<FnId> = binds
            .iter()
            .map(|b| b.0)
            .chain(def_binds.iter().map(|b| b.0))
            .collect();
        check_binds(spec, &bound, &used)?;
        let label = spec
            .budget
            .label
            .clone()
            .unwrap_or_else(|| spec.name.clone());
        let rows = loader.rows.take();
        drop(loader);
        Ok(Phase {
            name: spec.name.clone(),
            label: Box::leak(label.into_boxed_str()),
            budget: spec.budget.clone(),
            scope: spec.over.clone(),
            walk,
            modules: used,
            links,
            data: Vec::new(),
            defs: compiled,
            overrides,
            index_count: 0,
            enclosing: Vec::new(),
            rows,
            indexers: Vec::new(),
            body,
            slots,
        })
    }

    /// Every declared index, compiled for this phase's languages.
    fn indexers(
        spec: &PhaseSpec,
        files: &RuleFiles,
        names: &HashMap<String, u16>,
        lang: &Lang,
        indexes: &[IndexSpec],
    ) -> Result<Vec<Indexer>, LoadError> {
        let (binds, def_binds) = binds(spec, &files.shared.defs)?;
        let links = super::Links::load(&binds, &def_binds)?;
        let registry = RefCell::new(indexes.to_vec());
        let loader = Loader {
            modules: all_modules(),
            links: &links,
            names,
            defs: files
                .shared
                .defs
                .iter()
                .enumerate()
                .map(|(at, d)| (d.name.clone(), (at as u16, d.params.len())))
                .collect(),
            data: &files.shared.data,
            lang_data: files
                .langs
                .iter()
                .map(|file| {
                    let mut merged = files.shared.data.clone();
                    if let Some(file) = file {
                        merged.extend(file.data.clone());
                    }
                    merged
                })
                .collect(),
            lang,
            indexes: &registry,
            rows: RefCell::new(Vec::new()),
        };
        let mut out = Vec::new();
        for (id, index) in indexes.iter().enumerate() {
            let owners: Vec<Option<u8>> = match index.pattern.contains('%') {
                true => (0..files.langs.len()).map(|at| Some(at as u8)).collect(),
                false => vec![None],
            };
            for owner in owners {
                let at = loader
                    .matcher(&index.pattern, None, owner, &[])
                    .map_err(|e| LoadError(format!("index {}: {}", index.pattern, e.0)))?;
                let key = match &index.key {
                    Some(src) => Expr::parse(
                        src,
                        &loader.scope(&RefCell::new(at.slots.clone()), false, None),
                    )?,
                    None if at.slots.len() > 1 => Expr::Var(1),
                    None => Expr::Var(0),
                };
                out.push(Indexer {
                    at,
                    id: id as u32,
                    key,
                });
            }
        }
        Ok(out)
    }
}

impl Body {
    fn calls(&self, f: &mut dyn FnMut(Result<FnId, u16>)) {
        let on_match = |m: &Match, f: &mut dyn FnMut(Result<FnId, u16>)| {
            if let Some(w) = &m.when {
                w.calls(f);
            }
        };
        match self {
            Body::Rewrite(stages) | Body::Tags(stages) => {
                for r in stages.iter().flatten().flat_map(|s| &s.rules) {
                    on_match(&r.at, f);
                    for (_, v) in &r.tags {
                        if let TagVal::Expr(e) = v {
                            e.calls(f);
                        }
                    }
                }
            }
            Body::Edges(groups) => {
                for (m, emits) in groups.iter().flat_map(|k| &k.rules) {
                    on_match(m, f);
                    for e in emits {
                        e.to.calls(f);
                        if let Some(from) = &e.from {
                            from.calls(f);
                        }
                        if let Some(site) = &e.site {
                            site.calls(f);
                        }
                    }
                }
            }
            Body::Rows(groups) => {
                for (m, rs) in groups.iter().flatten() {
                    on_match(m, f);
                    rs.iter()
                        .flat_map(|r| r.exprs.iter())
                        .for_each(|e| e.calls(f));
                }
            }
        }
    }
}

impl Def {
    fn calls(&self, f: &mut dyn FnMut(Result<FnId, u16>)) {
        match &self.body {
            DefImpl::Expr(e) => e.calls(f),
            DefImpl::Cases(cases) => cases.iter().for_each(|(m, e)| {
                if let Some(w) = &m.when {
                    w.calls(f);
                }
                e.calls(f);
            }),
        }
    }
}

type Binds = (Vec<(FnId, FnId)>, Vec<(FnId, u16)>);

/// `module.function: provider`, where the provider is another module's
/// function or the name of a definition.
fn binds(spec: &PhaseSpec, defs: &[super::lang::DefSpec]) -> Result<Binds, LoadError> {
    let id = |path: &str| {
        let (module, name) = path
            .split_once('.')
            .ok_or_else(|| LoadError(format!("bind {path}: expected <module>.<function>")))?;
        super::lookup(module, name).ok_or_else(|| LoadError(format!("bind {path}: unknown")))
    };
    let (mut binds, mut to_defs) = (Vec::new(), Vec::new());
    for (of, to) in &spec.bind {
        let of = id(of)?;
        let declared = super::function(of);
        if !declared.required {
            return Err(LoadError(format!(
                "bind {}: only a required function is bound",
                declared.name
            )));
        }
        if !to.contains('.') {
            let at = defs
                .iter()
                .position(|d| &d.name == to)
                .ok_or_else(|| LoadError(format!("bind {}: no definition {to}", declared.name)))?;
            if defs[at].params.len() != declared.sig.len() {
                return Err(LoadError(format!(
                    "bind {}: {to} takes {} parameters",
                    declared.name,
                    defs[at].params.len()
                )));
            }
            to_defs.push((of, at as u16));
            continue;
        }
        let to = id(to)?;
        let provider = super::function(to);
        if provider.required {
            return Err(LoadError(format!(
                "bind {}: binds to a required function",
                declared.name
            )));
        }
        let kinds = matches!(
            (declared.kind, provider.kind),
            (Kind::Flow, Kind::Flow | Kind::Pure) | (Kind::Pure, Kind::Pure)
        );
        if declared.sig != provider.sig || !kinds {
            return Err(LoadError(format!(
                "bind {}: signatures differ",
                declared.name
            )));
        }
        binds.push((of, to));
    }
    Ok((binds, to_defs))
}

/// The modules a phase's rules call, directly or through the definitions
/// they reach.
fn modules_reached(body: &Body, defs: &[Def]) -> Vec<u8> {
    let mut calls: Vec<Result<FnId, u16>> = Vec::new();
    body.calls(&mut |c| calls.push(c));
    let mut seen = vec![false; defs.len()];
    let mut used: Vec<u8> = Vec::new();
    while let Some(c) = calls.pop() {
        match c {
            Ok(id) => {
                let slot = super::function(id).slot;
                if !used.contains(&slot) {
                    used.push(slot);
                }
            }
            Err(def) => {
                let def = usize::from(def);
                if def < defs.len() && !seen[def] {
                    seen[def] = true;
                    defs[def].calls(&mut |c| calls.push(c));
                }
            }
        }
    }
    used.sort_unstable();
    used
}

/// Every module a phase calls has its required functions bound.
fn check_binds(spec: &PhaseSpec, bound: &[FnId], used: &[u8]) -> Result<(), LoadError> {
    for &slot in used {
        let module = super::modules::ALL[usize::from(slot)].name;
        if let Some(id) = super::required(module).find(|id| !bound.contains(id)) {
            return Err(LoadError(format!(
                "phase {}: {module}.{} needs a bind",
                spec.name,
                super::function(id).name
            )));
        }
    }
    Ok(())
}

impl Loader<'_> {
    fn scope<'s>(
        &'s self,
        slots: &'s RefCell<Slots>,
        effects: bool,
        shared: Option<(u16, usize)>,
    ) -> Scope<'s> {
        Scope {
            slots,
            modules: &self.modules,
            links: self.links,
            lang: self.lang,
            data: self.names,
            defs: &self.defs,
            indexes: self.indexes,
            effects,
            shared,
        }
    }

    fn expr(&self, src: &str, slots: &Slots) -> Result<Expr, LoadError> {
        let cell = RefCell::new(slots.clone());
        Expr::parse(src, &self.scope(&cell, true, None))
    }

    fn matcher(
        &self,
        pattern: &str,
        when: Option<&str>,
        lang: Option<u8>,
        params: &[String],
    ) -> Result<Match, LoadError> {
        let mut slots = Slots::params(params);
        let values = match lang {
            Some(l) => &self.lang_data[usize::from(l)],
            None => self.data,
        };
        let data = |name: &str| {
            let spec = values.get(name)?;
            let mut out = Vec::new();
            data_texts(spec, &mut out);
            Some(out)
        };
        let pat = pattern::parse_pattern(pattern, &mut slots, self.lang, &data)?;
        let (depth, filter) = pattern::anchor_kinds(&pat)
            .ok_or_else(|| LoadError("must match a node kind or capture $ROOT".into()))?;
        let kinds = match filter.kinds.is_empty() || filter.negate {
            true => vec![0],
            false => filter.kinds.clone(),
        };
        let when = when.map(|src| self.expr(src, &slots)).transpose()?;
        Ok(Match {
            pat,
            depth,
            kinds,
            slots,
            when,
            lang,
        })
    }

    fn def(
        &self,
        d: &super::lang::DefSpec,
        lang: Option<u8>,
        shared: Option<(u16, usize)>,
    ) -> Result<Def, LoadError> {
        let at = |e: LoadError| LoadError(format!("{}: define {}: {}", d.origin, d.name, e.0));
        let params = Slots::params(&d.params);
        match &d.body {
            DefBody::Expr(src) => {
                let cell = RefCell::new(params);
                let expr = Expr::parse(src, &self.scope(&cell, false, shared)).map_err(at)?;
                let slots = cell.borrow().len();
                Ok(Def {
                    params: d.params.len(),
                    body: DefImpl::Expr(expr),
                    deps: 0,
                    slots,
                })
            }
            DefBody::Cases(cases) => {
                let mut compiled = Vec::new();
                let mut slots = d.params.len().max(1);
                for (pattern, when, value) in cases {
                    let pattern = if pattern == "_" {
                        "(_)"
                    } else {
                        pattern.as_str()
                    };
                    let m = self
                        .matcher(pattern, when.as_deref(), lang, &d.params)
                        .map_err(at)?;
                    let cell = RefCell::new(m.slots.clone());
                    let value =
                        Expr::parse(value, &self.scope(&cell, false, shared)).map_err(at)?;
                    slots = slots.max(cell.borrow().len());
                    compiled.push((m, value));
                }
                Ok(Def {
                    params: d.params.len(),
                    body: DefImpl::Cases(compiled),
                    deps: 0,
                    slots,
                })
            }
        }
    }

    /// Each definition's dependencies: the states of every module function
    /// it can reach through other definitions.
    fn deps(&self, defs: &[Def], shared: &[super::lang::DefSpec], _: &RuleFiles) -> Vec<u8> {
        let _ = shared;
        let mut calls: Vec<Vec<Result<FnId, u16>>> = Vec::new();
        for def in defs {
            let mut used = Vec::new();
            def.calls(&mut |c| used.push(c));
            calls.push(used);
        }
        let mut deps: Vec<u8> = calls
            .iter()
            .map(|used| {
                used.iter()
                    .filter_map(|c| c.ok())
                    .fold(0, |acc, id| acc | self.links.deps_of(id))
            })
            .collect();
        loop {
            let mut changed = false;
            for (at, used) in calls.iter().enumerate() {
                for &c in used {
                    if let Err(def) = c {
                        let add = deps[usize::from(def)];
                        if deps[at] | add != deps[at] {
                            deps[at] |= add;
                            changed = true;
                        }
                    }
                }
            }
            if !changed {
                return deps;
            }
        }
    }

    fn stage(&self, spec: &StageSpec, lang: u8, tags_only: bool) -> Result<Stage, LoadError> {
        let mut rules = Vec::new();
        let mut by_kind: HashMap<u16, Vec<u16>> = HashMap::new();
        for pair in &spec.pairs {
            let at =
                |e: LoadError| LoadError(format!("{}: {:?}: {}", pair.origin, pair.pattern, e.0));
            let rule = self.rewrite(pair, lang).map_err(at)?;
            if tags_only && !matches!(rule.out, Out::Tag) {
                return Err(at(LoadError("a tags phase only sets tags".into())));
            }
            if spec.repeat && matches!(rule.out, Out::Append(_)) {
                return Err(at(LoadError("a repeat stage cannot append".into())));
            }
            for &kind in &rule.at.kinds {
                by_kind.entry(kind).or_default().push(rules.len() as u16);
            }
            rules.push(rule);
        }
        let any = by_kind.remove(&0).unwrap_or_default();
        for list in by_kind.values_mut() {
            list.extend(&any);
            list.sort_unstable();
            list.dedup();
        }
        Ok(Stage {
            rules,
            by_kind,
            any,
            repeat: spec.repeat,
        })
    }

    /// A rewrite or tag pair. Its value is `[template | + t1 + t2] [unique P]
    /// [#key[@kind]=value]...`.
    fn rewrite(&self, pair: &Pair, lang: u8) -> Result<Rewrite, LoadError> {
        let Value::String(value) = &pair.value else {
            return Err(LoadError("a rewrite is pattern: template".into()));
        };
        let at = self.matcher(&pair.pattern, pair.when.as_deref(), Some(lang), &[])?;
        let (body, notes) = split_notes(value);
        let (body, unique) = match body.find(" unique ") {
            Some(i) => (body[..i].trim(), Some(body[i + " unique ".len()..].trim())),
            None => (body.trim(), None),
        };
        let cell = RefCell::new(at.slots.clone());
        let scope = self.scope(&cell, false, None);
        let mut text = |src: &str| Expr::parse(src, &scope);
        let out = if body.is_empty() {
            Out::Tag
        } else if let Some(rest) = body.strip_prefix('+') {
            Out::Append(
                rest.split(" + ")
                    .map(|t| pattern::parse_template(t.trim(), &at.slots, self.lang, &mut text))
                    .collect::<Result<_, _>>()?,
            )
        } else {
            Out::Replace(pattern::parse_template(
                body, &at.slots, self.lang, &mut text,
            )?)
        };
        let mut tag_on = None;
        let mut tags = Vec::new();
        for (key, kind, val) in notes {
            if let Some(kind) = kind {
                let kind = self.lang.intern_kind(kind);
                if tag_on.replace(kind).is_some_and(|k| k != kind) {
                    return Err(LoadError("tags go on one kind".into()));
                }
            }
            let val = match val.starts_with('$') || val.starts_with("@$") {
                true => TagVal::Expr(Expr::parse(val, &scope)?),
                false => TagVal::Lit(self.lang.syms.intern(val.trim_matches('\''))),
            };
            tags.push((self.lang.syms.intern(key), val));
        }
        let unique = unique
            .map(|src| {
                let mut slots = Slots::new();
                let pat = pattern::parse_pattern(src, &mut slots, self.lang, &|_| None)?;
                let (_, kind) =
                    pattern::anchor(&pat).ok_or_else(|| LoadError(format!("unique {src:?}")))?;
                Ok::<_, LoadError>((pat, kind, slots.len()))
            })
            .transpose()?;
        Ok(Rewrite {
            at,
            out,
            tags,
            tag_on,
            unique,
        })
    }

    /// The edge groups: a language's pairs in a group come before the
    /// shared ones.
    fn edges(
        &self,
        shared: &[StageSpec],
        langs: &[Vec<StageSpec>],
    ) -> Result<Vec<EdgeRules>, LoadError> {
        let mut groups: Vec<(&str, Vec<(Option<u8>, &Pair)>)> = Vec::new();
        let staged = langs
            .iter()
            .enumerate()
            .flat_map(|(at, own)| own.iter().map(move |s| (s, Some(at as u8))))
            .chain(shared.iter().map(|s| (s, None)));
        for (stage, owner) in staged {
            let at = match groups.iter().position(|(g, _)| *g == stage.name) {
                Some(at) => at,
                None => {
                    groups.push((&stage.name, Vec::new()));
                    groups.len() - 1
                }
            };
            groups[at].1.extend(stage.pairs.iter().map(|p| (owner, p)));
        }
        let mut out = Vec::new();
        for (_, pairs) in groups {
            let mut rules = Vec::new();
            let mut by_kind: HashMap<u16, Vec<u16>> = HashMap::new();
            for (owner, pair) in pairs {
                let owners: Vec<Option<u8>> = match (owner, pair.pattern.contains('%')) {
                    (None, true) => (0..langs.len()).map(|at| Some(at as u8)).collect(),
                    (owner, _) => vec![owner],
                };
                for owner in owners {
                    let at = |e: LoadError| {
                        LoadError(format!("{}: {:?}: {}", pair.origin, pair.pattern, e.0))
                    };
                    let m = self
                        .matcher(&pair.pattern, pair.when.as_deref(), owner, &[])
                        .map_err(at)?;
                    let emits = self.emits(&pair.value, &m.slots).map_err(at)?;
                    for &k in &m.kinds {
                        by_kind.entry(k).or_default().push(rules.len() as u16);
                    }
                    rules.push((m, emits));
                }
            }
            let any = by_kind.remove(&0).unwrap_or_default();
            for list in by_kind.values_mut() {
                list.extend(&any);
                list.sort_unstable();
                list.dedup();
            }
            out.push(EdgeRules {
                rules,
                by_kind,
                any,
            });
        }
        Ok(out)
    }

    /// `{ kind: targets, ... }`; targets are an expression, a map with
    /// `to`/`from`/`site`/`resolution`, or a list of those. `none` stops the
    /// group without emitting.
    fn emits(&self, value: &Value, slots: &Slots) -> Result<Vec<Emit>, LoadError> {
        let map = match value {
            Value::Object(map) => map,
            Value::String(s) if s == "none" => return Ok(Vec::new()),
            _ => return Err(LoadError("an edge pair maps edge kinds to targets".into())),
        };
        let mut out = Vec::new();
        for (kind, spec) in map {
            let kind = super::expr::edge_kind(kind)
                .ok_or_else(|| LoadError(format!("unknown edge kind {kind}")))?;
            match spec {
                Value::Array(items) => {
                    for item in items {
                        out.push(self.emit(kind, item, slots)?);
                    }
                }
                one => out.push(self.emit(kind, one, slots)?),
            }
        }
        Ok(out)
    }

    fn emit(&self, kind: EdgeKind, value: &Value, slots: &Slots) -> Result<Emit, LoadError> {
        fn text(v: Option<&Value>) -> Option<&str> {
            match v {
                Some(Value::String(s)) => Some(s.as_str()),
                _ => None,
            }
        }
        let (to, from, site, res) = match value {
            Value::String(to) => (to.as_str(), None, None, None),
            Value::Object(map) => {
                if let Some(key) = map
                    .keys()
                    .find(|k| !matches!(k.as_str(), "to" | "from" | "site" | "resolution"))
                {
                    return Err(LoadError(format!("unknown edge field {key}")));
                }
                (
                    text(map.get("to")).ok_or_else(|| LoadError("an edge needs to:".into()))?,
                    text(map.get("from")),
                    text(map.get("site")),
                    text(map.get("resolution")),
                )
            }
            _ => {
                return Err(LoadError(
                    "an edge is an expression or { to, from, site, resolution }".into(),
                ));
            }
        };
        Ok(Emit {
            kind,
            to: self.expr(to, slots)?,
            from: from.map(|f| self.expr(f, slots)).transpose()?,
            site: site.map(|s| self.expr(s, slots)).transpose()?,
            resolution: match res {
                Some(name) => resolution(name)
                    .ok_or_else(|| LoadError(format!("unknown resolution {name}")))?,
                None => CallResolution::default(),
            },
        })
    }

    /// The row groups: a language's pairs in a group come before the shared
    /// ones; the first pair of a group that matches emits its rows.
    fn rows_of(
        &self,
        shared: &[StageSpec],
        langs: &[Vec<StageSpec>],
    ) -> Result<Vec<Vec<(Match, Vec<RowRule>)>>, LoadError> {
        let mut groups: Vec<(&str, Vec<(Match, Vec<RowRule>)>)> = Vec::new();
        let staged = langs
            .iter()
            .enumerate()
            .flat_map(|(at, own)| own.iter().map(move |s| (s, Some(at as u8))))
            .chain(shared.iter().map(|s| (s, None)));
        for (stage, owner) in staged {
            let at = match groups.iter().position(|(g, _)| *g == stage.name) {
                Some(at) => at,
                None => {
                    groups.push((&stage.name, Vec::new()));
                    groups.len() - 1
                }
            };
            for pair in &stage.pairs {
                let err = |e: LoadError| {
                    LoadError(format!("{}: {:?}: {}", pair.origin, pair.pattern, e.0))
                };
                let m = self
                    .matcher(&pair.pattern, pair.when.as_deref(), owner, &[])
                    .map_err(err)?;
                let specs: Vec<RowSpec> = match &pair.value {
                    Value::String(s) if s == "none" => Vec::new(),
                    Value::Array(_) => serde_json::from_value(pair.value.clone())
                        .map_err(|e| err(LoadError(format!("row: {e}"))))?,
                    _ => vec![
                        serde_json::from_value(pair.value.clone())
                            .map_err(|e| err(LoadError(format!("row: {e}"))))?,
                    ],
                };
                let rules = specs
                    .iter()
                    .map(|s| self.row(&m.slots, s))
                    .collect::<Result<_, _>>()
                    .map_err(err)?;
                groups[at].1.push((m, rules));
            }
        }
        Ok(groups.into_iter().map(|(_, rules)| rules).collect())
    }

    fn row(&self, slots: &Slots, spec: &RowSpec) -> Result<RowRule, LoadError> {
        let columns: Vec<String> = spec.columns.keys().cloned().collect();
        let mut exprs = spec
            .columns
            .values()
            .map(|src| self.expr(src, slots))
            .collect::<Result<Vec<_>, _>>()?;
        let column = |name: &str| {
            columns
                .iter()
                .position(|c| c == name)
                .ok_or_else(|| LoadError(format!("row {}: no column {name}", spec.table)))
        };
        let id = match &spec.id {
            None => None,
            Some(id) => {
                let mut parts = Vec::new();
                for part in &id.parts {
                    parts.push(match columns.iter().position(|c| c == part) {
                        Some(at) => Part::Column(at),
                        None => {
                            exprs.push(self.expr(part, slots)?);
                            Part::Value(exprs.len() - 1)
                        }
                    });
                }
                Some((id.seed.clone(), parts))
            }
        };
        let expect = spec
            .expect
            .iter()
            .map(|(name, table)| Ok((column(name)?, table.clone())))
            .collect::<Result<_, LoadError>>()?;
        let follows = spec.follows.as_deref().map(column).transpose()?;
        let mut rows = self.rows.borrow_mut();
        rows.push(RowShape {
            table: spec.table.clone(),
            columns,
            id,
            expect,
            follows,
        });
        Ok(RowRule {
            shape: u16::try_from(rows.len() - 1)
                .map_err(|_| LoadError("more than 65536 row shapes".into()))?,
            exprs: exprs.into(),
        })
    }
}

#[derive(Debug, Clone, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct RowSpec {
    table: String,
    id: Option<IdSpec>,
    columns: BTreeMap<String, String>,
    #[serde(default)]
    expect: BTreeMap<String, String>,
    follows: Option<String>,
}

#[derive(Debug, Clone, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct IdSpec {
    seed: String,
    parts: Vec<String>,
}

/// Splits `template #key=value #key@kind=value` into the template and its
/// annotations. An annotation starts at a top-level ` #` or at the start.
fn split_notes(value: &str) -> (&str, Vec<(&str, Option<&str>, &str)>) {
    let mut cuts = Vec::new();
    let (mut depth, mut quote) = (0i32, false);
    for (i, c) in value.char_indices() {
        match c {
            '"' => quote = !quote,
            '(' if !quote => depth += 1,
            ')' if !quote => depth -= 1,
            '#' if !quote && depth == 0 && (i == 0 || value[..i].ends_with(' ')) => cuts.push(i),
            _ => {}
        }
    }
    let body = &value[..cuts.first().copied().unwrap_or(value.len())];
    let notes = cuts
        .iter()
        .enumerate()
        .filter_map(|(n, &from)| {
            let to = cuts.get(n + 1).copied().unwrap_or(value.len());
            let note = value[from + 1..to].trim();
            let (head, val) = note.split_once('=')?;
            let (key, kind) = match head.split_once('@') {
                Some((k, kind)) => (k.trim(), Some(kind.trim())),
                None => (head.trim(), None),
            };
            Some((key, kind, val.trim()))
        })
        .collect();
    (body, notes)
}

impl Emit {
    fn run<S: Store>(&self, run: &mut Run<S>, caps: &[Vals]) {
        let sites = match &self.site {
            Some(s) => s.eval(run, caps),
            None => Vals::new(),
        };
        let site = sites.first().and_then(|v| v.loc()).map(|l| l.node);
        let from = match &self.from {
            Some(f) => f.eval(run, caps),
            None => SmallVec::from_elem(Val::Node(run.enclosing_loc()), 1),
        };
        if from.is_empty() {
            return;
        }
        let to = self.to.eval(run, caps);
        for from in from.iter().filter_map(|v| v.loc()) {
            for to in to.iter().filter_map(|v| v.loc()) {
                run.edges.push(Edge {
                    site,
                    call_resolution: self.resolution,
                    ..Edge::new(from.fi, from.node, to.fi, to.node, self.kind)
                });
            }
        }
    }
}

impl RowRule {
    /// One row per combination of the values; a column with no value is empty.
    fn run<S: Store>(&self, run: &mut Run<S>, caps: &[Vals]) {
        let Some(at) = caps[0].first().and_then(|v| v.loc()) else {
            return;
        };
        let vals: Vec<Vals> = self.exprs.iter().map(|e| e.eval(run, caps)).collect();
        let total: usize = vals.iter().map(|v| v.len().max(1)).product();
        for mut n in 0..total {
            let values = vals
                .iter()
                .map(|v| {
                    let len = v.len().max(1);
                    let value = v.get(n % len).copied();
                    n /= len;
                    value
                })
                .collect();
            run.rows.push(super::Row {
                shape: self.shape,
                at,
                values,
            });
        }
    }
}

struct Scratch<S: Store> {
    caps: Vec<Cap<S::Id>>,
    vals: Vec<Vals>,
}

impl<S: Store> Scratch<S> {
    fn new(slots: usize) -> Self {
        Self {
            caps: vec![SmallVec::new(); slots],
            vals: vec![Vals::new(); slots],
        }
    }

    fn matches(&mut self, run: &Run<S>, at: &Match, c: Cursor<'_, S>) -> bool {
        if at.lang.is_some_and(|l| usize::from(l) != run.lang) {
            return false;
        }
        let n = at.slots.len();
        if self.caps.len() < n {
            self.caps.resize(n, SmallVec::new());
            self.vals.resize(n, Vals::new());
        }
        for cap in &mut self.caps[..n] {
            cap.clear();
        }
        let tree = c.tree();
        let lang = run.lang_ref;
        let id = tree.storage.id(c.index());
        let found = match at.depth {
            0 => pattern::matches(tree, lang, id, None, &at.pat, &mut self.caps),
            depth => (0..depth)
                .try_fold(c, |node, _| node.parent())
                .is_some_and(|top| {
                    let top = tree.storage.id(top.index());
                    pattern::matches(tree, lang, top, Some(id), &at.pat, &mut self.caps)
                }),
        };
        if !found {
            return false;
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

    fn holds(&self, run: &mut Run<S>, at: &Match) -> bool {
        at.when
            .as_ref()
            .is_none_or(|when| super::truthy(&when.eval(run, &self.vals)))
    }
}

impl Phase {
    /// Walks a tree in source order and runs the edges or rows rules at each
    /// node, with the innermost enclosing scope known. `all` visits the root.
    pub fn flow<S: Store>(&self, run: &mut Run<S>, root: Cursor<'_, S>, all: bool) {
        let mut w = Scratch::<S>::new(self.slots);
        let mut stack: Vec<(u32, bool)> = match all {
            true => vec![(root.index(), false)],
            false => root.children_rev().map(|c| (c.index(), false)).collect(),
        };
        while let Some((node, leaving)) = stack.pop() {
            if !run.step() {
                return;
            }
            if leaving {
                run.leave_def();
                continue;
            }
            let c = root.jump(root.fi(), node);
            match &self.body {
                Body::Edges(groups) => {
                    for rules in groups {
                        let list = rules.by_kind.get(&c.kind()).unwrap_or(&rules.any);
                        for &i in list {
                            let (m, emits) = &rules.rules[usize::from(i)];
                            if w.matches(run, m, c) && w.holds(run, m) {
                                emits.iter().for_each(|e| e.run(run, &w.vals));
                                break;
                            }
                        }
                    }
                }
                Body::Rows(groups) => {
                    for group in groups {
                        for (m, rules) in group {
                            if m.kinds.iter().any(|&k| k == 0 || k == c.kind())
                                && w.matches(run, m, c)
                                && w.holds(run, m)
                            {
                                rules.iter().for_each(|r| r.run(run, &w.vals));
                                break;
                            }
                        }
                    }
                }
                _ => return,
            }
            if self.enclosing.contains(&c.kind()) {
                run.enter_def(c.index());
                stack.push((c.index(), true));
            }
            stack.extend(c.children_rev().map(|child| (child.index(), false)));
        }
    }

    /// One tree's piece of the declared index `id`: each match filed under
    /// its innermost enclosing scope and under the whole tree.
    pub fn piece<S: Store>(
        &self,
        run: &mut Run<S>,
        root: Cursor<'_, S>,
        id: u32,
    ) -> crate::tree::Piece {
        let mut out = crate::tree::Piece::default();
        let mut w = Scratch::<S>::new(self.slots.max(4));
        let mine: Vec<&Indexer> = self.indexers.iter().filter(|i| i.id == id).collect();
        for c in std::iter::once(root).chain(root.descendants()) {
            for indexer in mine
                .iter()
                .filter(|i| i.at.kinds.iter().any(|&k| k == 0 || k == c.kind()))
            {
                if !w.matches(run, &indexer.at, c) || !w.holds(run, &indexer.at) {
                    continue;
                }
                let owner = c
                    .ancestors()
                    .find(|a| self.enclosing.contains(&a.kind()) || a.parent().is_none())
                    .map_or(c.index(), |a| a.index());
                for key in indexer
                    .key
                    .eval(run, &w.vals)
                    .iter()
                    .filter_map(|&k| run.text_of(k))
                {
                    out.entry((owner, key)).or_default().push(c.index());
                    out.entry((super::WHOLE, key)).or_default().push(c.index());
                }
            }
        }
        out
    }

    fn stages_for(&self, lang: usize) -> &[Stage] {
        match &self.body {
            Body::Rewrite(stages) | Body::Tags(stages) => {
                stages.get(lang).map_or(&[][..], Vec::as_slice)
            }
            _ => &[],
        }
    }

    pub fn has_rules(&self, rules: usize) -> bool {
        self.stages_for(rules).iter().any(|s| !s.rules.is_empty())
    }

    /// Every tag key a rewrite or tags phase can set, for the rules at `rules`.
    pub fn tag_keys(&self, rules: usize) -> Vec<u32> {
        self.stages_for(rules)
            .iter()
            .flat_map(|s| &s.rules)
            .flat_map(|r| r.tags.iter().map(|(key, _)| *key))
            .collect()
    }

    /// Rewrites a tree stage by stage; a repeating stage reruns until a pass
    /// replaces nothing.
    pub fn rewrite(
        &self,
        tree: &mut Tree<Mutable>,
        env: &Env,
        budget: &Sentinel,
        file: &Sentinel,
    ) -> Result<(), Killed> {
        self.rewrite_with(tree, env, env.rules_index(&tree.label), budget, file)
    }

    pub fn rewrite_with(
        &self,
        tree: &mut Tree<Mutable>,
        env: &Env,
        rules: usize,
        budget: &Sentinel,
        file: &Sentinel,
    ) -> Result<(), Killed> {
        let sentinels = [budget, file];
        for stage in self.stages_for(rules) {
            loop {
                let replaced = self.pass(tree, env, stage, budget, file)?;
                if !stage.repeat || !replaced {
                    break;
                }
                sentinels.iter().try_for_each(|s| s.check())?;
            }
        }
        Ok(())
    }

    /// Sets the tags of a tags phase on one tree.
    pub fn tag<S: Store>(
        &self,
        tree: &mut Tree<S>,
        fi: u32,
        env: &Env,
        graph: &super::EdgeIndex,
        budget: &Sentinel,
        file: &Sentinel,
    ) -> Result<(), Killed> {
        let sentinels = [budget, file];
        for stage in self.stages_for(env.rules_index(&tree.label)) {
            let mut w = Scratch::<S>::new(self.slots);
            for target in order(tree, true) {
                sentinels.iter().try_for_each(|s| s.check())?;
                let fired: Vec<(u16, Vec<(u32, u32)>)> = {
                    let mut run = self.run(&*tree, env, budget, file);
                    run.graph = Some(graph);
                    run.graph_fi = fi;
                    candidates(stage, tree, target)
                        .filter_map(|i| {
                            let rule = &stage.rules[i as usize];
                            Some((i, self.decide(&mut run, &mut w, rule, target)?.0))
                        })
                        .collect()
                };
                for (i, tags) in fired {
                    set_tags(tree, target, &stage.rules[i as usize], &tags);
                }
            }
        }
        Ok(())
    }

    fn run<'t, S: Store>(
        &'t self,
        tree: &'t Tree<S>,
        env: &'t Env,
        budget: &'t Sentinel,
        file: &'t Sentinel,
    ) -> Run<'t, S> {
        Run::new(std::slice::from_ref(tree), 0, env, self, budget, file)
    }

    fn pass(
        &self,
        tree: &mut Tree<Mutable>,
        env: &Env,
        stage: &Stage,
        budget: &Sentinel,
        file: &Sentinel,
    ) -> Result<bool, Killed> {
        let sentinels = [budget, file];
        let lang = &env.lang;
        let mut replaced = false;
        let mut w = Scratch::<Mutable>::new(self.slots);
        for target in order(tree, false) {
            sentinels.iter().try_for_each(|s| s.check())?;
            if tree.storage.is_removed(tree.storage.id(target)) {
                continue;
            }
            for i in candidates(stage, tree, target) {
                let rule = &stage.rules[i as usize];
                let fired = {
                    let mut run = self.run(&*tree, env, budget, file);
                    let Some((tags, texts)) = self.decide(&mut run, &mut w, rule, target) else {
                        continue;
                    };
                    let mut caps = w.caps.clone();
                    caps[0] = SmallVec::from_elem(tree.storage.id(target), 1);
                    Fired { caps, tags, texts }
                };
                let at = tree.storage.id(target);
                match &rule.out {
                    Out::Tag => set_tags(tree, target, rule, &fired.tags),
                    Out::Append(tpls) => {
                        for (tpl, texts) in tpls.iter().zip(&fired.texts) {
                            let built = pattern::build(
                                tree,
                                lang,
                                at,
                                tpl,
                                &fired.caps,
                                None,
                                &mut |i, _| texts[i],
                            );
                            drop(built);
                        }
                    }
                    Out::Replace(tpl) => {
                        let mv = rule.unique.is_none().then_some(at);
                        let texts = &fired.texts[0];
                        let holder = tree.storage.0.new_node(crate::tree::Node::default());
                        let built = pattern::build(
                            tree,
                            lang,
                            holder,
                            tpl,
                            &fired.caps,
                            mv,
                            &mut |i, _| texts[i],
                        );
                        for &n in &built {
                            n.detach(&mut tree.storage.0);
                        }
                        holder.remove(&mut tree.storage.0);
                        if let Some(unique) = &rule.unique
                            && taken(tree, lang, at, built.first().copied(), unique)
                        {
                            built
                                .into_iter()
                                .for_each(|n| n.remove_subtree(&mut tree.storage.0));
                            break;
                        }
                        let first = built.first().copied();
                        tree.replace(at, built);
                        if let Some(root) = first {
                            set_tags(tree, Tree::to_raw(root), rule, &fired.tags);
                        }
                        replaced = true;
                        break;
                    }
                }
            }
        }
        Ok(replaced)
    }

    fn decide<S: Store>(
        &self,
        run: &mut Run<S>,
        w: &mut Scratch<S>,
        rule: &Rewrite,
        target: u32,
    ) -> Option<(Vec<(u32, u32)>, Vec<Vec<u32>>)> {
        let c = Cursor::new(run.trees, run.fi, target);
        if !w.matches(run, &rule.at, c) || !w.holds(run, &rule.at) {
            return None;
        }
        let tags = rule
            .tags
            .iter()
            .filter_map(|(key, val)| {
                let val = match val {
                    TagVal::Lit(sym) => *sym,
                    TagVal::Expr(expr) => {
                        let vals = expr.eval(run, &w.vals);
                        first_sym(run, &vals)?
                    }
                };
                Some((*key, val))
            })
            .collect();
        let tpls: Vec<&Tpl<Expr>> = match &rule.out {
            Out::Replace(tpl) => vec![tpl],
            Out::Append(tpls) => tpls.iter().collect(),
            Out::Tag => Vec::new(),
        };
        let texts = tpls
            .into_iter()
            .map(|tpl| {
                tpl.texts()
                    .into_iter()
                    .map(|expr| {
                        let vals = expr.eval(run, &w.vals);
                        first_sym(run, &vals).unwrap_or(0)
                    })
                    .collect()
            })
            .collect();
        Some((tags, texts))
    }
}

struct Fired {
    caps: Vec<Cap<NodeId>>,
    tags: Vec<(u32, u32)>,
    texts: Vec<Vec<u32>>,
}

fn first_sym<S: Store>(run: &Run<S>, vals: &Vals) -> Option<u32> {
    vals.iter().find_map(|v| match v {
        Val::Sym(s) => Some(*s),
        Val::Node(loc) => Some(run.sym_at(*loc)).filter(|&s| s != 0),
        _ => None,
    })
}

fn order<S: Store>(tree: &Tree<S>, preorder: bool) -> Vec<u32> {
    let root = tree.root();
    let mut all: Vec<u32> = std::iter::once(root.index())
        .chain(root.descendants().map(|c| c.index()))
        .collect();
    if !preorder {
        all.reverse();
    }
    all
}

fn candidates<'a, S: Store>(
    stage: &'a Stage,
    tree: &Tree<S>,
    target: u32,
) -> impl Iterator<Item = u16> + 'a {
    let kind = tree.storage.node(target).kind;
    stage
        .by_kind
        .get(&kind)
        .unwrap_or(&stage.any)
        .iter()
        .copied()
}

fn set_tags<S: Store>(tree: &mut Tree<S>, target: u32, rule: &Rewrite, tags: &[(u32, u32)]) {
    let at = rule
        .tag_on
        .and_then(|k| {
            std::iter::once(tree.cursor(target))
                .chain(tree.cursor(target).descendants())
                .find(|n| n.kind() == k)
                .map(|n| n.index())
        })
        .unwrap_or(target);
    for &(key, val) in tags {
        tree.set_tag(at, key, val);
    }
}

fn taken(
    tree: &Tree<Mutable>,
    lang: &Lang,
    target: NodeId,
    built: Option<NodeId>,
    unique: &(Pat, u16, usize),
) -> bool {
    let (pat, kind, slots) = unique;
    let Some(built) = built else {
        return false;
    };
    let key = tree.cursor(Tree::to_raw(built)).child_sym_of_kind(*kind);
    let mut caps: Vec<Cap<NodeId>> = vec![SmallVec::new(); *slots];
    target.parent(&tree.storage.0).is_some_and(|parent| {
        parent
            .children(&tree.storage.0)
            .filter(|&c| c != target)
            .any(|c| {
                pattern::matches(tree, lang, c, None, pat, &mut caps)
                    && tree.cursor(Tree::to_raw(c)).child_sym_of_kind(*kind) == key
            })
    })
}
