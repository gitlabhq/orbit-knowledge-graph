//! Compiles pipeline.yaml and the rule files into phases. Every rule is a
//! case whose value is an expression; what a rule writes (edges, rows, tags,
//! trees) is an op in that expression.

use std::cell::RefCell;
use std::collections::{BTreeMap, HashMap};

use serde_json::Value;

use super::expr::{Expr, IndexSpec, Scope};
use super::lang::{DataSpec, DefBody, DefSpec, Pair, RuleFiles, StageSpec, TableSpec};
use super::pattern::{self, Slots, Tpl};
use super::program::{
    BudgetSpec, Cases, Def, DefImpl, Indexer, Match, Part, Pass, Phase, Pipeline, Roles, RowShape,
    Template,
};
use super::{FnId, Kind, Registration, Registry, Val, Vals};
use crate::error::LoadError;
use crate::intern::Lang;
use crate::tree::EdgeKind;

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct PipelineSpec {
    scopes: Vec<String>,
    edges: Vec<String>,
    kinds: Vec<String>,
    #[serde(default)]
    modules: BTreeMap<String, ModuleSpec>,
    phases: Vec<PhaseSpec>,
}

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct PhaseSpec {
    name: String,
    over: String,
    /// `children_first` walks each tree's children before their parent.
    #[serde(default)]
    order: Option<String>,
    #[serde(default)]
    until: Option<String>,
    #[serde(default)]
    bind: BTreeMap<String, String>,
    #[serde(default)]
    budget: BudgetSpec,
}

/// How pipeline.yaml fills one module's kind and tag roles.
#[derive(serde::Deserialize, Default)]
#[serde(deny_unknown_fields)]
struct ModuleSpec {
    #[serde(default)]
    kinds: BTreeMap<String, String>,
    #[serde(default)]
    tags: BTreeMap<String, String>,
}

impl Pipeline {
    /// Compiles `sources` against the mounted `modules`; a tree's rules are
    /// its language's entry in `sources.langs`.
    pub fn load(
        lang: &Lang,
        sources: &super::lang::Sources,
        modules: &'static [Registration],
    ) -> Result<Self, LoadError> {
        let registry = Registry::new(modules);
        let spec: PipelineSpec = orbit_utils::yaml::from_str(sources.pipeline)?;
        for edge in &spec.edges {
            lang.edges.intern(edge);
        }
        for kind in &spec.kinds {
            lang.vocabulary.intern(kind);
            lang.intern_kind(kind);
        }
        let tree = bind_roles(&spec, lang, "tree", crate::tree::TreeKinds::NAMES, &[])?;
        let _ = lang.tree.set(crate::tree::TreeKinds::bound(&tree.kinds));
        let files = super::lang::load(sources)?;
        let names: HashMap<String, u16> = files
            .shared
            .data
            .keys()
            .enumerate()
            .map(|(at, name)| (name.clone(), at as u16))
            .collect();
        let values = data_values(&files, lang)?;
        let enclosing: Vec<u16> = spec.scopes.iter().map(|k| lang.intern_kind(k)).collect();
        let roles = roles(&registry, &spec, lang)?;
        let (rows, tables) = tables(&files.shared.tables)?;
        let indexes = RefCell::new(Vec::new());
        let loader = Loader::new(&registry, &files, &names, &tables, lang, &indexes)?;
        let (defs, overrides) = loader.defs(&files)?;
        let (defs, overrides): (std::sync::Arc<[Def]>, _) = (defs.into(), overrides.into());
        let mut phases = spec
            .phases
            .iter()
            .map(|p| {
                let mut phase = compile_phase(&loader, p, &files, &defs, &overrides)?;
                phase.rows = rows.clone();
                phase.data = values.clone();
                phase.enclosing = enclosing.clone();
                phase.roles = roles.clone();
                Ok(phase)
            })
            .collect::<Result<Vec<_>, LoadError>>()?;
        let indexers: std::sync::Arc<[Indexer]> = indexers(&loader, &files)?.into();
        drop(loader);
        let indexes = indexes.into_inner();
        for phase in &mut phases {
            phase.index_count = indexes.len();
            phase.indexers = indexers.clone();
        }
        let settings = merged_data(&files);
        Ok(Self {
            registry,
            phases,
            settings,
        })
    }
}

/// Each language's data: the shared values, overridden by its own.
fn merged_data(files: &RuleFiles) -> Vec<BTreeMap<String, DataSpec>> {
    files
        .langs
        .iter()
        .map(|file| {
            let mut merged = files.shared.data.clone();
            if let Some(file) = file {
                merged.extend(file.data.clone());
            }
            merged
        })
        .collect()
}

/// Each module's roles, by slot: every role bound, every kind in the vocabulary.
fn roles(registry: &Registry, spec: &PipelineSpec, lang: &Lang) -> Result<Vec<Roles>, LoadError> {
    registry
        .modules
        .iter()
        .map(|module| bind_roles(spec, lang, module.name, module.kinds, module.tags))
        .collect()
}

/// The ids pipeline.yaml binds to one user's kind and tag roles.
fn bind_roles(
    spec: &PipelineSpec,
    lang: &Lang,
    user: &str,
    kinds: &[&str],
    tags: &[&str],
) -> Result<Roles, LoadError> {
    let empty = ModuleSpec::default();
    let bound = spec.modules.get(user).unwrap_or(&empty);
    let pick = |map: &BTreeMap<String, String>, role: &str, what: &str| {
        map.get(role)
            .cloned()
            .ok_or_else(|| LoadError(format!("{user}: {what} role {role} is not bound")))
    };
    let kinds = kinds
        .iter()
        .map(|role| {
            let kind = pick(&bound.kinds, role, "kind")?;
            match spec.kinds.contains(&kind) {
                true => Ok(lang.intern_kind(&kind)),
                false => Err(LoadError(format!(
                    "{user}: kind {kind} is not in pipeline.yaml kinds"
                ))),
            }
        })
        .collect::<Result<_, _>>()?;
    let tags = tags
        .iter()
        .map(|role| Ok(lang.syms.intern(&pick(&bound.tags, role, "tag")?)))
        .collect::<Result<_, LoadError>>()?;
    Ok(Roles { kinds, tags })
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
        DataSpec::Str(s) => out.push(match lang.vocabulary.lookup(s) {
            0 => Val::Sym(lang.syms.intern(s)),
            _ => Val::Kind(lang.intern_kind(s)),
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

/// Whether a phase walks children first, and whether it reruns until its
/// edges stop changing.
fn walk_of(spec: &PhaseSpec) -> Result<(bool, bool), LoadError> {
    let err = |what: &str, value: &str| {
        LoadError(format!("phase {}: unknown {what}: {value}", spec.name))
    };
    let post = match spec.order.as_deref() {
        None => false,
        Some("children_first") => true,
        Some(other) => return Err(err("order", other)),
    };
    let stable = match spec.until.as_deref() {
        None => false,
        Some("stable") => true,
        Some(other) => return Err(err("until", other)),
    };
    if !matches!(spec.over.as_str(), "tree" | "project" | "family" | "graph") {
        return Err(err("over", &spec.over));
    }
    Ok((post, stable))
}

fn compile_phase(
    loader: &Loader,
    spec: &PhaseSpec,
    files: &RuleFiles,
    defs: &std::sync::Arc<[Def]>,
    overrides: &std::sync::Arc<[HashMap<u16, u16>]>,
) -> Result<Phase, LoadError> {
    let registry = loader.registry;
    let (post, until_stable) = walk_of(spec)?;
    let (binds, def_binds) = binds(registry, spec, &files.shared.defs)?;
    let links = super::Links::load(registry, &binds, &def_binds)?;
    let def_deps = def_deps(defs, &links);
    let stages_of = |rules: Option<&Vec<StageSpec>>| rules.cloned().unwrap_or_default();
    let shared = stages_of(files.shared.phases.get(&spec.name));
    let langs: Vec<Vec<StageSpec>> = files
        .langs
        .iter()
        .map(|f| stages_of(f.as_ref().and_then(|f| f.phases.get(&spec.name))))
        .collect();
    let grouped = shared
        .iter()
        .chain(langs.iter().flatten())
        .map(|s| s.grouped);
    let grouped = match grouped.clone().all(|g| g) {
        true => true,
        false if grouped.clone().any(|g| g) => {
            return Err(LoadError(format!(
                "phase {}: a phase's sections are all groups or all passes",
                spec.name
            )));
        }
        false => false,
    };
    let (passes, by_lang, region_by_lang) = match grouped {
        false => {
            let mut passes = Vec::new();
            let (mut by_lang, mut regions) = (Vec::new(), Vec::new());
            for (at, own) in langs.iter().enumerate() {
                let (mut mine, mut inside) = (Vec::new(), Vec::new());
                for stage in own.iter().chain(&shared) {
                    let list = if stage.before_regions {
                        &mut inside
                    } else {
                        &mut mine
                    };
                    list.push(passes.len() as u16);
                    passes.push(loader.stage(stage, at as u8, post)?);
                }
                by_lang.push(mine);
                regions.push(inside);
            }
            (passes, by_lang, regions)
        }
        true => {
            let pass = loader.groups(&shared, &langs, post)?;
            (
                vec![pass],
                vec![vec![0]; langs.len()],
                vec![Vec::new(); langs.len()],
            )
        }
    };
    let used = modules_reached(registry, &links, &passes, defs);
    let bound: Vec<FnId> = binds
        .iter()
        .map(|b| b.0)
        .chain(def_binds.iter().map(|b| b.0))
        .collect();
    check_binds(registry, spec, &bound, &used)?;
    let label = spec
        .budget
        .label
        .clone()
        .unwrap_or_else(|| spec.name.clone());
    Ok(Phase {
        name: spec.name.clone(),
        label: Box::leak(label.into_boxed_str()),
        budget: spec.budget.clone(),
        scope: spec.over.clone(),
        until_stable,
        modules: used,
        links,
        data: Vec::new(),
        defs: defs.clone(),
        def_deps,
        overrides: overrides.clone(),
        index_count: 0,
        enclosing: Vec::new(),
        roles: Vec::new(),
        rows: Vec::new(),
        indexers: Vec::new().into(),
        passes,
        by_lang,
        region_by_lang,
    })
}

/// Each definition's dependencies under a phase's binds: the states of every
/// module function it can reach through other definitions.
fn def_deps(defs: &[Def], links: &super::Links) -> Vec<u64> {
    let calls: Vec<Vec<Result<FnId, u16>>> = defs
        .iter()
        .map(|def| {
            let mut used = Vec::new();
            def.calls(&mut |c| used.push(c));
            used
        })
        .collect();
    let mut deps: Vec<u64> = calls
        .iter()
        .map(|used| {
            used.iter()
                .filter_map(|c| c.ok())
                .fold(0, |acc, id| acc | links.deps_of(id))
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

/// Every declared index, compiled for every language. Compiling a key can
/// declare no further index.
fn indexers(loader: &Loader, files: &RuleFiles) -> Result<Vec<Indexer>, LoadError> {
    let indexes = loader.indexes.borrow().clone();
    let mut out = Vec::new();
    for (id, index) in indexes.iter().enumerate() {
        let owners: Vec<Option<u8>> = match index.pattern.contains('%') {
            true => (0..files.langs.len()).map(|at| Some(at as u8)).collect(),
            false => vec![None],
        };
        for owner in owners {
            let (at, _) = loader
                .matcher(&index.pattern, None, owner, &[])
                .map_err(|e| LoadError(format!("index {}: {}", index.pattern, e.0)))?;
            let key = match &index.key {
                Some(src) => loader.parse(src, &RefCell::new(at.slots.clone()), false)?,
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

type Binds = (Vec<(FnId, FnId)>, Vec<(FnId, u16)>);

/// `module.function: provider`, where the provider is another module's
/// function or the name of a definition.
fn binds(registry: &Registry, spec: &PhaseSpec, defs: &[DefSpec]) -> Result<Binds, LoadError> {
    let id = |path: &str| {
        let (module, name) = path
            .split_once('.')
            .ok_or_else(|| LoadError(format!("bind {path}: expected <module>.<function>")))?;
        registry
            .lookup(module, name)
            .ok_or_else(|| LoadError(format!("bind {path}: unknown")))
    };
    let (mut binds, mut to_defs) = (Vec::new(), Vec::new());
    for (of, to) in &spec.bind {
        let of = id(of)?;
        let declared = registry.function(of);
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
        let provider = registry.function(to);
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
fn modules_reached(
    registry: &Registry,
    links: &super::Links,
    passes: &[Pass],
    defs: &[Def],
) -> Vec<u8> {
    let mut calls: Vec<Result<FnId, u16>> = Vec::new();
    for pass in passes {
        pass.cases.calls(&mut |c| calls.push(c));
    }
    let mut seen = vec![false; defs.len()];
    let mut used: Vec<u8> = Vec::new();
    while let Some(c) = calls.pop() {
        match c {
            Ok(id) => {
                if let Some(def) = links.def_of(id) {
                    calls.push(Err(def));
                    continue;
                }
                let slot = registry.function(links.to(id)).slot;
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
fn check_binds(
    registry: &Registry,
    spec: &PhaseSpec,
    bound: &[FnId],
    used: &[u8],
) -> Result<(), LoadError> {
    for &slot in used {
        let module = registry.module(slot).name;
        if let Some(id) = registry.required(module).find(|id| !bound.contains(id)) {
            return Err(LoadError(format!(
                "phase {}: {module}.{} needs a bind",
                spec.name,
                registry.function(id).name
            )));
        }
    }
    Ok(())
}

/// What compiling one phase's rules reads: the modules, the phase's binds,
/// the shared definitions and data, and the declared indexes and row shapes
/// the rules add to.
struct Loader<'a> {
    registry: &'a Registry,
    modules: Vec<String>,
    names: &'a HashMap<String, u16>,
    defs: HashMap<String, (u16, usize)>,
    data: &'a BTreeMap<String, DataSpec>,
    lang_data: Vec<BTreeMap<String, DataSpec>>,
    lang: &'a Lang,
    indexes: &'a RefCell<Vec<IndexSpec>>,
    tables: &'a Tables,
}

impl<'a> Loader<'a> {
    fn new(
        registry: &'a Registry,
        files: &'a RuleFiles,
        names: &'a HashMap<String, u16>,
        tables: &'a Tables,
        lang: &'a Lang,
        indexes: &'a RefCell<Vec<IndexSpec>>,
    ) -> Result<Self, LoadError> {
        let mut defs = HashMap::new();
        for (at, d) in files.shared.defs.iter().enumerate() {
            if defs
                .insert(d.name.clone(), (at as u16, d.params.len()))
                .is_some()
            {
                return Err(LoadError(format!("{}: define {} twice", d.origin, d.name)));
            }
        }
        Ok(Self {
            registry,
            modules: registry.modules.iter().map(|m| m.name.to_owned()).collect(),
            names,
            defs,
            data: &files.shared.data,
            lang_data: merged_data(files),
            lang,
            indexes,
            tables,
        })
    }

    fn scope<'s>(
        &'s self,
        slots: &'s RefCell<Slots>,
        effects: bool,
        shared: Option<(u16, usize)>,
    ) -> Scope<'s> {
        Scope {
            slots,
            modules: &self.modules,
            registry: self.registry,
            lang: self.lang,
            data: self.names,
            defs: &self.defs,
            indexes: self.indexes,
            tables: self.tables,
            effects,
            shared,
        }
    }

    /// A call to one of the core module's functions.
    fn core(&self, name: &str, args: Vec<Expr>) -> Expr {
        let id = self
            .registry
            .lookup("core", name)
            .expect("the core module declares the rule writes");
        Expr::Call(id, args.into())
    }

    fn parse(&self, src: &str, slots: &RefCell<Slots>, effects: bool) -> Result<Expr, LoadError> {
        Expr::parse(src, &self.scope(slots, effects, None))
    }

    /// A pattern and its guard, written `pattern when guard`.
    fn matcher(
        &self,
        pattern: &str,
        when: Option<&str>,
        lang: Option<u8>,
        params: &[String],
    ) -> Result<(Match, Option<Expr>), LoadError> {
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
        let src: Box<str> = match when {
            Some(w) => format!("{pattern} when {w}").into(),
            None => pattern.into(),
        };
        let guard = when
            .map(|src| self.parse(src, &RefCell::new(slots.clone()), true))
            .transpose()?;
        let m = Match {
            src,
            pat,
            depth,
            kinds,
            slots,
            lang,
        };
        Ok((m, guard))
    }

    /// The shared definitions, then each language's redefinitions, with the
    /// module states each depends on; and per language, which shared
    /// definition its own replaces.
    fn defs(&self, files: &RuleFiles) -> Result<(Vec<Def>, Vec<HashMap<u16, u16>>), LoadError> {
        let mut defs = Vec::new();
        for d in &files.shared.defs {
            defs.push(self.def(d, None, None)?);
        }
        let mut overrides = Vec::new();
        for (at, file) in files.langs.iter().enumerate() {
            let mut own = HashMap::new();
            for d in file.iter().flat_map(|f| &f.defs) {
                let &(shared, arity) = self.defs.get(&d.name).ok_or_else(|| {
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
                defs.push(self.def(d, Some(at as u8), Some((shared, arity)))?);
                own.insert(shared, (defs.len() - 1) as u16);
            }
            overrides.push(own);
        }
        Ok((defs, overrides))
    }

    fn def(
        &self,
        d: &DefSpec,
        lang: Option<u8>,
        shared: Option<(u16, usize)>,
    ) -> Result<Def, LoadError> {
        let at = |e: LoadError| LoadError(format!("{}: define {}: {}", d.origin, d.name, e.0));
        let name: Box<str> = match lang {
            Some(_) => format!("{} ({})", d.name, d.origin).into(),
            None => d.name.clone().into(),
        };
        let (body, slots) = match &d.body {
            DefBody::Expr(src) => {
                let cell = RefCell::new(Slots::params(&d.params));
                let expr = Expr::parse(src, &self.scope(&cell, false, shared)).map_err(at)?;
                let slots = cell.borrow().len();
                (DefImpl::Expr(expr), slots)
            }
            DefBody::Cases(cases) => {
                let mut compiled = Vec::new();
                let mut slots = d.params.len().max(1);
                for (pattern, when, value) in cases {
                    let owners: Vec<Option<u8>> = match (lang, pattern.contains('%')) {
                        (None, true) => (0..self.lang_data.len()).map(|l| Some(l as u8)).collect(),
                        (lang, _) => vec![lang],
                    };
                    for owner in owners {
                        let (m, own) = match pattern.as_str() {
                            "_" => (None, Slots::params(&d.params)),
                            pattern => {
                                let (m, _) =
                                    self.matcher(pattern, None, owner, &d.params).map_err(at)?;
                                let own = m.slots.clone();
                                (Some(m), own)
                            }
                        };
                        let cell = RefCell::new(own);
                        let scope = self.scope(&cell, false, shared);
                        let guard = when
                            .as_deref()
                            .map(|src| Expr::parse(src, &scope))
                            .transpose()
                            .map_err(at)?;
                        let value = Expr::parse(value, &scope).map_err(at)?;
                        slots = slots.max(cell.borrow().len());
                        compiled.push((0, (m, guard, value)));
                    }
                }
                let stops = vec![false; compiled.len()];
                (DefImpl::Cases(Cases::new(compiled, stops)), slots)
            }
        };
        Ok(Def {
            name,
            params: d.params.len(),
            body,
            slots,
        })
    }

    /// A rewrite or tags stage as a pass: every pair its own group, a
    /// replacing pair stopping its node. Rewrites run children first.
    fn stage(&self, spec: &StageSpec, lang: u8, post: bool) -> Result<Pass, LoadError> {
        let mut cases = Vec::new();
        let mut stops = Vec::new();
        let mut templates = Vec::new();
        let mut tag_keys = Vec::new();
        let mut slots = 1;
        for (at, pair) in spec.pairs.iter().enumerate() {
            let err =
                |e: LoadError| LoadError(format!("{}: {:?}: {}", pair.origin, pair.pattern, e.0));
            let (m, guard) = self
                .matcher(&pair.pattern, pair.when.as_deref(), Some(lang), &[])
                .map_err(err)?;
            let cell = RefCell::new(m.slots.clone());
            let built = templates.len();
            let value = self
                .rewrite(pair, &m, at as u16, &cell, &mut templates, &mut tag_keys)
                .map_err(err)?;
            let made = &templates[built..];
            if spec.repeat && made.iter().any(|t| !t.replace) {
                return Err(err(LoadError("a repeat stage cannot append".into())));
            }
            stops.push(made.iter().any(|t| t.replace));
            slots = slots.max(cell.borrow().len());
            cases.push((at as u16, (Some(m), guard, value)));
        }
        Ok(Pass {
            cases: Cases::new(cases, stops),
            slots,
            templates,
            post,
            repeat: spec.repeat,
            tag_keys,
        })
    }

    /// A rewrite or tag pair's value: a call, `name(...)`, read as an
    /// expression; or `[template | + t1 + t2] [unique P] [#key[@kind]=value]...`,
    /// a `build` per template and a `tag` per annotation not set on a
    /// replacing build.
    fn rewrite(
        &self,
        pair: &Pair,
        m: &Match,
        case: u16,
        slots: &RefCell<Slots>,
        templates: &mut Vec<Template>,
        tag_keys: &mut Vec<u32>,
    ) -> Result<Expr, LoadError> {
        let Value::String(value) = &pair.value else {
            return Err(LoadError("a rewrite is pattern: template".into()));
        };
        let head = value.split('(').next().unwrap_or("");
        if head.len() < value.len()
            && !head.is_empty()
            && head.chars().all(|c| c.is_ascii_lowercase() || c == '_')
        {
            return self.parse(value, slots, true);
        }
        let (body, notes) = split_notes(value);
        let (body, unique) = match body.find(" unique ") {
            Some(i) => (body[..i].trim(), Some(body[i + " unique ".len()..].trim())),
            None => (body.trim(), None),
        };
        let scope = self.scope(slots, false, None);
        let mut text = |src: &str| Expr::parse(src, &scope);
        let (replace, tpls): (bool, Vec<Tpl<Expr>>) = match body.strip_prefix('+') {
            _ if body.is_empty() => (false, Vec::new()),
            Some(rest) => (
                false,
                rest.split(" + ")
                    .map(|t| pattern::parse_template(t.trim(), &m.slots, self.lang, &mut text))
                    .collect::<Result<_, _>>()?,
            ),
            None => (
                true,
                vec![pattern::parse_template(
                    body, &m.slots, self.lang, &mut text,
                )?],
            ),
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
                true => Expr::parse(val, &scope)?,
                false => Expr::Lit(Val::Sym(self.lang.syms.intern(val.trim_matches('\'')))),
            };
            let key = self.lang.syms.intern(key);
            tag_keys.push(key);
            tags.push((key, val));
        }
        let mut unique = unique
            .map(|src| {
                let mut slots = Slots::new();
                let pat = pattern::parse_pattern(src, &mut slots, self.lang, &|_| None)?;
                let (_, kind) =
                    pattern::anchor(&pat).ok_or_else(|| LoadError(format!("unique {src:?}")))?;
                Ok::<_, LoadError>((pat, kind, slots.len()))
            })
            .transpose()?;
        let mut writes = Vec::new();
        if !replace {
            for (key, val) in tags.drain(..) {
                let on = u32::from(tag_on.unwrap_or(0));
                writes.push(self.core("set_tag", vec![number(key), number(on), Expr::Var(0), val]));
            }
        }
        for tpl in tpls {
            let mut args = vec![number(templates.len() as u32), Expr::Var(0)];
            args.extend(tags.iter().map(|(_, val)| val.clone()));
            args.extend(tpl.texts().into_iter().cloned());
            writes.push(self.core("build", args));
            templates.push(Template {
                case,
                tpl,
                replace,
                unique: unique.take(),
                tags: tags.iter().map(|(key, _)| *key).collect(),
                tag_on,
            });
        }
        Ok(union(writes))
    }

    /// An edges or rows phase as one pass: its rule lists are groups, a
    /// language's pairs in a group before the shared ones, and a shared
    /// pattern reading `%data` compiles once per language.
    fn groups(
        &self,
        shared: &[StageSpec],
        langs: &[Vec<StageSpec>],
        post: bool,
    ) -> Result<Pass, LoadError> {
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
        let mut cases = Vec::new();
        let mut slots = 1;
        for (group, (_, pairs)) in groups.into_iter().enumerate() {
            for (owner, pair) in pairs {
                let owners: Vec<Option<u8>> = match (owner, pair.pattern.contains('%')) {
                    (None, true) => (0..langs.len()).map(|at| Some(at as u8)).collect(),
                    (owner, _) => vec![owner],
                };
                for owner in owners {
                    let err = |e: LoadError| {
                        LoadError(format!("{}: {:?}: {}", pair.origin, pair.pattern, e.0))
                    };
                    let (m, guard) = self
                        .matcher(&pair.pattern, pair.when.as_deref(), owner, &[])
                        .map_err(err)?;
                    let cell = RefCell::new(m.slots.clone());
                    let value = self.group_value(&pair.value, &cell).map_err(err)?;
                    slots = slots.max(cell.borrow().len());
                    cases.push((group as u16, (Some(m), guard, value)));
                }
            }
        }
        let stops = vec![false; cases.len()];
        Ok(Pass {
            cases: Cases::new(cases, stops),
            slots,
            templates: Vec::new(),
            post,
            repeat: false,
            tag_keys: Vec::new(),
        })
    }

    /// `{ kind: targets, ... }`; targets are an expression, a map with
    /// `to`/`from`/`site`/`resolution`, or a list of those. `none` writes
    /// nothing and stops the group.
    fn edges(&self, value: &Value, slots: &RefCell<Slots>) -> Result<Expr, LoadError> {
        let map = match value {
            Value::Object(map) => map,
            Value::String(s) if s == "none" => return Ok(Expr::Empty),
            _ => return Err(LoadError("an edge pair maps edge kinds to targets".into())),
        };
        let mut out = Vec::new();
        for (kind, spec) in map {
            let kind = self.lang.edge_kind(kind).ok_or_else(|| {
                LoadError(format!("edge kind {kind} is not in pipeline.yaml edges"))
            })?;
            match spec {
                Value::Array(items) => {
                    for item in items {
                        out.push(self.edge(kind, item, slots)?);
                    }
                }
                one => out.push(self.edge(kind, one, slots)?),
            }
        }
        Ok(union(out))
    }

    /// An edge's write: from the innermost scope above the node unless it
    /// says otherwise, at the site it names, if any. A resolution that is not
    /// a fixed name is an expression over each target, read as `TARGET`.
    fn edge(
        &self,
        kind: EdgeKind,
        value: &Value,
        slots: &RefCell<Slots>,
    ) -> Result<Expr, LoadError> {
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
        let to = self.parse(to, slots, true)?;
        let from = match from {
            Some(f) => self.parse(f, slots, true)?,
            None => self.core("scope", vec![Expr::Var(0)]),
        };
        let site = match site {
            Some(s) => self.parse(s, slots, true)?,
            None => Expr::Empty,
        };
        let kind = number(u32::from(kind.0));
        let literal = |name: &str| name.chars().all(|c| c.is_ascii_lowercase() || c == '_');
        Ok(match res {
            Some(name) if !literal(name) => {
                let slot = slots.borrow_mut().slot("TARGET");
                let of = self.parse(name, slots, true)?;
                let edge = self.core("edge", vec![kind, from, Expr::Var(slot), site, of]);
                Expr::Every(Box::new(to), slot, Box::new(edge))
            }
            Some(name) => {
                let fixed = Expr::Lit(Val::Sym(self.lang.syms.intern(name)));
                self.core("edge", vec![kind, from, to, site, fixed])
            }
            None => self.core("edge", vec![kind, from, to, site, Expr::Empty]),
        })
    }

    /// A group pair's value by its shape: a map from edge kinds to targets,
    /// or expressions.
    fn group_value(&self, value: &Value, slots: &RefCell<Slots>) -> Result<Expr, LoadError> {
        match value {
            Value::Object(_) => self.edges(value, slots),
            _ => self.expressions(value, slots),
        }
    }

    /// An expression, or a list of them for their union; `none` writes
    /// nothing and stops the group.
    fn expressions(&self, value: &Value, slots: &RefCell<Slots>) -> Result<Expr, LoadError> {
        match value {
            Value::String(s) if s == "none" => Ok(Expr::Empty),
            Value::String(src) => self.parse(src, slots, true),
            Value::Array(items) => {
                let parts = items
                    .iter()
                    .map(|item| self.expressions(item, slots))
                    .collect::<Result<_, _>>()?;
                Ok(union(parts))
            }
            _ => Err(LoadError(
                "a value is an expression or a list of them".into(),
            )),
        }
    }
}

/// Each row shape's id and value names, by shape name.
type Tables = HashMap<String, (u16, Vec<String>)>;

/// The export's row shapes, in name order, and their names.
fn tables(specs: &BTreeMap<String, TableSpec>) -> Result<(Vec<RowShape>, Tables), LoadError> {
    let mut rows = Vec::new();
    let mut tables = HashMap::new();
    for (name, spec) in specs {
        let missing = |what: &str| LoadError(format!("table {name}: no column {what}"));
        let names: Vec<String> = spec.columns.iter().chain(&spec.keys).cloned().collect();
        let find = |list: &[String], what: &str| {
            list.iter()
                .position(|n| n == what)
                .ok_or_else(|| missing(what))
        };
        let id = match &spec.id {
            None => None,
            Some(id) => {
                let parts = id
                    .parts
                    .iter()
                    .map(|p| {
                        let i = find(&names, p)?;
                        Ok(match i < spec.columns.len() {
                            true => Part::Column(i),
                            false => Part::Value(i),
                        })
                    })
                    .collect::<Result<_, LoadError>>()?;
                Some((id.seed.clone(), parts))
            }
        };
        let expect = spec
            .expect
            .iter()
            .map(|(c, table)| Ok((find(&spec.columns, c)?, table.clone())))
            .collect::<Result<_, LoadError>>()?;
        let follows = spec
            .follows
            .as_deref()
            .map(|c| find(&spec.columns, c))
            .transpose()?;
        tables.insert(name.clone(), (rows.len() as u16, names));
        rows.push(RowShape {
            table: spec.table.clone(),
            columns: spec.columns.clone(),
            id,
            expect,
            follows,
        });
    }
    Ok((rows, tables))
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

/// One expression for several writes.
fn union(mut parts: Vec<Expr>) -> Expr {
    match parts.len() {
        0 => Expr::Empty,
        1 => parts.pop().unwrap_or(Expr::Empty),
        _ => Expr::Union(parts.into()),
    }
}

fn number(n: u32) -> Expr {
    Expr::Lit(Val::Sym(n))
}
