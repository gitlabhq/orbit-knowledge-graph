use std::collections::{BTreeMap, HashMap};

use indextree::NodeId;
use smallvec::SmallVec;

use super::expr::{Expr, Scope};
use super::lang::{
    ActionSpec, DataSpec, EmitMap, LangFile, List, PhaseRules, RuleFiles, RuleSpec, StageSpec,
};
use super::pattern::{self, Cap, Pat, Slots, Tpl};
use super::{Cursor, FnId, Kind, Loc, Run, Store, Tree, Val, Vals};
use crate::canonical::{CANONICAL_BASE, Canonical as C};
use crate::env::Env;
use crate::error::LoadError;
use crate::intern::Lang;
use crate::sentinel::{Killed, Sentinel};
use crate::tree::{CallResolution, Edge, EdgeKind, Mutable, Storage};

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct PhaseSpec {
    name: String,
    scope: String,
    walk: String,
    #[serde(default)]
    after: Vec<String>,
    outputs: Vec<String>,
    modules: Vec<String>,
    #[serde(default)]
    bind: BTreeMap<String, String>,
    #[serde(default)]
    resolve: BTreeMap<String, String>,
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
struct PipelineSpec {
    phases: Vec<PhaseSpec>,
}

const EMIT_KINDS: [(&str, EdgeKind); 5] = [
    ("calls", EdgeKind::Calls),
    ("imports", EdgeKind::Imports),
    ("extends", EdgeKind::Extends),
    ("defines", EdgeKind::Defines),
    ("type_flow", EdgeKind::TypeFlow),
];

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum Walk {
    Flow,
    Each,
    Fixpoint,
}

pub fn resolution(name: &str) -> Option<CallResolution> {
    match name {
        "reference" => Some(CallResolution::Reference),
        "callable" => Some(CallResolution::Callable),
        "non_callable" => Some(CallResolution::NonCallable),
        _ => None,
    }
}

struct Emit {
    kind: EdgeKind,
    from: Expr,
    to: Expr,
    site: Option<Expr>,
    resolution: CallResolution,
    resolve: Option<FnId>,
}

enum Action {
    Do(Expr),
    Emit(Box<[Emit]>),
    Revisit,
}

enum Children {
    Visit,
    Skip,
    Of(Box<[Expr]>),
}

struct Match {
    pat: Pat,
    depth: u8,
    kinds: Vec<u16>,
    slots: Slots,
    when: Option<Expr>,
    lang: Option<u8>,
}

struct FlowRule {
    at: Match,
    group: u8,
    enter: Box<[Action]>,
    children: Children,
    between: Box<[Action]>,
    exit: Box<[Action]>,
    late: Box<[Action]>,
}

enum TagVal {
    Lit(u32),
    Expr(Expr),
}

enum Out {
    Replace(Tpl<Expr>),
    Append(Vec<Tpl<Expr>>),
    Tag,
}

struct Rewrite {
    at: Match,
    out: Out,
    tags: Vec<(u32, TagVal)>,
    tag_on: Option<u16>,
    unique: Option<(Pat, u16, usize)>,
}

struct Stage {
    rules: Vec<Rewrite>,
    by_kind: HashMap<u16, Vec<u16>>,
    repeat: bool,
}

enum Body {
    Flow {
        rules: Vec<FlowRule>,
        by_kind: Vec<SmallVec<[u16; 4]>>,
    },
    Rewrite {
        stages: Vec<Vec<Stage>>,
        preorder: bool,
    },
}

pub struct Phase {
    pub name: String,
    pub label: &'static str,
    pub budget: BudgetSpec,
    pub scope: String,
    pub after: Vec<String>,
    pub walk: Walk,
    pub(super) modules: Vec<u8>,
    pub(super) links: super::Links,
    pub(super) data: Vec<Vec<Vals>>,
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
        let phases = spec
            .phases
            .iter()
            .map(|p| {
                let mut phase = Phase::load(p, &files, &names, lang)?;
                phase.data = values.clone();
                Ok(phase)
            })
            .collect::<Result<_, LoadError>>()?;
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
            .expect("pipeline.yaml declares every phase the crate runs")
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

fn walk_of(name: &str) -> Result<Walk, LoadError> {
    match name {
        "flow" => Ok(Walk::Flow),
        "each" => Ok(Walk::Each),
        "fixpoint" => Ok(Walk::Fixpoint),
        _ => Err(LoadError(format!("unknown walk {name}"))),
    }
}

struct Loader<'a> {
    spec: &'a PhaseSpec,
    walk: Walk,
    links: &'a super::Links,
    defaults: Vec<(EdgeKind, FnId)>,
    names: &'a HashMap<String, u16>,
    data: &'a BTreeMap<String, DataSpec>,
    lang: &'a Lang,
}

impl Phase {
    fn load(
        spec: &PhaseSpec,
        files: &RuleFiles,
        names: &HashMap<String, u16>,
        lang: &Lang,
    ) -> Result<Self, LoadError> {
        let links = super::Links::load(&binds(spec)?)?;
        let modules = spec
            .modules
            .iter()
            .map(|m| super::slot_of(m).ok_or_else(|| LoadError(format!("no module named {m}"))))
            .collect::<Result<_, _>>()?;
        let walk = walk_of(&spec.walk)?;
        let defaults = spec
            .resolve
            .iter()
            .map(|(kind, f)| Ok((edge_kind(kind)?, resolver(f, &links)?)))
            .collect::<Result<Vec<_>, LoadError>>()?;
        let loader = Loader {
            spec,
            walk,
            links: &links,
            defaults,
            names,
            data: &files.shared.data,
            lang,
        };
        let shared = files.shared.phases.get(&spec.name);
        fn own<'f>(file: &'f Option<LangFile>, name: &str) -> Option<&'f PhaseRules> {
            file.as_ref().and_then(|f| f.phases.get(name))
        }
        let family = spec.scope == "family";
        let (body, slots) = match walk {
            _ if family => loader.flow(
                shared,
                files.langs.iter().map(|f| own(f, &spec.name)).collect(),
            )?,
            Walk::Flow => loader.flow(
                shared,
                files.langs.iter().map(|f| own(f, &spec.name)).collect(),
            )?,
            Walk::Each | Walk::Fixpoint => {
                let stages = files
                    .langs
                    .iter()
                    .map(|file| loader.stages(own(file, &spec.name), shared))
                    .collect::<Result<Vec<_>, _>>()?;
                let slots = stages
                    .iter()
                    .flatten()
                    .flat_map(|s| s.rules.iter().map(|r| r.at.slots.len()))
                    .max()
                    .unwrap_or(1);
                let preorder = walk == Walk::Each;
                (Body::Rewrite { stages, preorder }, slots)
            }
        };
        for name in spec.budget.tree.iter().chain(&spec.budget.scope) {
            if crate::sentinel::Limits::UNLIMITED.ms(name).is_none() {
                return Err(LoadError(format!(
                    "phase {}: no budget named {name}",
                    spec.name
                )));
            }
        }
        let label = spec
            .budget
            .label
            .clone()
            .unwrap_or_else(|| spec.name.clone());
        Ok(Phase {
            name: spec.name.clone(),
            label: Box::leak(label.into_boxed_str()),
            budget: spec.budget.clone(),
            scope: spec.scope.clone(),
            after: spec.after.clone(),
            walk,
            modules,
            data: Vec::new(),
            body,
            slots,
            links,
        })
    }
}

fn rules_of(rules: Option<&PhaseRules>) -> Result<&[RuleSpec], LoadError> {
    match rules {
        None => Ok(&[]),
        Some(PhaseRules::Rules(rules)) => Ok(rules),
        Some(PhaseRules::Stages(_)) => {
            Err(LoadError("a flow phase takes rules, not stages".into()))
        }
    }
}

impl Loader<'_> {
    fn scope<'s>(&'s self, slots: &'s Slots, effects: bool) -> Scope<'s> {
        Scope {
            slots,
            modules: &self.spec.modules,
            links: self.links,
            lang: self.lang,
            data: self.names,
            effects,
        }
    }

    fn at<'s>(&self, spec: &'s RuleSpec) -> impl Fn(LoadError) -> LoadError + 's {
        let phase = self.spec.name.clone();
        move |e| LoadError(format!("{phase} rule {:?}: {}", spec.pattern, e.0))
    }

    fn matcher(&self, spec: &RuleSpec, lang: Option<u8>) -> Result<Match, LoadError> {
        let mut slots = Slots::new();
        let data = |name: &str| {
            let spec = self.data.get(name)?;
            let mut out = Vec::new();
            data_texts(spec, &mut out);
            Some(out)
        };
        let pat = pattern::parse_pattern(&spec.pattern, &mut slots, self.lang, &data)?;
        let (depth, filter) = pattern::anchor_kinds(&pat)
            .ok_or_else(|| LoadError("must match a node kind or capture $ROOT".into()))?;
        let kinds = match filter.kinds.is_empty() || filter.negate {
            true => vec![0],
            false => filter.kinds.clone(),
        };
        let when = spec
            .when
            .as_deref()
            .map(|src| Expr::parse(src, &self.scope(&slots, false)))
            .transpose()?;
        Ok(Match {
            pat,
            depth,
            kinds,
            slots,
            when,
            lang,
        })
    }

    fn check_hooks(&self, spec: &RuleSpec) -> Result<(), LoadError> {
        let allowed = |hook: &str| self.spec.outputs.iter().any(|o| o == hook);
        let flow = [
            ("enter", spec.enter.is_some()),
            ("children", spec.children.is_some()),
            ("between", spec.between.is_some()),
            ("exit", spec.exit.is_some()),
            ("late", spec.late.is_some()),
        ];
        let flows = self.walk == Walk::Flow || self.spec.scope == "family";
        for (hook, used) in flow {
            if used && !flows {
                return Err(LoadError(format!("hook {hook} needs a flow walk")));
            }
        }
        let rewrite = [
            ("replace", spec.replace.is_some()),
            ("append", spec.append.is_some()),
            ("replace", spec.unique.is_some()),
        ];
        for (hook, used) in rewrite {
            if used && (self.walk != Walk::Fixpoint || !allowed(hook)) {
                return Err(LoadError(format!(
                    "hook {hook} is not allowed in {}",
                    self.spec.name
                )));
            }
        }
        for (hook, used) in [("emit", spec.emit.is_some()), ("tag", spec.tag.is_some())] {
            if used && !allowed(hook) {
                return Err(LoadError(format!(
                    "{hook} is not allowed in {}",
                    self.spec.name
                )));
            }
        }
        Ok(())
    }

    fn flow(
        &self,
        shared: Option<&PhaseRules>,
        langs: Vec<Option<&PhaseRules>>,
    ) -> Result<(Body, usize), LoadError> {
        let shared = rules_of(shared)?;
        let mut groups: Vec<String> = Vec::new();
        for r in shared {
            let g = r.group.clone().unwrap_or_else(|| "default".into());
            if !groups.contains(&g) {
                groups.push(g);
            }
        }
        let mut ordered: Vec<(Option<u8>, &RuleSpec)> = Vec::new();
        for (at, rules) in langs.iter().enumerate() {
            ordered.extend(rules_of(*rules)?.iter().map(|r| (Some(at as u8), r)));
        }
        ordered.extend(shared.iter().map(|r| (None, r)));
        let mut rules = Vec::new();
        let mut steering: BTreeMap<u16, u8> = BTreeMap::new();
        let mut by_kind: Vec<SmallVec<[u16; 4]>> = Vec::new();
        for (owner, spec) in ordered {
            let at = self.at(spec);
            self.check_hooks(spec).map_err(&at)?;
            let name = spec.group.clone().unwrap_or_else(|| "default".into());
            let group = match groups.iter().position(|g| *g == name) {
                Some(found) => found,
                None => {
                    groups.push(name.clone());
                    groups.len() - 1
                }
            };
            let group = u8::try_from(group)
                .ok()
                .filter(|g| *g < 64)
                .ok_or_else(|| at(LoadError("more than 64 rule groups".into())))?;
            let rule = self.flow_rule(spec, group, owner).map_err(&at)?;
            for &kind in &rule.at.kinds {
                if kind < CANONICAL_BASE {
                    return Err(at(LoadError("must fire on a canonical kind".into())));
                }
                if !matches!(rule.children, Children::Visit)
                    && let Some(other) = steering.insert(kind, group)
                    && other != group
                {
                    return Err(at(LoadError(format!(
                        "groups {} and {name} both choose children for one kind",
                        groups[other as usize]
                    ))));
                }
                let index = usize::from(kind - CANONICAL_BASE);
                if by_kind.len() <= index {
                    by_kind.resize(index + 1, SmallVec::new());
                }
                by_kind[index].push(rules.len() as u16);
            }
            rules.push(rule);
        }
        let slots = rules.iter().map(|r| r.at.slots.len()).max().unwrap_or(1);
        Ok((Body::Flow { rules, by_kind }, slots))
    }

    fn actions(
        &self,
        slots: &Slots,
        list: &Option<Vec<ActionSpec>>,
    ) -> Result<Vec<Action>, LoadError> {
        list.as_deref()
            .unwrap_or_default()
            .iter()
            .map(|item| match item {
                ActionSpec::Emit { emit } => Ok(Action::Emit(self.emits(slots, emit)?)),
                ActionSpec::Expr(src) if src == "revisit" => Ok(Action::Revisit),
                ActionSpec::Expr(src) => {
                    let parsed = Expr::parse(src, &self.scope(slots, true))?;
                    match parsed.ends_in_effect() {
                        true => Ok(Action::Do(parsed)),
                        false => Err(LoadError(format!("action {src:?} must end in an effect"))),
                    }
                }
            })
            .collect()
    }

    fn emits(&self, slots: &Slots, map: &EmitMap) -> Result<Box<[Emit]>, LoadError> {
        map.iter()
            .map(|(kind, fields)| self.emit(slots, kind, fields))
            .collect()
    }

    fn emit(
        &self,
        slots: &Slots,
        kind: &str,
        fields: &BTreeMap<String, String>,
    ) -> Result<Emit, LoadError> {
        let edge = edge_kind(kind)?;
        if let Some(name) = fields
            .keys()
            .find(|n| !["from", "to", "site", "resolution", "resolve"].contains(&n.as_str()))
        {
            return Err(LoadError(format!("emit {kind}: unknown field {name}")));
        }
        let scope = self.scope(slots, false);
        let field = |name: &str| {
            fields
                .get(name)
                .map(|src| Expr::parse(src, &scope))
                .transpose()
        };
        let resolution = match fields.get("resolution") {
            None => CallResolution::Unknown,
            Some(name) => {
                resolution(name).ok_or_else(|| LoadError(format!("unknown resolution {name}")))?
            }
        };
        let resolve = match fields.get("resolve").map(String::as_str) {
            Some("none") => None,
            Some(name) => Some(resolver(name, self.links)?),
            None => self
                .defaults
                .iter()
                .find(|(k, _)| *k == edge)
                .map(|(_, f)| *f),
        };
        Ok(Emit {
            kind: edge,
            from: field("from")?.ok_or_else(|| LoadError(format!("emit {kind} needs from")))?,
            to: field("to")?.ok_or_else(|| LoadError(format!("emit {kind} needs to")))?,
            site: field("site")?,
            resolution,
            resolve,
        })
    }

    fn flow_rule(
        &self,
        spec: &RuleSpec,
        group: u8,
        lang: Option<u8>,
    ) -> Result<FlowRule, LoadError> {
        let at = self.matcher(spec, lang)?;
        let slots = &at.slots;
        let mut enter = self.actions(slots, &spec.enter)?;
        if let Some(map) = &spec.emit {
            enter.push(Action::Emit(self.emits(slots, map)?));
        }
        let children = match spec.children.as_ref().map(List::items) {
            None => Children::Visit,
            Some([one]) if one == "visit" => Children::Visit,
            Some([one]) if one == "skip" => Children::Skip,
            Some(items) => Children::Of(
                items
                    .iter()
                    .map(|s| Expr::parse(s, &self.scope(slots, false)))
                    .collect::<Result<_, _>>()?,
            ),
        };
        Ok(FlowRule {
            group,
            enter: enter.into(),
            children,
            between: self.actions(slots, &spec.between)?.into(),
            exit: self.actions(slots, &spec.exit)?.into(),
            late: self.actions(slots, &spec.late)?.into(),
            at,
        })
    }

    /// A language's own stages run first, then the shared stages.
    fn stages(
        &self,
        own: Option<&PhaseRules>,
        shared: Option<&PhaseRules>,
    ) -> Result<Vec<Stage>, LoadError> {
        fn listed(rules: Option<&PhaseRules>) -> Vec<(&[RuleSpec], bool)> {
            match rules {
                None => Vec::new(),
                Some(PhaseRules::Rules(rules)) => vec![(rules.as_slice(), false)],
                Some(PhaseRules::Stages(stages)) => stages
                    .iter()
                    .map(|s: &StageSpec| (s.rules.as_slice(), s.repeat.unwrap_or(false)))
                    .collect(),
            }
        }
        listed(own)
            .into_iter()
            .chain(listed(shared))
            .map(|(rules, repeat)| self.stage(rules, repeat))
            .collect()
    }

    fn stage(&self, specs: &[RuleSpec], repeat: bool) -> Result<Stage, LoadError> {
        let mut rules = Vec::new();
        let mut by_kind: HashMap<u16, Vec<u16>> = HashMap::new();
        for spec in specs {
            let at = self.at(spec);
            self.check_hooks(spec).map_err(&at)?;
            let rule = self.rewrite(spec).map_err(&at)?;
            if repeat && matches!(rule.out, Out::Append(_)) {
                return Err(at(LoadError("a repeat stage cannot append".into())));
            }
            for &kind in &rule.at.kinds {
                by_kind.entry(kind).or_default().push(rules.len() as u16);
            }
            rules.push(rule);
        }
        Ok(Stage {
            rules,
            by_kind,
            repeat,
        })
    }

    fn rewrite(&self, spec: &RuleSpec) -> Result<Rewrite, LoadError> {
        let at = self.matcher(spec, None)?;
        let scope = self.scope(&at.slots, false);
        let mut text = |src: &str| Expr::parse(src, &scope);
        let template = |src: &str, text: &mut dyn FnMut(&str) -> Result<Expr, LoadError>| {
            pattern::parse_template(src, &at.slots, self.lang, text)
        };
        let out = match (&spec.replace, &spec.append) {
            (Some(tpl), _) => Out::Replace(template(tpl, &mut text)?),
            (None, Some(tpls)) => Out::Append(
                tpls.iter()
                    .map(|t| template(t, &mut text))
                    .collect::<Result<_, _>>()?,
            ),
            (None, None) if spec.tag.is_some() => Out::Tag,
            (None, None) => return Err(LoadError("needs replace, append or tag".into())),
        };
        let tags = spec
            .tag
            .iter()
            .flatten()
            .map(|(key, val)| {
                let val = match val.starts_with('$') || val.starts_with("@$") {
                    true => TagVal::Expr(Expr::parse(val, &scope)?),
                    false => TagVal::Lit(self.lang.syms.intern(val)),
                };
                Ok((self.lang.syms.intern(key), val))
            })
            .collect::<Result<_, LoadError>>()?;
        let unique = spec
            .unique
            .as_deref()
            .map(|src| {
                let mut slots = Slots::new();
                let pat = pattern::parse_pattern(src, &mut slots, self.lang, &|_| None)?;
                let (_, kind) =
                    pattern::anchor(&pat).ok_or_else(|| LoadError(format!("unique {src:?}")))?;
                Ok::<_, LoadError>((pat, kind, slots.len()))
            })
            .transpose()?;
        Ok(Rewrite {
            tag_on: spec.tag_on.as_deref().map(|k| self.lang.intern_kind(k)),
            out,
            tags,
            unique,
            at,
        })
    }
}

fn edge_kind(name: &str) -> Result<EdgeKind, LoadError> {
    EMIT_KINDS
        .iter()
        .find(|(n, _)| *n == name)
        .map(|(_, kind)| *kind)
        .ok_or_else(|| LoadError(format!("unknown edge kind {name}")))
}

fn resolver(name: &str, links: &super::Links) -> Result<FnId, LoadError> {
    let (module, f) = name
        .strip_prefix("mod.")
        .and_then(|n| n.split_once('.'))
        .unwrap_or(("core", name));
    let id =
        super::lookup(module, f).ok_or_else(|| LoadError(format!("unknown resolve {name}")))?;
    let id = links.to(id);
    match super::function(id).sig == [super::Arg::One, super::Arg::Set] {
        true => Ok(id),
        false => Err(LoadError(format!("resolve {name} takes (target, site)"))),
    }
}

fn binds(spec: &PhaseSpec) -> Result<Vec<(FnId, FnId)>, LoadError> {
    let id = |path: &str| {
        let (module, name) = path
            .split_once('.')
            .ok_or_else(|| LoadError(format!("bind {path}: expected <module>.<function>")))?;
        if !spec.modules.iter().any(|m| m == module) {
            return Err(LoadError(format!(
                "bind {path}: {module} is not in the phase"
            )));
        }
        super::lookup(module, name).ok_or_else(|| LoadError(format!("bind {path}: unknown")))
    };
    let mut binds = Vec::new();
    for (of, to) in &spec.bind {
        let (of, to) = (id(of)?, id(to)?);
        let (declared, provider) = (super::function(of), super::function(to));
        if !declared.required || provider.required {
            return Err(LoadError(format!(
                "bind {}: binds a required function to a provider",
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
    for module in &spec.modules {
        if let Some(id) = super::required(module).find(|id| binds.iter().all(|(of, _)| of != id)) {
            return Err(LoadError(format!(
                "phase {}: {module}.{} needs a bind",
                spec.name,
                super::function(id).name
            )));
        }
    }
    Ok(binds)
}

impl Emit {
    fn run<S: Store>(&self, run: &mut Run<S>, caps: &[Vals]) {
        let sites = self
            .site
            .as_ref()
            .map(|s| s.eval(run, caps))
            .unwrap_or_default();
        let site = sites.first().and_then(|v| v.loc()).map(|l| l.node);
        let from = self.from.eval(run, caps);
        if from.is_empty() {
            return;
        }
        let to = self.to.eval(run, caps);
        for from in from.iter().filter_map(|v| v.loc()) {
            for (&target, to) in to.iter().filter_map(|v| Some((v, v.loc()?))) {
                let resolved = self.resolve.and_then(|f| {
                    let found = run.call(f, &[SmallVec::from_elem(target, 1), sites.clone()]);
                    match found.first() {
                        Some(&Val::Sym(name)) => resolution(run.lang_ref.syms.resolve(name)),
                        _ => None,
                    }
                });
                run.edges.push(Edge {
                    site,
                    call_resolution: resolved.unwrap_or(self.resolution),
                    ..Edge::new(from.fi, from.node, to.fi, to.node, self.kind)
                });
            }
        }
    }
}

enum Item {
    Visit(u32),
    Between(u16, u32),
    Exit(u16, u32),
    Leave,
}

pub struct Late {
    rule: u16,
    node: u32,
    def: Option<u32>,
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

    fn run(&self, run: &mut Run<S>, actions: &[Action]) -> bool {
        let mut revisit = false;
        for action in actions {
            match action {
                Action::Do(expr) => {
                    expr.eval(run, &self.vals);
                }
                Action::Emit(emits) => emits.iter().for_each(|e| e.run(run, &self.vals)),
                Action::Revisit => revisit = true,
            }
        }
        revisit
    }
}

impl Phase {
    /// Walks the tree in source order. Returns the nodes whose rules have
    /// `late` actions that no `revisit` ran yet.
    pub fn flow<S: Store>(&self, run: &mut Run<S>, root: Cursor<'_, S>) -> Vec<Late> {
        let Body::Flow { rules, by_kind } = &self.body else {
            return Vec::new();
        };
        let mut w = Scratch::<S>::new(self.slots);
        let mut late = Vec::new();
        let mut stack: Vec<Item> = root
            .children_rev()
            .map(|c| Item::Visit(c.index()))
            .collect();
        while let Some(item) = stack.pop() {
            if !run.step() {
                return Vec::new();
            }
            match item {
                Item::Visit(node) => {
                    let c = root.jump(root.fi(), node);
                    let candidates = c
                        .kind()
                        .checked_sub(CANONICAL_BASE)
                        .and_then(|i| by_kind.get(usize::from(i)))
                        .map_or(&[][..], |r| r.as_slice());
                    visit(rules, candidates, run, &mut w, &mut stack, &mut late, c);
                }
                Item::Leave => run.leave_def(),
                Item::Between(rule, node) | Item::Exit(rule, node) => {
                    let rule = &rules[rule as usize];
                    w.matches(run, &rule.at, root.jump(root.fi(), node));
                    let actions = match item {
                        Item::Between(..) => &rule.between,
                        _ => &rule.exit,
                    };
                    if w.run(run, actions) {
                        self.revisit(run, root, std::mem::take(&mut late));
                    }
                }
            }
        }
        late
    }

    /// Runs the `late` actions of each deferred node, with the def that
    /// enclosed it when it was visited.
    pub fn revisit<S: Store>(&self, run: &mut Run<S>, root: Cursor<'_, S>, late: Vec<Late>) {
        let Body::Flow { rules, .. } = &self.body else {
            return;
        };
        let mut w = Scratch::<S>::new(self.slots);
        for Late { rule, node, def } in late {
            let rule = &rules[rule as usize];
            if let Some(def) = def {
                run.enter_def(def);
            }
            if w.matches(run, &rule.at, root.jump(root.fi(), node)) {
                w.run(run, &rule.late);
            }
            if def.is_some() {
                run.leave_def();
            }
        }
    }
}

fn visit<S: Store>(
    rules: &[FlowRule],
    candidates: &[u16],
    run: &mut Run<S>,
    w: &mut Scratch<S>,
    stack: &mut Vec<Item>,
    late: &mut Vec<Late>,
    c: Cursor<'_, S>,
) {
    let mut fired = 0u64;
    let mut exits: SmallVec<[u16; 4]> = SmallVec::new();
    let mut chosen: Option<(u16, Option<Vec<Vals>>)> = None;
    for &index in candidates {
        let rule = &rules[index as usize];
        let bit = 1u64 << rule.group;
        if fired & bit != 0 || !w.matches(run, &rule.at, c) || !w.holds(run, &rule.at) {
            continue;
        }
        fired |= bit;
        w.run(run, &rule.enter);
        if !rule.late.is_empty() {
            let def = run.defs.last().copied();
            late.push(Late {
                rule: index,
                node: c.index(),
                def,
            });
        }
        if !rule.exit.is_empty() {
            exits.push(index);
        }
        match &rule.children {
            Children::Visit => {}
            Children::Skip => chosen = Some((index, None)),
            Children::Of(groups) => {
                let groups = groups.iter().map(|g| g.eval(run, &w.vals)).collect();
                chosen = Some((index, Some(groups)));
            }
        }
    }
    stack.extend(exits.iter().map(|&rule| Item::Exit(rule, c.index())));
    if c.is(C::Def) {
        run.enter_def(c.index());
        stack.push(Item::Leave);
    }
    match chosen {
        None => stack.extend(c.children_rev().map(|child| Item::Visit(child.index()))),
        Some((_, None)) => {}
        Some((rule, Some(groups))) => {
            let between = !rules[rule as usize].between.is_empty();
            for (at, group) in groups.iter().enumerate().rev() {
                stack.extend(
                    group
                        .iter()
                        .rev()
                        .filter_map(|v| v.loc())
                        .map(|loc| Item::Visit(loc.node)),
                );
                if at == 1 && between {
                    stack.push(Item::Between(rule, c.index()));
                }
            }
        }
    }
}

/// What a rewrite rule decided for one node, computed before the tree is
/// mutated: the captures, the tag values and the template texts.
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

impl Phase {
    fn stages_for(&self, lang: usize) -> (&[Stage], bool) {
        match &self.body {
            Body::Rewrite { stages, preorder } => {
                (stages.get(lang).map_or(&[][..], Vec::as_slice), *preorder)
            }
            Body::Flow { .. } => (&[], false),
        }
    }

    /// Applies a rewrite phase to one tree in place. Each stage runs once, or
    /// until a pass replaces nothing when it repeats.
    pub fn rewrite(
        &self,
        tree: &mut Tree<Mutable>,
        env: &Env,
        budget: &Sentinel,
        file: &Sentinel,
    ) -> Result<(), Killed> {
        self.rewrite_with(tree, env, env.rules_index(&tree.label), budget, file)
    }

    pub fn has_rules(&self, rules: usize) -> bool {
        self.stages_for(rules).0.iter().any(|s| !s.rules.is_empty())
    }

    /// Every tag key a rewrite phase's rules can set, for the rules at `rules`.
    pub fn tag_keys(&self, rules: usize) -> Vec<u32> {
        self.stages_for(rules)
            .0
            .iter()
            .flat_map(|s| &s.rules)
            .flat_map(|r| r.tags.iter().map(|(key, _)| *key))
            .collect()
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
        let (stages, preorder) = self.stages_for(rules);
        for stage in stages {
            loop {
                let replaced = self.pass(tree, env, stage, preorder, budget, file)?;
                if !stage.repeat || !replaced {
                    break;
                }
                sentinels.iter().try_for_each(|s| s.check())?;
            }
        }
        Ok(())
    }

    /// Sets the tags of a tag-only phase, such as display, on one tree.
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
        let (stages, preorder) = self.stages_for(env.rules_index(&tree.label));
        for stage in stages {
            for target in order(tree, preorder) {
                sentinels.iter().try_for_each(|s| s.check())?;
                let fired: Vec<(u16, Vec<(u32, u32)>)> = {
                    let mut run = self.run(&*tree, env, budget, file);
                    run.graph = Some(graph);
                    run.graph_fi = fi;
                    let mut w = Scratch::<S>::new(self.slots);
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
        Run::new(
            std::slice::from_ref(tree),
            0,
            env,
            &self.links,
            &self.data,
            budget,
            file,
        )
    }

    fn pass(
        &self,
        tree: &mut Tree<Mutable>,
        env: &Env,
        stage: &Stage,
        preorder: bool,
        budget: &Sentinel,
        file: &Sentinel,
    ) -> Result<bool, Killed> {
        let sentinels = [budget, file];
        let lang = &env.lang;
        let mut replaced = false;
        for target in order(tree, preorder) {
            sentinels.iter().try_for_each(|s| s.check())?;
            if tree.storage.is_removed(tree.storage.id(target)) {
                continue;
            }
            for i in candidates(stage, tree, target).collect::<Vec<_>>() {
                let rule = &stage.rules[i as usize];
                let fired = {
                    let mut run = self.run(&*tree, env, budget, file);
                    let mut w = Scratch::<Mutable>::new(self.slots);
                    let Some((tags, texts)) = self.decide(&mut run, &mut w, rule, target) else {
                        continue;
                    };
                    let mut caps = w.caps;
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
                            for id in built {
                                at.append(id, &mut tree.storage.0);
                            }
                        }
                    }
                    Out::Replace(tpl) => {
                        let mv = rule.unique.is_none().then_some(at);
                        let texts = &fired.texts[0];
                        let built =
                            pattern::build(tree, lang, at, tpl, &fired.caps, mv, &mut |i, _| {
                                texts[i]
                            });
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
    let specific = stage.by_kind.get(&kind).map_or(&[][..], Vec::as_slice);
    let any = stage.by_kind.get(&0).map_or(&[][..], Vec::as_slice);
    let mut merged: Vec<u16> = specific.iter().chain(any).copied().collect();
    merged.sort_unstable();
    merged.dedup();
    merged.into_iter()
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
