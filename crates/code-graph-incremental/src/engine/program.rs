use std::collections::{BTreeMap, HashMap};

use smallvec::SmallVec;

use super::expr::{Expr, Scope};
use super::matching::{matches, matches_anchored};
use super::{Cursor, FnId, Kind, Loc, Run, Val, Vals};
use crate::canonical::{CANONICAL_BASE, Canonical as C};
use crate::dsl::parser::parse;
use crate::dsl::types::{Cap, Ctx, Pat};
use crate::error::LoadError;
use crate::intern::Lang;
use crate::tree::{CallResolution, Edge, EdgeKind};

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
}

#[derive(serde::Deserialize)]
struct PipelineSpec {
    phases: Vec<PhaseSpec>,
}

#[derive(serde::Deserialize)]
#[serde(untagged)]
enum List {
    One(String),
    Many(Vec<String>),
}

impl List {
    fn items(&self) -> &[String] {
        match self {
            List::One(one) => std::slice::from_ref(one),
            List::Many(many) => many,
        }
    }
}

type EmitMap = BTreeMap<String, BTreeMap<String, String>>;

#[derive(serde::Deserialize)]
#[serde(untagged)]
enum ActionSpec {
    Emit { emit: EmitMap },
    Expr(String),
}

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct RuleSpec {
    group: Option<String>,
    #[serde(rename = "match")]
    pattern: String,
    when: Option<String>,
    enter: Option<Vec<ActionSpec>>,
    children: Option<List>,
    between: Option<Vec<ActionSpec>>,
    exit: Option<Vec<ActionSpec>>,
    late: Option<Vec<ActionSpec>>,
    emit: Option<EmitMap>,
}

const EMIT_KINDS: [(&str, EdgeKind); 5] = [
    ("calls", EdgeKind::Calls),
    ("imports", EdgeKind::Imports),
    ("extends", EdgeKind::Extends),
    ("defines", EdgeKind::Defines),
    ("type_flow", EdgeKind::TypeFlow),
];

struct Emit {
    kind: EdgeKind,
    from: Expr,
    to: Expr,
    site: Option<Expr>,
    resolution: CallResolution,
    resolve: Option<FnId>,
}

pub fn resolution(name: &str) -> Option<CallResolution> {
    match name {
        "reference" => Some(CallResolution::Reference),
        "callable" => Some(CallResolution::Callable),
        "non_callable" => Some(CallResolution::NonCallable),
        _ => None,
    }
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

struct Rule {
    lang: Option<u8>,
    group: u8,
    pat: Pat,
    depth: u8,
    filters: Vec<Vec<u16>>,
    slots: usize,
    when: Option<Expr>,
    enter: Box<[Action]>,
    children: Children,
    between: Box<[Action]>,
    exit: Box<[Action]>,
    late: Box<[Action]>,
}

pub struct Phase {
    pub name: String,
    pub scope: String,
    pub after: Vec<String>,
    pub(super) modules: Vec<u8>,
    pub(super) links: super::Links,
    rules: Vec<Rule>,
    by_kind: Vec<SmallVec<[u16; 4]>>,
    slots: usize,
}

pub struct Pipeline {
    pub phases: Vec<Phase>,
}

#[derive(serde::Deserialize, Default)]
struct LangRules {
    #[serde(default)]
    link: Vec<RuleSpec>,
}

impl LangRules {
    fn phase(&self, name: &str) -> &[RuleSpec] {
        match name {
            "link" => &self.link,
            _ => &[],
        }
    }
}

impl Pipeline {
    /// `languages` holds each language's rule file, by its index in the env.
    pub fn load(lang: &Lang, languages: &[Option<&str>]) -> Result<Self, LoadError> {
        let spec: PipelineSpec =
            orbit_utils::yaml::from_str(include_str!("../../config/pipeline.yaml"))?;
        let mut canonical: HashMap<String, Vec<RuleSpec>> =
            orbit_utils::yaml::from_str(include_str!("../../config/canonical.yaml"))?;
        let languages: Vec<LangRules> = languages
            .iter()
            .map(|yaml| yaml.map_or_else(|| Ok(LangRules::default()), orbit_utils::yaml::from_str))
            .collect::<Result<_, _>>()?;
        let phases = spec
            .phases
            .into_iter()
            .map(|p| {
                let rules = canonical.remove(&p.name).unwrap_or_default();
                let own: Vec<&[RuleSpec]> = languages.iter().map(|l| l.phase(&p.name)).collect();
                Phase::load(p, &rules, &own, lang)
            })
            .collect::<Result<_, _>>()?;
        if let Some(name) = canonical.keys().next() {
            return Err(LoadError(format!("canonical.yaml: no phase named {name}")));
        }
        Ok(Self { phases })
    }

    pub fn phase(&self, name: &str) -> &Phase {
        self.phases
            .iter()
            .find(|p| p.name == name)
            .expect("pipeline.yaml declares every phase the crate runs")
    }
}

fn hooks_allowed(walk: &str) -> &'static [&'static str] {
    match walk {
        "flow" => &["enter", "children", "between", "exit", "late", "emit"],
        "each" => &["emit"],
        _ => &[],
    }
}

impl Phase {
    /// Language rules come first in each kind's list, so within a group a
    /// language's rule wins over the shared one; groups keep the order the
    /// shared rules declare.
    fn load(
        spec: PhaseSpec,
        rules: &[RuleSpec],
        languages: &[&[RuleSpec]],
        lang: &Lang,
    ) -> Result<Self, LoadError> {
        let links = super::Links::load(&binds(&spec)?)?;
        let modules = spec
            .modules
            .iter()
            .map(|m| super::slot_of(m).ok_or_else(|| LoadError(format!("no module named {m}"))))
            .collect::<Result<_, _>>()?;
        let defaults = spec
            .resolve
            .iter()
            .map(|(kind, f)| Ok((edge_kind(kind)?, resolver(f, &links)?)))
            .collect::<Result<Vec<_>, LoadError>>()?;
        let mut phase = Phase {
            name: spec.name.clone(),
            scope: spec.scope.clone(),
            after: spec.after.clone(),
            modules,
            links,
            rules: Vec::new(),
            by_kind: Vec::new(),
            slots: 1,
        };
        let mut groups: Vec<&str> = rules
            .iter()
            .map(|r| r.group.as_deref().unwrap_or("default"))
            .fold(Vec::new(), |mut groups, g| {
                if !groups.contains(&g) {
                    groups.push(g);
                }
                groups
            });
        let mut steering: BTreeMap<u16, u8> = BTreeMap::new();
        let mut kinds: Vec<(u16, u16)> = Vec::new();
        let ordered = languages
            .iter()
            .enumerate()
            .flat_map(|(at, rules)| rules.iter().map(move |r| (Some(at as u8), r)))
            .chain(rules.iter().map(|r| (None, r)));
        for (owner, rule) in ordered {
            let at = |e: LoadError| {
                LoadError(format!("{} rule {:?}: {}", phase.name, rule.pattern, e.0))
            };
            rule.check_hooks(&spec).map_err(at)?;
            let name = rule.group.as_deref().unwrap_or("default");
            let group = match groups.iter().position(|g| *g == name) {
                Some(at) => at,
                None => {
                    groups.push(name);
                    groups.len() - 1
                }
            };
            let group = u8::try_from(group)
                .ok()
                .filter(|g| *g < 64)
                .ok_or_else(|| at(LoadError("more than 64 rule groups".into())))?;
            let mut rule =
                Rule::load(rule, group, &spec, &phase.links, &defaults, lang).map_err(at)?;
            rule.lang = owner;
            let kind = rule.kind().map_err(at)?;
            if !matches!(rule.children, Children::Visit)
                && let Some(other) = steering.insert(kind, group)
                && other != group
            {
                return Err(at(LoadError(format!(
                    "groups {} and {name} both choose children for one kind",
                    groups[other as usize]
                ))));
            }
            kinds.push((kind, phase.rules.len() as u16));
            phase.slots = phase.slots.max(rule.slots);
            phase.rules.push(rule);
        }
        for (kind, rule) in kinds {
            let index = usize::from(kind - CANONICAL_BASE);
            if phase.by_kind.len() <= index {
                phase.by_kind.resize(index + 1, SmallVec::new());
            }
            phase.by_kind[index].push(rule);
        }
        Ok(phase)
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

/// Each abstract function of the phase's modules, resolved once to the
/// function `bind:` provides for it.
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

impl RuleSpec {
    fn check_hooks(&self, phase: &PhaseSpec) -> Result<(), LoadError> {
        let used = [
            ("enter", self.enter.is_some()),
            ("children", self.children.is_some()),
            ("between", self.between.is_some()),
            ("exit", self.exit.is_some()),
            ("late", self.late.is_some()),
            ("emit", self.emit.is_some()),
        ];
        let allowed = hooks_allowed(&phase.walk);
        let emits = phase.outputs.iter().any(|o| o == "emit");
        for (hook, used) in used {
            if used && (!allowed.contains(&hook) || hook == "emit" && !emits) {
                return Err(LoadError(format!(
                    "hook {hook} is not allowed in a {} phase",
                    phase.walk
                )));
            }
        }
        Ok(())
    }
}

/// How far above the matched node the pattern starts, and the kind of the
/// node it fires on: the `$ROOT` capture, or the pattern's own root.
fn anchor(p: &Pat, depth: u8) -> Option<(u8, u16)> {
    match p {
        Pat::Cap {
            slot: 0,
            kind,
            guard,
            ..
        } => {
            let guarded = guard.as_deref().and_then(|g| match g {
                Pat::Node { kind, .. } => Some(*kind),
                _ => None,
            });
            Some((depth, kind.or(guarded)?))
        }
        Pat::Cap { guard: Some(g), .. } => anchor(g, depth),
        Pat::Node { kids, .. } => kids.iter().find_map(|kid| anchor(kid, depth + 1)),
        _ => None,
    }
}

impl Rule {
    fn load(
        spec: &RuleSpec,
        group: u8,
        phase: &PhaseSpec,
        links: &super::Links,
        defaults: &[(EdgeKind, FnId)],
        lang: &Lang,
    ) -> Result<Self, LoadError> {
        let mut ctx = Ctx::new(lang);
        ctx.slot("ROOT")?;
        let pat = parse(&mut ctx, &spec.pattern)?;
        let slots = &ctx.slots;
        let scope = |effects| Scope {
            slots,
            modules: &phase.modules,
            links,
            lang,
            effects,
        };
        let expr = |src: &str| Expr::parse(src, &scope(false));
        let emit = |map: &EmitMap| -> Result<Box<[Emit]>, LoadError> {
            map.iter()
                .map(|(kind, fields)| Emit::load(kind, fields, &scope(false), links, defaults))
                .collect()
        };
        let actions = |list: &Option<Vec<ActionSpec>>| -> Result<Vec<Action>, LoadError> {
            let items = list.as_deref().unwrap_or_default();
            items
                .iter()
                .map(|item| match item {
                    ActionSpec::Emit { emit: map } => Ok(Action::Emit(emit(map)?)),
                    ActionSpec::Expr(src) if src == "revisit" => Ok(Action::Revisit),
                    ActionSpec::Expr(src) => {
                        let parsed = Expr::parse(src, &scope(true))?;
                        match parsed.ends_in_effect() {
                            true => Ok(Action::Do(parsed)),
                            false => {
                                Err(LoadError(format!("action {src:?} must end in an effect")))
                            }
                        }
                    }
                })
                .collect()
        };
        let mut enter = actions(&spec.enter)?;
        if let Some(map) = &spec.emit {
            enter.push(Action::Emit(emit(map)?));
        }
        let children = match spec.children.as_ref().map(List::items) {
            None => Children::Visit,
            Some([one]) if one == "visit" => Children::Visit,
            Some([one]) if one == "skip" => Children::Skip,
            Some(items) => Children::Of(items.iter().map(|s| expr(s)).collect::<Result<_, _>>()?),
        };
        let (depth, _) = anchor(&pat, 0).unwrap_or((0, 0));
        Ok(Rule {
            lang: None,
            group,
            slots: ctx.slots.len(),
            filters: ctx.filters.clone(),
            when: spec.when.as_deref().map(expr).transpose()?,
            enter: enter.into(),
            children,
            between: actions(&spec.between)?.into(),
            exit: actions(&spec.exit)?.into(),
            late: actions(&spec.late)?.into(),
            depth,
            pat,
        })
    }

    fn kind(&self) -> Result<u16, LoadError> {
        let kind = match anchor(&self.pat, 0) {
            Some((_, kind)) => kind,
            None => match self.pat {
                Pat::Node { kind, .. } => kind,
                _ => return Err(LoadError("must match a node kind".into())),
            },
        };
        match kind >= CANONICAL_BASE {
            true => Ok(kind),
            false => Err(LoadError("must fire on a canonical kind".into())),
        }
    }
}

impl Emit {
    fn load(
        kind: &str,
        fields: &BTreeMap<String, String>,
        scope: &Scope,
        links: &super::Links,
        defaults: &[(EdgeKind, FnId)],
    ) -> Result<Self, LoadError> {
        let edge = edge_kind(kind)?;
        for name in fields.keys() {
            if !["from", "to", "site", "resolution", "resolve"].contains(&name.as_str()) {
                return Err(LoadError(format!("emit {kind}: unknown field {name}")));
            }
        }
        let field = |name: &str| {
            fields
                .get(name)
                .map(|src| Expr::parse(src, scope))
                .transpose()
        };
        let resolution = match fields.get("resolution").map(String::as_str) {
            None => CallResolution::Unknown,
            Some(name) => {
                resolution(name).ok_or_else(|| LoadError(format!("unknown resolution {name}")))?
            }
        };
        let resolve = match fields.get("resolve").map(String::as_str) {
            Some("none") => None,
            Some(name) => Some(resolver(name, links)?),
            None => defaults.iter().find(|(k, _)| *k == edge).map(|(_, f)| *f),
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

    fn run(&self, run: &mut Run, caps: &[Vals]) {
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
                        Some(&Val::Sym(name)) => resolution(run.env.lang.syms.resolve(name)),
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

impl Phase {
    fn candidates(&self, kind: u16) -> &[u16] {
        kind.checked_sub(CANONICAL_BASE)
            .and_then(|index| self.by_kind.get(usize::from(index)))
            .map_or(&[], |rules| rules.as_slice())
    }

    /// Walks the tree in source order. Returns the nodes whose rules have
    /// `late` actions that no `revisit` ran yet.
    pub fn flow(&self, cx: &mut Run, root: Cursor<'_>) -> Vec<Late> {
        let mut w = Walk::new(self.slots);
        w.stack = root
            .children_rev()
            .map(|c| Item::Visit(c.index()))
            .collect();
        while let Some(item) = w.stack.pop() {
            if !cx.step() {
                return Vec::new();
            }
            match item {
                Item::Visit(node) => self.visit(cx, &mut w, root.jump(root.fi(), node)),
                Item::Leave => cx.leave_def(),
                Item::Between(rule, node) | Item::Exit(rule, node) => {
                    let rule = &self.rules[rule as usize];
                    w.matches(cx, rule, root.jump(root.fi(), node));
                    let actions = match item {
                        Item::Between(..) => &rule.between,
                        _ => &rule.exit,
                    };
                    if w.run(cx, actions) {
                        self.revisit(cx, root, std::mem::take(&mut w.late));
                    }
                }
            }
        }
        w.late
    }

    /// Runs the `late` actions of each deferred node, with the def that
    /// enclosed it when it was visited.
    pub fn revisit(&self, cx: &mut Run, root: Cursor<'_>, late: Vec<Late>) {
        let mut w = Walk::new(self.slots);
        for Late { rule, node, def } in late {
            let rule = &self.rules[rule as usize];
            if let Some(def) = def {
                cx.enter_def(def);
            }
            if w.matches(cx, rule, root.jump(root.fi(), node)) {
                w.run(cx, &rule.late);
            }
            if def.is_some() {
                cx.leave_def();
            }
        }
    }

    fn visit(&self, cx: &mut Run, w: &mut Walk, c: Cursor<'_>) {
        let mut fired = 0u64;
        let mut exits: SmallVec<[u16; 4]> = SmallVec::new();
        let mut chosen: Option<(u16, Option<Vec<Vals>>)> = None;
        for &index in self.candidates(c.kind()) {
            let rule = &self.rules[index as usize];
            let bit = 1u64 << rule.group;
            let foreign = rule.lang.is_some_and(|l| usize::from(l) != cx.lang);
            if foreign || fired & bit != 0 || !w.matches(cx, rule, c) {
                continue;
            }
            if let Some(when) = &rule.when
                && !super::truthy(&when.eval(cx, &w.vals))
            {
                continue;
            }
            fired |= bit;
            w.run(cx, &rule.enter);
            if !rule.late.is_empty() {
                let def = cx.defs.last().copied();
                w.late.push(Late {
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
                    let groups = groups.iter().map(|g| g.eval(cx, &w.vals)).collect();
                    chosen = Some((index, Some(groups)));
                }
            }
        }
        w.stack
            .extend(exits.iter().map(|&rule| Item::Exit(rule, c.index())));
        w.enter(cx, c);
        match chosen {
            None => w.push_children(c),
            Some((_, None)) => {}
            Some((rule, Some(groups))) => {
                let between = !self.rules[rule as usize].between.is_empty();
                for (at, group) in groups.iter().enumerate().rev() {
                    let nodes = group.iter().rev().filter_map(|v| v.loc());
                    w.stack.extend(nodes.map(|loc| Item::Visit(loc.node)));
                    if at == 1 && between {
                        w.stack.push(Item::Between(rule, c.index()));
                    }
                }
            }
        }
    }
}

pub struct Late {
    rule: u16,
    node: u32,
    def: Option<u32>,
}

struct Walk {
    caps: Vec<Cap<u32>>,
    vals: Vec<Vals>,
    stack: Vec<Item>,
    late: Vec<Late>,
}

impl Walk {
    fn new(slots: usize) -> Self {
        Self {
            caps: vec![SmallVec::new(); slots],
            vals: vec![Vals::new(); slots],
            stack: Vec::new(),
            late: Vec::new(),
        }
    }

    fn matches(&mut self, cx: &Run, rule: &Rule, c: Cursor<'_>) -> bool {
        for cap in &mut self.caps[..rule.slots] {
            cap.clear();
        }
        let tree = c.tree();
        let lang = &cx.env.lang;
        let found = match rule.depth {
            0 => matches(tree, lang, c.index(), &rule.pat, &mut self.caps),
            depth => {
                let top = (0..depth).try_fold(c, |node, _| node.parent());
                top.is_some_and(|top| {
                    matches_anchored(
                        tree,
                        lang,
                        top.index(),
                        c.index(),
                        &rule.pat,
                        &mut self.caps,
                    )
                })
            }
        };
        if !found {
            return false;
        }
        let fi = c.fi();
        self.vals[0] = SmallVec::from_elem(Val::Node(c.into()), 1);
        for (slot, (val, cap)) in self.vals[1..rule.slots]
            .iter_mut()
            .zip(&self.caps[1..])
            .enumerate()
        {
            let filter = rule.filters.get(slot + 1).map_or(&[][..], Vec::as_slice);
            *val = cap
                .iter()
                .filter(|&&node| filter.is_empty() || filter.contains(&c.jump(fi, node).kind()))
                .map(|&node| Val::Node(Loc { fi, node }))
                .collect();
        }
        true
    }

    /// Runs `actions` and reports whether one of them asked for a revisit.
    fn run(&self, cx: &mut Run, actions: &[Action]) -> bool {
        let mut revisit = false;
        for action in actions {
            match action {
                Action::Do(expr) => {
                    expr.eval(cx, &self.vals);
                }
                Action::Emit(emits) => emits.iter().for_each(|e| e.run(cx, &self.vals)),
                Action::Revisit => revisit = true,
            }
        }
        revisit
    }

    fn enter(&mut self, cx: &mut Run, c: Cursor<'_>) {
        if c.is(C::Def) {
            cx.enter_def(c.index());
            self.stack.push(Item::Leave);
        }
    }

    fn push_children(&mut self, c: Cursor<'_>) {
        self.stack
            .extend(c.children_rev().map(|child| Item::Visit(child.index())));
    }
}
