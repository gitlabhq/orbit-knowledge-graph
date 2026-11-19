use std::collections::{BTreeMap, HashMap};

use smallvec::SmallVec;

use super::expr::{Expr, Scope};
use super::matching::matches;
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
    #[serde(rename = "match")]
    pattern: String,
    when: Option<List>,
    enter: Option<Vec<ActionSpec>>,
    children: Option<List>,
    between: Option<Vec<ActionSpec>>,
    exit: Option<Vec<ActionSpec>>,
    emit: Option<EmitMap>,
}

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
}

enum Children {
    Visit,
    Skip,
    Of(Box<[Expr]>),
}

struct Rule {
    pat: Pat,
    slots: usize,
    when: Box<[Expr]>,
    enter: Box<[Action]>,
    children: Children,
    between: Box<[Action]>,
    exit: Box<[Action]>,
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

impl Pipeline {
    pub fn load(lang: &Lang) -> Result<Self, LoadError> {
        let spec: PipelineSpec =
            orbit_utils::yaml::from_str(include_str!("../../config/pipeline.yaml"))?;
        let mut canonical: HashMap<String, Vec<RuleSpec>> =
            orbit_utils::yaml::from_str(include_str!("../../config/canonical.yaml"))?;
        let phases = spec
            .phases
            .into_iter()
            .map(|p| {
                let rules = canonical.remove(&p.name).unwrap_or_default();
                Phase::load(p, &rules, lang)
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

impl Phase {
    fn load(spec: PhaseSpec, rules: &[RuleSpec], lang: &Lang) -> Result<Self, LoadError> {
        let links = super::Links::load(&binds(&spec)?)?;
        let mut phase = Phase {
            name: spec.name,
            scope: spec.scope,
            after: spec.after,
            modules: spec
                .modules
                .iter()
                .map(|m| super::slot_of(m).ok_or_else(|| LoadError(format!("no module named {m}"))))
                .collect::<Result<_, _>>()?,
            links,
            rules: Vec::new(),
            by_kind: Vec::new(),
            slots: 1,
        };
        for rule in rules {
            let at = |e: LoadError| {
                LoadError(format!("{} rule {:?}: {}", phase.name, rule.pattern, e.0))
            };
            let hooks = [
                ("enter", rule.enter.is_some()),
                ("children", rule.children.is_some()),
                ("between", rule.between.is_some()),
                ("exit", rule.exit.is_some()),
                ("emit", rule.emit.is_some()),
            ];
            let allowed: &[&str] = match spec.walk.as_str() {
                "flow" => &["enter", "children", "between", "exit", "emit"],
                "each" => &["emit"],
                _ => &[],
            };
            for (hook, used) in hooks {
                let output = hook != "emit" || spec.outputs.iter().any(|o| o == "emit");
                if used && (!allowed.contains(&hook) || !output) {
                    return Err(at(LoadError(format!(
                        "hook {hook} is not allowed in a {} phase",
                        spec.walk
                    ))));
                }
            }
            let rule = Rule::load(rule, &spec.modules, &phase.links, lang).map_err(at)?;
            let Pat::Node { kind, .. } = rule.pat else {
                return Err(at(LoadError("must match a node kind".into())));
            };
            if kind < CANONICAL_BASE {
                return Err(at(LoadError("must match a canonical kind".into())));
            }
            let index = usize::from(kind - CANONICAL_BASE);
            if phase.by_kind.len() <= index {
                phase.by_kind.resize(index + 1, SmallVec::new());
            }
            phase.by_kind[index].push(phase.rules.len() as u16);
            phase.slots = phase.slots.max(rule.slots);
            phase.rules.push(rule);
        }
        Ok(phase)
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

impl Rule {
    fn load(
        spec: &RuleSpec,
        modules: &[String],
        links: &super::Links,
        lang: &Lang,
    ) -> Result<Self, LoadError> {
        let mut ctx = Ctx::new(lang);
        ctx.slot("ROOT")?;
        let pat = parse(&mut ctx, &spec.pattern)?;
        let slots = &ctx.slots;
        let scope = |effects| Scope {
            slots,
            modules,
            links,
            lang,
            effects,
        };
        let exprs = |list: &Option<List>| -> Result<Box<[Expr]>, LoadError> {
            let items = list.as_ref().map_or(&[][..], List::items);
            items
                .iter()
                .map(|s| Expr::parse(s, &scope(false)))
                .collect()
        };
        let emit = |map: &EmitMap| -> Result<Box<[Emit]>, LoadError> {
            map.iter()
                .map(|(kind, fields)| Emit::load(kind, fields, &scope(false)))
                .collect()
        };
        let actions = |list: &Option<Vec<ActionSpec>>| -> Result<Vec<Action>, LoadError> {
            let items = list.as_deref().unwrap_or_default();
            items
                .iter()
                .map(|item| match item {
                    ActionSpec::Emit { emit: map } => Ok(Action::Emit(emit(map)?)),
                    ActionSpec::Expr(src) => {
                        let expr = Expr::parse(src, &scope(true))?;
                        match expr.ends_in_effect() {
                            true => Ok(Action::Do(expr)),
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
            Some(_) => Children::Of(exprs(&spec.children)?),
        };
        Ok(Rule {
            slots: ctx.slots.len(),
            pat,
            when: exprs(&spec.when)?,
            enter: enter.into(),
            children,
            between: actions(&spec.between)?.into(),
            exit: actions(&spec.exit)?.into(),
        })
    }
}

impl Emit {
    fn load(
        kind: &str,
        fields: &BTreeMap<String, String>,
        scope: &Scope,
    ) -> Result<Self, LoadError> {
        let edge = match kind {
            "calls" => EdgeKind::Calls,
            "imports" => EdgeKind::Imports,
            "extends" => EdgeKind::Extends,
            "defines" => EdgeKind::Defines,
            "type_flow" => EdgeKind::TypeFlow,
            _ => return Err(LoadError(format!("unknown emit kind {kind}"))),
        };
        let field = |name: &str| {
            fields
                .get(name)
                .map(|src| Expr::parse(src, scope))
                .transpose()
        };
        for name in fields.keys() {
            if !["from", "to", "site", "resolution", "resolve"].contains(&name.as_str()) {
                return Err(LoadError(format!("emit {kind}: unknown field {name}")));
            }
        }
        let resolution = match fields.get("resolution").map(String::as_str) {
            None => CallResolution::Unknown,
            Some(name) => {
                resolution(name).ok_or_else(|| LoadError(format!("unknown resolution {name}")))?
            }
        };
        let resolve = match fields.get("resolve") {
            None => None,
            Some(name) => {
                let (module, f) = name
                    .strip_prefix("mod.")
                    .and_then(|n| n.split_once('.'))
                    .unwrap_or(("core", name));
                let id = super::lookup(module, f)
                    .ok_or_else(|| LoadError(format!("emit {kind}: unknown resolve {name}")))?;
                let id = scope.links.to(id);
                if super::function(id).sig != [super::Arg::One, super::Arg::Set] {
                    return Err(LoadError(format!(
                        "emit {kind}: resolve {name} takes (target, site)"
                    )));
                }
                Some(id)
            }
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

    pub fn flow(&self, cx: &mut Run, root: Cursor<'_>) {
        let mut w = Walk {
            caps: vec![SmallVec::new(); self.slots],
            vals: vec![Vals::new(); self.slots],
            stack: root
                .children_rev()
                .map(|c| Item::Visit(c.index()))
                .collect(),
        };
        while let Some(item) = w.stack.pop() {
            if !cx.step() {
                return;
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
                    w.run(cx, actions);
                }
            }
        }
    }

    fn visit(&self, cx: &mut Run, w: &mut Walk, c: Cursor<'_>) {
        for &index in self.candidates(c.kind()) {
            let rule = &self.rules[index as usize];
            if !w.matches(cx, rule, c)
                || !rule
                    .when
                    .iter()
                    .all(|e| super::truthy(&e.eval(cx, &w.vals)))
            {
                continue;
            }
            w.run(cx, &rule.enter);
            if !rule.exit.is_empty() {
                w.stack.push(Item::Exit(index, c.index()));
            }
            w.enter(cx, c);
            match &rule.children {
                Children::Visit => w.push_children(c),
                Children::Skip => {}
                Children::Of(groups) => {
                    let groups: Vec<Vals> = groups.iter().map(|g| g.eval(cx, &w.vals)).collect();
                    for (at, group) in groups.iter().enumerate().rev() {
                        let nodes = group.iter().rev().filter_map(|v| v.loc());
                        w.stack.extend(nodes.map(|loc| Item::Visit(loc.node)));
                        if at == 1 && !rule.between.is_empty() {
                            w.stack.push(Item::Between(index, c.index()));
                        }
                    }
                }
            }
            return;
        }
        w.enter(cx, c);
        w.push_children(c);
    }
}

struct Walk {
    caps: Vec<Cap<u32>>,
    vals: Vec<Vals>,
    stack: Vec<Item>,
}

impl Walk {
    fn matches(&mut self, cx: &Run, rule: &Rule, c: Cursor<'_>) -> bool {
        for cap in &mut self.caps[..rule.slots] {
            cap.clear();
        }
        if !matches(c.tree(), &cx.env.lang, c.index(), &rule.pat, &mut self.caps) {
            return false;
        }
        let fi = c.fi();
        self.vals[0] = SmallVec::from_elem(Val::Node(c.into()), 1);
        for (val, cap) in self.vals[1..rule.slots].iter_mut().zip(&self.caps[1..]) {
            *val = cap
                .iter()
                .map(|&node| Val::Node(Loc { fi, node }))
                .collect();
        }
        true
    }

    fn run(&self, cx: &mut Run, actions: &[Action]) {
        for action in actions {
            match action {
                Action::Do(expr) => {
                    expr.eval(cx, &self.vals);
                }
                Action::Emit(emits) => emits.iter().for_each(|e| e.run(cx, &self.vals)),
            }
        }
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
