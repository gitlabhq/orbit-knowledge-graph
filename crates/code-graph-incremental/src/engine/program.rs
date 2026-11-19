use std::collections::{BTreeMap, HashMap};

use smallvec::SmallVec;

use super::expr::{Expr, Scope};
use super::matching::matches;
use super::{Cursor, Cx, Loc, Val, Vals};
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

enum Target {
    Edge(EdgeKind),
    Refs,
}

struct Emit {
    target: Target,
    from: Expr,
    to: Expr,
    site: Option<Expr>,
    resolution: CallResolution,
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
        let mut phase = Phase {
            name: spec.name,
            scope: spec.scope,
            after: spec.after,
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
                "each" => &["enter", "emit"],
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
            let rule = Rule::load(rule, &spec.modules, lang).map_err(at)?;
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

impl Rule {
    fn load(spec: &RuleSpec, modules: &[String], lang: &Lang) -> Result<Self, LoadError> {
        let mut ctx = Ctx::new(lang);
        ctx.slot("ROOT")?;
        let pat = parse(&mut ctx, &spec.pattern)?;
        let slots = &ctx.slots;
        let scope = |effects| Scope {
            slots,
            modules,
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
        let target = match kind {
            "calls" => Target::Edge(EdgeKind::Calls),
            "imports" => Target::Edge(EdgeKind::Imports),
            "extends" => Target::Edge(EdgeKind::Extends),
            "defines" => Target::Edge(EdgeKind::Defines),
            "type_flow" => Target::Edge(EdgeKind::TypeFlow),
            "refs" => Target::Refs,
            _ => return Err(LoadError(format!("unknown emit kind {kind}"))),
        };
        let field = |name: &str| {
            fields
                .get(name)
                .map(|src| Expr::parse(src, scope))
                .transpose()
        };
        for name in fields.keys() {
            if !["from", "to", "site", "resolution"].contains(&name.as_str()) {
                return Err(LoadError(format!("emit {kind}: unknown field {name}")));
            }
        }
        let resolution = match fields.get("resolution").map(String::as_str) {
            None => CallResolution::Unknown,
            Some("reference") => CallResolution::Reference,
            Some("callable") => CallResolution::Callable,
            Some("non_callable") => CallResolution::NonCallable,
            Some(other) => return Err(LoadError(format!("unknown resolution {other}"))),
        };
        Ok(Emit {
            target,
            from: field("from")?.ok_or_else(|| LoadError(format!("emit {kind} needs from")))?,
            to: field("to")?.ok_or_else(|| LoadError(format!("emit {kind} needs to")))?,
            site: field("site")?,
            resolution,
        })
    }

    fn run(&self, cx: &mut Cx, caps: &[Vals]) {
        let site = self.site.as_ref().map(|s| s.eval(cx, caps));
        let site = site
            .and_then(|s| s.first().and_then(|v| v.loc()))
            .map(|l| l.node);
        let from = self.from.eval(cx, caps);
        let to = self.to.eval(cx, caps);
        for from in from.iter().filter_map(|v| v.loc()) {
            for &target in &to {
                let edges: SmallVec<[(EdgeKind, Loc); 2]> = match self.target {
                    Target::Edge(kind) => target.loc().map(|loc| (kind, loc)).into_iter().collect(),
                    Target::Refs => crate::modules::core::refs(cx, target),
                };
                for (kind, to) in edges {
                    cx.edges.push(Edge {
                        site,
                        call_resolution: self.resolution,
                        ..Edge::new(from.fi, from.node, to.fi, to.node, kind)
                    });
                }
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

    pub fn flow(&self, cx: &mut Cx, root: Cursor<'_>) {
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
                Item::Leave => {
                    cx.defs.pop();
                }
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

    fn visit(&self, cx: &mut Cx, w: &mut Walk, c: Cursor<'_>) {
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
    fn matches(&mut self, cx: &Cx, rule: &Rule, c: Cursor<'_>) -> bool {
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

    fn run(&self, cx: &mut Cx, actions: &[Action]) {
        for action in actions {
            match action {
                Action::Do(expr) => {
                    expr.eval(cx, &self.vals);
                }
                Action::Emit(emits) => emits.iter().for_each(|e| e.run(cx, &self.vals)),
            }
        }
    }

    fn enter(&mut self, cx: &mut Cx, c: Cursor<'_>) {
        if c.is(C::Def) {
            cx.defs.push(c.index());
            self.stack.push(Item::Leave);
        }
    }

    fn push_children(&mut self, c: Cursor<'_>) {
        self.stack
            .extend(c.children_rev().map(|child| Item::Visit(child.index())));
    }
}
