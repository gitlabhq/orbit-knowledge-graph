use std::collections::HashMap;

use smallvec::SmallVec;
use strum::EnumProperty;

use super::{Func, Host, Loc, Memo, Val, call};
use crate::canonical::{CANONICAL_BASE, Canonical as C};
use crate::dsl::matching::matches;
use crate::dsl::parser::parse;
use crate::dsl::types::{Cap, Ctx, Pat};
use crate::error::LoadError;
use crate::intern::Lang;
use crate::tree::Compact;

type Cursor<'a> = crate::tree::Cursor<'a, Compact>;

#[derive(Clone, Copy, strum::EnumString, strum::EnumProperty)]
enum Builtin {
    #[strum(serialize = "tree.parent", props(arity = "1"))]
    Parent,
    #[strum(serialize = "tree.named", props(arity = "1"))]
    Named,
    #[strum(serialize = "tree.root_name", props(arity = "1"))]
    RootName,
    #[strum(serialize = "tree.tagged", props(arity = "2"))]
    Tagged,
    #[strum(serialize = "tree.without", props(arity = "2"))]
    Without,
    #[strum(serialize = "tree.child", props(arity = "2"))]
    Child,
    #[strum(serialize = "tree.lacks", props(arity = "2"))]
    Lacks,
    #[strum(serialize = "def.enclosing", props(arity = "0"))]
    Enclosing,
    #[strum(serialize = "def.owner", props(arity = "1"))]
    Owner,
}

enum Expr<F> {
    Call(F, Memo, Box<[Expr<F>]>),
    Builtin(Builtin, Box<[Expr<F>]>),
    Cap(u16),
    Lit(Val),
    Or(Box<[Expr<F>]>),
}

enum Children<F> {
    Visit,
    Skip,
    Of(Expr<F>),
}

struct Rule<F> {
    pat: Pat,
    slots: usize,
    when: Box<[Expr<F>]>,
    act: Box<[Expr<F>]>,
    orelse: Option<Expr<F>>,
    then: Option<Expr<F>>,
    exit: Option<Expr<F>>,
    children: Children<F>,
}

pub struct Program<F> {
    rules: Vec<Rule<F>>,
    by_kind: Vec<SmallVec<[u16; 4]>>,
    slots: usize,
}

#[derive(serde::Deserialize)]
#[serde(untagged)]
enum Exprs {
    One(String),
    Many(Vec<String>),
}

impl Exprs {
    fn list(&self) -> &[String] {
        match self {
            Exprs::One(one) => std::slice::from_ref(one),
            Exprs::Many(many) => many,
        }
    }
}

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct Spec {
    #[serde(rename = "match")]
    pattern: String,
    when: Option<Exprs>,
    #[serde(rename = "do")]
    act: Option<Exprs>,
    #[serde(rename = "else")]
    orelse: Option<String>,
    then: Option<String>,
    exit: Option<String>,
    children: Option<String>,
}

const WHEN: &str = "WHEN";

impl<F: Func> Program<F> {
    pub fn load(yaml: &str, lang: &Lang) -> Result<Self, LoadError> {
        let specs: Vec<Spec> = orbit_utils::yaml::from_str(yaml)?;
        let mut program = Program {
            rules: Vec::new(),
            by_kind: Vec::new(),
            slots: 1,
        };
        for spec in &specs {
            let at = |e: LoadError| LoadError(format!("rule {:?}: {}", spec.pattern, e.0));
            let rule = Self::rule(spec, lang).map_err(at)?;
            let Pat::Node { kind, .. } = rule.pat else {
                return Err(at(LoadError("must match a node kind".into())));
            };
            let index = usize::from(kind.wrapping_sub(CANONICAL_BASE));
            if kind < CANONICAL_BASE {
                return Err(at(LoadError("must match a canonical kind".into())));
            }
            if program.by_kind.len() <= index {
                program.by_kind.resize(index + 1, SmallVec::new());
            }
            program.by_kind[index].push(program.rules.len() as u16);
            program.slots = program.slots.max(rule.slots + 1);
            program.rules.push(rule);
        }
        Ok(program)
    }

    fn rule(spec: &Spec, lang: &Lang) -> Result<Rule<F>, LoadError> {
        let mut ctx = Ctx::new(lang);
        ctx.slot("ROOT")?;
        let pat = parse(&mut ctx, &spec.pattern)?;
        let mut slots = ctx.slots;
        let when_slot = slots.len() as u16;
        let root_only: HashMap<Box<str>, u16> = HashMap::from([("ROOT".into(), 0)]);
        slots.insert(WHEN.into(), when_slot);
        let exprs = |list: &Option<Exprs>| -> Result<Box<[Expr<F>]>, LoadError> {
            let list = list.as_ref().map_or(&[][..], Exprs::list);
            list.iter().map(|s| Parser::run(s, &slots, lang)).collect()
        };
        let children = match spec.children.as_deref() {
            None | Some("visit") => Children::Visit,
            Some("skip") => Children::Skip,
            Some(src) => Children::Of(Parser::run(src, &slots, lang)?),
        };
        Ok(Rule {
            pat,
            slots: when_slot as usize,
            when: exprs(&spec.when)?,
            act: exprs(&spec.act)?,
            orelse: spec
                .orelse
                .as_deref()
                .map(|s| Parser::run(s, &slots, lang))
                .transpose()?,
            then: spec
                .then
                .as_deref()
                .map(|s| Parser::run(s, &root_only, lang))
                .transpose()?,
            exit: spec
                .exit
                .as_deref()
                .map(|s| Parser::run(s, &root_only, lang))
                .transpose()?,
            children,
        })
    }

    pub fn walk<H: Host<F = F>>(&self, h: &mut H, lang: &Lang, root: Cursor<'_>) {
        let mut walk = Walk {
            caps: vec![SmallVec::new(); self.slots],
            vals: vec![Val::None; self.slots],
            stack: root
                .children_rev()
                .map(|child| Item::Visit(child.index()))
                .collect(),
            root,
            lang,
        };
        while let Some(item) = walk.stack.pop() {
            if !h.step() {
                return;
            }
            match item {
                Item::Visit(node) => self.visit(h, &mut walk, root.jump(root.fi(), node)),
                Item::Leave => {
                    h.defs().pop();
                }
                Item::Then(rule, node) | Item::Exit(rule, node) => {
                    walk.vals[0] = Val::Node(Loc {
                        fi: root.fi(),
                        node,
                    });
                    let rule = &self.rules[rule as usize];
                    let expr = match item {
                        Item::Then(..) => &rule.then,
                        _ => &rule.exit,
                    };
                    if let Some(expr) = expr {
                        walk.eval(h, expr);
                    }
                }
            }
        }
    }

    fn visit<'t, H: Host<F = F>>(&self, h: &mut H, walk: &mut Walk<'t>, c: Cursor<'t>) {
        let candidates = c
            .kind()
            .checked_sub(CANONICAL_BASE)
            .and_then(|index| self.by_kind.get(usize::from(index)))
            .map_or(&[][..], |rules| rules.as_slice());
        for &index in candidates {
            let rule = &self.rules[index as usize];
            if !walk.matches(rule, c) {
                continue;
            }
            let mut when = Val::Yes;
            for guard in &rule.when {
                when = walk.eval(h, guard);
                if when.is_none() {
                    break;
                }
            }
            if when.is_none() {
                continue;
            }
            walk.vals[rule.slots] = when;
            let mut acted = false;
            for act in &rule.act {
                acted |= !walk.eval(h, act).is_none();
            }
            if !acted && let Some(orelse) = &rule.orelse {
                walk.eval(h, orelse);
            }
            if rule.exit.is_some() {
                walk.stack.push(Item::Exit(index, c.index()));
            }
            walk.enter(h, c);
            match &rule.children {
                Children::Visit => walk.push_children(c),
                Children::Skip => {}
                Children::Of(expr) => {
                    let children = walk.eval(h, expr);
                    walk.stack
                        .extend(children.nodes().iter().rev().map(|loc| match loc.node {
                            node if node == c.index() => Item::Then(index, node),
                            node => Item::Visit(node),
                        }));
                }
            }
            return;
        }
        walk.enter(h, c);
        walk.push_children(c);
    }
}

enum Item {
    Visit(u32),
    Then(u16, u32),
    Exit(u16, u32),
    Leave,
}

struct Walk<'t> {
    caps: Vec<Cap<u32>>,
    vals: Vec<Val>,
    stack: Vec<Item>,
    root: Cursor<'t>,
    lang: &'t Lang,
}

impl<'t> Walk<'t> {
    fn matches<F>(&mut self, rule: &Rule<F>, c: Cursor<'t>) -> bool {
        for cap in &mut self.caps[..rule.slots] {
            cap.clear();
        }
        if !matches(c.tree(), self.lang, c.index(), &rule.pat, &mut self.caps) {
            return false;
        }
        let fi = c.fi();
        self.vals[0] = Val::Node(c.into());
        for (val, cap) in self.vals[1..rule.slots].iter_mut().zip(&self.caps[1..]) {
            *val = Val::of(cap.iter().map(|&node| Loc { fi, node }));
        }
        true
    }

    fn enter<H: Host>(&mut self, h: &mut H, c: Cursor<'t>) {
        if c.is(C::Def) {
            h.defs().push(c.index());
            self.stack.push(Item::Leave);
        }
    }

    fn push_children(&mut self, c: Cursor<'t>) {
        let children = c.children_rev().map(|child| Item::Visit(child.index()));
        self.stack.extend(children);
    }

    fn cursor(&self, val: &Val) -> Option<Cursor<'t>> {
        val.node().map(|loc| self.root.jump(loc.fi, loc.node))
    }

    fn eval<H: Host>(&mut self, h: &mut H, expr: &Expr<H::F>) -> Val {
        match expr {
            Expr::Cap(slot) => self.vals[*slot as usize].clone(),
            Expr::Lit(val) => val.clone(),
            Expr::Or(alternatives) => alternatives
                .iter()
                .map(|e| self.eval(h, e))
                .find(|v| !v.is_none())
                .unwrap_or_default(),
            Expr::Call(f, memo, args) => {
                let args: SmallVec<[Val; 4]> = args.iter().map(|a| self.eval(h, a)).collect();
                call(h, *f, *memo, &args)
            }
            Expr::Builtin(b, args) => {
                let args: SmallVec<[Val; 2]> = args.iter().map(|a| self.eval(h, a)).collect();
                self.builtin(h, *b, &args)
            }
        }
    }

    fn builtin<H: Host>(&self, h: &mut H, b: Builtin, args: &[Val]) -> Val {
        if let Builtin::Enclosing = b {
            let fi = self.root.fi();
            return Val::of(h.defs().last().map(|&node| Loc { fi, node }));
        }
        let Some(c) = self.cursor(&args[0]) else {
            return Val::None;
        };
        let found = match b {
            Builtin::Parent => c.parent(),
            Builtin::Named => c.sym_opt().map(|_| c),
            Builtin::RootName => Some(c.reference().chain_root()).filter(|r| r.sym_opt().is_some()),
            Builtin::Tagged => c.has_tag(args[1].sym()).then_some(c),
            Builtin::Owner => c.parent().filter(|p| p.is(C::Def)),
            Builtin::Child => c.children().find(|child| child.kind() == args[1].kind()),
            Builtin::Lacks => {
                (!c.children().any(|child| child.kind() == args[1].kind())).then_some(c)
            }
            Builtin::Without => {
                let kind = args[1].kind();
                return Val::of(c.children().filter(|child| child.kind() != kind));
            }
            Builtin::Enclosing => None,
        };
        Val::of(found)
    }
}

struct Parser<'s> {
    src: &'s str,
    at: usize,
}

impl<'s> Parser<'s> {
    fn run<F: Func>(
        src: &'s str,
        slots: &HashMap<Box<str>, u16>,
        lang: &Lang,
    ) -> Result<Expr<F>, LoadError> {
        let mut p = Parser { src, at: 0 };
        let expr = p.or(slots, lang)?;
        p.space();
        match p.at == src.len() {
            true => Ok(expr),
            false => Err(p.error("trailing input")),
        }
    }

    fn error(&self, what: &str) -> LoadError {
        LoadError(format!("expression {:?} at {}: {what}", self.src, self.at))
    }

    fn rest(&self) -> &'s str {
        &self.src[self.at..]
    }

    fn space(&mut self) {
        self.at = self.src.len() - self.rest().trim_start().len();
    }

    fn eat(&mut self, token: &str) -> bool {
        self.space();
        let found = self.rest().starts_with(token);
        if found {
            self.at += token.len();
        }
        found
    }

    fn name(&mut self) -> &'s str {
        let rest = self.rest();
        let len = rest
            .find(|c: char| !(c.is_ascii_alphanumeric() || c == '_' || c == '.'))
            .unwrap_or(rest.len());
        self.at += len;
        &rest[..len]
    }

    fn or<F: Func>(
        &mut self,
        slots: &HashMap<Box<str>, u16>,
        lang: &Lang,
    ) -> Result<Expr<F>, LoadError> {
        let mut terms = vec![self.term(slots, lang)?];
        while self.eat("??") {
            terms.push(self.term(slots, lang)?);
        }
        Ok(match terms.len() {
            1 => terms.remove(0),
            _ => Expr::Or(terms.into()),
        })
    }

    fn term<F: Func>(
        &mut self,
        slots: &HashMap<Box<str>, u16>,
        lang: &Lang,
    ) -> Result<Expr<F>, LoadError> {
        if self.eat("$") {
            let name = self.name();
            let slot = slots
                .get(name)
                .ok_or_else(|| self.error("unknown capture"))?;
            return Ok(Expr::Cap(*slot));
        }
        if self.eat("\"") {
            let len = self
                .rest()
                .find('"')
                .ok_or_else(|| self.error("open string"))?;
            let text = &self.rest()[..len];
            self.at += len + 1;
            let kind = text.parse::<C>().map_or(0, |kind| kind as u16);
            return Ok(Expr::Lit(Val::Lit(lang.syms.intern(text), kind)));
        }
        if !self.eat("mod.") {
            return Err(self.error("expected mod.<module>.<func>, $CAPTURE or \"literal\""));
        }
        let name = self.name();
        if !self.eat("(") {
            return Err(self.error("expected ("));
        }
        let mut args = Vec::new();
        if !self.eat(")") {
            loop {
                args.push(self.or(slots, lang)?);
                if self.eat(")") {
                    break;
                }
                if !self.eat(",") {
                    return Err(self.error("expected , or )"));
                }
            }
        }
        let args: Box<[Expr<F>]> = args.into();
        let arity = |props: Option<&str>| props.and_then(|a| a.parse::<usize>().ok());
        if let Ok(b) = name.parse::<Builtin>() {
            return match arity(b.get_str("arity")) == Some(args.len()) {
                true => Ok(Expr::Builtin(b, args)),
                false => Err(self.error(&format!(
                    "mod.{name} takes {:?} arguments",
                    b.get_str("arity")
                ))),
            };
        }
        let f: F = name
            .parse()
            .map_err(|_| self.error(&format!("unknown function mod.{name}")))?;
        if arity(f.get_str("arity")) != Some(args.len()) {
            return Err(self.error(&format!(
                "mod.{name} takes {:?} arguments",
                f.get_str("arity")
            )));
        }
        Ok(Expr::Call(f, Memo::of(f), args))
    }
}
