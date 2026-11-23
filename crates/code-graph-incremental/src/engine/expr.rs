use std::cell::RefCell;
use std::collections::HashMap;

use rustc_hash::FxHashSet;
use smallvec::SmallVec;

use super::pattern::Slots;
use super::{Arg, FnId, Kind, Run, Store, Val, Vals, function, lookup, module_of};
use crate::error::LoadError;
use crate::intern::Lang;
use crate::tree::{CallResolution, EdgeKind};

#[derive(Clone)]
pub enum Expr {
    Var(u16),
    Lit(Val),
    Empty,
    Data(u16),
    Union(Box<[Expr]>),
    Def(u16, Box<[Expr]>),
    Call(FnId, Box<[Expr]>),
    Or(Box<Expr>, Box<Expr>),
    First(Box<Expr>, u16, Box<Expr>),
    /// The values every element's body yields.
    Every(Box<Expr>, u16, Box<Expr>),
    Op(Op, Box<[Expr]>),
}

/// The engine's own operations over trees and the graph.
#[derive(Clone, Copy)]
pub enum Op {
    /// Matches of a declared index directly in a scope.
    Select(u32),
    /// Matches of a declared index anywhere under a node.
    SelectAll(u32),
    /// Matches of a declared index in every file.
    SelectProject(u32),
    Enclose,
    /// Every ancestor of a node, innermost first.
    Ancestors,
    Follow(EdgeKind),
    /// The targets of edges of a kind made at a node.
    Site(EdgeKind),
    File,
    /// The project tree's root.
    Project,
    /// The first set without the values of the second.
    Minus,
    /// The set if it holds exactly one value.
    Unique,
    /// The values that stand for an instance of a type, not the type itself.
    Instances,
    /// Each node value, an instance as its type's node.
    Nodes,
    /// The last value of a set.
    Last,
    /// The innermost scope the walk is in, or the file root.
    Enclosing,
    /// Writes an edge from each node of the first set to each of the second,
    /// at the third's first node; a fourth set names the call resolution.
    Edge(EdgeKind, CallResolution),
    /// Writes one row of a shape at the first set's node per combination of
    /// the other sets' values.
    Row(u16),
    /// Records that case `n` of a stage fired, with the first symbol of each
    /// set.
    Fired(u16),
}

/// A `select` pattern and its key, shared by every phase so trees can keep
/// one piece per index.
#[derive(Clone, PartialEq)]
pub struct IndexSpec {
    pub pattern: String,
    pub key: Option<String>,
}

pub struct Scope<'a> {
    pub slots: &'a RefCell<Slots>,
    pub modules: &'a [String],
    pub links: &'a super::Links,
    pub lang: &'a Lang,
    pub data: &'a HashMap<String, u16>,
    pub defs: &'a HashMap<String, (u16, usize)>,
    pub indexes: &'a RefCell<Vec<IndexSpec>>,
    pub effects: bool,
    /// In a language's redefinition: the shared definition and its arity.
    pub shared: Option<(u16, usize)>,
}

impl Expr {
    pub fn parse(src: &str, scope: &Scope) -> Result<Expr, LoadError> {
        let mut p = Parser { src, at: 0, scope };
        let expr = p.expr()?;
        p.space();
        match p.at == src.len() {
            true => Ok(expr),
            false => Err(p.error("trailing input")),
        }
    }

    /// Every module function and YAML function this expression calls.
    pub fn calls(&self, f: &mut dyn FnMut(Result<FnId, u16>)) {
        let each = |parts: &[Expr], f: &mut dyn FnMut(Result<FnId, u16>)| {
            parts.iter().for_each(|p| p.calls(f))
        };
        match self {
            Expr::Call(id, args) => {
                f(Ok(*id));
                each(args, f);
            }
            Expr::Def(def, args) => {
                f(Err(*def));
                each(args, f);
            }
            Expr::Union(parts) | Expr::Op(_, parts) => each(parts, f),
            Expr::Or(a, b) | Expr::First(a, _, b) | Expr::Every(a, _, b) => {
                a.calls(f);
                b.calls(f);
            }
            _ => {}
        }
    }

    pub fn eval<'t, S: Store>(&'t self, cx: &mut Run<'t, S>, caps: &[Vals]) -> Vals {
        super::machine::eval(cx, self, caps)
    }
}

pub(super) fn apply<S: Store>(cx: &mut Run<S>, id: FnId, args: &[Vals]) -> Vals {
    let sig = function(id).sig;
    let spread = sig
        .iter()
        .zip(args)
        .position(|(arg, vals)| *arg != Arg::Set && vals.len() != 1);
    let Some(at) = spread else {
        return cx.call(id, args);
    };
    let mut out = Vals::new();
    let mut seen: Option<FxHashSet<Val>> = None;
    let mut each: SmallVec<[Vals; 3]> = SmallVec::from(args);
    for val in &args[at] {
        each[at] = SmallVec::from_elem(*val, 1);
        for found in apply(cx, id, &each) {
            let new = match &mut seen {
                Some(seen) => seen.insert(found),
                None => !out.contains(&found),
            };
            if new {
                out.push(found);
                if seen.is_none() && out.len() > 16 {
                    seen = Some(out.iter().copied().collect());
                }
            }
        }
    }
    out
}

struct Parser<'s, 'a> {
    src: &'s str,
    at: usize,
    scope: &'a Scope<'a>,
}

impl<'s> Parser<'s, '_> {
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

    /// `token` as a whole word, not the start of a longer name.
    fn keyword(&mut self, token: &str) -> bool {
        self.space();
        let rest = self.rest();
        let whole = rest.starts_with(token)
            && !rest[token.len()..].starts_with(|c: char| c.is_ascii_alphanumeric() || c == '_');
        if whole {
            self.at += token.len();
        }
        whole
    }

    fn ident(&mut self) -> &'s str {
        let rest = self.rest();
        let len = rest
            .find(|c: char| !(c.is_ascii_alphanumeric() || c == '_'))
            .unwrap_or(rest.len());
        self.at += len;
        &rest[..len]
    }

    fn quoted(&mut self, quote: char) -> Result<&'s str, LoadError> {
        let len = self
            .rest()
            .find(quote)
            .ok_or_else(|| self.error("open string"))?;
        let text = &self.rest()[..len];
        self.at += len + 1;
        Ok(text)
    }

    fn expr(&mut self) -> Result<Expr, LoadError> {
        let mut expr = self.pipe()?;
        while self.keyword("or") {
            expr = Expr::Or(Box::new(expr), Box::new(self.pipe()?));
        }
        Ok(expr)
    }

    fn pipe(&mut self) -> Result<Expr, LoadError> {
        let mut expr = self.term()?;
        while self.eat("|") {
            self.space();
            expr = self.call(Some(expr))?;
        }
        Ok(expr)
    }

    fn var(&mut self, name: &str) -> Option<u16> {
        self.scope.slots.borrow().get(name)
    }

    fn term(&mut self) -> Result<Expr, LoadError> {
        self.space();
        if self.eat("(") {
            let inner = self.expr()?;
            return match self.eat(")") {
                true => Ok(inner),
                false => Err(self.error("expected )")),
            };
        }
        let text = self.eat("@");
        if self.eat("$$$") || self.eat("$") || text {
            let name = self.ident();
            return self
                .var(name)
                .map(Expr::Var)
                .ok_or_else(|| self.error("unknown capture"));
        }
        if self.eat("%") {
            let name = self.ident();
            let at = self.scope.data.get(name).copied();
            return at.map(Expr::Data).ok_or_else(|| self.error("unknown data"));
        }
        if self.eat("\"") {
            let text = self.quoted('"')?;
            let lang = self.scope.lang;
            return Ok(Expr::Lit(match lang.vocabulary.lookup(text) {
                0 => Val::Sym(lang.syms.intern(text)),
                _ => Val::Kind(lang.intern_kind(text)),
            }));
        }
        let start = self.at;
        let word = self.ident();
        match word {
            "true" | "false" => return Ok(Expr::Lit(Val::Bool(word == "true"))),
            "opaque" => return Ok(Expr::Lit(Val::Opaque)),
            "none" => return Ok(Expr::Empty),
            _ if !word.is_empty() && word.bytes().all(|b| b.is_ascii_digit()) => {
                return Ok(Expr::Lit(Val::Sym(
                    word.parse().map_err(|_| self.error("number"))?,
                )));
            }
            _ => {}
        }
        if let Some(slot) = self.var(word) {
            return Ok(Expr::Var(slot));
        }
        self.at = start;
        self.call(None)
    }

    fn args(&mut self, piped: Option<Expr>) -> Result<Vec<Expr>, LoadError> {
        let mut args: Vec<Expr> = piped.into_iter().collect();
        if self.eat("(") && !self.eat(")") {
            loop {
                args.push(self.expr()?);
                if self.eat(")") {
                    break;
                }
                if !self.eat(",") {
                    return Err(self.error("expected , or )"));
                }
            }
        }
        Ok(args)
    }

    fn call(&mut self, piped: Option<Expr>) -> Result<Expr, LoadError> {
        let start = self.at;
        let word = self.ident();
        match word {
            "super" => {
                let (def, arity) = self
                    .scope
                    .shared
                    .ok_or_else(|| self.error("super outside a redefinition"))?;
                let args = self.args(piped)?;
                return match args.len() == arity {
                    true => Ok(Expr::Def(def, args.into())),
                    false => Err(self.error(&format!("super takes {arity} arguments"))),
                };
            }
            "first" | "every" => return self.each(word, piped),
            "union" => return Ok(Expr::Union(self.args(piped)?.into())),
            "minus" | "unique" | "instances" | "nodes" | "last" => {
                let args = self.args(piped)?;
                let (op, arity) = match word {
                    "minus" => (Op::Minus, 2),
                    "unique" => (Op::Unique, 1),
                    "nodes" => (Op::Nodes, 1),
                    "last" => (Op::Last, 1),
                    _ => (Op::Instances, 1),
                };
                if args.len() != arity {
                    return Err(self.error(&format!("{word} takes {arity} arguments")));
                }
                return Ok(Expr::Op(op, args.into()));
            }
            "select" | "select_all" => return self.select(word, piped),
            "project" => {
                if !(self.eat("(") && self.eat(")")) {
                    return Err(self.error("project() takes no arguments"));
                }
                return Ok(Expr::Op(Op::Project, Box::new([])));
            }
            "enclose" | "ancestors" | "file" => {
                let args = self.args(piped)?;
                if args.len() != 1 {
                    return Err(self.error(&format!("{word} takes 1 argument")));
                }
                let op = match word {
                    "enclose" => Op::Enclose,
                    "ancestors" => Op::Ancestors,
                    _ => Op::File,
                };
                return Ok(Expr::Op(op, args.into()));
            }
            "follow" | "sited" => {
                let site = word == "sited";
                let mut args: Vec<Expr> = piped.into_iter().collect();
                if !self.eat("(") {
                    return Err(self.error("follow(node, kind)"));
                }
                if args.is_empty() {
                    args.push(self.expr()?);
                    if !self.eat(",") {
                        return Err(self.error("follow(node, kind)"));
                    }
                }
                self.space();
                let kind = self.ident();
                let kind = self.scope.lang.edge_kind(kind).ok_or_else(|| {
                    self.error(&format!("edge kind {kind} is not in pipeline.yaml edges"))
                })?;
                if !self.eat(")") {
                    return Err(self.error("expected )"));
                }
                let op = if site {
                    Op::Site(kind)
                } else {
                    Op::Follow(kind)
                };
                return Ok(Expr::Op(op, args.into()));
            }
            _ => {}
        }
        if let Some(&(def, arity)) = self.scope.defs.get(word) {
            let args = self.args(piped)?;
            if args.len() != arity {
                return Err(self.error(&format!("{word} takes {arity} arguments")));
            }
            return Ok(Expr::Def(def, args.into()));
        }
        self.at = start;
        let (module, name) = match self.ident() {
            "mod" if self.eat(".") => {
                let module = self.ident();
                if !self.eat(".") {
                    return Err(self.error("expected mod.<module>.<func>"));
                }
                (module, self.ident())
            }
            "" => return Err(self.error("expected a capture, literal or function")),
            name => {
                let module = self
                    .scope
                    .modules
                    .iter()
                    .find(|m| lookup(m, name).is_some())
                    .ok_or_else(|| {
                        self.error(&format!(
                            "no definition, operation or module function {name}"
                        ))
                    })?;
                (module.as_str(), name)
            }
        };
        let id = lookup(module, name)
            .ok_or_else(|| self.error(&format!("unknown function {module}.{name}")))?;
        if !self.scope.modules.iter().any(|m| m == module_of(id)) {
            return Err(self.error(&format!("module {module} is not allowed in this phase")));
        }
        let id = self.scope.links.to(id);
        let f = function(id);
        if f.kind == Kind::Effect && !self.scope.effects {
            return Err(self.error(&format!("{module}.{name} has effects")));
        }
        let mut args = self.args(piped)?;
        if f.sig.last() == Some(&Arg::Set) && args.len() > f.sig.len() {
            let rest = args.split_off(f.sig.len() - 1);
            args.push(Expr::Union(rest.into()));
        }
        if args.len() != f.sig.len() {
            return Err(self.error(&format!("{module}.{name} takes {} arguments", f.sig.len())));
        }
        Ok(Expr::Call(id, args.into()))
    }

    /// `first(list, x => body)`: the first element's body that is not empty.
    /// `every(list, x => body)`: the values every element's body yields.
    fn each(&mut self, word: &str, piped: Option<Expr>) -> Result<Expr, LoadError> {
        if !self.eat("(") {
            return Err(self.error(&format!("{word}(list, x => body)")));
        }
        let list = match piped {
            Some(list) => list,
            None => {
                let list = self.expr()?;
                if !self.eat(",") {
                    return Err(self.error(&format!("{word}(list, x => body)")));
                }
                list
            }
        };
        self.space();
        let name = self.ident().to_owned();
        if !self.eat("=>") {
            return Err(self.error("expected =>"));
        }
        let slot = self.scope.slots.borrow_mut().slot(&name);
        let body = self.expr()?;
        if !self.eat(")") {
            return Err(self.error("expected )"));
        }
        Ok(match word {
            "every" => Expr::Every(Box::new(list), slot, Box::new(body)),
            _ => Expr::First(Box::new(list), slot, Box::new(body)),
        })
    }

    /// `select(scope, 'pattern' [by key], key)` and `select_all(region | project, …)`.
    fn select(&mut self, word: &str, piped: Option<Expr>) -> Result<Expr, LoadError> {
        if !self.eat("(") {
            return Err(self.error("select(region, 'pattern', key)"));
        }
        let mut args: Vec<Expr> = Vec::new();
        let mut project = false;
        match piped {
            Some(region) => args.push(region),
            None if !self.rest().trim_start().starts_with("project(")
                && self.keyword("project") =>
            {
                project = true
            }
            None => args.push(self.expr()?),
        }
        if !project && args.len() == 1 && !self.eat(",") {
            return Err(self.error("expected , after the region"));
        }
        if project && !self.eat(",") {
            return Err(self.error("expected , after project"));
        }
        if !self.eat("'") {
            return Err(self.error("the pattern of a select is a quoted literal"));
        }
        let pattern = self.quoted('\'')?.to_owned();
        let key = match self.keyword("by") {
            true => {
                let from = self.at;
                let mut depth = 0i32;
                while let Some(c) = self.rest().chars().next() {
                    match c {
                        '(' => depth += 1,
                        ')' if depth == 0 => break,
                        ')' => depth -= 1,
                        ',' if depth == 0 => break,
                        _ => {}
                    }
                    self.at += c.len_utf8();
                }
                Some(self.src[from..self.at].trim().to_owned())
            }
            false => None,
        };
        let keyless = self.eat(")");
        let key = match keyless {
            true => Some("\"\"".to_owned()),
            false => key,
        };
        let spec = IndexSpec { pattern, key };
        let id = {
            let mut indexes = self.scope.indexes.borrow_mut();
            match indexes.iter().position(|i| *i == spec) {
                Some(at) => at,
                None => {
                    indexes.push(spec);
                    indexes.len() - 1
                }
            }
        } as u32;
        match keyless {
            true => args.push(Expr::Lit(Val::Sym(self.scope.lang.syms.intern("")))),
            false => {
                if !self.eat(",") {
                    return Err(self.error("expected , before the key"));
                }
                args.push(self.expr()?);
                if !self.eat(")") {
                    return Err(self.error("expected )"));
                }
            }
        }
        let op = match (word, project) {
            ("select", false) => Op::Select(id),
            ("select_all", false) => Op::SelectAll(id),
            ("select_all", true) => Op::SelectProject(id),
            _ => return Err(self.error("select takes a scope; use select_all for project")),
        };
        Ok(Expr::Op(op, args.into()))
    }
}
