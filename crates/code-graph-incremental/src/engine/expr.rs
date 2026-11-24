use crate::tree::EdgeKind;
use std::cell::RefCell;
use std::collections::HashMap;

use super::pattern::Slots;
use super::{Arg, FnId, Kind, Registry, Run, Store, Val, Vals};
use crate::error::LoadError;
use crate::intern::Lang;

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
    /// Only edge steps build this, so no value a rule sees repeats.
    Edges(EdgeKind, Option<u8>),
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
    pub registry: &'a Registry,
    pub lang: &'a Lang,
    pub data: &'a HashMap<String, u16>,
    pub defs: &'a HashMap<String, (u16, usize)>,
    pub indexes: &'a RefCell<Vec<IndexSpec>>,
    /// Each row shape's id and value names, by shape name.
    pub tables: &'a HashMap<String, (u16, Vec<String>)>,
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

    /// Every module function and YAML function this expression calls, with
    /// the call's arguments.
    pub fn calls<'a>(&'a self, f: &mut dyn FnMut(Result<FnId, u16>, &'a [Expr])) {
        let each = |parts: &'a [Expr], f: &mut dyn FnMut(Result<FnId, u16>, &'a [Expr])| {
            parts.iter().for_each(|p| p.calls(f))
        };
        match self {
            Expr::Call(id, args) => {
                f(Ok(*id), args);
                each(args, f);
            }
            Expr::Def(def, args) => {
                f(Err(*def), args);
                each(args, f);
            }
            Expr::Union(parts) => each(parts, f),
            Expr::Or(a, b) | Expr::First(a, _, b) | Expr::Every(a, _, b) => {
                a.calls(f);
                b.calls(f);
            }
            _ => {}
        }
    }

    pub fn eval<'t, S: Store>(&'t self, cx: &mut Run<'t, S>, caps: &[Vals]) -> Vals {
        super::vm::eval(cx, self, caps)
    }
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
                    .find(|m| self.scope.registry.lookup(m, name).is_some())
                    .ok_or_else(|| self.error(&format!("no definition or function {name}")))?;
                (module.as_str(), name)
            }
        };
        let registry = self.scope.registry;
        let id = registry
            .lookup(module, name)
            .ok_or_else(|| self.error(&format!("unknown function {module}.{name}")))?;
        if !self
            .scope
            .modules
            .iter()
            .any(|m| m == registry.module_of(id))
        {
            return Err(self.error(&format!("module {module} is not allowed in this phase")));
        }
        let f = registry.function(id);
        if f.kind == Kind::Effect && !self.scope.effects {
            return Err(self.error(&format!("{module}.{name} has effects")));
        }
        let mut args = self.typed_args(f.sig, piped)?;
        let rest = f.sig.last() == Some(&Arg::Rest);
        let arity = f.sig.len() + f.sig.iter().filter(|&&a| a == Arg::Region).count();
        if f.sig.last() == Some(&Arg::Set) && args.len() > arity {
            let more = args.split_off(arity - 1);
            args.push(Expr::Union(more.into()));
        }
        if args.len() != arity && !(rest && args.len() >= arity - 1) {
            return Err(self.error(&format!("{module}.{name} takes {} arguments", arity)));
        }
        Ok(Expr::Call(id, args.into()))
    }

    /// A call's arguments, each read as its signature says: an expression,
    /// an edge kind's name, a declared index's pattern, or a row shape's name
    /// whose values follow as `name: value`, in the shape's order and empty
    /// where not given. A pattern with no key and nothing after it is keyed
    /// by the empty text.
    fn typed_args(&mut self, sig: &[Arg], piped: Option<Expr>) -> Result<Vec<Expr>, LoadError> {
        let mut args: Vec<Expr> = piped.into_iter().collect();
        let mut named: Option<(Vec<String>, Vec<Option<Expr>>)> = None;
        let mut regions = 0;
        if !self.eat("(") || self.eat(")") {
            return Ok(args);
        }
        loop {
            let back = self.at;
            self.space();
            let name = self.ident();
            match &mut named {
                Some((names, values)) if !name.is_empty() && self.eat(":") => {
                    let at = names
                        .iter()
                        .position(|n| n == name)
                        .ok_or_else(|| self.error(&format!("the shape has no value {name}")))?;
                    values[at] = Some(self.expr()?);
                }
                _ => {
                    self.at = back;
                    match sig[(args.len() - regions).min(sig.len().saturating_sub(1))] {
                        Arg::Edge => {
                            self.space();
                            let name = self.ident();
                            let kind = self.scope.lang.edge_kind(name).ok_or_else(|| {
                                self.error(&format!(
                                    "edge kind {name} is not in pipeline.yaml edges"
                                ))
                            })?;
                            args.push(Expr::Lit(Val::Sym(u32::from(kind.0))));
                        }
                        Arg::Region => {
                            self.space();
                            let name = self.ident();
                            let lang = self.scope.lang;
                            let (of, region) = match name {
                                "owned" | "under" => {
                                    if !self.eat("(") {
                                        return Err(self.error(&format!("{name}(x)")));
                                    }
                                    let of = self.expr()?;
                                    if !self.eat(")") {
                                        return Err(self.error("expected )"));
                                    }
                                    (of, u32::from(name == "under"))
                                }
                                _ => match lang.tree_set(name) {
                                    Some(set) => (Expr::Empty, super::SETS + set as u32),
                                    None => {
                                        return Err(self.error(&format!(
                                            "{name} is not owned(x), under(x) or a tree set"
                                        )));
                                    }
                                },
                            };
                            args.push(of);
                            args.push(Expr::Lit(Val::Sym(region)));
                            regions += 1;
                        }
                        Arg::TreeSet => {
                            self.space();
                            let name = self.ident();
                            let set =
                                self.scope.lang.tree_set(name).ok_or_else(|| {
                                    self.error(&format!("{name} is not a tree set"))
                                })?;
                            args.push(Expr::Lit(Val::Sym(set as u32)));
                        }
                        Arg::EdgeKey => {
                            self.space();
                            let name = self.ident();
                            let lang = self.scope.lang;
                            let key = match name {
                                "from" => Some(0),
                                _ => lang
                                    .edge_attribute(name)
                                    .filter(|&slot| lang.edge_attributes()[slot].node)
                                    .map(|slot| slot as u32 + 1),
                            };
                            let key = key.ok_or_else(|| {
                                self.error(&format!("{name} is not from or a node edge attribute"))
                            })?;
                            args.push(Expr::Lit(Val::Sym(key)));
                        }
                        Arg::Shape => {
                            self.space();
                            let name = self.ident();
                            let (id, names) = self
                                .scope
                                .tables
                                .get(name)
                                .cloned()
                                .ok_or_else(|| self.error(&format!("no table {name}")))?;
                            args.push(Expr::Lit(Val::Sym(u32::from(id))));
                            let values = vec![None; names.len()];
                            named = Some((names, values));
                        }
                        Arg::Pattern => {
                            let (id, keyless) = self.index()?;
                            args.push(Expr::Lit(Val::Sym(id)));
                            if keyless {
                                args.push(Expr::Lit(Val::Sym(self.scope.lang.syms.intern(""))));
                                return Ok(args);
                            }
                        }
                        _ => args.push(self.expr()?),
                    }
                }
            }
            if self.eat(")") {
                if let Some((_, values)) = named {
                    args.extend(values.into_iter().map(|v| v.unwrap_or(Expr::Empty)));
                }
                return Ok(args);
            }
            if !self.eat(",") {
                return Err(self.error("expected , or )"));
            }
        }
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
    /// `'pattern' [by key]`, declared once across the rules: its id, and
    /// whether it has no key and ends the call.
    fn index(&mut self) -> Result<(u32, bool), LoadError> {
        self.space();
        if !self.eat("'") {
            return Err(self.error("an index pattern is a quoted literal"));
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
        let mut indexes = self.scope.indexes.borrow_mut();
        let id = match indexes.iter().position(|i| *i == spec) {
            Some(at) => at,
            None => {
                indexes.push(spec);
                indexes.len() - 1
            }
        };
        Ok((id as u32, keyless))
    }
}
