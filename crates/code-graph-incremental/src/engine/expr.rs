use std::collections::HashMap;

use rustc_hash::FxHashSet;
use smallvec::SmallVec;

use super::{Arg, FnId, Kind, Run, Val, Vals, function, lookup, module_of};
use crate::canonical::Canonical as C;
use crate::error::LoadError;
use crate::intern::Lang;

pub enum Expr {
    Cap(u16),
    Text(u16),
    Lit(Val),
    Call(FnId, Box<[Expr]>),
}

pub struct Scope<'a> {
    pub slots: &'a HashMap<Box<str>, u16>,
    pub modules: &'a [String],
    pub links: &'a super::Links,
    pub lang: &'a Lang,
    pub effects: bool,
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

    pub fn ends_in_effect(&self) -> bool {
        matches!(self, Expr::Call(id, _) if function(*id).kind == Kind::Effect)
    }

    pub fn eval(&self, cx: &mut Run, caps: &[Vals]) -> Vals {
        match self {
            Expr::Cap(slot) => caps[*slot as usize].clone(),
            Expr::Text(slot) => caps[*slot as usize]
                .iter()
                .filter_map(|v| v.loc())
                .map(|loc| Val::Sym(super::Cursor::new(cx.trees, loc.fi, loc.node).sym()))
                .collect(),
            Expr::Lit(val) => SmallVec::from_elem(*val, 1),
            Expr::Call(id, args) => {
                let args: SmallVec<[Vals; 3]> = args.iter().map(|a| a.eval(cx, caps)).collect();
                apply(cx, *id, &args)
            }
        }
    }
}

fn apply(cx: &mut Run, id: FnId, args: &[Vals]) -> Vals {
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

    fn ident(&mut self) -> &'s str {
        let rest = self.rest();
        let len = rest
            .find(|c: char| !(c.is_ascii_alphanumeric() || c == '_'))
            .unwrap_or(rest.len());
        self.at += len;
        &rest[..len]
    }

    fn expr(&mut self) -> Result<Expr, LoadError> {
        let mut expr = self.term()?;
        while self.eat("|") {
            self.space();
            expr = self.call(Some(expr))?;
        }
        Ok(expr)
    }

    fn capture(&mut self) -> Result<u16, LoadError> {
        let name = self.ident();
        self.scope
            .slots
            .get(name)
            .copied()
            .ok_or_else(|| self.error("unknown capture"))
    }

    fn term(&mut self) -> Result<Expr, LoadError> {
        self.space();
        if self.eat("@$") {
            return Ok(Expr::Text(self.capture()?));
        }
        if self.eat("$$$") || self.eat("$") {
            return Ok(Expr::Cap(self.capture()?));
        }
        if self.eat("\"") {
            let len = self
                .rest()
                .find('"')
                .ok_or_else(|| self.error("open string"))?;
            let text = &self.rest()[..len];
            self.at += len + 1;
            return Ok(Expr::Lit(match text.parse::<C>() {
                Ok(kind) => Val::Kind(kind as u16),
                Err(_) => Val::Sym(self.scope.lang.syms.intern(text)),
            }));
        }
        let start = self.at;
        let word = self.ident();
        match word {
            "true" | "false" => return Ok(Expr::Lit(Val::Bool(word == "true"))),
            _ if !word.is_empty() && word.bytes().all(|b| b.is_ascii_digit()) => {
                return Ok(Expr::Lit(Val::Sym(
                    word.parse().map_err(|_| self.error("number"))?,
                )));
            }
            _ => self.at = start,
        }
        self.call(None)
    }

    fn call(&mut self, piped: Option<Expr>) -> Result<Expr, LoadError> {
        let (module, name) = match self.ident() {
            "mod" if self.eat(".") => {
                let module = self.ident();
                if !self.eat(".") {
                    return Err(self.error("expected mod.<module>.<func>"));
                }
                (module, self.ident())
            }
            "" => return Err(self.error("expected a capture, literal or function")),
            name => ("core", name),
        };
        let id = lookup(module, name)
            .ok_or_else(|| self.error(&format!("unknown function {module}.{name}")))?;
        if !self.scope.modules.iter().any(|m| m == module_of(id)) {
            return Err(self.error(&format!("module {module} is not allowed in this phase")));
        }
        let id = self.scope.links.to(id);
        let f = function(id);
        if f.kind == Kind::Effect && !self.scope.effects {
            return Err(self.error(&format!(
                "{module}.{name} has effects; use it in emit, enter or exit"
            )));
        }
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
        if args.len() != f.sig.len() {
            return Err(self.error(&format!("{module}.{name} takes {} arguments", f.sig.len())));
        }
        Ok(Expr::Call(id, args.into()))
    }
}
