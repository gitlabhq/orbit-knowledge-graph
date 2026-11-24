use std::fmt::Display;

use super::{KindFilter, Pat, Slots, Test, Text, Tpl, TplText};
use crate::error::LoadError;
use crate::intern::Lang;

type R<T> = Result<T, LoadError>;
type Data<'s> = &'s dyn Fn(&str) -> Option<Vec<String>>;

pub fn parse_pattern(src: &str, slots: &mut Slots, lang: &Lang, data: Data) -> R<Pat> {
    let mut p = PatParser {
        s: Src::new("pattern", src),
        slots,
        lang,
        data,
    };
    p.s.ws();
    let pat = p.element(0)?;
    p.s.end()?;
    match pat {
        Pat::Node { .. } | Pat::Cap { .. } => Ok(pat),
        _ => Err(p.s.err("a pattern starts with a node or a capture")),
    }
}

pub fn parse_template<T>(
    src: &str,
    slots: &Slots,
    lang: &Lang,
    text: &mut dyn FnMut(&str) -> R<T>,
) -> R<Tpl<T>> {
    let mut p = TplParser {
        s: Src::new("template", src),
        slots,
        lang,
        text,
        texts: 0,
    };
    p.s.ws();
    let tpl = p.element(0)?;
    p.s.end()?;
    Ok(tpl)
}

struct Src<'a> {
    what: &'static str,
    src: &'a str,
    at: usize,
}

fn is_name(b: u8) -> bool {
    b.is_ascii_alphanumeric() || b == b'_'
}

fn is_ident(b: u8) -> bool {
    is_name(b) || b == b'.' || b == b'*'
}

impl<'a> Src<'a> {
    fn new(what: &'static str, src: &'a str) -> Self {
        Self { what, src, at: 0 }
    }

    fn err(&self, msg: impl Display) -> LoadError {
        LoadError(format!(
            "{} {:?} at {}: {msg}",
            self.what, self.src, self.at
        ))
    }

    fn rest(&self) -> &'a str {
        &self.src[self.at..]
    }

    fn peek(&self) -> Option<u8> {
        self.rest().bytes().next()
    }

    fn ws(&mut self) {
        self.at = self.src.len() - self.rest().trim_start().len();
    }

    fn eat(&mut self, token: &str) -> bool {
        let found = self.rest().starts_with(token);
        if found {
            self.at += token.len();
        }
        found
    }

    fn end(&mut self) -> R<()> {
        self.ws();
        match self.at == self.src.len() {
            true => Ok(()),
            false => Err(self.err("trailing input")),
        }
    }

    fn take(&mut self, what: &str, ok: fn(u8) -> bool) -> R<&'a str> {
        let rest = self.rest();
        let len = rest.bytes().take_while(|&b| ok(b)).count();
        if len == 0 {
            return Err(self.err(format_args!("expected {what}")));
        }
        self.at += len;
        Ok(&rest[..len])
    }

    fn name(&mut self) -> R<&'a str> {
        self.take("a name", is_name)
    }

    fn ident(&mut self) -> R<&'a str> {
        self.take("a kind", is_ident)
    }

    fn quoted(&mut self) -> R<&'a str> {
        let open = self.at;
        self.at += 1;
        let Some(len) = self.rest().find('"') else {
            self.at = open;
            return Err(self.err("unclosed quote"));
        };
        let inner = &self.rest()[..len];
        self.at += len + 1;
        Ok(inner)
    }

    fn field(&mut self) -> R<&'a str> {
        let name = self.ident()?;
        match self.eat(":") {
            true => Ok(name),
            false => Err(self.err(format_args!("expected `{name}:` or an element"))),
        }
    }

    fn rekind(&mut self, lang: &Lang) -> R<Option<(u16, bool)>> {
        let leaf = if self.eat("=>") {
            true
        } else if self.eat("->") {
            false
        } else {
            return Ok(None);
        };
        Ok(Some((lang.intern_kind(self.ident()?), leaf)))
    }

    fn test(&mut self, lang: &Lang, data: Option<Data>) -> R<Test> {
        let negate = self.eat("!");
        let (mut kinds, mut named) = (Vec::new(), false);
        loop {
            if self.eat("%") {
                let name = self.name()?;
                let data = data.ok_or_else(|| self.err("data is not available here"))?;
                let values =
                    data(name).ok_or_else(|| self.err(format_args!("unknown data %{name}")))?;
                kinds.extend(values.iter().map(|k| lang.intern_kind(k)));
            } else if self.peek() == Some(b'"') {
                kinds.push(lang.intern_kind(self.quoted()?));
            } else {
                match self.ident()? {
                    "_*_named" => named = true,
                    kind => kinds.push(lang.intern_kind(kind)),
                }
            }
            if !self.eat("|") {
                break;
            }
        }
        let kinds = match kinds.is_empty() && !negate {
            true => KindFilter::any(),
            false => KindFilter { kinds, negate },
        };
        Ok(Test { kinds, named })
    }
}

struct PatParser<'a, 's> {
    s: Src<'a>,
    slots: &'s mut Slots,
    lang: &'s Lang,
    data: Data<'s>,
}

impl PatParser<'_, '_> {
    fn data(&self, name: &str) -> R<Vec<String>> {
        (self.data)(name).ok_or_else(|| self.s.err(format_args!("unknown data %{name}")))
    }

    fn element(&mut self, field: u16) -> R<Pat> {
        if self.s.eat("(") {
            return self.node(field);
        }
        if self.s.eat("!") {
            return Ok(Pat::Not(Box::new(self.element(field)?)));
        }
        if self.s.eat("**") {
            let stop = match self.s.peek() {
                Some(b'!') => {
                    self.s.at += 1;
                    self.s.test(self.lang, Some(self.data))?.kinds
                }
                _ => KindFilter {
                    kinds: Vec::new(),
                    negate: false,
                },
            };
            if !self.s.eat("/") {
                return Err(self.s.err("expected / after **"));
            }
            return Ok(Pat::Desc(Box::new(self.element(0)?), stop));
        }
        if self.s.eat("$$$") {
            let slot = self.slots.slot(self.s.name()?);
            let test = match self.s.eat(":") {
                true => self.test(slot)?,
                false => Test::any(),
            };
            return Ok(Pat::Var { slot, field, test });
        }
        if self.s.eat("$") {
            let slot = self.slots.slot(self.s.name()?);
            let optional = self.s.eat("?");
            let (mut test, mut guard) = (Test::any(), None);
            if self.s.eat(":") {
                match self.s.eat("(") {
                    true => guard = Some(Box::new(self.node(0)?)),
                    false => test = self.test(slot)?,
                }
            }
            return Ok(Pat::Cap {
                slot,
                field,
                test,
                guard,
                optional,
            });
        }
        Err(self.s.err("expected an element"))
    }

    fn test(&mut self, slot: u16) -> R<Test> {
        let test = self.s.test(self.lang, Some(self.data))?;
        self.slots.set_filter(slot, test.kinds.clone());
        Ok(test)
    }

    fn node(&mut self, field: u16) -> R<Pat> {
        self.s.ws();
        let mut param = None;
        let kind = match self.s.peek() {
            Some(b'"') => KindFilter::one(self.lang.intern_kind(self.s.quoted()?)),
            Some(b'%') => {
                self.s.at += 1;
                let name = self.s.name()?;
                let kinds = self.data(name)?;
                KindFilter {
                    kinds: kinds.iter().map(|k| self.lang.intern_kind(k)).collect(),
                    negate: false,
                }
            }
            _ => match self.s.ident()? {
                "_" => KindFilter::any(),
                name if is_param(name) && self.slots.get(name).is_some() => {
                    param = self.slots.get(name);
                    KindFilter::any()
                }
                kind => KindFilter::one(self.lang.intern_kind(kind)),
            },
        };
        let optional = self.s.eat("?");
        let (mut text, mut kids, mut tags) = (Text::Any, Vec::new(), Vec::new());
        let (mut flags, mut unflags) = (0u8, 0u8);
        loop {
            self.s.ws();
            match self.s.peek() {
                None => return Err(self.s.err("unclosed node")),
                Some(b'#') => {
                    self.s.at += 1;
                    let absent = self.s.eat("!");
                    let name = self.s.name()?;
                    match (super::FLAGS.iter().position(|f| *f == name), absent) {
                        (Some(bit), false) => flags |= 1 << bit,
                        (Some(bit), true) => unflags |= 1 << bit,
                        (None, _) => tags.push((self.lang.syms.intern(name), !absent)),
                    }
                }
                Some(b')') => {
                    self.s.at += 1;
                    break;
                }
                Some(b'"') => text = self.text()?,
                Some(b'%') => {
                    self.s.at += 1;
                    let name = self.s.name()?;
                    let syms = self.data(name)?;
                    text = Text::Syms(syms.iter().map(|s| self.lang.syms.intern(s)).collect());
                }
                Some(b) if is_ident(b) && !self.s.rest().starts_with("**") => {
                    let field = self.lang.intern_field(self.s.field()?);
                    let opt = self.s.eat("?");
                    self.s.ws();
                    let mut kid = self.element(field)?;
                    if opt {
                        match &mut kid {
                            Pat::Node { optional, .. } | Pat::Cap { optional, .. } => {
                                *optional = true
                            }
                            _ => {
                                return Err(self.s.err("only a node or a capture can be optional"));
                            }
                        }
                    }
                    kids.push(kid);
                }
                Some(_) => kids.push(self.element(0)?),
            }
        }
        Ok(Pat::Node {
            kind,
            param,
            field,
            text,
            tags: tags.into(),
            flags,
            unflags,
            kids: kids.into(),
            optional,
        })
    }

    fn text(&mut self) -> R<Text> {
        let lit = self.s.quoted()?;
        if let Some(prefix) = lit.strip_prefix('^') {
            return Ok(Text::Prefix(prefix.into()));
        }
        if let Some(re) = lit.strip_prefix('/').and_then(|r| r.strip_suffix('/')) {
            return Ok(Text::Regex(regex::Regex::new(re)?));
        }
        Ok(Text::Syms([self.lang.syms.intern(lit)].into()))
    }
}

struct TplParser<'a, 's, T> {
    s: Src<'a>,
    slots: &'s Slots,
    lang: &'s Lang,
    text: &'s mut dyn FnMut(&str) -> R<T>,
    texts: usize,
}

impl<T> TplParser<'_, '_, T> {
    fn slot(&mut self) -> R<u16> {
        let name = self.s.name()?;
        self.slots
            .get(name)
            .ok_or_else(|| self.s.err(format_args!("unknown capture ${name}")))
    }

    fn element(&mut self, field: u16) -> R<Tpl<T>> {
        if self.s.eat("(") {
            return self.node(field);
        }
        if self.s.eat("$$$") {
            let slot = self.slot()?;
            let test = match self.s.eat(":") {
                true => self.s.test(self.lang, None)?,
                false => Test::any(),
            };
            let (rekind, leaf) = self.s.rekind(self.lang)?.unzip();
            let leaf = leaf.unwrap_or(false);
            return Ok(Tpl::Var {
                slot,
                test,
                rekind,
                leaf,
            });
        }
        if self.s.eat("@$") {
            let slot = self.slot()?;
            return self.cap(slot, field);
        }
        if self.s.eat("$") {
            let slot = self.slot()?;
            let after = self.s.at;
            self.s.ws();
            if self.s.eat("{") {
                return Ok(Tpl::Spread {
                    slot,
                    kids: self.kids("}")?.into(),
                });
            }
            self.s.at = after;
            return self.cap(slot, field);
        }
        Err(self.s.err("expected an element"))
    }

    fn cap(&mut self, slot: u16, field: u16) -> R<Tpl<T>> {
        match self.s.rekind(self.lang)? {
            Some((_, true)) => Err(self.s.err("`=>` needs a variadic")),
            rekind => Ok(Tpl::Cap {
                slot,
                field,
                rekind: rekind.map(|(k, _)| k),
            }),
        }
    }

    fn kids(&mut self, close: &str) -> R<Vec<Tpl<T>>> {
        let mut kids = Vec::new();
        loop {
            self.s.ws();
            if self.s.eat(close) {
                return Ok(kids);
            }
            if self.s.peek().is_none() {
                return Err(self.s.err(format_args!("expected `{close}`")));
            }
            kids.push(self.element(0)?);
        }
    }

    fn node(&mut self, field: u16) -> R<Tpl<T>> {
        self.s.ws();
        let kind = match self.s.peek() {
            Some(b'"') => self.s.quoted()?,
            _ => self.s.ident()?,
        };
        let kind = self.lang.intern_kind(kind);
        let optional = self.s.eat("?");
        let (mut text, mut kids) = (TplText::None, Vec::new());
        loop {
            self.s.ws();
            let start = self.s.at;
            match self.s.peek() {
                None => return Err(self.s.err("unclosed node")),
                Some(b')') => {
                    self.s.at += 1;
                    break;
                }
                Some(b'"') if !matches!(text, TplText::None) => {
                    return Err(self.s.err("a node has one text"));
                }
                Some(b'"') => text = TplText::Lit(self.lang.syms.intern(self.s.quoted()?)),
                Some(b'@') if self.s.eat("@$") => {
                    let slot = self.slot()?;
                    if self.s.rest().starts_with("->") {
                        kids.push(self.cap(slot, 0)?);
                        continue;
                    }
                    if !matches!(text, TplText::None) {
                        return Err(self.s.err("a node has one text"));
                    }
                    self.pipes()?;
                    let expr = (self.text)(&self.s.src[start..self.s.at])?;
                    text = TplText::Expr {
                        slot,
                        index: self.texts,
                        expr,
                    };
                    self.texts += 1;
                }
                Some(b) if is_ident(b) => {
                    let field = self.lang.intern_field(self.s.field()?);
                    self.s.ws();
                    kids.push(self.element(field)?);
                }
                Some(_) => kids.push(self.element(0)?),
            }
        }
        let labelled = match text {
            TplText::Expr { slot, .. } => kids
                .iter()
                .any(|kid| matches!(kid, Tpl::Cap { slot: s, .. } if *s == slot)),
            _ => false,
        };
        Ok(Tpl::Node {
            kind,
            field,
            text,
            kids: kids.into(),
            optional,
            labelled,
        })
    }

    fn pipes(&mut self) -> R<()> {
        loop {
            let after = self.s.at;
            self.s.ws();
            if !self.s.eat("|") {
                self.s.at = after;
                return Ok(());
            }
            self.s.ws();
            self.call()?;
        }
    }

    fn call(&mut self) -> R<()> {
        let start = self.s.at;
        let mut depth = 0u32;
        while let Some(b) = self.s.peek() {
            match b {
                b'"' => {
                    self.s.quoted()?;
                    continue;
                }
                b'(' => depth += 1,
                b')' if depth > 0 => depth -= 1,
                b')' | b'|' if depth == 0 => break,
                b if b.is_ascii_whitespace() && depth == 0 => break,
                _ => {}
            }
            self.s.at += 1;
        }
        match (self.s.at == start, depth) {
            (true, _) => Err(self.s.err("expected a function after `|`")),
            (_, 0) => Ok(()),
            _ => Err(self.s.err("unclosed `(`")),
        }
    }
}

/// Whether a kind name is a definition parameter: written in capitals.
fn is_param(name: &str) -> bool {
    name.bytes()
        .all(|b| b.is_ascii_uppercase() || b == b'_' || b.is_ascii_digit())
}
