//! Pair helpers, error helpers, and literal conversion shared by the lowering
//! modules.

use compiler::QueryError;
use ontology::Ontology;
use serde_json::Value as Json;

use crate::syntax::{P, Rule, located};
use crate::{MAX_FILTER_STRING_CHARS, MAX_IN_VALUES, Parameters};

pub type Result<T> = std::result::Result<T, QueryError>;

pub fn child<'i>(pair: &P<'i>, rule: Rule) -> Option<P<'i>> {
    pair.clone().into_inner().find(|c| c.as_rule() == rule)
}

pub fn children<'i>(pair: &P<'i>, rule: Rule) -> impl Iterator<Item = P<'i>> {
    pair.clone()
        .into_inner()
        .filter(move |c| c.as_rule() == rule)
}

pub fn first<'i>(pair: &P<'i>) -> P<'i> {
    pair.clone()
        .into_inner()
        .next()
        .unwrap_or_else(|| panic!("{:?} has an inner pair", pair.as_rule()))
}

pub fn start(pair: &P<'_>) -> usize {
    pair.as_span().start()
}

pub fn ident_name(pair: &P<'_>) -> String {
    let raw = pair.as_str();
    raw.strip_prefix('`')
        .and_then(|s| s.strip_suffix('`'))
        .unwrap_or(raw)
        .to_string()
}

/// An identifier with the offset it came from, for error locations.
#[derive(Clone)]
pub struct Named {
    pub name: String,
    pub at: usize,
}

/// Mirrors the DSL `Identifier` schema pattern `^[a-zA-Z_][a-zA-Z0-9_]{0,63}$`.
pub fn is_valid_identifier(name: &str) -> bool {
    let mut chars = name.chars();
    let Some(head) = chars.next() else {
        return false;
    };
    (head.is_ascii_alphabetic() || head == '_')
        && name.len() <= 64
        && chars.all(|c| c.is_ascii_alphanumeric() || c == '_')
}

pub struct Ctx<'a> {
    pub source: &'a str,
    pub params: &'a Parameters,
    pub ontology: &'a Ontology,
}

impl Ctx<'_> {
    pub fn fail<T>(&self, at: usize, msg: impl AsRef<str>) -> Result<T> {
        Err(QueryError::Validation(located(self.source, at, msg)))
    }

    pub fn unbound<T>(&self, variable: &Named) -> Result<T> {
        Err(QueryError::ReferenceError(located(
            self.source,
            variable.at,
            format!(
                "variable `{}` is not bound in the MATCH pattern",
                variable.name
            ),
        )))
    }

    pub fn allowlist<T>(&self, at: usize, msg: impl AsRef<str>) -> Result<T> {
        Err(QueryError::AllowlistRejected(located(self.source, at, msg)))
    }

    pub fn named(&self, ident: &P<'_>) -> Result<Named> {
        let name = ident_name(ident);
        if !is_valid_identifier(&name) {
            return self.fail(
                start(ident),
                format!("identifier `{name}` must match ^[a-zA-Z_][a-zA-Z0-9_]{{0,63}}$"),
            );
        }
        Ok(Named {
            name,
            at: start(ident),
        })
    }

    /// A literal or `$parameter` as the JSON frontend would carry it.
    pub fn json(&self, value: &P<'_>) -> Result<Json> {
        let inner = first(value);
        let at = start(&inner);
        Ok(match inner.as_rule() {
            Rule::string => Json::String(self.string(unescape(inner.as_str()), at)?),
            Rule::integer => match inner.as_str().parse::<i64>() {
                Ok(i) => Json::from(i),
                Err(_) => return self.fail(at, "integer literal is out of range"),
            },
            Rule::float => match inner
                .as_str()
                .parse::<f64>()
                .ok()
                .and_then(serde_json::Number::from_f64)
            {
                Some(n) => Json::Number(n),
                None => return self.fail(at, "number literal is out of range"),
            },
            Rule::boolean => Json::Bool(inner.as_str().eq_ignore_ascii_case("true")),
            Rule::list => {
                let items: Vec<P<'_>> = children(&inner, Rule::value).collect();
                self.list_len(items.len(), at)?;
                let mut out = Vec::with_capacity(items.len());
                for item in &items {
                    if first(item).as_rule() == Rule::list {
                        return self.fail(start(item), "lists cannot be nested");
                    }
                    out.push(self.json(item)?);
                }
                Json::Array(out)
            }
            Rule::parameter => {
                let name = &inner.as_str()[1..];
                match self.params.get(name) {
                    Some(bound) => self.param(bound, at, true)?,
                    None => return self.fail(at, format!("parameter ${name} is not bound")),
                }
            }
            other => unreachable!("value form {other:?}"),
        })
    }

    fn param(&self, bound: &Json, at: usize, allow_list: bool) -> Result<Json> {
        match bound {
            Json::Null => self.fail(
                at,
                "a NULL parameter is not a value; use IS NULL or IS NOT NULL",
            ),
            Json::Bool(_) => Ok(bound.clone()),
            Json::Number(n) if n.is_i64() || n.is_f64() => Ok(bound.clone()),
            Json::Number(n) => self.fail(at, format!("parameter value {n} is out of range")),
            Json::String(s) => Ok(Json::String(self.string(s.clone(), at)?)),
            Json::Array(items) if allow_list => {
                self.list_len(items.len(), at)?;
                items
                    .iter()
                    .map(|i| self.param(i, at, false))
                    .collect::<Result<_>>()
                    .map(Json::Array)
            }
            Json::Array(_) => self.fail(at, "lists cannot be nested"),
            Json::Object(_) => {
                self.fail(at, "a parameter must be a scalar or a list, not an object")
            }
        }
    }

    fn string(&self, s: String, at: usize) -> Result<String> {
        let len = s.chars().count();
        if len > MAX_FILTER_STRING_CHARS {
            return self.fail(
                at,
                format!("string is {len} characters; the maximum is {MAX_FILTER_STRING_CHARS}"),
            );
        }
        Ok(s)
    }

    fn list_len(&self, len: usize, at: usize) -> Result<()> {
        if len > MAX_IN_VALUES {
            return Err(QueryError::LimitExceeded(located(
                self.source,
                at,
                format!("list has {len} values; the maximum is {MAX_IN_VALUES}"),
            )));
        }
        Ok(())
    }
}

/// openCypher escapes (`\n`, `\t`, `\uXXXX`, ...) plus the doubled-quote form.
pub fn unescape(quoted: &str) -> String {
    let quote = quoted
        .chars()
        .next()
        .expect("quoted string has a delimiter");
    let body = &quoted[1..quoted.len() - 1];
    let mut out = String::with_capacity(body.len());
    let mut chars = body.chars().peekable();
    while let Some(c) = chars.next() {
        match c {
            _ if c == quote && chars.peek() == Some(&quote) => {
                chars.next();
                out.push(quote);
            }
            '\\' => match chars.next() {
                Some('n') => out.push('\n'),
                Some('t') => out.push('\t'),
                Some('r') => out.push('\r'),
                Some('b') => out.push('\u{8}'),
                Some('f') => out.push('\u{c}'),
                Some('u') => {
                    let hex: String = chars.by_ref().take(4).collect();
                    match u32::from_str_radix(&hex, 16).ok().and_then(char::from_u32) {
                        Some(ch) => out.push(ch),
                        None => out.push_str(&format!("\\u{hex}")),
                    }
                }
                Some(other) => out.push(other),
                None => out.push('\\'),
            },
            _ => out.push(c),
        }
    }
    out
}
