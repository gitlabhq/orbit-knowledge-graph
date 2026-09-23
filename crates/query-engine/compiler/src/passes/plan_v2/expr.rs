//! Scalar expressions. Every column reference is qualified (`alias.column`),
//! so an expression means the same thing wherever it sits in the tree; that
//! is what lets the optimizer move predicates and rewrite joins safely.
//!
//! Plans mostly write expressions with `pe!` (see `parse.rs`). The builders
//! here exist for the parts that carry **user data**: ids, filter values,
//! relationship kinds. Those become typed literals and never touch
//! expression text.

use super::prelude::*;
use serde::Serialize;

// ── Types ─────────────────────────────────────────────────────────────────────

/// Plan-level scalar expression. Every column reference is qualified, so an
/// expression means the same thing wherever it sits in the tree.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub enum PExpr {
    Col(String, String),
    /// Bare identifier: an output alias in ORDER BY, or a lambda parameter.
    Ident(String),
    Lit(Lit),
    Func(String, Vec<PExpr>),
    Cmp(CmpOp, Box<PExpr>, Box<PExpr>),
    And(Vec<PExpr>),
    Or(Vec<PExpr>),
    In(Box<PExpr>, Vec<Lit>),
    Lambda(String, Box<PExpr>),
    DateTrunc(TruncateUnit, Box<PExpr>),
    /// A user filter on `alias.property`; lowering owns operator and
    /// parameter typing (`filter_to_expr`).
    NodeFilter {
        alias: String,
        property: String,
        #[serde(skip)]
        filter: InputFilter,
    },
    /// Namespace scope restriction on `alias.traversal_path`.
    Scope(String, #[serde(skip)] crate::scope::ScopePrefix),
    /// The scope's anchor resolved to a real path (guards an elided anchor).
    ScopeResolved(#[serde(skip)] crate::scope::ScopePrefix),
}

// ── Expressions ─────────────────────────────────────────────────────────────

/// `(alias, column)`: a fully qualified column reference.
pub type Col = (String, String);

#[derive(Debug, Clone, PartialEq, Serialize)]
pub enum Lit {
    Int(i64),
    Str(String),
    Bool(bool),
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize)]
pub enum CmpOp {
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
}

// ── Builders for user data ────────────────────────────────────────────────────

pub fn deleted_false(alias: &str) -> PExpr {
    pe!("{alias}._deleted = false")
}

/// `alias.column IN ids`, or `= id` for a single value.
pub fn id_in(alias: &str, column: &str, ids: &[i64]) -> PExpr {
    match ids {
        [id] => pe!("{alias}.{column} = {id}"),
        _ => PExpr::In(
            Box::new(pe!("{alias}.{column}")),
            ids.iter().map(|&i| Lit::Int(i)).collect(),
        ),
    }
}

pub fn id_range(alias: &str, column: &str, r: &InputIdRange) -> PExpr {
    pe!(
        "{alias}.{column} >= {} AND {alias}.{column} <= {}",
        r.start,
        r.end
    )
}

/// Relationship kinds come from the query; they are user data and go
/// through a typed literal, not the expression text.
pub fn rel_kind(alias: &str, types: &[String]) -> Option<PExpr> {
    if crate::passes::normalize::is_wildcard(types) {
        return None;
    }
    let kind = Box::new(pe!("{alias}.relationship_kind"));
    let lits: Vec<Lit> = types.iter().map(|t| Lit::Str(t.clone())).collect();
    Some(match lits.as_slice() {
        [t] => PExpr::Cmp(CmpOp::Eq, kind, Box::new(PExpr::Lit(t.clone()))),
        _ => PExpr::In(kind, lits),
    })
}

/// User filters on a node's properties, in property order for stable output.
pub fn node_filters(alias: &str, filters: &HashMap<String, Vec<InputFilter>>) -> Vec<PExpr> {
    let mut props: Vec<_> = filters.iter().collect();
    props.sort_unstable_by_key(|(k, _)| *k);
    props
        .into_iter()
        .flat_map(|(prop, fs)| {
            fs.iter().map(|f| PExpr::NodeFilter {
                alias: alias.to_string(),
                property: prop.clone(),
                filter: f.clone(),
            })
        })
        .collect()
}

// ── Denormalized tags ───────────────────────────────────────────────────────

/// `has(tags, 'key:value')` / `hasAny(tags, [...])` for an eq or in filter.
pub fn denorm_tag(edge: &str, tag_col: &str, tag_key: &str, f: &InputFilter) -> Option<PExpr> {
    let scalar = |v: &serde_json::Value| match v {
        serde_json::Value::String(s) => Some(s.clone()),
        serde_json::Value::Bool(b) => Some(b.to_string()),
        serde_json::Value::Number(n) => Some(n.to_string()),
        _ => None,
    };
    let tags: Vec<String> = match (&f.op, &f.value) {
        (None | Some(FilterOp::Eq), Some(v)) => vec![scalar(v)?],
        (Some(FilterOp::In), Some(serde_json::Value::Array(vs))) => {
            vs.iter().filter_map(scalar).collect()
        }
        _ => return None,
    };
    // Tag values are user data: typed literals, never expression text.
    let tags: Vec<PExpr> = tags
        .into_iter()
        .map(|t| PExpr::Lit(Lit::Str(format!("{tag_key}:{t}"))))
        .collect();
    let tags_col = pe!("{edge}.{tag_col}");
    match tags.len() {
        0 => None,
        1 => Some(PExpr::Func(
            "has".into(),
            vec![tags_col, tags.into_iter().next().unwrap()],
        )),
        _ => Some(PExpr::Func(
            "hasAny".into(),
            vec![tags_col, PExpr::Func("array".into(), tags)],
        )),
    }
}

// ── Rewriting ─────────────────────────────────────────────────────────────────

impl PExpr {
    /// Rebuilds the expression bottom-up; `leaf` may replace any node.
    pub fn map(&self, leaf: &dyn Fn(&PExpr) -> Option<PExpr>) -> PExpr {
        if let Some(e) = leaf(self) {
            return e;
        }
        let go = |x: &PExpr| x.map(leaf);
        match self {
            PExpr::Func(n, xs) => PExpr::Func(n.clone(), xs.iter().map(go).collect()),
            PExpr::And(xs) => PExpr::And(xs.iter().map(go).collect()),
            PExpr::Or(xs) => PExpr::Or(xs.iter().map(go).collect()),
            PExpr::Cmp(op, l, r) => PExpr::Cmp(*op, Box::new(go(l)), Box::new(go(r))),
            PExpr::In(x, vs) => PExpr::In(Box::new(go(x)), vs.clone()),
            PExpr::Lambda(p, b) => PExpr::Lambda(p.clone(), Box::new(go(b))),
            PExpr::DateTrunc(unit, value) => PExpr::DateTrunc(*unit, Box::new(go(value))),
            other => other.clone(),
        }
    }

    /// Rewrites column references through `s`.
    pub fn subst(&self, s: &HashMap<Col, PExpr>) -> PExpr {
        self.map(&|e| match e {
            PExpr::Col(a, c) => s.get(&(a.clone(), c.clone())).cloned(),
            _ => None,
        })
    }

    /// Renames every reference to alias `from`.
    pub fn realias(&self, from: &str, to: &str) -> PExpr {
        self.map(&|e| match e {
            PExpr::Col(a, c) if a == from => Some(PExpr::Col(to.into(), c.clone())),
            PExpr::Scope(a, p) if a == from => Some(PExpr::Scope(to.into(), p.clone())),
            PExpr::NodeFilter {
                alias,
                property,
                filter,
            } if alias == from => Some(PExpr::NodeFilter {
                alias: to.into(),
                property: property.clone(),
                filter: filter.clone(),
            }),
            _ => None,
        })
    }

    pub fn aliases(&self, out: &mut HashSet<String>) {
        match self {
            PExpr::Col(a, _) | PExpr::Scope(a, _) | PExpr::NodeFilter { alias: a, .. } => {
                out.insert(a.clone());
            }
            PExpr::Ident(_) | PExpr::Lit(_) | PExpr::ScopeResolved(_) => {}
            PExpr::Func(_, xs) | PExpr::And(xs) | PExpr::Or(xs) => {
                xs.iter().for_each(|x| x.aliases(out))
            }
            PExpr::Cmp(_, l, r) => {
                l.aliases(out);
                r.aliases(out);
            }
            PExpr::In(x, _) | PExpr::Lambda(_, x) | PExpr::DateTrunc(_, x) => x.aliases(out),
        }
    }

    /// The column this predicate constrains, for `col = lit`, `col IN`, and
    /// `col >= .. AND col <= ..` shapes.
    pub fn constrained_col(&self) -> Option<(&str, &str)> {
        match self {
            PExpr::Cmp(_, l, _) | PExpr::In(l, _) => match l.as_ref() {
                PExpr::Col(a, c) => Some((a, c)),
                _ => None,
            },
            PExpr::And(xs) => xs.first().and_then(|x| x.constrained_col()),
            _ => None,
        }
    }
}
