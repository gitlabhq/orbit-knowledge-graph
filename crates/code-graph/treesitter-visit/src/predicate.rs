//! Composable boolean predicates over tree-sitter nodes.
//!
//! `Pred` = `Exists(Extract)` + boolean logic. That's it.
//!
//! ```ignore
//! use treesitter_visit::predicate::*;
//!
//! has_name()
//! parent_is("module") & has_child(&["decorator"])
//! !kind_ends_with("_statement")
//! ```

use crate::extract::{self, Extract};
use crate::node::{Axis, Match, Node};
use crate::source::Doc;

#[derive(Clone)]
pub enum Pred {
    Exists(Box<Extract>),
    And(Box<Pred>, Box<Pred>),
    Or(Box<Pred>, Box<Pred>),
    Not(Box<Pred>),
}

impl Pred {
    pub fn test<D: Doc>(&self, node: &Node<'_, D>) -> bool {
        match self {
            Pred::Exists(e) => e.navigate(node).is_some(),
            Pred::And(a, b) => a.test(node) && b.test(node),
            Pred::Or(a, b) => a.test(node) || b.test(node),
            Pred::Not(p) => !p.test(node),
        }
    }

    pub fn and(self, other: Pred) -> Pred {
        Pred::And(Box::new(self), Box::new(other))
    }

    pub fn or(self, other: Pred) -> Pred {
        Pred::Or(Box::new(self), Box::new(other))
    }
}

impl std::ops::Not for Pred {
    type Output = Pred;
    fn not(self) -> Pred {
        Pred::Not(Box::new(self))
    }
}

// ── Constructors ────────────────────────────────────────────────
// Each is just Exists(some Extract pipeline).

fn exists(e: Extract) -> Pred {
    Pred::Exists(Box::new(e))
}

fn check(m: Match<'static>) -> Pred {
    exists(extract::text().where_(m))
}

fn check_at(axis: Axis<'static>, m: Match<'static>) -> Pred {
    exists(Extract::one(axis, m))
}

pub fn has_name() -> Pred {
    exists(extract::field("name"))
}

pub fn parent_is(kind: &'static str) -> Pred {
    check_at(Axis::Parent, Match::Kind(kind))
}

pub fn grandparent_is(kind: &'static str) -> Pred {
    exists(Extract::one(Axis::Parent, Match::Any).nav(Axis::Parent, Match::Kind(kind)))
}

pub fn ancestor_is(kinds: &'static [&'static str]) -> Pred {
    check_at(Axis::Ancestor, Match::AnyKind(kinds))
}

pub fn has_child(kinds: &'static [&'static str]) -> Pred {
    check_at(Axis::Child, Match::AnyKind(kinds))
}

pub fn has_descendant(kind: &'static str) -> Pred {
    check_at(Axis::Descendant, Match::Kind(kind))
}

pub fn field_kind(field: &'static str, kinds: &'static [&'static str]) -> Pred {
    check_at(Axis::Field(field), Match::AnyKind(kinds))
}

pub fn nearest_ancestor(
    candidates: &'static [&'static str],
    target: &'static [&'static str],
) -> Pred {
    exists(Extract::one(Axis::Ancestor, Match::AnyKind(candidates)).where_(Match::AnyKind(target)))
}

pub fn kind_ends_with(suffix: &'static str) -> Pred {
    check(Match::KindEndsWith(suffix))
}

pub fn kind_starts_with(prefix: &'static str) -> Pred {
    check(Match::KindStartsWith(prefix))
}

pub fn parent_ends_with(suffix: &'static str) -> Pred {
    exists(Extract::one(Axis::Parent, Match::Any).where_(Match::KindEndsWith(suffix)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{LanguageExt, SupportLang};

    fn py(code: &str) -> crate::Root<crate::StrDoc<SupportLang>> {
        SupportLang::Python.ast_grep(code)
    }

    #[test]
    fn basics() {
        let root = py("def foo(): pass");
        let func = root.root().children().next().unwrap();
        assert!(has_name().test(&func));
        assert!(parent_is("module").test(&func));
        assert!(!parent_is("class_definition").test(&func));
    }

    #[test]
    fn kind_patterns() {
        let root = py("def foo(): pass");
        let func = root.root().children().next().unwrap();
        assert!(kind_ends_with("_definition").test(&func));
        assert!(kind_starts_with("function").test(&func));
    }

    #[test]
    fn boolean_logic() {
        let root = py("def foo(): pass");
        let func = root.root().children().next().unwrap();
        assert!((has_name().and(parent_is("module"))).test(&func));
        assert!((parent_is("class").or(parent_is("module"))).test(&func));
        assert!((!parent_is("class_definition")).test(&func));
    }

    #[test]
    fn ancestor() {
        let root = py("class Foo:\n    def bar(self): pass");
        let func = root
            .root()
            .find(Axis::Descendant, Match::Kind("function_definition"))
            .unwrap();
        assert!(ancestor_is(&["class_definition"]).test(&func));
    }

    #[test]
    fn field_kind_check() {
        let root = py("def foo(): pass");
        let func = root.root().children().next().unwrap();
        assert!(field_kind("name", &["identifier"]).test(&func));
        assert!(!field_kind("name", &["type_identifier"]).test(&func));
    }

    #[test]
    fn grandparent() {
        let root = py("class Foo:\n    def bar(self): pass");
        let func = root
            .root()
            .find(Axis::Descendant, Match::Kind("function_definition"))
            .unwrap();
        assert!(grandparent_is("class_definition").test(&func));
        assert!(!grandparent_is("module").test(&func));
    }
}
