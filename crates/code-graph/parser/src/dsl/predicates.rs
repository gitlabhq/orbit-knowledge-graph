use treesitter_visit::tree_sitter::StrDoc;
use treesitter_visit::{Node, SupportLang};

type N<'a> = Node<'a, StrDoc<SupportLang>>;

/// A condition on a tree-sitter node.
#[derive(Clone)]
pub enum Cond {
    HasName,
    ParentIs(&'static str),

    GrandparentIs(&'static str),
    AncestorIs(&'static [&'static str]),
    NearestAncestor {
        candidates: &'static [&'static str],
        target: &'static [&'static str],
    },
    HasChild(&'static [&'static str]),
    FieldKind {
        field: &'static str,
        kinds: &'static [&'static str],
    },
    FieldDescends {
        field: &'static str,
        wrappers: &'static [&'static str],
        targets: &'static [&'static str],
        reject: &'static [&'static str],
    },
    /// Node kind ends with suffix (e.g. "_definition", "_declaration").
    KindEndsWith(&'static str),
    /// Node kind starts with prefix (e.g. "decorated_", "abstract_").
    KindStartsWith(&'static str),
    /// Parent node kind ends with suffix.
    ParentKindEndsWith(&'static str),
    /// Node kind matches a regex pattern.
    KindMatches(regex::Regex),
    /// Parent node kind matches a regex pattern.
    ParentKindMatches(regex::Regex),
}

impl Cond {
    fn test(&self, node: &N<'_>) -> bool {
        match self {
            Cond::HasName => node.field("name").is_some(),
            Cond::ParentIs(kind) => node.parent().is_some_and(|p| p.kind() == *kind),

            Cond::KindEndsWith(suffix) => node.kind().as_ref().ends_with(suffix),
            Cond::KindStartsWith(prefix) => node.kind().as_ref().starts_with(prefix),
            Cond::ParentKindEndsWith(suffix) => node
                .parent()
                .is_some_and(|p| p.kind().as_ref().ends_with(suffix)),
            Cond::KindMatches(re) => re.is_match(node.kind().as_ref()),
            Cond::ParentKindMatches(re) => node
                .parent()
                .is_some_and(|p| re.is_match(p.kind().as_ref())),
            Cond::GrandparentIs(kind) => node
                .parent()
                .and_then(|p| p.parent())
                .is_some_and(|gp| gp.kind() == *kind),
            Cond::AncestorIs(kinds) => {
                let mut cur = node.parent();
                while let Some(a) = cur {
                    let k = a.kind();
                    if kinds.iter().any(|t| *t == k) {
                        return true;
                    }
                    cur = a.parent();
                }
                false
            }
            Cond::NearestAncestor { candidates, target } => {
                let mut cur = node.parent();
                while let Some(a) = cur {
                    let k = a.kind();
                    if candidates.iter().any(|c| *c == k) {
                        return target.iter().any(|t| *t == k);
                    }
                    cur = a.parent();
                }
                false
            }
            Cond::HasChild(kinds) => node.children().any(|child| {
                let k = child.kind();
                kinds.iter().any(|t| *t == k)
            }),
            Cond::FieldKind { field, kinds } => node.field(field).is_some_and(|f| {
                let k = f.kind();
                kinds.iter().any(|t| *t == k)
            }),
            Cond::FieldDescends {
                field,
                wrappers,
                targets,
                reject,
            } => node
                .field(field)
                .is_some_and(|f| descends(&f, wrappers, targets, reject)),
        }
    }
}

fn descends(node: &N<'_>, wrappers: &[&str], targets: &[&str], reject: &[&str]) -> bool {
    let kind = node.kind();
    if targets.iter().any(|t| *t == kind) {
        return true;
    }
    if reject.iter().any(|r| *r == kind) {
        return false;
    }
    if wrappers.iter().any(|w| *w == kind)
        && let Some(child) = node.child(0)
    {
        return descends(&child, wrappers, targets, reject);
    }
    false
}

/// Boolean logic over conditions.
#[derive(Clone)]
pub enum Pred {
    Cond(Cond),
    And(Box<Pred>, Box<Pred>),
    Or(Box<Pred>, Box<Pred>),
    Not(Box<Pred>),
}

impl std::ops::Not for Pred {
    type Output = Pred;
    fn not(self) -> Pred {
        Pred::Not(Box::new(self))
    }
}

impl Pred {
    pub fn and(self, other: Pred) -> Pred {
        Pred::And(Box::new(self), Box::new(other))
    }

    pub fn or(self, other: Pred) -> Pred {
        Pred::Or(Box::new(self), Box::new(other))
    }

    pub fn test(&self, node: &N<'_>) -> bool {
        match self {
            Pred::Cond(c) => c.test(node),
            Pred::And(a, b) => a.test(node) && b.test(node),
            Pred::Or(a, b) => a.test(node) || b.test(node),
            Pred::Not(inner) => !inner.test(node),
        }
    }
}

impl From<Cond> for Pred {
    fn from(c: Cond) -> Self {
        Pred::Cond(c)
    }
}

/// Generate constructor functions that lift `Cond` variants into `Pred`.
macro_rules! cond_constructors {
    ($( fn $name:ident( $($arg:ident : $ty:ty),* ) => $variant:expr; )*) => {
        $( pub fn $name( $($arg: $ty),* ) -> Pred { $variant.into() } )*
    };
}

cond_constructors! {
    fn has_name() => Cond::HasName;
    fn parent_is(kind: &'static str) => Cond::ParentIs(kind);
    fn parent_ends_with(suffix: &'static str) => Cond::ParentKindEndsWith(suffix);
    fn grandparent_is(kind: &'static str) => Cond::GrandparentIs(kind);
    fn ancestor_is(kinds: &'static [&'static str]) => Cond::AncestorIs(kinds);
    fn has_child(kinds: &'static [&'static str]) => Cond::HasChild(kinds);
    fn kind_ends_with(suffix: &'static str) => Cond::KindEndsWith(suffix);
    fn kind_starts_with(prefix: &'static str) => Cond::KindStartsWith(prefix);
    fn parent_kind_ends_with(suffix: &'static str) => Cond::ParentKindEndsWith(suffix);
}

/// Node kind matches a regex pattern (compiled once at rule construction).
pub fn kind_matches(pattern: &str) -> Pred {
    Cond::KindMatches(regex::Regex::new(pattern).expect("invalid regex in DSL predicate")).into()
}

/// Parent node kind matches a regex pattern.
pub fn parent_kind_matches(pattern: &str) -> Pred {
    Cond::ParentKindMatches(regex::Regex::new(pattern).expect("invalid regex in DSL predicate"))
        .into()
}

// Multi-arg constructors don't fit the macro pattern cleanly.
pub fn nearest_ancestor(
    candidates: &'static [&'static str],
    target: &'static [&'static str],
) -> Pred {
    Cond::NearestAncestor { candidates, target }.into()
}

pub fn field_kind(field: &'static str, kinds: &'static [&'static str]) -> Pred {
    Cond::FieldKind { field, kinds }.into()
}

pub fn field_descends(
    field: &'static str,
    wrappers: &'static [&'static str],
    targets: &'static [&'static str],
    reject: &'static [&'static str],
) -> Pred {
    Cond::FieldDescends {
        field,
        wrappers,
        targets,
        reject,
    }
    .into()
}

#[cfg(test)]
mod tests {
    use super::*;
    use treesitter_visit::LanguageExt;

    fn parse_python(code: &str) -> treesitter_visit::Root<StrDoc<SupportLang>> {
        SupportLang::Python.ast_grep(code)
    }

    #[test]
    fn test_basic_conditions() {
        let root = parse_python("def foo(): pass");
        let module = root.root();
        let func = module.children().next().unwrap();

        assert!(has_name().test(&func));
        assert!(!has_name().test(&module));
        assert!(parent_is("module").test(&func));
    }

    #[test]
    fn test_suffix_prefix_regex() {
        let root = parse_python("def foo(): pass");
        let func = root.root().children().next().unwrap();

        assert!(kind_ends_with("_definition").test(&func));
        assert!(kind_starts_with("function").test(&func));
        assert!(!kind_ends_with("_declaration").test(&func));
        assert!(kind_matches("^function_.*").test(&func));
        assert!(!kind_matches("^class_.*").test(&func));
        assert!(parent_kind_ends_with("").test(&func)); // parent is "module"
    }

    #[test]
    fn test_boolean_logic() {
        let root = parse_python("def foo(): pass");
        let func = root.root().children().next().unwrap();

        assert!(has_name().and(parent_is("module")).test(&func));
        assert!(
            parent_is("class_definition")
                .or(parent_is("module"))
                .test(&func)
        );
        assert!(!((!parent_is("module")).test(&func)));
    }
}
