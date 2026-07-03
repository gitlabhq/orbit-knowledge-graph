//! [`EdgeFn<T>`] over a [`Hop`]; [`EdgePred`] = `EdgeFn<bool>`, chainable with `&`/`|`/`!`.

use std::collections::HashSet;
use std::rc::Rc;

use super::walk::Hop;

#[derive(Clone)]
pub struct EdgeFn<T>(Rc<dyn Fn(&Hop<'_>) -> T>);

impl<T> EdgeFn<T> {
    pub fn of(f: impl Fn(&Hop<'_>) -> T + 'static) -> Self {
        Self(Rc::new(f))
    }

    pub(crate) fn eval(&self, hop: &Hop<'_>) -> T {
        (self.0)(hop)
    }
}

pub type EdgePred = EdgeFn<bool>;

impl EdgePred {
    pub(crate) fn test(&self, hop: &Hop<'_>) -> bool {
        self.eval(hop)
    }
}

impl std::ops::BitAnd for EdgePred {
    type Output = EdgePred;
    fn bitand(self, rhs: EdgePred) -> EdgePred {
        EdgePred::of(move |h| self.test(h) && rhs.test(h))
    }
}
impl std::ops::BitOr for EdgePred {
    type Output = EdgePred;
    fn bitor(self, rhs: EdgePred) -> EdgePred {
        EdgePred::of(move |h| self.test(h) || rhs.test(h))
    }
}
impl std::ops::Not for EdgePred {
    type Output = EdgePred;
    fn not(self) -> EdgePred {
        EdgePred::of(move |h| !self.test(h))
    }
}

#[must_use]
pub fn any() -> EdgePred {
    EdgePred::of(|_| true)
}

#[must_use]
pub fn synthesized() -> EdgePred {
    EdgePred::of(|h| h.synthesized)
}

/// A declared triple edge (not FK-synthesized).
#[must_use]
pub fn triple() -> EdgePred {
    EdgePred::of(|h| !h.synthesized)
}

#[must_use]
pub fn to(node: &str) -> EdgePred {
    let node = node.to_string();
    EdgePred::of(move |h| h.to == node)
}

/// Relationship kind in `types` (empty matches nothing).
#[must_use]
pub fn kinds_in(types: &HashSet<&str>) -> EdgePred {
    let types: HashSet<String> = types.iter().map(|s| (*s).to_string()).collect();
    EdgePred::of(move |h| types.contains(h.relationship_kind))
}
