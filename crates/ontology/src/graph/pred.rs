//! Composable edge functions. [`EdgePred`] (the boolean case) is the edge-filter
//! algebra, chainable with `&`, `|`, `!` — the analog of `treesitter-visit`'s
//! `Match`. [`EdgeFn`] generalizes the codomain so the same combinator serves
//! filtering (`bool`), weighting (`u32`), and marking (later).

use std::collections::HashSet;
use std::rc::Rc;

use super::walk::Hop;

/// A cloneable function over one [`Hop`]. `EdgeFn<bool>` is [`EdgePred`];
/// non-boolean codomains (cost, mark) reuse the same construction.
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

/// A boolean edge filter. Chain with `&`, `|`, `!`.
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

/// Any edge.
#[must_use]
pub fn any() -> EdgePred {
    EdgePred::of(|_| true)
}

/// A synthesized FK edge.
#[must_use]
pub fn synthesized() -> EdgePred {
    EdgePred::of(|h| h.synthesized)
}

/// A declared triple edge (not FK-synthesized).
#[must_use]
pub fn triple() -> EdgePred {
    EdgePred::of(|h| !h.synthesized)
}

/// The far node kind equals `node`.
#[must_use]
pub fn to(node: &str) -> EdgePred {
    let node = node.to_string();
    EdgePred::of(move |h| h.to == node)
}

/// The relationship kind is in `types` (empty set matches nothing).
#[must_use]
pub fn kinds_in(types: &HashSet<&str>) -> EdgePred {
    let types: HashSet<String> = types.iter().map(|s| (*s).to_string()).collect();
    EdgePred::of(move |h| types.contains(h.relationship_kind))
}
