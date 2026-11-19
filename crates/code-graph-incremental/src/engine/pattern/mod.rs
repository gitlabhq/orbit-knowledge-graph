mod matching;
mod parse;
mod template;

#[cfg(test)]
mod tests;

use smallvec::SmallVec;

use crate::tree::Node;

pub use matching::{anchor, anchor_kinds, matches};
pub use parse::{parse_pattern, parse_template};
pub use template::build;

pub type Cap<I> = SmallVec<[I; 1]>;

pub struct Slots {
    names: Vec<Box<str>>,
    filters: Vec<KindFilter>,
}

impl Default for Slots {
    fn default() -> Self {
        Self::new()
    }
}

impl Slots {
    pub fn new() -> Self {
        Self {
            names: vec!["ROOT".into()],
            filters: vec![KindFilter::any()],
        }
    }

    pub fn slot(&mut self, name: &str) -> u16 {
        if let Some(slot) = self.get(name) {
            return slot;
        }
        self.names.push(name.into());
        self.filters.push(KindFilter::any());
        (self.names.len() - 1) as u16
    }

    pub fn get(&self, name: &str) -> Option<u16> {
        self.names
            .iter()
            .position(|n| &**n == name)
            .map(|i| i as u16)
    }

    pub fn len(&self) -> usize {
        self.names.len()
    }

    pub fn is_empty(&self) -> bool {
        self.names.is_empty()
    }

    pub fn filter(&self, slot: u16) -> &KindFilter {
        &self.filters[slot as usize]
    }

    fn set_filter(&mut self, slot: u16, filter: KindFilter) {
        self.filters[slot as usize] = filter;
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct KindFilter {
    pub kinds: Vec<u16>,
    pub negate: bool,
}

impl KindFilter {
    pub fn any() -> Self {
        Self {
            kinds: Vec::new(),
            negate: true,
        }
    }

    pub fn one(kind: u16) -> Self {
        Self {
            kinds: vec![kind],
            negate: false,
        }
    }

    pub fn admits(&self, kind: u16) -> bool {
        self.kinds.contains(&kind) != self.negate
    }

    fn single(&self) -> Option<u16> {
        match (self.negate, self.kinds.as_slice()) {
            (false, &[kind]) => Some(kind),
            _ => None,
        }
    }
}

#[derive(Clone, Debug)]
pub struct Test {
    pub kinds: KindFilter,
    pub named: bool,
}

impl Test {
    fn any() -> Self {
        Self {
            kinds: KindFilter::any(),
            named: false,
        }
    }

    fn admits(&self, n: &Node) -> bool {
        self.kinds.admits(n.kind) && (n.named || !self.named)
    }
}

pub enum Text {
    Any,
    Syms(Box<[u32]>),
    Prefix(Box<str>),
    Regex(regex::Regex),
}

/// Node flags a pattern can test with `#name`; any other `#name` is a tag.
pub const FLAGS: [&str; 4] = ["lifted", "labelled", "named", "synth"];

pub enum Pat {
    Node {
        kind: KindFilter,
        field: u16,
        text: Text,
        tags: Box<[(u32, bool)]>,
        flags: u8,
        unflags: u8,
        kids: Box<[Pat]>,
        optional: bool,
    },
    Cap {
        slot: u16,
        field: u16,
        test: Test,
        guard: Option<Box<Pat>>,
        optional: bool,
    },
    Var {
        slot: u16,
        field: u16,
        test: Test,
    },
    Not(Box<Pat>),
    /// `**/P` matches when a descendant matches P; `**!k|k/P` does not
    /// enter nodes of the listed kinds.
    Desc(Box<Pat>, KindFilter),
}

pub enum TplText<T> {
    None,
    Lit(u32),
    Expr { slot: u16, index: usize, expr: T },
}

pub enum Tpl<T> {
    Node {
        kind: u16,
        field: u16,
        text: TplText<T>,
        kids: Box<[Tpl<T>]>,
        optional: bool,
        labelled: bool,
    },
    Cap {
        slot: u16,
        field: u16,
        rekind: Option<u16>,
    },
    Var {
        slot: u16,
        test: Test,
        rekind: Option<u16>,
        leaf: bool,
    },
    Spread {
        slot: u16,
        kids: Box<[Tpl<T>]>,
    },
}

impl<T> Tpl<T> {
    pub fn texts(&self) -> Vec<&T> {
        fn walk<'a, T>(tpl: &'a Tpl<T>, out: &mut Vec<(usize, &'a T)>) {
            match tpl {
                Tpl::Node { text, kids, .. } => {
                    if let TplText::Expr { index, expr, .. } = text {
                        out.push((*index, expr));
                    }
                    kids.iter().for_each(|kid| walk(kid, out));
                }
                Tpl::Spread { kids, .. } => kids.iter().for_each(|kid| walk(kid, out)),
                Tpl::Cap { .. } | Tpl::Var { .. } => {}
            }
        }
        let mut out = Vec::new();
        walk(self, &mut out);
        out.sort_unstable_by_key(|&(index, _)| index);
        out.into_iter().map(|(_, expr)| expr).collect()
    }
}
