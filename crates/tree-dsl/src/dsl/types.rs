use std::collections::HashMap;

use indextree::NodeId;
use smallvec::SmallVec;

use crate::intern::Lang;
use crate::tree::{Edge, EdgeKind};

use super::parser::parse;

#[derive(Clone)]
pub enum Tf {
    Id,
    Strip(Box<str>),
    Field(u16),
    Const(&'static str),
    Child(u16),
    FieldChild(u16, u16),
    StripLeading(char),
    SplitLast(Box<str>),
    SplitFirst(Box<str>),
    Replace(Box<str>, Box<str>),
    StripSuffix(Box<str>),
    Prepend(Box<str>),
    ToRel(char),
    Lowercase,
    Pipeline(Vec<Tf>),
    ParentSym(u16),
    AncestorSym(u16),
    Concat(Box<str>, Box<Tf>, Box<Tf>),
    Stem,
    CollapseIndex(Vec<Box<str>>),
    Map(Vec<(Box<str>, Box<str>)>),
    HasEdge(EdgeKind, EdgeDir),
    AncestorTag(u32),
    Tag(u32),
    LitSym(u32),
    Regex(regex::Regex, Box<str>),
    RegexLoop(regex::Regex, Box<str>),
    RegexMatch(regex::Regex),
}

impl Tf {
    pub fn is_node_tf(&self) -> bool {
        matches!(
            self,
            Tf::Field(_)
                | Tf::Child(_)
                | Tf::FieldChild(_, _)
                | Tf::Const(_)
                | Tf::ParentSym(_)
                | Tf::AncestorSym(_)
                | Tf::AncestorTag(_)
                | Tf::Tag(_)
                | Tf::LitSym(_)
                | Tf::Concat(_, _, _)
                | Tf::HasEdge(_, _)
        )
    }
}

#[derive(Clone, Copy)]
pub enum EdgeDir {
    Incoming,
    Outgoing,
}

pub struct EdgeCtx<'a> {
    pub tree_index: u32,
    pub edges: &'a [Edge],
}

pub enum Text {
    Any,
    Lit(u32),
    From(u16, Tf),
}

pub enum Pat {
    Node {
        kind: u16,
        field: u16,
        text: Text,
        kids: Vec<Pat>,
        optional: bool,
    },
    Cap {
        slot: u16,
        field: u16,
        kind: Option<u16>,
        rekind: Option<u16>,
        guard: Option<Box<Pat>>,
        optional: bool,
        named_only: bool,
    },
    Var {
        slot: u16,
        field: u16,
        rekind: Option<u16>,
        leaf_only: bool,
        guard: Option<Box<Pat>>,
        named_only: bool,
    },
    Not(Box<Pat>),
    Desc(Box<Pat>),
    Spread {
        slot: u16,
        inject: Vec<Pat>,
    },
}

impl Pat {
    pub fn cap(slot: u16, field: u16) -> Self {
        Pat::Cap {
            slot,
            field,
            kind: None,
            rekind: None,
            guard: None,
            optional: false,
            named_only: false,
        }
    }

    pub fn with_kind(mut self, k: u16) -> Self {
        if let Pat::Cap { kind, .. } = &mut self {
            *kind = Some(k);
        }
        self
    }

    pub fn with_rekind(mut self, k: u16) -> Self {
        if let Pat::Cap { rekind, .. } = &mut self {
            *rekind = Some(k);
        }
        self
    }

    pub fn with_guard(mut self, g: Pat) -> Self {
        if let Pat::Cap { guard, .. } = &mut self {
            *guard = Some(Box::new(g));
        }
        self
    }

    pub fn with_optional(mut self) -> Self {
        match &mut self {
            Pat::Cap { optional, .. } | Pat::Node { optional, .. } => *optional = true,
            _ => {}
        }
        self
    }

    pub fn with_named_only(mut self) -> Self {
        if let Pat::Cap { named_only, .. } = &mut self {
            *named_only = true;
        }
        self
    }
}

pub struct TagEntry {
    pub key: u32,
    pub slot: u16,
    pub val: Tf,
}

pub enum Out {
    Replace(Pat, Option<Vec<TagEntry>>),
    Append(Vec<Pat>),
    Tag(Vec<TagEntry>),
}

pub struct Rewrite {
    pub pat: Pat,
    pub out: Out,
    pub nslots: usize,
    pub filters: Vec<Vec<u16>>,
    pub guards: Vec<(u16, u16, bool)>,
    pub slots: HashMap<Box<str>, u16>,
}

pub struct Ctx<'l> {
    pub lang: &'l Lang,
    pub(crate) slots: HashMap<Box<str>, u16>,
    pub(crate) filters: Vec<Vec<u16>>,
    is_template: bool,
}

impl<'l> Ctx<'l> {
    pub(crate) fn new(lang: &'l Lang) -> Self {
        Ctx {
            lang,
            slots: HashMap::new(),
            filters: Vec::new(),
            is_template: false,
        }
    }

    pub fn slot(&mut self, n: &str) -> u16 {
        if self.is_template {
            *self
                .slots
                .get(n)
                .unwrap_or_else(|| panic!("template references unknown slot: {n}"))
        } else {
            let next = self.slots.len() as u16;
            let s = *self.slots.entry(n.into()).or_insert(next);
            if self.filters.len() <= s as usize {
                self.filters.resize(s as usize + 1, Vec::new());
            }
            s
        }
    }

    pub fn intern_kind(&mut self, k: &str) -> u16 {
        self.lang.intern_kind(k)
    }

    pub fn intern_field(&mut self, f: &str) -> u16 {
        self.lang.intern_field(f)
    }

    pub(crate) fn apply_filter(&mut self, slot: u16, kinds: Vec<u16>) {
        if !self.is_template {
            self.filters[slot as usize] = kinds;
        }
    }

    fn as_template(&mut self) -> &mut Self {
        self.is_template = true;
        self
    }

    pub fn template(&mut self, src: &str) -> Pat {
        self.is_template = true;
        parse(self, src)
    }
}

pub(crate) type Cap = SmallVec<[NodeId; 1]>;

impl Rewrite {
    pub fn new(lang: &Lang, src: &str, out: impl FnOnce(&mut Ctx) -> Out) -> Rewrite {
        let mut ctx = Ctx::new(lang);
        ctx.slot("ROOT");
        let pat = parse(&mut ctx, src);
        let out = out(ctx.as_template());
        let nslots = ctx.slots.len();
        Rewrite {
            pat,
            out,
            nslots,
            slots: ctx.slots.clone(),
            filters: ctx.filters,
            guards: vec![],
        }
    }

    pub fn with_guards(mut self, guards: Vec<(u16, u16, bool)>) -> Self {
        self.guards = guards;
        self
    }
}
