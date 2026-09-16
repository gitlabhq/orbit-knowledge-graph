use std::collections::HashMap;
use std::marker::PhantomData;

use crate::lang::Lang;

pub struct Match;
pub struct Template;

pub trait Phase {
    fn resolve_slot(
        slots: &mut HashMap<Box<str>, u16>,
        filters: &mut Vec<Vec<u16>>,
        n: &str,
    ) -> u16;
    fn apply_filter(filters: &mut [Vec<u16>], slot: u16, kinds: Vec<u16>);
}

impl Phase for Match {
    fn resolve_slot(
        slots: &mut HashMap<Box<str>, u16>,
        filters: &mut Vec<Vec<u16>>,
        n: &str,
    ) -> u16 {
        let next = slots.len() as u16;
        let s = *slots.entry(n.into()).or_insert(next);
        if filters.len() <= s as usize {
            filters.resize(s as usize + 1, Vec::new());
        }
        s
    }
    fn apply_filter(filters: &mut [Vec<u16>], slot: u16, kinds: Vec<u16>) {
        filters[slot as usize] = kinds;
    }
}

impl Phase for Template {
    fn resolve_slot(
        slots: &mut HashMap<Box<str>, u16>,
        _filters: &mut Vec<Vec<u16>>,
        n: &str,
    ) -> u16 {
        *slots
            .get(n)
            .unwrap_or_else(|| panic!("template references unknown slot: {n}"))
    }
    fn apply_filter(_filters: &mut [Vec<u16>], _slot: u16, _kinds: Vec<u16>) {}
}

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
}

impl Tf {
    pub(crate) fn apply_to_str(&self, s: &str) -> String {
        match self {
            Tf::Id => s.to_string(),
            Tf::Strip(p) => s.strip_prefix(&**p).unwrap_or(s).to_string(),
            Tf::StripSuffix(p) => s.strip_suffix(&**p).unwrap_or(s).to_string(),
            Tf::StripLeading(ch) => s.trim_start_matches(*ch).to_string(),
            Tf::SplitLast(sep) => s.rsplit_once(&**sep).map_or(s, |(_, r)| r).to_string(),
            Tf::SplitFirst(sep) => s.split_once(&**sep).map_or(s, |(l, _)| l).to_string(),
            Tf::Replace(from, to) => s.replace(&**from, to),
            Tf::Prepend(p) => format!("{p}{s}"),
            Tf::Lowercase => s.to_lowercase(),
            Tf::ToRel(ch) => {
                let count = s.chars().take_while(|c| c == ch).count();
                let rest = s[count..].replace(*ch, "/");
                match count {
                    0 => rest,
                    1 => format!("./{rest}"),
                    n => {
                        let prefix = "../".repeat(n - 1);
                        format!("{prefix}{rest}")
                    }
                }
            }
            Tf::Pipeline(steps) => {
                let mut result = s.to_string();
                for step in steps {
                    result = step.apply_to_str(&result);
                }
                result
            }
            Tf::Field(_) | Tf::Child(_) | Tf::FieldChild(_, _) | Tf::Const(_) => {
                unreachable!("tree-context transform used as string transform")
            }
        }
    }

    pub(crate) fn apply_sym(
        &self,
        t: &crate::tree::Tree,
        lang: &mut Lang,
        id: indextree::NodeId,
    ) -> u32 {
        match self {
            Tf::Id => t.node(id).sym,
            Tf::Field(f) => {
                let fallback = t.node(id).sym;
                id.children(&t.arena)
                    .find(|&c| t.node(c).field == *f)
                    .map_or(fallback, |c| t.node(c).sym)
            }
            Tf::Const(s) => lang.syms.intern(s),
            Tf::Child(k) => id
                .children(&t.arena)
                .find(|&c| t.node(c).kind == *k)
                .map_or(0, |c| t.node(c).sym),
            Tf::FieldChild(f, k) => id
                .children(&t.arena)
                .find(|&c| t.node(c).field == *f)
                .and_then(|n| n.children(&t.arena).find(|&c| t.node(c).kind == *k))
                .map_or(0, |c| t.node(c).sym),
            _ => {
                let sym = t.node(id).sym;
                if sym == 0 {
                    return 0;
                }
                let s = lang.syms.resolve(sym).to_string();
                let result = self.apply_to_str(&s);
                lang.syms.intern(&result)
            }
        }
    }
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

pub enum Out {
    Replace(Pat),
}

pub struct Rewrite {
    pub pat: Pat,
    pub out: Out,
    pub nslots: usize,
    pub filters: Vec<Vec<u16>>,
    pub guards: Vec<(u16, u16, bool)>,
    pub slots: HashMap<Box<str>, u16>,
}

pub struct Ctx<'l, P: Phase> {
    pub lang: &'l mut Lang,
    pub(crate) slots: HashMap<Box<str>, u16>,
    pub(crate) filters: Vec<Vec<u16>>,
    pub(crate) _phase: PhantomData<P>,
}

impl<'l, P: Phase> Ctx<'l, P> {
    pub fn slot(&mut self, n: &str) -> u16 {
        P::resolve_slot(&mut self.slots, &mut self.filters, n)
    }

    pub fn intern_kind(&mut self, k: &str) -> u16 {
        self.lang.intern_kind(k)
    }

    pub fn intern_field(&mut self, f: &str) -> u16 {
        self.lang.intern_field(f)
    }

    pub(crate) fn apply_filter(&mut self, slot: u16, kinds: Vec<u16>) {
        P::apply_filter(&mut self.filters, slot, kinds);
    }
}

impl<'l> Ctx<'l, Match> {
    pub(crate) fn new(lang: &'l mut Lang) -> Self {
        Ctx {
            lang,
            slots: HashMap::new(),
            filters: Vec::new(),
            _phase: PhantomData,
        }
    }

    pub(crate) fn freeze(self) -> Ctx<'l, Template> {
        Ctx {
            lang: self.lang,
            slots: self.slots,
            filters: self.filters,
            _phase: PhantomData,
        }
    }
}

impl Ctx<'_, Template> {
    pub fn template(&mut self, src: &str) -> Pat {
        super::pattern_parse::parse(self, src)
    }
}

#[derive(Clone, Debug)]
pub(crate) enum Cap {
    Empty,
    One(indextree::NodeId),
    Many(Vec<indextree::NodeId>),
}

impl Cap {
    pub(crate) fn is_empty(&self) -> bool {
        matches!(self, Cap::Empty)
    }
    pub(crate) fn one(&self) -> Option<indextree::NodeId> {
        match self {
            Cap::One(id) => Some(*id),
            Cap::Many(ids) => ids.first().copied(),
            Cap::Empty => None,
        }
    }
    pub(crate) fn many(&self) -> &[indextree::NodeId] {
        match self {
            Cap::Many(ids) => ids,
            Cap::One(id) => std::slice::from_ref(id),
            Cap::Empty => &[],
        }
    }
}

impl Rewrite {
    pub fn new(lang: &mut Lang, src: &str, out: impl FnOnce(&mut Ctx<Template>) -> Out) -> Rewrite {
        let mut mc = Ctx::<Match>::new(lang);
        mc.slot("ROOT");
        let pat = super::pattern_parse::parse(&mut mc, src);
        let mut tc = mc.freeze();
        let out = out(&mut tc);
        let nslots = tc.slots.len();
        Rewrite {
            pat,
            out,
            nslots,
            slots: tc.slots.clone(),
            filters: tc.filters,
            guards: vec![],
        }
    }

    pub fn with_guards(mut self, guards: Vec<(u16, u16, bool)>) -> Self {
        self.guards = guards;
        self
    }
}
