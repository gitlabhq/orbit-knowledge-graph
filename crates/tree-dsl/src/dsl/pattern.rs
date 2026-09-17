use std::collections::HashMap;

use indextree::NodeId;

use crate::intern::Lang;
use crate::tree::{Node, Tree};

use super::parser::parse;

pub enum TfValue {
    Sym(u32),
    List(Vec<u32>),
}

impl TfValue {
    pub fn as_sym(&self) -> u32 {
        match self {
            TfValue::Sym(s) => *s,
            TfValue::List(_) => panic!("expected Sym, got List"),
        }
    }
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
    Ancestors(u16),
    Reverse,
    Join(Box<str>),
    Collapse(Vec<Box<str>>),
    FileStem,
    PathToMod(Box<str>),
    PrependValue(Box<Tf>),
    DefType,
    ImportType,
    FilePath,
    Fqn {
        separator: Box<str>,
        collapse: Vec<Box<str>>,
        skip_root: bool,
    },
}

impl Tf {
    pub(crate) fn from_func(name: &str, args: &[&str], ctx: Option<&mut Ctx>) -> Tf {
        match name {
            "replace" => {
                assert_eq!(args.len(), 2, "replace needs 2 args");
                Tf::Replace(args[0].into(), args[1].into())
            }
            "strip_prefix" | "strip" => Tf::Strip(args[0].into()),
            "strip_suffix" => Tf::StripSuffix(args[0].into()),
            "prepend" => Tf::Prepend(args[0].into()),
            "to_rel" => Tf::ToRel(args[0].chars().next().expect("to_rel arg")),
            "split_last" => Tf::SplitLast(args[0].into()),
            "split_first" => Tf::SplitFirst(args[0].into()),
            "lowercase" => Tf::Lowercase,
            "field" => Tf::Field(ctx.expect("field needs context").intern_field(args[0])),
            "ancestors" => {
                let c = ctx.expect("ancestors needs context");
                Tf::Ancestors(c.lang.intern_kind(args[0]))
            }
            "reverse" => Tf::Reverse,
            "join" => Tf::Join(args[0].into()),
            "collapse" => Tf::Collapse(args.iter().map(|a| (*a).into()).collect()),
            "file_stem" => Tf::FileStem,
            "path_to_mod" => Tf::PathToMod(args[0].into()),
            "def_type" => Tf::DefType,
            "import_type" => Tf::ImportType,
            "file_path" => Tf::FilePath,
            "fqn" => {
                let sep = args.first().copied().unwrap_or(".");
                let collapse: Vec<Box<str>> = args
                    .get(1..)
                    .unwrap_or(&[])
                    .iter()
                    .map(|a| (*a).into())
                    .collect();
                let skip = args.last().copied() == Some("skip_root");
                let collapse = if skip {
                    collapse[..collapse.len().saturating_sub(1)].to_vec()
                } else {
                    collapse
                };
                Tf::Fqn {
                    separator: sep.into(),
                    collapse,
                    skip_root: skip,
                }
            }
            _ => panic!("unknown transform: {name}"),
        }
    }

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
            Tf::Field(_)
            | Tf::Child(_)
            | Tf::FieldChild(_, _)
            | Tf::Const(_)
            | Tf::Ancestors(_)
            | Tf::Reverse
            | Tf::Join(_)
            | Tf::Collapse(_)
            | Tf::FileStem
            | Tf::PathToMod(_)
            | Tf::PrependValue(_)
            | Tf::DefType
            | Tf::ImportType
            | Tf::FilePath
            | Tf::Fqn { .. } => {
                unreachable!("tree-context transform used as string transform")
            }
        }
    }

    pub(crate) fn apply(&self, t: &Tree, lang: &Lang, id: indextree::NodeId) -> TfValue {
        match self {
            Tf::Id => TfValue::Sym(t.node(id).sym),
            Tf::Field(f) => {
                let fallback = t.node(id).sym;
                TfValue::Sym(
                    id.children(&t.arena)
                        .find(|&c| t.node(c).field == *f)
                        .map_or(fallback, |c| t.node(c).sym),
                )
            }
            Tf::Const(s) => TfValue::Sym(lang.syms.intern(s)),
            Tf::Child(k) => TfValue::Sym(
                id.children(&t.arena)
                    .find(|&c| t.node(c).kind == *k)
                    .map_or(0, |c| t.node(c).sym),
            ),
            Tf::FieldChild(f, k) => TfValue::Sym(
                id.children(&t.arena)
                    .find(|&c| t.node(c).field == *f)
                    .and_then(|n| n.children(&t.arena).find(|&c| t.node(c).kind == *k))
                    .map_or(0, |c| t.node(c).sym),
            ),
            Tf::Ancestors(kind) => {
                let mut syms = Vec::new();
                let mut cur = id;
                while let Some(parent) = cur.ancestors(&t.arena).nth(1) {
                    cur = parent;
                    let child_sym = parent
                        .children(&t.arena)
                        .find(|&c| t.node(c).kind == *kind)
                        .map_or(0, |c| t.node(c).sym);
                    if child_sym != 0 {
                        syms.push(child_sym);
                    }
                }
                TfValue::List(syms)
            }
            Tf::Reverse => TfValue::List(Vec::new()),
            Tf::Join(sep) => TfValue::Sym(lang.syms.intern(&**sep)),
            Tf::Collapse(_) => TfValue::Sym(0),
            Tf::FileStem => {
                let sym = t.node(id).sym;
                if sym == 0 {
                    return TfValue::Sym(0);
                }
                let path = lang.syms.resolve(sym);
                let stem = path.rsplit_once('.').map_or(path, |(base, _)| base);
                TfValue::Sym(lang.syms.intern(stem))
            }
            Tf::PathToMod(sep) => {
                let sym = t.node(id).sym;
                if sym == 0 {
                    return TfValue::Sym(0);
                }
                let s = lang.syms.resolve(sym).replace('/', &**sep);
                TfValue::Sym(lang.syms.intern(&s))
            }
            Tf::PrependValue(_inner) => TfValue::List(Vec::new()),
            Tf::DefType => {
                let cursor = t.cursor(Tree::to_raw(id));
                let dtk = crate::canonical::def_type_of(cursor);
                let display = match dtk {
                    Some(dk) => {
                        let flavor = cursor.children().find_map(|c| {
                            let ck = crate::canonical::Canonical::try_from_u16(c.kind())?;
                            (ck.is_flavor() && c.sym() != 0).then(|| lang.syms.resolve(c.sym()))
                        });
                        if let Some(f) = flavor {
                            f
                        } else if dk == crate::canonical::Canonical::Function {
                            if let Some(parent) = cursor.parent() {
                                let pk = crate::canonical::def_type_of(parent);
                                if pk == Some(crate::canonical::Canonical::ImplBlock)
                                    || pk == Some(crate::canonical::Canonical::Trait)
                                {
                                    "AssociatedFunction"
                                } else {
                                    dk.display_name()
                                }
                            } else {
                                dk.display_name()
                            }
                        } else {
                            dk.display_name()
                        }
                    }
                    None => "",
                };
                TfValue::Sym(lang.syms.intern(display))
            }
            Tf::ImportType => {
                use crate::canonical::Canonical as C;
                let cursor = t.cursor(Tree::to_raw(id));
                let is_cjs = cursor.children().any(|c| c.is(C::CjsRequire));
                let names: Vec<_> = cursor.names().collect();
                let source_sym = cursor.child_sym(C::Source).unwrap_or(0);
                let source_eq_name = names.len() == 1 && names[0].sym() == source_sym;
                let label = if names.is_empty() {
                    "Import"
                } else {
                    let n = names[0];
                    let ns = lang.syms.resolve(n.sym());
                    let als = n.child_sym(C::Alias).unwrap_or(0);
                    if is_cjs {
                        "CjsRequire"
                    } else if ns == "*" && als != 0 {
                        "NamespaceImport"
                    } else if ns == "*" {
                        "WildcardImport"
                    } else if ns == "default" {
                        "DefaultImport"
                    } else if als != 0 && als != n.sym() {
                        "AliasedImport"
                    } else if source_eq_name {
                        "Import"
                    } else {
                        "NamedImport"
                    }
                };
                TfValue::Sym(lang.syms.intern(label))
            }
            Tf::FilePath => {
                let root_sym = t.root().sym();
                TfValue::Sym(root_sym)
            }
            Tf::Fqn {
                separator,
                collapse,
                skip_root,
            } => {
                let cursor = t.cursor(Tree::to_raw(id));
                let _defname_kind: u16 = crate::canonical::Canonical::DefName.into();
                let mut parts = Vec::new();
                let is_property = crate::canonical::def_type_of(cursor)
                    == Some(crate::canonical::Canonical::Property);
                for a in std::iter::once(cursor).chain(cursor.ancestors()) {
                    if is_property
                        && a.index() != cursor.index()
                        && crate::canonical::def_type_of(a).is_some_and(|k| {
                            matches!(
                                k,
                                crate::canonical::Canonical::Function
                                    | crate::canonical::Canonical::Method
                                    | crate::canonical::Canonical::Lambda
                            )
                        })
                    {
                        continue;
                    }
                    if crate::canonical::has_def_type(a) {
                        if let Some(ns) = a.child_sym(crate::canonical::Canonical::DefName) {
                            let name = lang.syms.resolve(ns).to_string();
                            if !name.is_empty() {
                                parts.push(name);
                            }
                        }
                    } else if a.parent().is_none() && !skip_root {
                        let path = lang.syms.resolve(a.sym()).to_string();
                        let stem = path.rsplit_once('.').map_or(path.as_str(), |(b, _)| b);
                        let collapsed = collapse_path(stem, collapse);
                        let modname = collapsed.replace('/', &**separator);
                        if !modname.is_empty() {
                            parts.push(modname);
                        }
                    }
                }
                parts.reverse();
                let fqn = parts.join(&**separator);
                TfValue::Sym(lang.syms.intern(&fqn))
            }
            Tf::Pipeline(steps) => {
                let mut val = steps[0].apply(t, lang, id);
                for step in &steps[1..] {
                    val = match (step, val) {
                        (Tf::Reverse, TfValue::List(mut v)) => {
                            v.reverse();
                            TfValue::List(v)
                        }
                        (Tf::Join(sep), TfValue::List(v)) => {
                            let joined: String = v
                                .iter()
                                .map(|&s| lang.syms.resolve(s).to_string())
                                .collect::<Vec<_>>()
                                .join(&**sep);
                            TfValue::Sym(lang.syms.intern(&joined))
                        }
                        (Tf::Collapse(patterns), TfValue::Sym(s)) => {
                            if s == 0 {
                                TfValue::Sym(0)
                            } else {
                                let text = lang.syms.resolve(s).to_string();
                                let collapsed = collapse_path(&text, patterns);
                                TfValue::Sym(lang.syms.intern(&collapsed))
                            }
                        }
                        (Tf::PrependValue(inner), TfValue::List(mut v)) => {
                            let prepended = inner.apply(t, lang, id);
                            if let TfValue::Sym(ps) = prepended {
                                if ps != 0 {
                                    v.insert(0, ps);
                                }
                            }
                            TfValue::List(v)
                        }
                        (_, TfValue::Sym(s)) => {
                            if s == 0 {
                                TfValue::Sym(0)
                            } else {
                                let text = lang.syms.resolve(s).to_string();
                                let result = step.apply_to_str(&text);
                                TfValue::Sym(lang.syms.intern(&result))
                            }
                        }
                        (_, v) => step.apply_pipeline_value(t, lang, id, v),
                    };
                }
                val
            }
            _ => {
                let sym = t.node(id).sym;
                if sym == 0 {
                    return TfValue::Sym(0);
                }
                let s = lang.syms.resolve(sym).to_string();
                let result = self.apply_to_str(&s);
                TfValue::Sym(lang.syms.intern(&result))
            }
        }
    }

    fn apply_pipeline_value(
        &self,
        t: &Tree,
        lang: &Lang,
        id: indextree::NodeId,
        val: TfValue,
    ) -> TfValue {
        match (self, val) {
            (Tf::Reverse, TfValue::List(mut v)) => {
                v.reverse();
                TfValue::List(v)
            }
            (Tf::Join(sep), TfValue::List(v)) => {
                let joined: String = v
                    .iter()
                    .map(|&s| lang.syms.resolve(s).to_string())
                    .collect::<Vec<_>>()
                    .join(&**sep);
                TfValue::Sym(lang.syms.intern(&joined))
            }
            (Tf::PrependValue(inner), TfValue::List(mut v)) => {
                let prepended = inner.apply(t, lang, id);
                if let TfValue::Sym(ps) = prepended {
                    if ps != 0 {
                        v.insert(0, ps);
                    }
                }
                TfValue::List(v)
            }
            _ => self.apply(t, lang, id),
        }
    }
}

fn collapse_path(path: &str, patterns: &[Box<str>]) -> String {
    for pat in patterns {
        if path.ends_with(&format!("/{pat}")) {
            return path[..path.len() - pat.len() - 1].to_string();
        }
        if path == &**pat {
            return String::new();
        }
    }
    path.to_string()
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
    Append(Vec<Pat>),
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

pub(crate) fn matches(t: &Tree, id: NodeId, p: &Pat, caps: &mut [Cap]) -> bool {
    let n = t.node(id);
    let field_ok = |f: u16| f == 0 || f == n.field;
    match p {
        Pat::Var { .. } | Pat::Spread { .. } => false,
        Pat::Not(_) | Pat::Desc(_) => false,
        Pat::Cap {
            slot,
            field,
            kind,
            guard,
            named_only,
            ..
        } => {
            if !field_ok(*field) || kind.is_some_and(|k| k != n.kind) {
                return false;
            }
            if *named_only && !n.named {
                return false;
            }
            if let Some(g) = guard
                && !matches(t, id, g, caps)
            {
                return false;
            }
            caps[*slot as usize] = Cap::One(id);
            true
        }
        Pat::Node {
            kind,
            field,
            text,
            kids,
            ..
        } => {
            if n.kind != *kind || !field_ok(*field) {
                return false;
            }
            if let Text::Lit(s) = text
                && n.sym != *s
            {
                return false;
            }
            let children: Vec<NodeId> = id.children(&t.arena).collect();
            let mut ci = 0;
            for (k, kid) in kids.iter().enumerate() {
                match kid {
                    Pat::Var { slot, guard, .. } => {
                        let start = ci;
                        while ci < children.len()
                            && !kids
                                .get(k + 1)
                                .is_some_and(|nx| matches(t, children[ci], nx, caps))
                        {
                            ci += 1;
                        }
                        let range: Vec<NodeId> = children[start..ci].to_vec();
                        if let Some(g) = guard {
                            let any_match = range.iter().any(|&e| matches(t, e, g, caps));
                            caps[*slot as usize] = if any_match {
                                Cap::Many(range)
                            } else {
                                Cap::Empty
                            };
                        } else {
                            caps[*slot as usize] = Cap::Many(range);
                        }
                    }
                    Pat::Not(inner) => {
                        for &child in &children[ci..] {
                            if matches(t, child, inner, caps) {
                                return false;
                            }
                        }
                    }
                    Pat::Desc(inner) => {
                        let mut found = false;
                        for desc in id.descendants(&t.arena).skip(1) {
                            if matches(t, desc, inner, caps) {
                                found = true;
                                break;
                            }
                        }
                        if !found {
                            return false;
                        }
                    }
                    _ => {
                        let saved = ci;
                        while ci < children.len() && !matches(t, children[ci], kid, caps) {
                            ci += 1;
                        }
                        if ci >= children.len() {
                            if is_optional(kid) {
                                mark_empty(kid, caps);
                                ci = saved;
                                continue;
                            }
                            return false;
                        }
                        ci += 1;
                    }
                }
            }
            true
        }
    }
}

fn is_optional(p: &Pat) -> bool {
    std::matches!(
        p,
        Pat::Cap { optional: true, .. } | Pat::Node { optional: true, .. }
    )
}

fn mark_empty(p: &Pat, caps: &mut [Cap]) {
    if let Pat::Cap { slot, .. } = p {
        caps[*slot as usize] = Cap::Empty;
    }
}

pub(crate) fn materialize(
    t: &Tree,
    lang: &Lang,
    p: &Pat,
    caps: &[Cap],
    filters: &[Vec<u16>],
    parent: NodeId,
    span: (u32, u32),
    out: &mut Tree,
) {
    match p {
        Pat::Cap {
            slot,
            field,
            rekind,
            ..
        } => {
            let Some(src) = caps[*slot as usize].one() else {
                return;
            };
            let copy = out.clone_subtree_from(t, src, Some(parent));
            if *field != 0 {
                out.arena[copy].get_mut().field = *field;
            }
            if let Some(k) = rekind {
                out.arena[copy].get_mut().kind = *k;
                out.arena[copy].get_mut().field = 0;
            }
        }
        Pat::Var {
            slot,
            rekind,
            leaf_only,
            guard,
            named_only,
            ..
        } => {
            if caps[*slot as usize].is_empty() {
                return;
            }
            let elems = caps[*slot as usize].many();
            let filter = filters
                .get(*slot as usize)
                .map(|f| f.as_slice())
                .unwrap_or(&[]);
            let mut scratch: Vec<Cap> = vec![Cap::Empty; caps.len()];
            for &e in elems {
                let en = t.node(e);
                if !filter.is_empty() && !filter.contains(&en.kind) {
                    continue;
                }
                if *named_only && !en.named {
                    continue;
                }
                if let Some(g) = guard
                    && !matches(t, e, g, &mut scratch)
                {
                    continue;
                }
                if *leaf_only {
                    if en.sym == 0 {
                        continue;
                    }
                    out.append(
                        parent,
                        Node {
                            kind: rekind.unwrap_or(en.kind),
                            field: 0,
                            named: true,
                            synth: true,
                            sym: en.sym,
                            start: en.start,
                            end: en.end,
                            start_row: en.start_row,
                            start_col: en.start_col,
                            end_row: en.end_row,
                            end_col: en.end_col,
                        },
                    );
                } else {
                    let copy = out.clone_subtree_from(t, e, Some(parent));
                    if let Some(k) = rekind {
                        out.arena[copy].get_mut().kind = *k;
                    }
                }
            }
        }
        Pat::Node {
            kind,
            field,
            text,
            kids,
            optional,
        } => {
            if *optional {
                let text_empty =
                    std::matches!(text, Text::From(slot, _) if caps[*slot as usize].is_empty());
                let kids_empty = kids.is_empty()
                    || kids.iter().all(|k| match k {
                        Pat::Cap { slot, .. } | Pat::Var { slot, .. } => {
                            caps[*slot as usize].is_empty()
                        }
                        _ => false,
                    });
                if text_empty || (kids_empty && std::matches!(text, Text::Any)) {
                    return;
                }
            }
            let sym = match text {
                Text::Any => 0,
                Text::Lit(s) => *s,
                Text::From(slot, tf) => {
                    let Some(src) = caps[*slot as usize].one() else {
                        return;
                    };
                    tf.apply(t, lang, src).as_sym()
                }
            };
            let pos_src = match text {
                Text::From(slot, _) if !caps[*slot as usize].is_empty() => {
                    caps[*slot as usize].one().unwrap()
                }
                _ => caps[0].one().unwrap_or(t.root),
            };
            let src = t.node(pos_src);
            let at = out.append(
                parent,
                Node {
                    kind: *kind,
                    field: *field,
                    named: true,
                    synth: true,
                    sym,
                    start: span.0,
                    end: span.1,
                    start_row: src.start_row,
                    start_col: src.start_col,
                    end_row: src.end_row,
                    end_col: src.end_col,
                },
            );
            for k in kids {
                materialize(t, lang, k, caps, filters, at, span, out);
            }
        }
        Pat::Spread { slot, inject } => {
            let Some(src) = caps[*slot as usize].one() else {
                return;
            };
            let copy = out.clone_subtree_from(t, src, Some(parent));
            for kid in inject {
                materialize(t, lang, kid, caps, filters, copy, span, out);
            }
        }
        Pat::Not(_) | Pat::Desc(_) => {}
    }
}

pub fn apply_rewrites(t: &mut Tree, lang: &Lang, rules: &[Rewrite]) {
    let max_slots = rules.iter().map(|r| r.nslots).max().unwrap_or(1);
    let mut caps: Vec<Cap> = (0..max_slots).map(|_| Cap::Empty).collect();

    let root_kinds: Vec<u16> = rules
        .iter()
        .map(|r| match &r.pat {
            Pat::Node { kind, .. } => *kind,
            _ => 0,
        })
        .collect();

    let candidates = t.postorder();

    for target in candidates {
        if target.is_removed(&t.arena) {
            continue;
        }

        let target_kind = t.node(target).kind;
        for (ri, r) in rules.iter().enumerate() {
            if root_kinds[ri] != 0 && root_kinds[ri] != target_kind {
                continue;
            }
            for c in &mut caps[..r.nslots] {
                *c = Cap::Empty;
            }
            if !matches(t, target, &r.pat, &mut caps) {
                continue;
            }
            caps[0] = Cap::One(target);
            if !r.guards.is_empty() {
                let mut guard_ok = true;
                for &(a, b, eq) in &r.guards {
                    let sym_a = caps[a as usize].one().map_or(0, |id| t.node(id).sym);
                    let sym_b = caps[b as usize].one().map_or(0, |id| t.node(id).sym);
                    if (sym_a == sym_b) != eq {
                        guard_ok = false;
                        break;
                    }
                }
                if !guard_ok {
                    continue;
                }
            }

            let root_node = t.node(target);
            let span = (root_node.start, root_node.end);

            match &r.out {
                Out::Replace(tpl) => {
                    let mut staging = Tree::new(Node::default());
                    materialize(
                        t,
                        lang,
                        tpl,
                        &caps,
                        &r.filters,
                        staging.root,
                        span,
                        &mut staging,
                    );

                    let replacement_roots: Vec<NodeId> =
                        staging.root.children(&staging.arena).collect();
                    let mut moved: Vec<NodeId> = Vec::with_capacity(replacement_roots.len());
                    for child in replacement_roots {
                        let imported = t.clone_subtree_from(&staging, child, None);
                        moved.push(imported);
                    }

                    t.replace(target, moved);
                }
                Out::Append(templates) => {
                    for tpl in templates {
                        let mut staging = Tree::new(Node::default());
                        materialize(
                            t,
                            lang,
                            tpl,
                            &caps,
                            &r.filters,
                            staging.root,
                            span,
                            &mut staging,
                        );
                        for child in staging.root.children(&staging.arena).collect::<Vec<_>>() {
                            let imported = t.clone_subtree_from(&staging, child, None);
                            target.append(imported, &mut t.arena);
                        }
                    }
                }
            }
            break;
        }
    }
}

impl Tree {
    pub(crate) fn clone_subtree_from(
        &mut self,
        source: &Tree,
        id: NodeId,
        parent: Option<NodeId>,
    ) -> NodeId {
        let children: Vec<NodeId> = id.children(&source.arena).collect();
        let copy = self.arena.new_node(*source.node(id));
        if let Some(parent) = parent {
            parent.append(copy, &mut self.arena);
        }
        for child in children {
            self.clone_subtree_from(source, child, Some(copy));
        }
        copy
    }
}
