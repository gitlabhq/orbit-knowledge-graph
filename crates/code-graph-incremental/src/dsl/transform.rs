use std::borrow::Cow;
use std::str::FromStr;

use crate::error::LoadError;
use crate::intern::Lang;
use crate::tree::{EdgeKind, Node, Storage, Tree};

use super::types::{Ctx, EdgeCtx, EdgeDir, Pairs, Tf};

const MAX_REGEX_LOOP_PASSES: usize = 64;

fn child_sym<S: Storage<Node = Node>>(
    t: &Tree<S>,
    lang: &Lang,
    id: u32,
    pred: impl Fn(&Node) -> bool,
) -> u32 {
    t.cursor(id)
        .children()
        .find(|c| pred(t.storage.node(c.index())))
        .map_or(0, |c| t.sym_at(c.index(), lang))
}

fn nonempty(lang: &Lang, sym: u32) -> Option<u32> {
    if sym == 0 {
        return None;
    }
    if lang.syms.resolve(sym).is_empty() {
        return None;
    }
    Some(sym)
}

fn need_ctx<'a, 'l>(
    name: &str,
    ctx: &'a mut Option<&mut Ctx<'l>>,
) -> Result<&'a mut Ctx<'l>, LoadError> {
    match ctx {
        Some(c) => Ok(c),
        None => Err(LoadError(format!("{name} needs a rule context"))),
    }
}

fn parse_nested_tf(spec: &str, ctx: Option<&mut Ctx>) -> Result<Tf, LoadError> {
    if let Some((name, arg)) = spec.split_once(':') {
        Tf::from_func(name, &[arg], ctx)
    } else {
        Tf::from_func(spec, &[], ctx)
    }
}

impl Tf {
    pub(crate) fn from_func(
        name: &str,
        args: &[&str],
        mut ctx: Option<&mut Ctx>,
    ) -> Result<Tf, LoadError> {
        let arg = |i: usize| -> Result<&str, LoadError> {
            args.get(i).copied().ok_or_else(|| {
                LoadError(format!(
                    "{name} needs {} argument(s), got {}",
                    i + 1,
                    args.len()
                ))
            })
        };
        let s = |i: usize| -> Result<Box<str>, LoadError> { Ok(arg(i)?.into()) };
        let re = |i: usize| -> Result<regex::Regex, LoadError> { Ok(regex::Regex::new(arg(i)?)?) };

        Ok(match name {
            "id" => Tf::Id,
            "lowercase" => Tf::Lowercase,
            "stem" => Tf::Stem,
            "strip_prefix" | "strip" => Tf::Strip(s(0)?),
            "strip_suffix" => Tf::StripSuffix(s(0)?),
            "prepend" => Tf::Prepend(s(0)?),
            "split_last" => Tf::SplitLast(s(0)?),
            "split_first" => Tf::SplitFirst(s(0)?),
            "replace" => Tf::Replace(s(0)?, s(1)?),

            "collapse_index" => Tf::CollapseIndex(args.iter().map(|a| (*a).into()).collect()),
            "map" => Tf::Map(
                args.iter()
                    .filter_map(|a| a.split_once(':'))
                    .map(|(k, v)| (k.into(), v.into()))
                    .collect(),
            ),
            "field" => Tf::Field(need_ctx(name, &mut ctx)?.intern_field(arg(0)?)),
            "child_sym" => Tf::Child(need_ctx(name, &mut ctx)?.intern_kind(arg(0)?)),
            "parent_sym" => Tf::ParentSym(need_ctx(name, &mut ctx)?.intern_kind(arg(0)?)),
            "regex_replace" => Tf::Regex(re(0)?, s(1)?),
            "regex_first" => Tf::RegexFirst(re(0)?, s(1)?),
            "regex_loop" => Tf::RegexLoop(re(0)?, s(1)?),
            "regex_match" => Tf::RegexMatch(re(0)?),
            "ancestor_sym" => Tf::AncestorSym(need_ctx(name, &mut ctx)?.intern_kind(arg(0)?)),
            "ancestor_tag" => Tf::AncestorTag(need_ctx(name, &mut ctx)?.lang.syms.intern(arg(0)?)),
            "tag" => Tf::Tag(need_ctx(name, &mut ctx)?.lang.syms.intern(arg(0)?)),
            "has_incoming" | "has_outgoing" => {
                let ek = EdgeKind::from_str(arg(0)?)
                    .map_err(|_| LoadError(format!("unknown edge kind: {}", args[0])))?;
                let dir = if name == "has_incoming" {
                    EdgeDir::Incoming
                } else {
                    EdgeDir::Outgoing
                };
                Tf::HasEdge(ek, dir)
            }
            "concat" => {
                let a = parse_nested_tf(arg(1)?, ctx.as_deref_mut())?;
                let b = parse_nested_tf(arg(2)?, ctx)?;
                Tf::Concat(s(0)?, Box::new(a), Box::new(b))
            }
            "keep_last" => {
                let count = |i: usize| {
                    arg(i)?
                        .parse::<usize>()
                        .ok()
                        .filter(|&count| count > 0)
                        .ok_or_else(|| LoadError(format!("{name} needs a positive count")))
                };
                Tf::KeepLast(s(0)?, count(1)?, count(2)?)
            }
            "or" => {
                let a = parse_nested_tf(arg(0)?, ctx.as_deref_mut())?;
                let b = parse_nested_tf(arg(1)?, ctx)?;
                Tf::Or(Box::new(a), Box::new(b))
            }
            "default" => Tf::Default(s(0)?),
            "tree_path" => Tf::TreePath(s(0)?),
            "sibling_index" => Tf::SiblingIndex,
            "tree_node_id" => Tf::TreeNodeId,
            "kind" => Tf::KindName,
            "source" | "source_before" => {
                let bytes = arg(0)?
                    .parse::<usize>()
                    .map_err(|_| LoadError(format!("{name} needs a byte count")))?;
                match name {
                    "source" => Tf::Source(bytes),
                    _ => Tf::SourceBefore(bytes),
                }
            }
            "top_until" => Tf::TopUntil(re(0)?, Pairs::new(arg(1)?, args.get(2).copied())),
            "top_after" => Tf::TopAfter(re(0)?, Pairs::new(arg(1)?, args.get(2).copied())),
            "top_split" => Tf::TopSplit(s(0)?, Pairs::new(arg(1)?, args.get(2).copied())),
            _ => return Err(LoadError(format!("unknown transform: {name}"))),
        })
    }

    pub(crate) fn apply_to_str<'a>(&self, s: &'a str) -> Cow<'a, str> {
        debug_assert!(
            !self.is_node_tf(),
            "node transform used as string transform"
        );
        match self {
            Tf::Id => Cow::Borrowed(s),
            Tf::KeepLast(sep, segments, bytes) => {
                Cow::Borrowed(keep_last(s, sep, *segments, *bytes))
            }
            Tf::Strip(p) => match s.strip_prefix(&**p) {
                Some(rest) => Cow::Borrowed(rest),
                None => Cow::Borrowed(s),
            },
            Tf::StripSuffix(p) => match s.strip_suffix(&**p) {
                Some(rest) => Cow::Borrowed(rest),
                None => Cow::Borrowed(s),
            },
            Tf::StripLeading(ch) => {
                let trimmed = s.trim_start_matches(*ch);
                if trimmed.len() == s.len() {
                    Cow::Borrowed(s)
                } else {
                    Cow::Borrowed(trimmed)
                }
            }
            Tf::SplitLast(sep) => match s.rsplit_once(&**sep) {
                Some((_, r)) => Cow::Borrowed(r),
                None => Cow::Borrowed(s),
            },
            Tf::SplitFirst(sep) => match s.split_once(&**sep) {
                Some((l, _)) => Cow::Borrowed(l),
                None => Cow::Borrowed(s),
            },
            Tf::Replace(from, to) => {
                if s.contains(&**from) {
                    Cow::Owned(s.replace(&**from, to))
                } else {
                    Cow::Borrowed(s)
                }
            }
            Tf::Prepend(p) => Cow::Owned(format!("{p}{s}")),
            Tf::Lowercase => Cow::Owned(s.to_lowercase()),

            Tf::Pipeline(steps) => {
                let mut owned = s.to_string();
                for step in steps {
                    owned = step.apply_to_str(&owned).into_owned();
                }
                Cow::Owned(owned)
            }
            Tf::Default(value) => {
                if s.is_empty() {
                    Cow::Owned(value.to_string())
                } else {
                    Cow::Borrowed(s)
                }
            }
            Tf::Stem => {
                let p = std::path::Path::new(s);
                Cow::Owned(p.with_extension("").to_string_lossy().into_owned())
            }
            Tf::Map(entries) => {
                for (k, v) in entries {
                    if s == &**k {
                        return Cow::Owned(v.to_string());
                    }
                }
                Cow::Borrowed(s)
            }
            Tf::CollapseIndex(names) => {
                for name in names {
                    if let Some(prefix) = s.strip_suffix(&**name).and_then(|p| p.strip_suffix('/'))
                    {
                        return Cow::Owned(prefix.to_string());
                    }
                    if s == &**name {
                        return Cow::Owned(String::new());
                    }
                }
                Cow::Borrowed(s)
            }
            Tf::Regex(re, replacement) => re.replace_all(s, &**replacement),
            Tf::RegexFirst(re, replacement) => re.replace(s, &**replacement),
            Tf::RegexLoop(re, replacement) => {
                let mut cur = Cow::Borrowed(s);
                for _ in 0..MAX_REGEX_LOOP_PASSES {
                    let next = re.replace_all(&cur, &**replacement);
                    if let Cow::Borrowed(_) = next {
                        break;
                    }
                    cur = Cow::Owned(next.into_owned());
                }
                cur
            }
            Tf::RegexMatch(re) => Cow::Borrowed(if re.is_match(s) { "true" } else { "false" }),
            Tf::TopUntil(re, pairs) => {
                Cow::Borrowed(pairs.find(s, re).map_or(s, |(at, _)| &s[..at]))
            }
            Tf::TopAfter(re, pairs) => {
                Cow::Borrowed(pairs.find(s, re).map_or("", |(_, end)| &s[end..]))
            }
            Tf::TopSplit(sep, pairs) => Cow::Owned(pairs.split(s, sep)),
            _ => Cow::Borrowed(s),
        }
    }

    pub(crate) fn apply_sym<S: Storage<Node = Node>>(
        &self,
        t: &Tree<S>,
        lang: &Lang,
        id: S::Id,
        edge_ctx: Option<&EdgeCtx>,
    ) -> u32 {
        self.apply_at(t, lang, S::index(id), edge_ctx)
    }

    pub(crate) fn apply_at<S: Storage<Node = Node>>(
        &self,
        t: &Tree<S>,
        lang: &Lang,
        id: u32,
        edge_ctx: Option<&EdgeCtx>,
    ) -> u32 {
        match self {
            Tf::Id => t.sym_at(id, lang),
            Tf::Field(f) => {
                let f = *f;
                let found = child_sym(t, lang, id, |n| n.field == f);
                if found != 0 {
                    found
                } else {
                    t.sym_at(id, lang)
                }
            }
            Tf::Const(s) => lang.syms.intern(s),
            Tf::Child(k) => child_sym(t, lang, id, |n| n.kind == *k),
            Tf::FieldChild(f, k) => {
                let (f, k) = (*f, *k);
                t.cursor(id)
                    .children()
                    .find(|c| c.field() == f)
                    .map_or(0, |n| child_sym(t, lang, n.index(), |n| n.kind == k))
            }
            Tf::ParentSym(k) => {
                let k = *k;
                t.cursor(id)
                    .parent()
                    .map_or(0, |p| child_sym(t, lang, p.index(), |n| n.kind == k))
            }
            Tf::AncestorSym(k) => {
                let k = *k;
                let mut cur = id;
                loop {
                    let sym = child_sym(t, lang, cur, |n| n.kind == k);
                    if sym != 0 {
                        break sym;
                    }
                    match t.storage.parent(cur) {
                        Some(p) => cur = p,
                        None => break 0,
                    }
                }
            }
            Tf::AncestorTag(key) => {
                let key = *key;
                let mut cur = id;
                loop {
                    match t.storage.parent(cur) {
                        Some(p) => {
                            if let Some(v) = t.get_tag(p, key) {
                                break v;
                            }
                            cur = p;
                        }
                        None => break 0,
                    }
                }
            }
            Tf::Tag(key) => t.get_tag(id, *key).unwrap_or(0),
            Tf::LitSym(s) => *s,
            Tf::HasEdge(kind, dir) => {
                let raw = id;
                let found =
                    edge_ctx.is_some_and(|ctx| ctx.edges.has(*kind, *dir, ctx.tree_index, raw));
                lang.syms.intern(if found { "true" } else { "false" })
            }
            Tf::Concat(sep, a, b) => {
                let sa = nonempty(lang, a.apply_at(t, lang, id, edge_ctx));
                let sb = nonempty(lang, b.apply_at(t, lang, id, edge_ctx));
                match (sa, sb) {
                    (None, None) => 0,
                    (Some(a), None) => a,
                    (None, Some(b)) => b,
                    (Some(a), Some(b)) => lang.syms.intern(&format!(
                        "{}{sep}{}",
                        lang.syms.resolve(a),
                        lang.syms.resolve(b)
                    )),
                }
            }
            Tf::Or(a, b) => match nonempty(lang, a.apply_at(t, lang, id, edge_ctx)) {
                Some(sym) => sym,
                None => b.apply_at(t, lang, id, edge_ctx),
            },
            Tf::TreePath(sep) => {
                let mut segments: Vec<&str> = std::iter::once(t.cursor(id))
                    .chain(t.cursor(id).ancestors())
                    .filter(|a| a.parent().is_some())
                    .map(|a| lang.syms.resolve(t.sym_at(a.index(), lang)))
                    .collect();
                segments.reverse();
                lang.syms.intern(&segments.join(sep))
            }
            Tf::KindName => lang.syms.intern(lang.kind_name(t.cursor(id).kind())),
            Tf::TreeNodeId => lang.syms.intern(&id.to_string()),
            Tf::SiblingIndex => {
                let index = std::iter::successors(t.storage.previous_sibling(id), |&id| {
                    t.storage.previous_sibling(id)
                })
                .filter(|&id| t.cursor(id).named())
                .count();
                lang.syms.intern(&index.to_string())
            }
            Tf::Source(_) | Tf::SourceBefore(_) => {
                lang.syms.intern(self.source_text(t, id, edge_ctx))
            }
            Tf::Pipeline(steps) => {
                let mut s = t.text_at(id, lang).to_string();
                for step in steps {
                    if matches!(step, Tf::Source(_) | Tf::SourceBefore(_)) {
                        s = step.source_text(t, id, edge_ctx).to_string();
                    } else if step.is_node_tf() {
                        s = lang
                            .syms
                            .resolve(step.apply_at(t, lang, id, edge_ctx))
                            .to_string();
                    } else {
                        s = step.apply_to_str(&s).into_owned();
                    }
                }
                lang.syms.intern(&s)
            }
            _ => {
                let s = t.text_at(id, lang);
                if s.is_empty() {
                    return 0;
                }
                let result = self.apply_to_str(s);
                lang.syms.intern(&result)
            }
        }
    }
}

impl Tf {
    fn source_text<'t, S: Storage<Node = Node>>(
        &self,
        t: &'t Tree<S>,
        id: u32,
        edge_ctx: Option<&'t EdgeCtx>,
    ) -> &'t str {
        let text = edge_ctx.and_then(|ctx| ctx.source).unwrap_or(&t.source);
        let node = t.storage.node(id);
        let (start, end) = match self {
            Tf::Source(bytes) => (
                node.start as usize,
                (node.end as usize).min(node.start as usize + bytes),
            ),
            Tf::SourceBefore(bytes) => (
                (node.start as usize).saturating_sub(*bytes),
                node.start as usize,
            ),
            _ => return "",
        };
        let end = text.floor_char_boundary(end.min(text.len()));
        text.get(text.ceil_char_boundary(start.min(end))..end)
            .unwrap_or("")
    }
}

impl Pairs {
    fn new(pairs: &str, quotes: Option<&str>) -> Self {
        let pairs: Vec<char> = pairs.chars().collect();
        Self {
            open: pairs.iter().step_by(2).copied().collect(),
            close: pairs.iter().skip(1).step_by(2).copied().collect(),
            quotes: quotes
                .map_or_else(|| "\"'`".to_string(), |q| q.replace("\\x22", "\""))
                .chars()
                .collect(),
        }
    }

    fn top(&self, s: &str) -> Vec<bool> {
        let mut top = vec![false; s.len() + 1];
        let mut stack = Vec::new();
        let mut quote: Option<char> = None;
        let mut escaped = false;
        for (at, ch) in s.char_indices() {
            top[at] = stack.is_empty() && quote.is_none();
            match quote {
                Some(_) if escaped => escaped = false,
                Some(_) if ch == '\\' => escaped = true,
                Some(q) if ch == q => quote = None,
                Some(_) => {}
                None if self.quotes.contains(&ch) => quote = Some(ch),
                None => {
                    if let Some(i) = self.open.iter().position(|&o| o == ch) {
                        stack.push(i);
                    } else if let Some(i) = self.close.iter().position(|&c| c == ch)
                        && stack.last() == Some(&i)
                    {
                        stack.pop();
                    }
                }
            }
        }
        top[s.len()] = stack.is_empty() && quote.is_none();
        top
    }

    fn find(&self, s: &str, re: &regex::Regex) -> Option<(usize, usize)> {
        let top = self.top(s);
        let mut at = 0;
        while let Some(found) = re.find_at(s, at) {
            if top[found.start()] {
                return Some((found.start(), found.end()));
            }
            at = s.ceil_char_boundary(found.start() + 1);
            if at > s.len() {
                break;
            }
        }
        None
    }

    fn split(&self, s: &str, sep: &str) -> String {
        let top = self.top(s);
        let mut parts = Vec::new();
        let mut start = 0;
        for (at, _) in s.match_indices(sep) {
            if top[at] && at >= start {
                parts.push(s[start..at].trim());
                start = at + sep.len();
            }
        }
        parts.push(s[start..].trim());
        parts.retain(|part| !part.is_empty());
        parts.join("\u{1f}")
    }
}

fn keep_last<'a>(s: &'a str, sep: &str, segments: usize, bytes: usize) -> &'a str {
    if sep.is_empty() {
        return s;
    }
    let mut start = s
        .rmatch_indices(sep)
        .nth(segments - 1)
        .map_or(0, |(i, _)| i + sep.len());
    if s.len() - start > bytes {
        let min = s.ceil_char_boundary(s.len() - bytes);
        start = s[min..]
            .find(sep)
            .map(|i| min + i + sep.len())
            .filter(|&i| i < s.len())
            .unwrap_or(min);
    }
    &s[start..]
}

#[cfg(test)]
mod tests {
    use super::*;

    fn run(name: &str, args: &[&str], input: &str) -> String {
        Tf::from_func(name, args, None)
            .unwrap()
            .apply_to_str(input)
            .into_owned()
    }

    #[test]
    fn top_level_scans_skip_brackets_and_quotes() {
        let sig = "fn f<'a>(a: Vec<Box<dyn Fn() -> u8>>, b: &'a str) -> Map<K, V> { a }";
        let pairs = "()[]{}<>";
        assert_eq!(
            run("top_until", &["\\{", pairs, "\\x22"], sig),
            "fn f<'a>(a: Vec<Box<dyn Fn() -> u8>>, b: &'a str) -> Map<K, V> "
        );
        let params = run("top_after", &["\\(", pairs, "\\x22"], sig);
        let params = run("top_until", &["\\)", pairs, "\\x22"], &params);
        assert_eq!(
            run("top_split", &[",", pairs, "\\x22"], &params),
            "a: Vec<Box<dyn Fn() -> u8>>\u{1f}b: &'a str"
        );
        assert_eq!(
            run(
                "top_until",
                &[":", "()[]{}"],
                "def f(a: int = {1: 2}, b=\":\") -> T:\n  x"
            ),
            "def f(a: int = {1: 2}, b=\":\") -> T"
        );
        assert_eq!(
            run("top_after", &["=>", "()"], "(a) => (b) => c"),
            " (b) => c"
        );
        assert_eq!(run("top_after", &["\\{", "()"], "f(a)"), "");
        assert_eq!(run("top_split", &[",", "()"], " , "), "");
    }
}
