//! Tree-sitter specific implementations.

pub mod traversal;

use crate::Language;
use crate::node::{KindId, Position, Root};
use crate::source::{Doc, SgNode};
use std::borrow::Cow;
use std::num::NonZero;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::time::Instant;
use thiserror::Error;

pub use traversal::TsPre;
pub use tree_sitter::Language as TSLanguage;
use tree_sitter::{Node, Parser, Tree};
pub use tree_sitter::{Point as TSPoint, Range as TSRange};

/// Represents tree-sitter related error
#[derive(Debug, Error)]
pub enum TSParseError {
    #[error("incompatible `Language` is assigned to a `Parser`.")]
    Language(#[from] tree_sitter::LanguageError),
    #[error("general error when tree-sitter fails to parse.")]
    TreeUnavailable,
}

#[inline]
fn parse_lang(
    parse_fn: impl Fn(&mut Parser) -> Option<Tree>,
    ts_lang: TSLanguage,
) -> Result<Tree, TSParseError> {
    let mut parser = Parser::new();
    parser.set_language(&ts_lang)?;
    if let Some(tree) = parse_fn(&mut parser) {
        Ok(tree)
    } else {
        Err(TSParseError::TreeUnavailable)
    }
}

/// A document backed by a String with a tree-sitter parse tree.
#[derive(Clone)]
pub struct StrDoc<L: LanguageExt> {
    pub src: String,
    pub lang: L,
    pub tree: Tree,
    // Node-kind names by `kind_id`. `tree_sitter::Node::kind()` reruns
    // `str::from_utf8` on the C kind string every call; this lets the walk
    // index by id in O(1) instead.
    kind_names: std::sync::Arc<[&'static str]>,
}

/// Default stall limit for the progress callback. 100K iterations at the same
/// byte offset is clearly pathological -- normal parsing always advances.
const DEFAULT_MAX_STALL: u64 = 100_000;

/// Sample the wall clock once every this many progress callbacks. `Instant::now`
/// is cheap but the callback is hot; a 2s budget tolerates this much slack.
const DEADLINE_SAMPLE_INTERVAL: u64 = 64;

/// Cooperative limits enforced from the parser's progress callback, the only
/// hook into tree-sitter's otherwise-uninterruptible C parse.
#[derive(Clone)]
pub struct ParseGuard {
    /// Abort if the parser stalls at one byte offset this many iterations.
    pub max_stall: u64,
    /// Abort once this instant passes (per-file wall-clock budget).
    pub deadline: Option<Instant>,
    /// Abort when an external watchdog sets this flag (the indexer sentinel).
    pub cancel: Option<Arc<AtomicBool>>,
}

impl Default for ParseGuard {
    fn default() -> Self {
        Self {
            max_stall: DEFAULT_MAX_STALL,
            deadline: None,
            cancel: None,
        }
    }
}

impl ParseGuard {
    pub fn with_deadline(mut self, deadline: Instant) -> Self {
        self.deadline = Some(deadline);
        self
    }

    pub fn with_cancel(mut self, cancel: Arc<AtomicBool>) -> Self {
        self.cancel = Some(cancel);
        self
    }
}

impl<L: LanguageExt> StrDoc<L> {
    pub fn try_new(src: &str, lang: L) -> Result<Self, String> {
        Self::try_new_with_guard(src, lang, &ParseGuard::default())
    }

    /// Parse source, aborting if the [`ParseGuard`] trips: a stall, the
    /// wall-clock deadline, or an externally-set cancel flag. An aborted parse
    /// returns [`TSParseError::TreeUnavailable`] (surfaced here as `Err`).
    pub fn try_new_with_guard(src: &str, lang: L, guard: &ParseGuard) -> Result<Self, String> {
        let src = src.to_string();
        let kind_names = lang.kind_names();
        let ts_lang = lang.get_ts_language();
        let tree = parse_lang(
            |p| {
                use std::ops::ControlFlow;
                use std::sync::atomic::{AtomicU64, Ordering};

                let max_stall = guard.max_stall;
                let deadline = guard.deadline;
                let cancel = guard.cancel.clone();
                let stall_count = AtomicU64::new(0);
                let last_offset = AtomicU64::new(u64::MAX);
                let ticks = AtomicU64::new(0);

                let mut progress = |state: &tree_sitter::ParseState| {
                    // Watchdog cancellation (e.g. the per-file sentinel).
                    if let Some(flag) = &cancel
                        && flag.load(Ordering::Relaxed)
                    {
                        tracing::debug!("tree-sitter parse aborted: cancelled by watchdog");
                        return ControlFlow::Break(());
                    }
                    // Wall-clock deadline, sampled to keep the callback cheap.
                    if let Some(deadline) = deadline
                        && ticks
                            .fetch_add(1, Ordering::Relaxed)
                            .is_multiple_of(DEADLINE_SAMPLE_INTERVAL)
                        && Instant::now() >= deadline
                    {
                        tracing::warn!("tree-sitter parse aborted: per-file deadline exceeded");
                        return ControlFlow::Break(());
                    }
                    // Stall detection: no forward progress for too long.
                    let offset = state.current_byte_offset() as u64;
                    if offset == last_offset.load(Ordering::Relaxed) {
                        if stall_count.fetch_add(1, Ordering::Relaxed) >= max_stall {
                            tracing::warn!(
                                "tree-sitter parse aborted: stalled at byte offset {offset} \
                                 (>{max_stall} iterations without progress)"
                            );
                            return ControlFlow::Break(());
                        }
                    } else {
                        last_offset.store(offset, Ordering::Relaxed);
                        stall_count.store(0, Ordering::Relaxed);
                    }
                    ControlFlow::Continue(())
                };

                let mut opts =
                    tree_sitter::ParseOptions::default().progress_callback(&mut progress);
                let mut read = |offset: usize, _: tree_sitter::Point| &src.as_bytes()[offset..];
                p.parse_with_options(&mut read, None, Some(opts.reborrow()))
            },
            ts_lang,
        )
        .map_err(|e| e.to_string())?;
        Ok(Self {
            src,
            lang,
            tree,
            kind_names,
        })
    }

    pub fn new(src: &str, lang: L) -> Self {
        Self::try_new(src, lang).expect("Parser tree error")
    }
}

impl<L: LanguageExt> Doc for StrDoc<L> {
    type Source = String;
    type Lang = L;
    type Node<'r> = Node<'r>;

    fn get_lang(&self) -> &Self::Lang {
        &self.lang
    }

    fn get_source(&self) -> &Self::Source {
        &self.src
    }

    fn root_node(&self) -> Node<'_> {
        self.tree.root_node()
    }

    fn get_node_text<'a>(&'a self, node: &Self::Node<'a>) -> Cow<'a, str> {
        // `src` is valid UTF-8 and node offsets land on char boundaries, so
        // slice it (O(1)) instead of revalidating bytes with `str::from_utf8`.
        Cow::Borrowed(&self.src[node.start_byte()..node.end_byte()])
    }

    fn node_kind<'a>(&'a self, node: &Self::Node<'a>) -> Cow<'a, str> {
        match self.kind_names.get(node.kind_id() as usize) {
            Some(&name) => Cow::Borrowed(name),
            // Out-of-range ids (e.g. ERROR/MISSING) fall back to the slow path.
            None => Cow::Borrowed(Node::kind(node)),
        }
    }
}

struct NodeWalker<'tree> {
    cursor: tree_sitter::TreeCursor<'tree>,
    count: usize,
}

impl<'tree> Iterator for NodeWalker<'tree> {
    type Item = Node<'tree>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.count == 0 {
            return None;
        }
        let ret = Some(self.cursor.node());
        self.cursor.goto_next_sibling();
        self.count -= 1;
        ret
    }
}

impl ExactSizeIterator for NodeWalker<'_> {
    fn len(&self) -> usize {
        self.count
    }
}

impl<'r> SgNode<'r> for Node<'r> {
    fn parent(&self) -> Option<Self> {
        Node::parent(self)
    }

    fn ancestors(&self, root: Self) -> impl Iterator<Item = Self> {
        let mut ancestor = Some(root);
        let self_id = self.id();
        std::iter::from_fn(move || {
            let inner = ancestor.take()?;
            if inner.id() == self_id {
                return None;
            }
            ancestor = inner.child_with_descendant(*self);
            Some(inner)
        })
        .collect::<Vec<_>>()
        .into_iter()
        .rev()
    }

    fn dfs(&self) -> impl Iterator<Item = Self> {
        TsPre::new(self)
    }

    fn child(&self, nth: usize) -> Option<Self> {
        Node::child(self, nth as u32)
    }

    fn children(&self) -> impl ExactSizeIterator<Item = Self> {
        let mut cursor = self.walk();
        cursor.goto_first_child();
        NodeWalker {
            cursor,
            count: self.child_count(),
        }
    }

    fn child_by_field_id(&self, field_id: u16) -> Option<Self> {
        Node::child_by_field_id(self, field_id)
    }

    fn next(&self) -> Option<Self> {
        self.next_sibling()
    }

    fn prev(&self) -> Option<Self> {
        self.prev_sibling()
    }

    fn next_all(&self) -> impl Iterator<Item = Self> {
        let node = self.parent().unwrap_or(*self);
        let mut cursor = node.walk();
        cursor.goto_first_child_for_byte(self.start_byte());
        std::iter::from_fn(move || {
            if cursor.goto_next_sibling() {
                Some(cursor.node())
            } else {
                None
            }
        })
    }

    fn prev_all(&self) -> impl Iterator<Item = Self> {
        let node = self.parent().unwrap_or(*self);
        let mut cursor = node.walk();
        cursor.goto_first_child_for_byte(self.start_byte());
        std::iter::from_fn(move || {
            if cursor.goto_previous_sibling() {
                Some(cursor.node())
            } else {
                None
            }
        })
    }

    fn is_named(&self) -> bool {
        Node::is_named(self)
    }

    fn is_named_leaf(&self) -> bool {
        self.named_child_count() == 0
    }

    fn is_leaf(&self) -> bool {
        self.child_count() == 0
    }

    fn kind(&self) -> Cow<'_, str> {
        Cow::Borrowed(Node::kind(self))
    }

    fn kind_id(&self) -> KindId {
        Node::kind_id(self)
    }

    fn node_id(&self) -> usize {
        self.id()
    }

    fn range(&self) -> std::ops::Range<usize> {
        self.start_byte()..self.end_byte()
    }

    fn start_pos(&self) -> Position {
        let pos = self.start_position();
        let byte = self.start_byte();
        Position::new(pos.row, pos.column, byte)
    }

    fn end_pos(&self) -> Position {
        let pos = self.end_position();
        let byte = self.end_byte();
        Position::new(pos.row, pos.column, byte)
    }

    fn is_missing(&self) -> bool {
        Node::is_missing(self)
    }

    fn is_error(&self) -> bool {
        Node::is_error(self)
    }

    fn field(&self, name: &str) -> Option<Self> {
        self.child_by_field_name(name)
    }

    fn field_children(&self, field_id: Option<u16>) -> impl Iterator<Item = Self> {
        let field_id = field_id.and_then(NonZero::new);
        let mut cursor = self.walk();
        cursor.goto_first_child();
        let mut done = field_id.is_none();

        std::iter::from_fn(move || {
            if done {
                return None;
            }
            while cursor.field_id() != field_id {
                if !cursor.goto_next_sibling() {
                    return None;
                }
            }
            let ret = cursor.node();
            if !cursor.goto_next_sibling() {
                done = true;
            }
            Some(ret)
        })
    }
}

/// Tree-sitter specific language trait
pub trait LanguageExt: Language {
    /// Create a [`Root`] instance for the language
    fn ast_grep<S: AsRef<str>>(&self, source: S) -> crate::Root<StrDoc<Self>> {
        crate::Root::new(source, self.clone())
    }

    /// Like [`Self::ast_grep`] but aborts the parse when `guard` trips (stall,
    /// deadline, or cancellation), returning `Err` instead of panicking.
    fn ast_grep_with_guard<S: AsRef<str>>(
        &self,
        source: S,
        guard: &ParseGuard,
    ) -> Result<crate::Root<StrDoc<Self>>, String> {
        crate::Root::try_new_with_guard(source, self.clone(), guard)
    }

    /// tree sitter language to parse the source
    fn get_ts_language(&self) -> TSLanguage;

    /// Grammar node-kind names indexed by `kind_id`. Implementations should
    /// memoize per language; the default rebuilds on each call and
    /// `SupportLang` overrides with a per-language cache.
    fn kind_names(&self) -> std::sync::Arc<[&'static str]> {
        let ts = self.get_ts_language();
        (0..ts.node_kind_count())
            .map(|id| ts.node_kind_for_id(id as u16).unwrap_or(""))
            .collect()
    }
}

impl<L: LanguageExt> crate::Root<StrDoc<L>> {
    pub fn new<S: AsRef<str>>(src: S, lang: L) -> Self {
        Self::try_new(src, lang).expect("should parse")
    }

    pub fn try_new<S: AsRef<str>>(src: S, lang: L) -> Result<Self, String> {
        let doc = StrDoc::try_new(src.as_ref(), lang)?;
        Ok(Root { doc })
    }

    pub fn try_new_with_guard<S: AsRef<str>>(
        src: S,
        lang: L,
        guard: &ParseGuard,
    ) -> Result<Self, String> {
        let doc = StrDoc::try_new_with_guard(src.as_ref(), lang, guard)?;
        Ok(Root { doc })
    }

    pub fn source(&self) -> &str {
        self.doc.get_source().as_str()
    }

    pub fn generate(self) -> String {
        self.doc.src
    }
}

#[cfg(test)]
mod tests {
    #[cfg(feature = "builtin-parser")]
    use super::StrDoc;
    use std::ops::ControlFlow;
    use std::sync::atomic::{AtomicU64, Ordering};

    /// Reproduce the stall detection logic and verify
    /// it fires correctly. This tests the algorithm directly rather than relying on
    /// tree-sitter's internal behavior which varies between debug and release builds.
    #[test]
    fn test_stall_detection_logic() {
        let max_stall: u64 = 3;
        let stall_count = AtomicU64::new(0);
        let last_offset = AtomicU64::new(u64::MAX);

        let check = |offset: u64| -> ControlFlow<()> {
            if offset == last_offset.load(Ordering::Relaxed) {
                if stall_count.fetch_add(1, Ordering::Relaxed) >= max_stall {
                    return ControlFlow::Break(());
                }
            } else {
                last_offset.store(offset, Ordering::Relaxed);
                stall_count.store(0, Ordering::Relaxed);
            }
            ControlFlow::Continue(())
        };

        // Advancing offsets: never stalls
        assert_eq!(check(0), ControlFlow::Continue(()));
        assert_eq!(check(1), ControlFlow::Continue(()));
        assert_eq!(check(2), ControlFlow::Continue(()));

        // Stall at offset 5: fetch_add returns previous value, >= fires after max_stall increments
        assert_eq!(check(5), ControlFlow::Continue(())); // first visit, sets last_offset=5
        assert_eq!(check(5), ControlFlow::Continue(())); // count: 0 -> 1
        assert_eq!(check(5), ControlFlow::Continue(())); // count: 1 -> 2
        assert_eq!(check(5), ControlFlow::Continue(())); // count: 2 -> 3
        assert_eq!(check(5), ControlFlow::Break(())); // count: 3 >= 3, abort

        // After advancing, counter resets
        stall_count.store(0, Ordering::Relaxed);
        assert_eq!(check(10), ControlFlow::Continue(()));
        assert_eq!(check(11), ControlFlow::Continue(())); // advance resets count
        assert_eq!(check(11), ControlFlow::Continue(())); // count: 0 -> 1
        assert_eq!(check(11), ControlFlow::Continue(())); // count: 1 -> 2
        assert_eq!(check(11), ControlFlow::Continue(())); // count: 2 -> 3
        assert_eq!(check(11), ControlFlow::Break(())); // count: 3 >= 3, abort
    }

    #[cfg(feature = "builtin-parser")]
    #[test]
    fn test_default_stall_limit_allows_valid_parse() {
        let result = StrDoc::try_new("def f(x):\n    return x\n", crate::SupportLang::Python);
        assert!(
            result.is_ok(),
            "Valid Python should parse: {:?}",
            result.err()
        );
    }

    #[cfg(feature = "builtin-parser")]
    #[test]
    fn past_deadline_aborts_large_parse() {
        use super::ParseGuard;
        use std::time::{Duration, Instant};
        // Large enough that tree-sitter invokes the progress callback.
        let src = "def f(x):\n    return x + 1\n".repeat(50_000);
        let guard = ParseGuard::default().with_deadline(Instant::now() - Duration::from_secs(1));
        let result = StrDoc::try_new_with_guard(&src, crate::SupportLang::Python, &guard);
        assert!(result.is_err(), "a past deadline must abort the parse");
    }

    #[cfg(feature = "builtin-parser")]
    #[test]
    fn future_deadline_allows_parse() {
        use super::ParseGuard;
        use std::time::{Duration, Instant};
        let guard = ParseGuard::default().with_deadline(Instant::now() + Duration::from_secs(300));
        let result = StrDoc::try_new_with_guard(
            "def f(x):\n    return x\n",
            crate::SupportLang::Python,
            &guard,
        );
        assert!(result.is_ok(), "a far-future deadline must not abort");
    }
}
