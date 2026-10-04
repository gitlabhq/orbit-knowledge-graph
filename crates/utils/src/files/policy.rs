//! What a domain writes: a `Pass` that turns a file's path, size and bytes
//! into a `Decision`.

use std::sync::OnceLock;

/// What becomes of a file. `Pending` is the start state and, after `header`,
/// means "the bytes decide"; it is never observable once the store is loaded.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum Decision<T> {
    #[default]
    Pending,
    Keep(T),
    List(&'static str),
    Drop(&'static str),
}

/// A domain's tag on a kept file. `Default` is what a `Pending` file becomes
/// when no pass objected to its bytes.
pub trait Tag: Copy + Default + Send + Sync + 'static {}
impl<T: Copy + Default + Send + Sync + 'static> Tag for T {}

/// One file of the repository and what the passes decided about it.
#[derive(Debug, Clone)]
pub struct File<T> {
    pub path: String,
    pub size: u64,
    decided: Decision<T>,
    /// A file linked from disk is checked by the content passes on its first
    /// read, after the store is frozen; this is that one late verdict.
    verdict: OnceLock<Decision<T>>,
}

impl<T: Tag> File<T> {
    pub fn new(path: String, size: u64) -> Self {
        Self {
            path,
            size,
            decided: Decision::Pending,
            verdict: OnceLock::new(),
        }
    }

    pub fn decision(&self) -> Decision<T> {
        self.verdict.get().copied().unwrap_or(self.decided)
    }

    pub fn decide(&mut self, decision: Decision<T>) {
        self.decided = decision;
    }

    pub fn keeps(&self) -> bool {
        matches!(self.decision(), Decision::Keep(_))
    }

    /// The one decision after the store is frozen: `content` runs once, on
    /// a copy, and its outcome is this file's decision from then on.
    pub(super) fn decide_once(&self, content: impl FnOnce(&mut Self)) -> Decision<T> {
        *self.verdict.get_or_init(|| {
            let mut copy = Self::new(self.path.clone(), self.size);
            copy.decided = self.decided;
            content(&mut copy);
            copy.decided
        })
    }

    /// `Pending` after the content passes means no policy objected.
    pub(super) fn keep_if_pending(&mut self) {
        if matches!(self.decided, Decision::Pending) {
            self.decided = Decision::Keep(T::default());
        }
    }
}

/// A pure function of path, size and bytes. It never sees a symlink, never
/// counts anything and cannot fail. `header` runs on every file; `content`
/// runs once, on the one read, for files still `Pending` or `Keep` after it.
/// A `Drop` from either leaves no node, except when the one read is a
/// parser's first `read` of a linked file: the store is frozen by then, so
/// that node stays and reads as `Unsupported`.
pub trait Pass: Send + Sync {
    type Tag: Tag;

    fn header(&self, _file: &mut File<Self::Tag>) {}

    fn content(&self, _file: &mut File<Self::Tag>, _bytes: &[u8]) {}

    fn then<B: Pass<Tag = Self::Tag>>(self, next: B) -> Then<Self, B>
    where
        Self: Sized,
    {
        Then(self, next)
    }
}

/// Two passes in order: the second sees the first's decision.
pub struct Then<A, B>(A, B);

impl<A: Pass, B: Pass<Tag = A::Tag>> Pass for Then<A, B> {
    type Tag = A::Tag;

    fn header(&self, file: &mut File<Self::Tag>) {
        self.0.header(file);
        self.1.header(file);
    }

    fn content(&self, file: &mut File<Self::Tag>, bytes: &[u8]) {
        self.0.content(file, bytes);
        self.1.content(file, bytes);
    }
}

/// No policy: keep everything.
impl Pass for () {
    type Tag = ();
}
