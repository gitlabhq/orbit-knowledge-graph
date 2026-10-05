//! Passes classify files by path, size and content. They do not enforce limits or see symlinks.
//! Header `Pending` requests content during loading; after content it becomes `Keep(Default)`.
//! Linked files kept by the header run content passes on first read. A late `Drop` remains
//! in the inventory with its reason because nodes are frozen; reading it returns `Unsupported`.
//! Passes are trusted policy code and should only change the decision.

use std::sync::OnceLock;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum Decision<T> {
    #[default]
    Pending,
    Keep(T),
    List(&'static str),
    Drop(&'static str),
}

pub trait Tag: Copy + Default + Send + Sync + 'static {}
impl<T: Copy + Default + Send + Sync + 'static> Tag for T {}

#[derive(Debug, Clone)]
pub struct File<T> {
    pub path: String,
    pub size: u64,
    decided: Decision<T>,
    content_decision: OnceLock<Decision<T>>,
}

impl<T: Tag> File<T> {
    pub fn new(path: String, size: u64) -> Self {
        Self {
            path,
            size,
            decided: Decision::Pending,
            content_decision: OnceLock::new(),
        }
    }

    pub fn decision(&self) -> Decision<T> {
        self.content_decision.get().copied().unwrap_or(self.decided)
    }

    pub fn decide(&mut self, decision: Decision<T>) {
        self.decided = decision;
        self.content_decision.take();
    }

    pub fn keeps(&self) -> bool {
        matches!(self.decision(), Decision::Keep(_))
    }

    pub(super) fn decide_once(&self, content: impl FnOnce(&mut Self)) -> Decision<T> {
        *self.content_decision.get_or_init(|| {
            let mut copy = Self::new(self.path.clone(), self.size);
            copy.decided = self.decided;
            content(&mut copy);
            copy.keep_if_pending();
            copy.decided
        })
    }

    pub(super) fn keep_if_pending(&mut self) {
        if matches!(self.decided, Decision::Pending) {
            self.decided = Decision::Keep(T::default());
        }
    }
}

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

impl Pass for () {
    type Tag = ();
}
