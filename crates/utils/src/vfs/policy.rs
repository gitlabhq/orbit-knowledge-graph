//! Passes classify files by path, size and content. They do not enforce limits or see symlinks.
//! Metadata `Pending` requests content during loading; after content it becomes `Keep(Default)`.
//! Linked files kept by metadata run content passes on first read. A late `Drop` remains
//! in the inventory with its reason because nodes are frozen; reading it returns `Unsupported`.
//! Passes return decisions; file metadata and cached decisions remain owned by the store.

use std::{borrow::Cow, sync::OnceLock};

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
pub struct File<'a, T> {
    pub path: Cow<'a, str>,
    pub size: u64,
    pub(super) metadata_decision: Decision<T>,
    content_decision: OnceLock<Decision<T>>,
    bytes: Option<&'a [u8]>,
}

impl<T: Tag> File<'static, T> {
    pub(super) fn new(path: String, size: u64, metadata_decision: Decision<T>) -> Self {
        Self {
            path: Cow::Owned(path),
            size,
            metadata_decision,
            content_decision: OnceLock::new(),
            bytes: None,
        }
    }
}

impl<'a, T: Tag> File<'a, T> {
    pub fn bytes(&self) -> Option<&'a [u8]> {
        self.bytes
    }

    pub fn decision(&self) -> Decision<T> {
        self.content_decision
            .get()
            .copied()
            .unwrap_or(self.metadata_decision)
    }

    pub fn keeps(&self) -> bool {
        matches!(self.decision(), Decision::Keep(_))
    }

    pub(super) fn classify(&self, pass: &dyn Pass<Tag = T>, bytes: &[u8]) -> Decision<T> {
        *self.content_decision.get_or_init(|| {
            match pass.content(&File {
                bytes: Some(bytes),
                ..self.view()
            }) {
                Decision::Pending => Decision::Keep(T::default()),
                decision => decision,
            }
        })
    }

    fn view(&self) -> File<'_, T> {
        File {
            path: Cow::Borrowed(&self.path),
            size: self.size,
            metadata_decision: self.metadata_decision,
            content_decision: self
                .content_decision
                .get()
                .copied()
                .map(OnceLock::from)
                .unwrap_or_default(),
            bytes: self.bytes,
        }
    }
}

pub trait Pass: Send + Sync {
    type Tag: Tag;

    fn metadata(&self, file: &File<'_, Self::Tag>) -> Decision<Self::Tag> {
        file.decision()
    }

    fn content(&self, file: &File<'_, Self::Tag>) -> Decision<Self::Tag> {
        file.decision()
    }

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

    fn metadata(&self, file: &File<'_, Self::Tag>) -> Decision<Self::Tag> {
        let next = File {
            metadata_decision: self.0.metadata(file),
            ..file.view()
        };
        self.1.metadata(&next)
    }

    fn content(&self, file: &File<'_, Self::Tag>) -> Decision<Self::Tag> {
        let next = File {
            content_decision: OnceLock::from(self.0.content(file)),
            ..file.view()
        };
        self.1.content(&next)
    }
}

impl Pass for () {
    type Tag = ();
}
