use std::sync::atomic::{AtomicU64, Ordering};

use crate::catalog::ModelId;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct PlanId(u64);

impl PlanId {
    pub(super) fn fresh() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        Self(NEXT.fetch_add(1, Ordering::Relaxed))
    }
}

macro_rules! index {
    ($name:ident) => {
        #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
        pub struct $name(pub(super) usize);

        impl $name {
            pub fn index(self) -> usize {
                self.0
            }
        }
    };
}

index!(NodeId);
index!(OutputId);
index!(ScopeId);
index!(BodyId);

#[derive(Debug, PartialEq, Eq)]
pub struct Rows {
    pub(super) plan: PlanId,
    pub(super) node: NodeId,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Ref {
    pub(super) plan: PlanId,
    pub(super) output: OutputId,
}

impl Ref {
    pub fn output(self) -> OutputId {
        self.output
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Body {
    pub(super) plan: PlanId,
    pub(super) body: BodyId,
}

impl Body {
    pub fn id(self) -> BodyId {
        self.body
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Declared<T> {
    snapshot: ModelId,
    pub(super) id: T,
}

impl<T: Copy> Declared<T> {
    pub fn new(snapshot: ModelId, id: T) -> Self {
        Self { snapshot, id }
    }

    pub fn snapshot(self) -> ModelId {
        self.snapshot
    }
}
