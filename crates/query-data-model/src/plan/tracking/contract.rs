use super::{OutputId, Ref};

#[derive(Debug, Clone, PartialEq)]
pub struct Declare<V> {
    pub name: Option<String>,
    pub value: V,
}

impl<V> Declare<V> {
    pub fn named(name: impl Into<String>, value: V) -> Self {
        Self {
            name: Some(name.into()),
            value,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct Named<T> {
    pub item: T,
    pub name: String,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Output<V> {
    Keep(Ref),
    Declare(Declare<V>),
}

#[derive(Debug, Clone, PartialEq)]
pub enum Outputs<V> {
    Preserve(Vec<Declare<V>>),
    Replace(Vec<Output<V>>),
}

impl<V> Outputs<V> {
    pub fn preserve() -> Self {
        Self::Preserve(Vec::new())
    }

    pub fn extend(declared: impl IntoIterator<Item = Declare<V>>) -> Self {
        Self::Preserve(declared.into_iter().collect())
    }

    pub fn replace(outputs: impl IntoIterator<Item = Output<V>>) -> Self {
        Self::Replace(outputs.into_iter().collect())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Item {
    Keep(OutputId),
    Own(OutputId),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum Spec {
    Preserve(Vec<OutputId>),
    Replace(Vec<Item>),
}

impl Spec {
    pub(super) fn owns(&self, output: OutputId) -> bool {
        match self {
            Self::Preserve(own) => own.contains(&output),
            Self::Replace(items) => items.contains(&Item::Own(output)),
        }
    }

    pub(super) fn own(&self) -> Vec<OutputId> {
        match self {
            Self::Preserve(own) => own.clone(),
            Self::Replace(items) => items
                .iter()
                .filter_map(|item| match item {
                    Item::Own(output) => Some(*output),
                    Item::Keep(_) => None,
                })
                .collect(),
        }
    }
}
