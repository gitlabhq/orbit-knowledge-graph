macro_rules! id {
    ($name:ident) => {
        #[derive(
            Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Deserialize,
        )]
        pub struct $name(pub(crate) usize);

        impl $name {
            pub fn index(self) -> usize {
                self.0
            }
        }
    };
}

id!(EntityId);
id!(PropertyId);
id!(RelationshipId);
id!(RelationshipVariantId);
id!(AttributeId);
id!(ConnectionId);
id!(StoreId);

/// A column an edge carries: its endpoints' ids and kinds, and its relationship.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum EdgeField {
    SourceId,
    SourceKind,
    TargetId,
    TargetKind,
    Kind,
}
