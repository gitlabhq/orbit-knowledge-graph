macro_rules! id {
    ($name:ident) => {
        #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
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
