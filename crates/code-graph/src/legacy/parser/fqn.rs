use std::hash::Hash;
use std::ops::Deref;
use std::sync::Arc;

use crate::utils::Range;
use internment::ArcIntern;

/// Generic FQN Part that can be language-specific
/// This allows for future extensibility where different languages
/// might need different metadata for their FQN parts
#[derive(Clone)]
pub struct FQNPart<T = String, M = ()>
where
    T: Eq + Hash + Send + Sync + 'static,
    M: Eq + Hash + Send + Sync + 'static,
{
    inner: ArcIntern<FQNPartData<T, M>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct FQNPartData<T, M>
where
    T: Eq + Hash + Send + Sync + 'static,
    M: Eq + Hash + Send + Sync + 'static,
{
    pub node_type: T,
    pub node_name: String,
    pub range: Range,
    pub metadata: Option<M>,
}

impl<T, M> std::fmt::Debug for FQNPart<T, M>
where
    T: std::fmt::Debug + Eq + Hash + Send + Sync + 'static,
    M: std::fmt::Debug + Eq + Hash + Send + Sync + 'static,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.inner.fmt(f)
    }
}

impl<T, M> PartialEq for FQNPart<T, M>
where
    T: Eq + Hash + Send + Sync + 'static,
    M: Eq + Hash + Send + Sync + 'static,
{
    fn eq(&self, other: &Self) -> bool {
        self.inner == other.inner
    }
}

impl<T, M> Eq for FQNPart<T, M>
where
    T: Eq + Hash + Send + Sync + 'static,
    M: Eq + Hash + Send + Sync + 'static,
{
}

impl<T, M> std::hash::Hash for FQNPart<T, M>
where
    T: Eq + Hash + Send + Sync + 'static,
    M: Eq + Hash + Send + Sync + 'static,
{
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.inner.hash(state);
    }
}

impl<T, M> FQNPart<T, M>
where
    T: Eq + Hash + Send + Sync + 'static,
    M: Eq + Hash + Send + Sync + 'static,
{
    /// Create a new FQN part with just node type and name
    pub fn new(node_type: T, node_name: String, range: Range) -> Self {
        Self {
            inner: ArcIntern::new(FQNPartData {
                node_type,
                node_name,
                range,
                metadata: None,
            }),
        }
    }

    /// Create a new FQN part with metadata
    pub fn with_metadata(node_type: T, node_name: String, range: Range, metadata: M) -> Self {
        Self {
            inner: ArcIntern::new(FQNPartData {
                node_type,
                node_name,
                range,
                metadata: Some(metadata),
            }),
        }
    }

    pub fn node_type(&self) -> &T {
        &self.inner.node_type
    }

    pub fn node_name(&self) -> &str {
        &self.inner.node_name
    }

    pub fn range(&self) -> Range {
        self.inner.range
    }

    pub fn metadata(&self) -> Option<&M> {
        self.inner.metadata.as_ref()
    }

    pub fn into_inner(self) -> ArcIntern<FQNPartData<T, M>> {
        self.inner
    }
}

impl<T, M> Deref for FQNPart<T, M>
where
    T: Eq + Hash + Send + Sync + 'static,
    M: Eq + Hash + Send + Sync + 'static,
{
    type Target = FQNPartData<T, M>;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl Fqn<FQNPart<String>> {
    pub fn to_string(&self, separator: &str) -> String {
        self.parts
            .iter()
            .map(|part| part.node_name().to_string())
            .collect::<Vec<_>>()
            .join(separator)
    }
}

/// Generic FQN (Fully Qualified Name) data structure
/// Currently optimized for performance using Arc<Vec<String>> pattern from old MR
/// but designed to be extensible for future FQNPart<T> usage
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Fqn<T = String> {
    pub parts: Arc<Vec<T>>,
}

impl<T> Fqn<T> {
    /// Creates a new FQN from a vector of parts
    pub fn new(parts: Vec<T>) -> Self {
        Self {
            parts: Arc::new(parts),
        }
    }

    /// Returns true if this is a top-level definition (no namespace)
    pub fn is_top_level(&self) -> bool {
        self.parts.len() == 1
    }

    /// Get the number of parts in this FQN
    pub fn len(&self) -> usize {
        self.parts.len()
    }

    /// Check if the FQN is empty
    pub fn is_empty(&self) -> bool {
        self.parts.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::utils::{Position, Range};

    #[test]
    fn test_fqn_creation_and_basic_operations() {
        // Test basic FQN creation
        let fqn = Fqn::new(vec![
            "Module".to_string(),
            "Class".to_string(),
            "method".to_string(),
        ]);

        assert_eq!(fqn.len(), 3);
        assert!(!fqn.is_empty());
        assert!(!fqn.is_top_level());

        let top_level_fqn = Fqn::new(vec!["TopLevelMethod".to_string()]);
        assert!(top_level_fqn.is_top_level());
    }

    #[test]
    fn test_fqn_part_creation() {
        let part: FQNPart<String, ()> = FQNPart::new(
            "Class".to_string(),
            "MyClass".to_string(),
            Range::new(Position::new(10, 20), Position::new(10, 20), (10, 20)),
        );

        assert_eq!(part.node_type(), "Class");
        assert_eq!(part.node_name(), "MyClass");
        assert!(part.metadata().is_none());

        // Test with String metadata
        let part_with_metadata = FQNPart::with_metadata(
            "Method".to_string(),
            "my_method".to_string(),
            Range::new(Position::new(10, 20), Position::new(10, 20), (10, 20)),
            "public".to_string(),
        );

        assert_eq!(part_with_metadata.node_type(), "Method");
        assert_eq!(part_with_metadata.node_name(), "my_method");
        assert_eq!(part_with_metadata.metadata(), Some(&"public".to_string()));
    }

    #[test]
    fn test_generic_fqn_with_fqn_parts() {
        let parts = vec![
            FQNPart::new(
                "Module".to_string(),
                "MyModule".to_string(),
                Range::new(Position::new(10, 20), Position::new(10, 20), (10, 20)),
            ),
            FQNPart::new(
                "Class".to_string(),
                "MyClass".to_string(),
                Range::new(Position::new(10, 20), Position::new(10, 20), (10, 20)),
            ),
            FQNPart::new(
                "Method".to_string(),
                "my_method".to_string(),
                Range::new(Position::new(10, 20), Position::new(10, 20), (10, 20)),
            ),
        ];

        let fqn: Fqn<FQNPart> = Fqn::new(parts);

        assert_eq!(fqn.len(), 3);
        assert!(!fqn.is_top_level());
        assert_eq!(fqn.parts[0].node_type(), "Module");
        assert_eq!(fqn.parts[1].node_name(), "MyClass");
        assert_eq!(fqn.parts[2].node_type(), "Method");
    }

    #[test]
    fn test_fqn_part_with_custom_metadata() {
        // Test with custom metadata types
        #[derive(Debug, Clone, PartialEq, Eq, Hash)]
        struct CustomMetadata {
            visibility: String,
            is_static: bool,
        }

        let part_with_custom_metadata = FQNPart::with_metadata(
            "Method".to_string(),
            "my_method".to_string(),
            Range::new(Position::new(10, 20), Position::new(10, 20), (10, 20)),
            CustomMetadata {
                visibility: "public".to_string(),
                is_static: false,
            },
        );

        assert_eq!(part_with_custom_metadata.node_type(), "Method");
        assert_eq!(part_with_custom_metadata.node_name(), "my_method");
        assert!(part_with_custom_metadata.metadata().is_some());
        assert_eq!(
            part_with_custom_metadata.metadata().unwrap().visibility,
            "public"
        );
    }
}
