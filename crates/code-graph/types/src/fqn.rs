use crate::IStr;
use smallvec::SmallVec;
use std::sync::Arc;

/// A fully qualified name as an ordered sequence of string parts.
///
/// The separator is language-determined ("::" for Ruby/Rust/TS, "." for
/// Python/Java/Kotlin/C#). The joined string is cached as an `IStr`
/// (interned) to avoid repeated allocation on `to_string()`.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Fqn {
    parts: Arc<SmallVec<[Arc<str>; 4]>>,
    separator: &'static str,
    /// Cached joined representation (interned).
    cached: IStr,
}

impl Fqn {
    pub fn new(parts: SmallVec<[Arc<str>; 4]>, separator: &'static str) -> Self {
        let joined = parts
            .iter()
            .map(|p| p.as_ref())
            .collect::<Vec<_>>()
            .join(separator);
        Self {
            parts: Arc::new(parts),
            separator,
            cached: IStr::from(joined.as_str()),
        }
    }

    pub fn from_parts(parts: &[&str], separator: &'static str) -> Self {
        let sv: SmallVec<[Arc<str>; 4]> = parts.iter().map(|s| Arc::from(*s)).collect();
        Self::new(sv, separator)
    }

    /// The cached interned string representation.
    pub fn as_istr(&self) -> IStr {
        self.cached
    }

    /// The leaf name (last segment).
    pub fn name(&self) -> &str {
        self.parts.last().map(|p| p.as_ref()).unwrap_or("")
    }

    /// The parent FQN (everything except the last segment), or None if
    /// this FQN has only one part.
    pub fn parent(&self) -> Option<Self> {
        if self.parts.len() <= 1 {
            return None;
        }
        let parent_parts: SmallVec<[Arc<str>; 4]> =
            self.parts[..self.parts.len() - 1].iter().cloned().collect();
        Some(Self::new(parent_parts, self.separator))
    }

    pub fn parts(&self) -> &[Arc<str>] {
        &self.parts
    }

    pub fn separator(&self) -> &'static str {
        self.separator
    }

    pub fn len(&self) -> usize {
        self.parts.len()
    }

    pub fn is_empty(&self) -> bool {
        self.parts.is_empty()
    }

    /// Build an FQN from a scope stack plus a leaf name.
    pub fn from_scope(scope: &[Arc<str>], name: &str, separator: &'static str) -> Self {
        let mut parts: SmallVec<[Arc<str>; 4]> = scope.iter().cloned().collect();
        parts.push(Arc::from(name));
        Self::new(parts, separator)
    }

    /// Build an FQN from just a scope stack (no additional leaf).
    /// Returns None if the scope is empty.
    pub fn from_scope_only(scope: &[Arc<str>], separator: &'static str) -> Option<Self> {
        if scope.is_empty() {
            None
        } else {
            let parts: SmallVec<[Arc<str>; 4]> = scope.iter().cloned().collect();
            Some(Self::new(parts, separator))
        }
    }
}

impl std::fmt::Display for Fqn {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.cached)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn display_joins_with_separator() {
        let fqn = Fqn::from_parts(&["com", "example", "UserService"], ".");
        assert_eq!(fqn.to_string(), "com.example.UserService");

        let fqn = Fqn::from_parts(&["User", "find"], "::");
        assert_eq!(fqn.to_string(), "User::find");
    }

    #[test]
    fn name_returns_last_part() {
        let fqn = Fqn::from_parts(&["Foo", "Bar", "baz"], "::");
        assert_eq!(fqn.name(), "baz");
    }

    #[test]
    fn name_empty_fqn() {
        let fqn = Fqn::new(SmallVec::new(), "::");
        assert_eq!(fqn.name(), "");
    }

    #[test]
    fn parent_strips_last() {
        let fqn = Fqn::from_parts(&["A", "B", "C"], ".");
        let parent = fqn.parent().unwrap();
        assert_eq!(parent.to_string(), "A.B");
        assert_eq!(parent.name(), "B");
    }

    #[test]
    fn parent_single_part_returns_none() {
        let fqn = Fqn::from_parts(&["A"], ".");
        assert!(fqn.parent().is_none());
    }

    #[test]
    fn parts_accessible() {
        let fqn = Fqn::from_parts(&["x", "y"], "::");
        assert_eq!(fqn.parts().len(), 2);
        assert_eq!(fqn.parts()[0].as_ref(), "x");
    }

    #[test]
    fn eq_and_hash() {
        let a = Fqn::from_parts(&["A", "B"], ".");
        let b = Fqn::from_parts(&["A", "B"], ".");
        assert_eq!(a, b);

        let c = Fqn::from_parts(&["A", "B"], "::");
        assert_ne!(a, c);
    }
}
