use crate::IStr;
use smallvec::SmallVec;
use std::sync::Arc;

/// A fully qualified name as interned parts + cached joined string.
///
/// Parts are `IStr` (8 bytes each, interned). Higher-scope parts like
/// "com", "example" are shared across thousands of FQNs at zero cost.
/// SmallVec inlines up to 4 parts (no heap allocation for typical FQNs).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Fqn {
    parts: SmallVec<[IStr; 4]>,
    separator: &'static str,
    cached: IStr,
}

impl Fqn {
    pub fn from_parts(parts: &[&str], separator: &'static str) -> Self {
        let iparts: SmallVec<[IStr; 4]> = parts.iter().map(|s| IStr::from(*s)).collect();
        let joined = parts.join(separator);
        Self {
            parts: iparts,
            separator,
            cached: IStr::from(joined.as_str()),
        }
    }

    /// Build an FQN from a scope stack plus a leaf name.
    pub fn from_scope(scope: &[Arc<str>], name: &str, separator: &'static str) -> Self {
        let mut iparts: SmallVec<[IStr; 4]> =
            scope.iter().map(|s| IStr::from(s.as_ref())).collect();
        iparts.push(IStr::from(name));
        let joined = iparts
            .iter()
            .map(|p| p.as_ref())
            .collect::<Vec<_>>()
            .join(separator);
        Self {
            parts: iparts,
            separator,
            cached: IStr::from(joined.as_str()),
        }
    }

    /// Build an FQN from just a scope stack (no additional leaf).
    pub fn from_scope_only(scope: &[Arc<str>], separator: &'static str) -> Option<Self> {
        if scope.is_empty() {
            return None;
        }
        let iparts: SmallVec<[IStr; 4]> = scope.iter().map(|s| IStr::from(s.as_ref())).collect();
        let joined = iparts
            .iter()
            .map(|p| p.as_ref())
            .collect::<Vec<_>>()
            .join(separator);
        Some(Self {
            parts: iparts,
            separator,
            cached: IStr::from(joined.as_str()),
        })
    }

    /// The cached interned string representation.
    pub fn as_istr(&self) -> IStr {
        self.cached
    }

    /// The full FQN as a string slice (borrows from the intern table).
    pub fn as_str(&self) -> &str {
        self.cached.as_ref()
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
        let parent_parts: SmallVec<[IStr; 4]> = self.parts[..self.parts.len() - 1].into();
        let joined = parent_parts
            .iter()
            .map(|p| p.as_ref())
            .collect::<Vec<_>>()
            .join(self.separator);
        Some(Self {
            parts: parent_parts,
            separator: self.separator,
            cached: IStr::from(joined.as_str()),
        })
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
    fn name_single_part() {
        let fqn = Fqn::from_parts(&["A"], ".");
        assert_eq!(fqn.name(), "A");
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
    fn len_counts_parts() {
        let fqn = Fqn::from_parts(&["x", "y"], "::");
        assert_eq!(fqn.len(), 2);
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
