use std::sync::Arc;

/// A fully qualified name: cached joined string + separator.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Fqn {
    cached: String,
    separator: &'static str,
}

impl Fqn {
    pub fn from_parts(parts: &[&str], separator: &'static str) -> Self {
        Self {
            cached: parts.join(separator),
            separator,
        }
    }

    pub fn from_scope(scope: &[Arc<str>], name: &str, separator: &'static str) -> Self {
        let mut s = scope
            .iter()
            .map(|s| s.as_ref())
            .collect::<Vec<_>>()
            .join(separator);
        if !s.is_empty() {
            s.push_str(separator);
        }
        s.push_str(name);
        Self {
            cached: s,
            separator,
        }
    }

    pub fn as_str(&self) -> &str {
        &self.cached
    }

    pub fn separator(&self) -> &'static str {
        self.separator
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
    fn eq_and_hash() {
        let a = Fqn::from_parts(&["A", "B"], ".");
        let b = Fqn::from_parts(&["A", "B"], ".");
        assert_eq!(a, b);

        let c = Fqn::from_parts(&["A", "B"], "::");
        assert_ne!(a, c);
    }
}
