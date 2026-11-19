use std::sync::Arc;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Fqn {
    cached: String,
    separator: &'static str,
    last_part_start: Option<usize>,
}

impl Fqn {
    pub fn from_parts(parts: &[&str], separator: &'static str) -> Self {
        Self {
            cached: parts.join(separator),
            separator,
            last_part_start: None,
        }
    }

    /// Like [`Fqn::from_parts`], but [`Fqn::parent`] drops exactly the last
    /// part, so a part may contain the separator as escaped text.
    pub fn from_segments(parts: &[&str], separator: &'static str) -> Self {
        let cached = parts.join(separator);
        let last_part_start = cached.len() - parts.last().map_or(0, |part| part.len());
        Self {
            cached,
            separator,
            last_part_start: Some(last_part_start),
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
            last_part_start: None,
        }
    }

    pub fn as_str(&self) -> &str {
        &self.cached
    }

    pub fn separator(&self) -> &'static str {
        self.separator
    }

    pub fn parent(&self) -> Option<&str> {
        match self.last_part_start {
            Some(0) => None,
            Some(start) => Some(&self.cached[..start - self.separator.len()]),
            None => self
                .cached
                .rsplit_once(self.separator)
                .map(|(parent, _)| parent),
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
    fn parent_drops_the_last_segment() {
        assert_eq!(
            Fqn::from_parts(&["root", "win\\", "child"], ".").parent(),
            Some("root.win\\")
        );
        assert_eq!(Fqn::from_parts(&["root"], ".").parent(), None);
        assert_eq!(
            Fqn::from_segments(&["guide", "Guide", "C\\# Basics"], "#").parent(),
            Some("guide#Guide")
        );
        assert_eq!(Fqn::from_segments(&["guide"], "#").parent(), None);
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
