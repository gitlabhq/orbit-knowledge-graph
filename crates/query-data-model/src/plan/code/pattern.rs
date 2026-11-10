use super::CodeError;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Pattern {
    text: String,
    captures: Vec<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, strum::EnumString, strum::IntoStaticStr)]
#[strum(serialize_all = "lowercase")]
pub enum Language {
    Go,
    Java,
    JavaScript,
    Python,
    Ruby,
    Rust,
    TypeScript,
}

impl Pattern {
    /// An ast-grep pattern; `$NAME` captures one node and `$$$NAME` a sequence.
    pub fn ast_grep(text: &str) -> Result<Self, CodeError> {
        let invalid = |reason: &str| CodeError::Pattern {
            pattern: text.to_string(),
            reason: reason.to_string(),
        };
        if text.trim().is_empty() {
            return Err(invalid("the pattern is empty"));
        }
        let mut captures = Vec::new();
        let mut rest = text;
        while let Some(start) = rest.find('$') {
            let after = rest[start..].trim_start_matches('$');
            let name: String = after
                .chars()
                .take_while(|c| c.is_ascii_uppercase() || c.is_ascii_digit() || *c == '_')
                .collect();
            if name.is_empty() || name.starts_with(|c: char| c.is_ascii_digit()) {
                return Err(invalid("a metavariable is $ followed by an uppercase name"));
            }
            if name != "_" && !captures.contains(&name) {
                captures.push(name.clone());
            }
            rest = &after[name.len()..];
        }
        Ok(Self {
            text: text.to_string(),
            captures,
        })
    }

    pub fn text(&self) -> &str {
        &self.text
    }

    pub fn captures(&self) -> &[String] {
        &self.captures
    }

    /// The identifier runs outside metavariables, which every matching file contains.
    pub fn literals(&self) -> Vec<&str> {
        let mut literals = Vec::new();
        let mut rest = self.text.as_str();
        while !rest.is_empty() {
            let skip = rest
                .find(|c: char| c.is_ascii_alphanumeric() || c == '_' || c == '$')
                .unwrap_or(rest.len());
            rest = &rest[skip..];
            let metavariable = rest.starts_with('$');
            let rest_of = rest.trim_start_matches('$');
            let end = rest_of
                .find(|c: char| !(c.is_ascii_alphanumeric() || c == '_'))
                .unwrap_or(rest_of.len());
            if !metavariable && end > 0 {
                literals.push(&rest_of[..end]);
            }
            rest = &rest_of[end..];
        }
        literals
    }
}
