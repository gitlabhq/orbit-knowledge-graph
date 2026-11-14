/// The key of a scope or an object, in the type its declaring property holds.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Serialize)]
#[serde(untagged)]
pub enum Key {
    Int(i64),
    Text(String),
    Uuid(String),
    Composite(Vec<Key>),
}

impl From<i64> for Key {
    fn from(key: i64) -> Self {
        Self::Int(key)
    }
}

impl From<String> for Key {
    fn from(key: String) -> Self {
        Self::Text(key)
    }
}

impl From<&str> for Key {
    fn from(key: &str) -> Self {
        Self::Text(key.to_string())
    }
}

/// The key as text: an integer in decimal, a string or uuid as itself, a composite as its parts
/// in parentheses.
impl std::fmt::Display for Key {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Int(key) => write!(f, "{key}"),
            Self::Text(key) | Self::Uuid(key) => f.write_str(key),
            Self::Composite(keys) => {
                let parts: Vec<String> = keys.iter().map(ToString::to_string).collect();
                write!(f, "({})", parts.join(", "))
            }
        }
    }
}
