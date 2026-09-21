use std::borrow::Cow;
use std::fmt;

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("kind {0:?} must match ^[a-z][a-z0-9_]{{0,63}}$")]
pub struct InvalidKind(pub String);

const fn is_valid_kind_name(name: &str) -> bool {
    let bytes = name.as_bytes();
    if bytes.is_empty() || bytes.len() > 64 || !bytes[0].is_ascii_lowercase() {
        return false;
    }
    let mut index = 1;
    while index < bytes.len() {
        let byte = bytes[index];
        if !(byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_') {
            return false;
        }
        index += 1;
    }
    true
}

macro_rules! kind_newtype {
    ($name:ident) => {
        #[derive(
            Clone,
            Debug,
            PartialEq,
            Eq,
            Hash,
            PartialOrd,
            Ord,
            serde::Serialize,
            serde::Deserialize,
        )]
        #[serde(try_from = "String", into = "String")]
        pub struct $name(Cow<'static, str>);

        impl $name {
            pub const fn new(name: &'static str) -> Self {
                assert!(
                    is_valid_kind_name(name),
                    "kind must be 1 to 64 bytes of lowercase ascii, digits, or underscore, starting with a letter"
                );
                Self(Cow::Borrowed(name))
            }

            pub fn parse(name: &str) -> Result<Self, InvalidKind> {
                if is_valid_kind_name(name) {
                    Ok(Self(Cow::Owned(name.to_owned())))
                } else {
                    Err(InvalidKind(name.to_owned()))
                }
            }

            pub fn as_str(&self) -> &str {
                &self.0
            }
        }

        impl TryFrom<String> for $name {
            type Error = InvalidKind;

            fn try_from(name: String) -> Result<Self, InvalidKind> {
                Self::parse(&name)
            }
        }

        impl From<$name> for String {
            fn from(kind: $name) -> String {
                kind.0.into_owned()
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str(&self.0)
            }
        }
    };
}

kind_newtype!(JobKind);
kind_newtype!(CampaignKind);
