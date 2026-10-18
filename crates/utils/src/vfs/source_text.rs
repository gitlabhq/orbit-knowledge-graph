use std::{io, ops::Deref, sync::Arc};

#[derive(Clone, Debug)]
pub struct SourceText(yoke::Yoke<&'static str, Arc<[u8]>>);

impl TryFrom<Arc<[u8]>> for SourceText {
    type Error = io::Error;

    fn try_from(bytes: Arc<[u8]>) -> Result<Self, Self::Error> {
        yoke::Yoke::<&str, _>::try_attach_to_cart(bytes, std::str::from_utf8)
            .map(Self)
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))
    }
}

impl From<&str> for SourceText {
    fn from(text: &str) -> Self {
        Self::try_from(Arc::from(text.as_bytes())).expect("valid UTF-8")
    }
}

impl From<String> for SourceText {
    fn from(text: String) -> Self {
        Self::try_from(Arc::from(text.into_bytes())).expect("valid UTF-8")
    }
}

impl Default for SourceText {
    fn default() -> Self {
        Self::from("")
    }
}

impl Deref for SourceText {
    type Target = str;

    fn deref(&self) -> &str {
        self.0.get()
    }
}
