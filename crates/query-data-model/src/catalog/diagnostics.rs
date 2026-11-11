use std::fmt;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SpecError {
    pub package: String,
    pub message: String,
}

impl fmt::Display for SpecError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}", self.package, self.message)
    }
}

#[derive(Debug, Default)]
pub struct Diagnostics {
    errors: Vec<SpecError>,
}

impl Diagnostics {
    pub fn error(&mut self, package: &str, message: impl Into<String>) {
        self.errors.push(SpecError {
            package: package.to_string(),
            message: message.into(),
        });
    }

    pub fn require<T>(
        &mut self,
        found: Option<T>,
        package: &str,
        message: impl FnOnce() -> String,
    ) -> Option<T> {
        if found.is_none() {
            self.error(package, message());
        }
        found
    }

    pub fn len(&self) -> usize {
        self.errors.len()
    }

    pub fn is_empty(&self) -> bool {
        self.errors.is_empty()
    }

    pub fn finish(self) -> Result<(), crate::DataModelError> {
        if self.errors.is_empty() {
            Ok(())
        } else {
            Err(crate::DataModelError::Spec(self.errors))
        }
    }
}
