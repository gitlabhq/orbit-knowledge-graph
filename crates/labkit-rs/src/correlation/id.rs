//! Correlation ID type and generation.
//!
//! Provides ULID-based correlation IDs for distributed tracing.

use std::fmt;

/// HTTP header name for correlation ID.
pub const HTTP_HEADER_CORRELATION_ID: &str = "x-request-id";

/// gRPC metadata key for correlation ID.
pub const GRPC_METADATA_CORRELATION_ID: &str = "x-gitlab-correlation-id";

/// HTTP header name for client identification.
pub const HTTP_HEADER_CLIENT_NAME: &str = "x-gitlab-client-name";

/// gRPC metadata key for client identification.
pub const GRPC_METADATA_CLIENT_NAME: &str = "x-gitlab-client-name";

/// Log field name for correlation ID.
pub const LOG_FIELD_CORRELATION_ID: &str = "correlation_id";

/// A correlation ID that uniquely identifies a request across services.
///
/// Generated using ULID for lexicographic sortability and uniqueness.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct CorrelationId(String);

impl CorrelationId {
    /// Generate a new correlation ID using ULID.
    ///
    /// Provides both uniqueness and lexicographic
    /// sortability by timestamp.
    #[must_use]
    pub fn generate() -> Self {
        let id = ulid::Ulid::new().to_string().to_lowercase();
        Self(id)
    }

    /// Create a correlation ID from an existing string value.
    ///
    /// Used when extracting from incoming request headers or metadata.
    #[must_use]
    pub fn from_string(s: impl Into<String>) -> Self {
        Self(s.into())
    }

    /// Get the correlation ID as a string slice.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Consume the correlation ID and return the inner string.
    #[must_use]
    pub fn into_string(self) -> String {
        self.0
    }
}

impl Default for CorrelationId {
    fn default() -> Self {
        Self::generate()
    }
}

impl fmt::Display for CorrelationId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<CorrelationId> for String {
    fn from(id: CorrelationId) -> String {
        id.0
    }
}

impl From<String> for CorrelationId {
    fn from(s: String) -> Self {
        Self(s)
    }
}

impl From<&str> for CorrelationId {
    fn from(s: &str) -> Self {
        Self(s.to_owned())
    }
}

impl AsRef<str> for CorrelationId {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generate_produces_valid_ulid() {
        let id = CorrelationId::generate();
        // ULIDs are 26 characters
        assert_eq!(id.as_str().len(), 26);
        // Should be lowercase
        assert_eq!(id.as_str(), id.as_str().to_lowercase());
    }

    #[test]
    fn from_string_preserves_value() {
        let id = CorrelationId::from_string("test-correlation-id");
        assert_eq!(id.as_str(), "test-correlation-id");
    }

    #[test]
    fn display_shows_id() {
        let id = CorrelationId::from_string("abc123");
        assert_eq!(format!("{id}"), "abc123");
    }

    #[test]
    fn generated_ids_are_unique() {
        let id1 = CorrelationId::generate();
        let id2 = CorrelationId::generate();
        assert_ne!(id1, id2);
    }
}
