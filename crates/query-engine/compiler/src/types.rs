//! Shared types used across the compiler and downstream crates.

use crate::error::{QueryError, Result};
use once_cell::sync::Lazy;
use regex::Regex;

/// Matches paths like "1/", "1/2/", "123/456/789/"
static TRAVERSAL_PATH_REGEX: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"^(\d+/)+$").expect("valid regex"));

/// Security context for request-level isolation.
///
/// Contains the org ID and traversal paths used to scope queries to
/// a specific organization's data, plus optional role metadata from
/// the JWT claims for access-gated features (e.g. debug output).
#[derive(Debug, Clone)]
pub struct SecurityContext {
    pub org_id: i64,
    pub traversal_paths: Vec<String>,
    pub admin: bool,
    pub min_access_level: Option<u32>,
}

impl SecurityContext {
    /// Create a new security context with validation.
    ///
    /// Validates that:
    /// - Each path matches the format `int/int/.../`
    /// - Each segment fits in i64
    /// - The first segment of each path equals org_id
    ///
    /// `admin` and `min_access_level` default to `false` / `None`.
    /// Use [`with_role`](Self::with_role) to set them from JWT claims.
    pub fn new(org_id: i64, traversal_paths: Vec<String>) -> Result<Self> {
        for path in &traversal_paths {
            Self::validate_traversal_path(path, org_id)?;
        }
        Ok(Self {
            org_id,
            traversal_paths,
            admin: false,
            min_access_level: None,
        })
    }

    pub fn with_role(mut self, admin: bool, min_access_level: Option<u32>) -> Self {
        self.admin = admin;
        self.min_access_level = min_access_level;
        self
    }

    fn validate_traversal_path(path: &str, org_id: i64) -> Result<()> {
        if !TRAVERSAL_PATH_REGEX.is_match(path) {
            return Err(QueryError::Security(format!(
                "invalid traversal_path format: '{path}' (expected pattern like '1/2/3/')"
            )));
        }

        let segments: Vec<&str> = path.trim_end_matches('/').split('/').collect();

        for segment in &segments {
            segment.parse::<i64>().map_err(|_| {
                QueryError::Security(format!(
                    "traversal_path segment '{segment}' exceeds i64 range"
                ))
            })?;
        }

        let first_segment: i64 = segments[0].parse().expect("validated above");
        if first_segment != org_id {
            return Err(QueryError::Security(format!(
                "traversal_path '{path}' does not start with org_id {org_id}"
            )));
        }

        Ok(())
    }
}
