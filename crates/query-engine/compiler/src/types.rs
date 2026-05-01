//! Shared types used across the compiler and downstream crates.

use crate::error::{QueryError, Result};
use once_cell::sync::Lazy;
use regex::Regex;

/// Matches paths like "1/", "1/2/", "123/456/789/"
static TRAVERSAL_PATH_REGEX: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"^(\d+/)+$").expect("valid regex"));

/// Default role assumed for a traversal path when the JWT does not supply an
/// explicit per-path role. Matches the historical behavior where Rails only
/// published Reporter+ paths and GKG treated every path uniformly.
pub const DEFAULT_PATH_ACCESS_LEVEL: u32 = 20;

/// GitLab access levels as sent in the JWT `min_access_level` field.
/// Values match `Gitlab::Access` constants in Rails.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum AccessLevel {
    Guest = 10,
    Reporter = 20,
    SecurityManager = 25,
    Developer = 30,
    Maintainer = 40,
    Owner = 50,
}

impl AccessLevel {
    pub fn from_u32(value: u32) -> Option<Self> {
        match value {
            10 => Some(Self::Guest),
            20 => Some(Self::Reporter),
            25 => Some(Self::SecurityManager),
            30 => Some(Self::Developer),
            40 => Some(Self::Maintainer),
            50 => Some(Self::Owner),
            _ => None,
        }
    }
}

/// A traversal path the user is entitled to see, plus the exact effective
/// access levels they hold on that path. The access levels are stored as raw
/// GitLab integers (matching `Gitlab::Access` in Rails) so that comparing
/// against an entity's `required_access_level` is a direct `>=` without a
/// role-table lookup.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TraversalPath {
    pub path: String,
    pub access_levels: Vec<u32>,
}

impl TraversalPath {
    pub fn new(path: impl Into<String>, access_level: u32) -> Self {
        Self::with_access_levels(path, vec![access_level])
    }

    pub fn with_access_levels(path: impl Into<String>, access_levels: Vec<u32>) -> Self {
        let mut access_levels = access_levels;
        access_levels.sort_unstable();
        access_levels.dedup();

        Self {
            path: path.into(),
            access_levels,
        }
    }
}

/// Security context for request-level isolation.
///
/// Contains the org ID and traversal paths used to scope queries to
/// a specific organization's data, plus optional role metadata from
/// the JWT claims for access-gated features (e.g. debug output).
///
/// Each entry in `traversal_paths` carries the user's exact role set on that
/// path. The compiler security pass uses the per-path roles to drop paths that
/// do not meet an entity's `required_access_level`, which is how
/// aggregation queries over e.g. Vulnerability are redacted for users who
/// only have Reporter access.
#[derive(Debug, Clone)]
pub struct SecurityContext {
    pub org_id: i64,
    pub traversal_paths: Vec<TraversalPath>,
    pub admin: bool,
    pub access_level: Option<AccessLevel>,
}

impl SecurityContext {
    /// Create a security context from plain path strings, tagging every path
    /// with the default Reporter access level. Kept for tests and internal
    /// callers that intentionally model the historical Reporter-only floor.
    pub fn new(org_id: i64, traversal_paths: Vec<String>) -> Result<Self> {
        let tagged = traversal_paths
            .into_iter()
            .map(|p| TraversalPath::new(p, DEFAULT_PATH_ACCESS_LEVEL))
            .collect();
        Self::new_with_roles(org_id, tagged)
    }

    /// Create a security context where every path carries an explicit role.
    ///
    /// Validates that:
    /// - Each path matches the format `int/int/.../`
    /// - Each segment fits in i64
    pub fn new_with_roles(org_id: i64, traversal_paths: Vec<TraversalPath>) -> Result<Self> {
        for tp in &traversal_paths {
            Self::validate_traversal_path(&tp.path)?;
            if tp.access_levels.is_empty() {
                return Err(QueryError::Security(format!(
                    "traversal_path '{}' has no access_levels",
                    tp.path
                )));
            }
        }
        Ok(Self {
            org_id,
            traversal_paths,
            admin: false,
            access_level: None,
        })
    }

    pub fn with_role(mut self, admin: bool, min_access_level: Option<u32>) -> Self {
        self.admin = admin;
        self.access_level = min_access_level.and_then(AccessLevel::from_u32);
        self
    }

    /// Return the subset of paths where one of the user's access levels meets
    /// `required_access_level`. Admin users bypass the filter because they
    /// already carry the synthetic org-root path at maximum role.
    pub fn paths_at_least(&self, required_access_level: u32) -> Vec<&str> {
        self.traversal_paths
            .iter()
            .filter(|tp| {
                tp.access_levels
                    .iter()
                    .any(|level| *level >= required_access_level)
            })
            .map(|tp| tp.path.as_str())
            .collect()
    }

    fn validate_traversal_path(path: &str) -> Result<()> {
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

        Ok(())
    }
}
