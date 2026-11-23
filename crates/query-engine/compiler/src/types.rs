use crate::error::{QueryError, Result};
use orbit_utils::traversal_path::TraversalPath;
use serde::Deserialize;

pub const DEFAULT_PATH_ACCESS_LEVEL: u32 = 20;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
pub enum Realm {
    #[serde(alias = "saas")]
    SaaS,
    #[serde(rename = "self-managed", alias = "self_managed")]
    SelfManaged,
    #[serde(alias = "dedicated")]
    Dedicated,
}

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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuthorizedPath {
    pub path: TraversalPath,
    pub access_levels: Vec<u32>,
}

impl AuthorizedPath {
    pub fn new(path: impl Into<TraversalPath>, access_level: u32) -> Self {
        Self::with_access_levels(path, vec![access_level])
    }

    pub fn with_access_levels(path: impl Into<TraversalPath>, access_levels: Vec<u32>) -> Self {
        let mut access_levels = access_levels;
        access_levels.sort_unstable();
        access_levels.dedup();

        Self {
            path: path.into(),
            access_levels,
        }
    }
}

#[derive(Debug, Clone)]
pub struct SecurityContext {
    pub org_id: i64,
    pub traversal_paths: Vec<AuthorizedPath>,
    pub admin: bool,
    pub access_level: Option<AccessLevel>,
    pub realm: Option<Realm>,
    pub is_gitlab_team_member: bool,
}

impl SecurityContext {
    pub fn new(org_id: i64, traversal_paths: Vec<String>) -> Result<Self> {
        let tagged = traversal_paths
            .into_iter()
            .map(|p| AuthorizedPath::new(p, DEFAULT_PATH_ACCESS_LEVEL))
            .collect();
        Self::new_with_roles(org_id, tagged)
    }

    pub fn new_with_roles(org_id: i64, traversal_paths: Vec<AuthorizedPath>) -> Result<Self> {
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
            realm: None,
            is_gitlab_team_member: false,
        })
    }

    pub fn with_role(mut self, admin: bool, min_access_level: Option<u32>) -> Self {
        self.admin = admin;
        self.access_level = min_access_level.and_then(AccessLevel::from_u32);
        self
    }

    pub fn with_realm(mut self, realm: Option<Realm>) -> Self {
        self.realm = realm;
        self
    }

    pub fn with_team_member(mut self, is_gitlab_team_member: bool) -> Self {
        self.is_gitlab_team_member = is_gitlab_team_member;
        self
    }

    pub fn paths_at_least(&self, required_access_level: u32) -> Vec<&TraversalPath> {
        self.traversal_paths
            .iter()
            .filter(|tp| {
                tp.access_levels
                    .iter()
                    .any(|level| *level >= required_access_level)
            })
            .map(|tp| &tp.path)
            .collect()
    }

    fn validate_traversal_path(path: &TraversalPath) -> Result<()> {
        path.validate().map_err(QueryError::Security)
    }
}
