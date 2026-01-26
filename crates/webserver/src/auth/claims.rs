use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Claims {
    pub sub: String,
    pub iss: String,
    pub aud: String,
    pub iat: i64,
    pub exp: i64,
    pub user_id: u64,
    pub username: String,
    #[serde(default)]
    pub admin: bool,
    #[serde(default)]
    pub organization_id: Option<u64>,
    #[serde(default)]
    pub min_access_level: Option<u32>,
    #[serde(default)]
    pub group_traversal_ids: Vec<String>,
    #[serde(default)]
    pub project_ids: Vec<u64>,
}

impl Claims {
    pub fn user_id(&self) -> u64 {
        self.user_id
    }

    pub fn username(&self) -> &str {
        &self.username
    }

    pub fn is_admin(&self) -> bool {
        self.admin
    }

    pub fn has_project_access(&self, project_id: u64) -> bool {
        self.project_ids.contains(&project_id)
    }

    pub fn has_group_access(&self, group_traversal_id: &str) -> bool {
        self.group_traversal_ids
            .iter()
            .any(|id| group_traversal_id.starts_with(id))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_claims() -> Claims {
        Claims {
            sub: "user:123".to_string(),
            iss: "gitlab".to_string(),
            aud: "gitlab-knowledge-graph".to_string(),
            iat: 1234567890,
            exp: 1234568190,
            user_id: 123,
            username: "testuser".to_string(),
            admin: false,
            organization_id: Some(1),
            min_access_level: Some(10),
            group_traversal_ids: vec!["1-22-".to_string(), "1-33-44-".to_string()],
            project_ids: vec![1, 2, 3],
        }
    }

    #[test]
    fn test_user_id() {
        let claims = sample_claims();
        assert_eq!(claims.user_id(), 123);
    }

    #[test]
    fn test_username() {
        let claims = sample_claims();
        assert_eq!(claims.username(), "testuser");
    }

    #[test]
    fn test_is_admin() {
        let claims = sample_claims();
        assert!(!claims.is_admin());
    }

    #[test]
    fn test_has_project_access() {
        let claims = sample_claims();
        assert!(claims.has_project_access(1));
        assert!(claims.has_project_access(2));
        assert!(!claims.has_project_access(999));
    }

    #[test]
    fn test_has_group_access() {
        let claims = sample_claims();
        assert!(claims.has_group_access("1-22-"));
        assert!(claims.has_group_access("1-22-55-"));
        assert!(!claims.has_group_access("9-99-"));
    }

    #[test]
    fn test_deserialize_claims() {
        let json = r#"{
            "sub": "user:123",
            "iss": "gitlab",
            "aud": "gitlab-knowledge-graph",
            "iat": 1234567890,
            "exp": 1234568190,
            "user_id": 123,
            "username": "testuser",
            "admin": false,
            "organization_id": 1,
            "min_access_level": 10,
            "group_traversal_ids": ["1-22-", "1-33-44-"],
            "project_ids": [1, 2, 3]
        }"#;

        let claims: Claims = serde_json::from_str(json).unwrap();
        assert_eq!(claims.user_id, 123);
        assert_eq!(claims.username, "testuser");
        assert_eq!(claims.project_ids, vec![1, 2, 3]);
    }
}
