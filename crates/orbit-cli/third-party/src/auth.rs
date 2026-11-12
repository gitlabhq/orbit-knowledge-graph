use anyhow::Result;
use serde::Deserialize;

use crate::credentials::{self, TokenFrom};

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(tag = "strategy", rename_all = "snake_case", deny_unknown_fields)]
pub enum AuthStrategy {
    Bearer {
        #[serde(default)]
        token_env: Option<String>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Credential {
    pub secret: String,
    pub from: TokenFrom,
}

impl AuthStrategy {
    pub fn bearer(token_env: Option<String>) -> Self {
        AuthStrategy::Bearer { token_env }
    }

    pub fn name(&self) -> &'static str {
        match self {
            AuthStrategy::Bearer { .. } => "bearer",
        }
    }

    pub fn token_env(&self) -> Option<&str> {
        match self {
            AuthStrategy::Bearer { token_env } => token_env.as_deref(),
        }
    }

    pub fn set_token_env(&mut self, env: String) {
        match self {
            AuthStrategy::Bearer { token_env } => *token_env = Some(env),
        }
    }

    pub fn credential(&self, source: &str) -> Option<Credential> {
        credentials::resolve(source, self.token_env())
            .map(|(secret, from)| Credential { secret, from })
    }

    pub fn headers(&self, secret: &str) -> Vec<(String, String)> {
        match self {
            AuthStrategy::Bearer { .. } => {
                vec![("Authorization".into(), format!("Bearer {secret}"))]
            }
        }
    }

    pub fn resolve(&self, source: &str) -> Result<Vec<(String, String)>> {
        Ok(self
            .credential(source)
            .map(|credential| self.headers(&credential.secret))
            .unwrap_or_default())
    }

    pub fn describe(&self, source: &str) -> String {
        match self.credential(source).map(|credential| credential.from) {
            Some(TokenFrom::Env) => format!("token from {}", self.token_env().unwrap_or_default()),
            Some(TokenFrom::Keychain) => "token in the OS keychain".into(),
            None => "no token".into(),
        }
    }
}

pub fn apply(
    mut request: reqwest::RequestBuilder,
    headers: &[(String, String)],
) -> reqwest::RequestBuilder {
    for (name, value) in headers {
        request = request.header(name, value);
    }
    request
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bearer_sends_one_header_and_rejects_other_shapes() {
        let auth: AuthStrategy =
            orbit_utils::yaml::from_str("strategy: bearer\ntoken_env: ORBIT_TEST_TOKEN_GOOD\n")
                .unwrap();
        assert_eq!(
            auth.resolve("auth-test").unwrap(),
            [("Authorization".to_string(), "Bearer xoxb-good".to_string())]
        );
        assert_eq!(
            auth.describe("auth-test"),
            "token from ORBIT_TEST_TOKEN_GOOD"
        );
        assert!(
            AuthStrategy::bearer(Some("ORBIT_TEST_TOKEN_NEVER_SET".into()))
                .resolve("auth-none")
                .unwrap()
                .is_empty()
        );
        for text in [
            "strategy: cookie\n",
            "strategy: bearer\nheader: Cookie\n",
            "token_env: X\n",
        ] {
            assert!(
                orbit_utils::yaml::from_str::<AuthStrategy>(text).is_err(),
                "{text}"
            );
        }
    }
}
