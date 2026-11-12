use anyhow::{Result, bail};
use serde::Deserialize;

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(tag = "strategy", rename_all = "snake_case", deny_unknown_fields)]
pub enum AuthStrategy {
    Bearer {
        #[serde(default)]
        token_env: Option<String>,
    },
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

    pub fn token(&self) -> Option<String> {
        self.token_env()
            .and_then(variable)
            .filter(|token| !token.trim().is_empty())
    }

    pub fn headers(&self, secret: &str) -> Vec<(String, String)> {
        match self {
            AuthStrategy::Bearer { .. } => {
                vec![("Authorization".into(), format!("Bearer {secret}"))]
            }
        }
    }

    pub fn resolve(&self, source: &str) -> Result<Vec<(String, String)>> {
        match self.token() {
            Some(token) => Ok(self.headers(&token)),
            None => bail!(
                "{source} needs a token in {}",
                self.token_env().unwrap_or("its token variable")
            ),
        }
    }

    pub fn describe(&self) -> String {
        let env = self.token_env().unwrap_or("no variable");
        match self.token() {
            Some(_) => format!("token from {env}"),
            None => format!("{env} is not set"),
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

#[cfg(not(test))]
fn variable(name: &str) -> Option<String> {
    std::env::var(name).ok()
}

#[cfg(test)]
fn variable(name: &str) -> Option<String> {
    match name {
        "ORBIT_TEST_TOKEN_GOOD" => Some("xoxb-good".into()),
        "ORBIT_TEST_TOKEN_GRID" => Some("xoxe-grid".into()),
        _ => std::env::var(name).ok(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bearer_reads_its_variable_and_rejects_other_shapes() {
        let auth: AuthStrategy =
            orbit_utils::yaml::from_str("strategy: bearer\ntoken_env: ORBIT_TEST_TOKEN_GOOD\n")
                .unwrap();
        assert_eq!(
            auth.resolve("test").unwrap(),
            [("Authorization".to_string(), "Bearer xoxb-good".to_string())]
        );
        assert_eq!(auth.describe(), "token from ORBIT_TEST_TOKEN_GOOD");
        let unset = AuthStrategy::bearer(Some("ORBIT_TEST_TOKEN_NEVER_SET".into()));
        assert!(
            unset
                .resolve("test")
                .unwrap_err()
                .to_string()
                .contains("ORBIT_TEST_TOKEN_NEVER_SET")
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
