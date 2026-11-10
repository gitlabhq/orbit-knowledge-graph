//! Rails forwards the caller's User-Agent header as `x-client-user-agent`.
//! The Orbit CLI adds the `Distribution/` and `Coding-Agent/` tags to it.

/// What a User-Agent says about the client that sent the request.
///
/// `curl/8.7.1` is curl at version 8.7.1.
/// `orbit/0.158.0 (macos, aarch64) Distribution/glab Coding-Agent/codex`
/// is the Orbit CLI, started by glab, inside Codex.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct UserAgent {
    pub client_name: Option<String>,
    pub client_version: Option<String>,
    pub coding_agent: Option<String>,
    pub distribution: Option<String>,
}

impl UserAgent {
    pub fn parse(header: Option<&str>) -> Self {
        let mut tokens = header.unwrap_or_default().split_whitespace();

        let Some(client) = tokens.next() else {
            return Self::default();
        };
        let (client_name, client_version) = split_name_and_value(client);
        let mut user_agent = Self {
            client_name: plain(&client_name.to_ascii_lowercase()),
            client_version: client_version.and_then(plain),
            ..Self::default()
        };

        for token in tokens {
            match split_name_and_value(token) {
                ("Coding-Agent", agent) => user_agent.coding_agent = agent.map(String::from),
                ("Distribution", launcher) => user_agent.distribution = launcher.and_then(plain),
                _ => {}
            }
        }

        user_agent
    }
}

fn split_name_and_value(token: &str) -> (&str, Option<&str>) {
    match token.split_once('/') {
        Some((name, value)) => (name, Some(value)),
        None => (token, None),
    }
}

/// The header is free text, so only plain names and versions reach analytics.
fn plain(value: &str) -> Option<String> {
    let is_plain_char = |c: char| c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '+' | '-');
    let fits = !value.is_empty() && value.len() <= 64;

    (fits && value.chars().all(is_plain_char)).then(|| value.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(header: &str) -> UserAgent {
        UserAgent::parse(Some(header))
    }

    #[test]
    fn orbit_cli_reports_every_tag() {
        assert_eq!(
            parse("orbit/0.157.0 (darwin, aarch64) Distribution/glab Coding-Agent/claude-code"),
            UserAgent {
                client_name: Some("orbit".into()),
                client_version: Some("0.157.0".into()),
                coding_agent: Some("claude-code".into()),
                distribution: Some("glab".into()),
            }
        );
    }

    #[test]
    fn coding_agent_extracts_known_agents() {
        let cases = [
            (
                "glab/1.50.0 (linux, amd64) Coding-Agent/claude-code",
                "claude-code",
            ),
            ("glab/1.50.0 (darwin, arm64) Coding-Agent/codex", "codex"),
            ("glab/1.50.0 (windows, amd64) Coding-Agent/cursor", "cursor"),
            (
                "glab/1.50.0 (linux, amd64) Coding-Agent/opencode",
                "opencode",
            ),
            (
                "glab/DEV (linux, amd64) Coding-Agent/custom-agent_2.1",
                "custom-agent_2.1",
            ),
        ];
        for (header, expected) in cases {
            assert_eq!(
                parse(header).coding_agent.as_deref(),
                Some(expected),
                "for header: {header}"
            );
        }
    }

    #[test]
    fn script_clients_are_named_by_their_http_library() {
        let cases = [
            ("curl/8.7.1", "curl", Some("8.7.1")),
            ("Wget/1.21.4", "wget", Some("1.21.4")),
            ("python-requests/2.31.0", "python-requests", Some("2.31.0")),
            ("Python-urllib/3.12", "python-urllib", Some("3.12")),
            ("libwww-perl/6.72", "libwww-perl", Some("6.72")),
            ("Go-http-client/2.0", "go-http-client", Some("2.0")),
            ("Ruby", "ruby", None),
            ("node", "node", None),
            (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15",
                "mozilla",
                Some("5.0"),
            ),
        ];
        for (header, name, version) in cases {
            let user_agent = parse(header);
            assert_eq!(
                user_agent.client_name.as_deref(),
                Some(name),
                "for header: {header}"
            );
            assert_eq!(
                user_agent.client_version.as_deref(),
                version,
                "for header: {header}"
            );
        }
    }

    #[test]
    fn unsafe_values_are_not_reported_verbatim() {
        let user_agent = parse("my|tool/1.0<script> Distribution/a;b");
        assert_eq!(user_agent.client_name, None);
        assert_eq!(user_agent.client_version, None);
        assert_eq!(user_agent.distribution, None);
    }

    #[test]
    fn missing_user_agent_reports_nothing() {
        assert_eq!(UserAgent::parse(None), UserAgent::default());
        assert_eq!(UserAgent::parse(Some("")), UserAgent::default());
    }
}
