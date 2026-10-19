use std::sync::LazyLock;

static ISSUABLE: LazyLock<regex::Regex> = LazyLock::new(|| {
    regex::Regex::new(r"^[A-Za-z0-9_.-]+(?:/[A-Za-z0-9_.-]+)*[!#][0-9]+$").unwrap()
});

static RELATIVE: LazyLock<regex::Regex> =
    LazyLock::new(|| regex::Regex::new(r"^[!#][0-9]+$").unwrap());

static NAMESPACE: LazyLock<regex::Regex> = LazyLock::new(|| {
    regex::Regex::new(r"^[A-Za-z0-9_][A-Za-z0-9_.-]*(?:/[A-Za-z0-9_][A-Za-z0-9_.-]*)*$").unwrap()
});

const SCHEMES: &[&str] = &["https://", "http://"];

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Typed<'a> {
    pub(crate) node: &'a str,
    pub(crate) id: i64,
    bracketed: bool,
}

impl Typed<'_> {
    pub(crate) fn names(&self, name: &str) -> bool {
        match self.bracketed {
            true => name.eq_ignore_ascii_case(self.node),
            false => name == self.node,
        }
    }
}

pub(crate) fn typed(target: &str) -> Option<Typed<'_>> {
    let bracketed = target
        .strip_suffix(']')
        .and_then(|target| target.split_once('['));
    let (node, id) = bracketed.or_else(|| target.split_once(':'))?;
    let valid = !node.is_empty()
        && node.bytes().all(|byte| byte.is_ascii_alphanumeric())
        && !id.is_empty()
        && id.bytes().all(|byte| byte.is_ascii_digit());
    Some(Typed {
        node,
        id: valid.then(|| id.parse().ok()).flatten()?,
        bracketed: bracketed.is_some(),
    })
}

pub(crate) fn entity_reference(target: &str) -> Option<String> {
    let schemed = target.starts_with("gid://") || SCHEMES.iter().any(|s| target.starts_with(s));
    if schemed || ISSUABLE.is_match(target) {
        return Some(target.to_string());
    }
    let host = target.split('/').next().unwrap_or_default();
    let hosted = host.contains('.') || host.starts_with("localhost");
    (hosted && target.contains("/-/")).then(|| format!("https://{target}"))
}

pub(crate) fn is_relative_reference(target: &str) -> bool {
    RELATIVE.is_match(target)
}

pub(crate) fn is_namespace_path(target: &str) -> bool {
    NAMESPACE.is_match(target)
        && !target
            .rsplit('/')
            .next()
            .is_some_and(|last| last.contains('.'))
}

pub(crate) fn url_host(target: &str) -> Option<String> {
    if SCHEMES.iter().any(|scheme| target.starts_with(scheme)) {
        return reqwest::Url::parse(target)
            .ok()?
            .host_str()
            .map(str::to_string);
    }
    target.contains("/-/").then(|| {
        target
            .split(['/', ':'])
            .next()
            .unwrap_or_default()
            .to_string()
    })
}

pub(crate) fn is_url(target: &str) -> bool {
    SCHEMES.iter().any(|scheme| target.starts_with(scheme)) || target.contains("/-/")
}

pub(crate) fn namespace(target: &str, api_host: &str) -> String {
    let parsed = SCHEMES
        .iter()
        .any(|scheme| target.starts_with(scheme))
        .then(|| reqwest::Url::parse(target).ok())
        .flatten();
    let mut path = match &parsed {
        Some(url) => url.path(),
        None => target.split(['?', '#']).next().unwrap_or_default(),
    };
    path = path.split("/-/").next().unwrap_or_default();
    if parsed.is_none()
        && let Some((host, rest)) = path.split_once('/')
        && host.split(':').next() == Some(api_host)
    {
        path = rest;
    }
    path = path.trim_start_matches('/');
    path = path.strip_prefix("groups/").unwrap_or(path);
    path.trim_matches('/').trim_end_matches(".git").to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn entity_references_take_every_form_the_context_endpoint_reads() {
        for (target, expected) in [
            ("gitlab-org/gitlab!123", "gitlab-org/gitlab!123"),
            ("gitlab-org/sub/proj#9", "gitlab-org/sub/proj#9"),
            ("gitlab-org#5", "gitlab-org#5"),
            ("gid://gitlab/MergeRequest/1", "gid://gitlab/MergeRequest/1"),
            (
                "https://gitlab.com/g/p/-/merge_requests/1",
                "https://gitlab.com/g/p/-/merge_requests/1",
            ),
            (
                "gitlab.com/g/p/-/merge_requests/1",
                "https://gitlab.com/g/p/-/merge_requests/1",
            ),
            (
                "localhost:3000/g/p/-/issues/2",
                "https://localhost:3000/g/p/-/issues/2",
            ),
        ] {
            assert_eq!(
                entity_reference(target).as_deref(),
                Some(expected),
                "{target}"
            );
        }
        for local in [
            "Foo#bar",
            "Module::Class#method",
            "src/a/-/b.rs",
            "src/main.rs",
        ] {
            assert_eq!(entity_reference(local), None, "{local}");
        }
        assert!(is_relative_reference("!42"));
        assert!(is_relative_reference("#7"));
        assert!(!is_relative_reference("#a"));
    }
}
