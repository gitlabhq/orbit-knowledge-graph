use std::path::Path;
use std::process::Command;

pub(crate) fn project(dir: &Path) -> Option<(String, String)> {
    split_host_and_path(&read_url(dir)?)
}

fn read_url(dir: &Path) -> Option<String> {
    let output = Command::new("git")
        .current_dir(dir)
        .args(["remote", "get-url", "origin"])
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    Some(String::from_utf8(output.stdout).ok()?.trim().to_string())
}

fn split_host_and_path(url: &str) -> Option<(String, String)> {
    let (host, path) = split_url_with_host(url).or_else(|| split_scp_remote(url))?;
    let full_path = path.trim_matches('/').trim_end_matches(".git").to_string();
    (!full_path.is_empty()).then_some((host, full_path))
}

fn split_url_with_host(url: &str) -> Option<(String, String)> {
    let url = reqwest::Url::parse(url).ok()?;
    Some((url.host_str()?.to_string(), url.path().to_string()))
}

fn split_scp_remote(url: &str) -> Option<(String, String)> {
    let (user_and_host, path) = url.split_once(':')?;
    let host = user_and_host.rsplit('@').next()?;
    Some((host.to_string(), path.to_string()))
}

#[cfg(test)]
mod tests {
    use super::split_host_and_path;

    #[test]
    fn https_and_scp_remotes_split_into_host_and_full_path() {
        for url in [
            "https://gitlab.com/gitlab-org/orbit/knowledge-graph.git",
            "git@gitlab.com:gitlab-org/orbit/knowledge-graph.git",
            "ssh://git@gitlab.com/gitlab-org/orbit/knowledge-graph",
        ] {
            assert_eq!(
                split_host_and_path(url),
                Some((
                    "gitlab.com".to_string(),
                    "gitlab-org/orbit/knowledge-graph".to_string()
                )),
                "{url}"
            );
        }
    }
}
