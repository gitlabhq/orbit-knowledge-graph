use std::path::Path;

pub(crate) fn project(dir: &Path) -> Option<(String, String)> {
    split_host_and_path(&read_url(dir)?)
}

fn read_url(dir: &Path) -> Option<String> {
    crate::workspace::repository_at(dir)
        .remote_url("origin")
        .ok()?
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
    use super::*;
    use crate::test_support::git;

    #[test]
    fn project_reads_the_origin_remote() {
        let repo = tempfile::TempDir::new().unwrap();
        git(repo.path(), &["init"]);
        git(
            repo.path(),
            &[
                "remote",
                "add",
                "origin",
                "git@gitlab.com:group/sub/proj.git",
            ],
        );

        assert_eq!(
            project(repo.path()),
            Some(("gitlab.com".to_string(), "group/sub/proj".to_string()))
        );
    }

    #[test]
    fn project_is_none_without_an_origin_remote_or_repository() {
        let repo = tempfile::TempDir::new().unwrap();
        assert_eq!(project(repo.path()), None);

        git(repo.path(), &["init"]);
        assert_eq!(project(repo.path()), None);
    }
}
