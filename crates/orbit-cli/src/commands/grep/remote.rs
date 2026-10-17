use std::path::{Path, PathBuf};

use anyhow::Result;
use orbit_search::query_alternatives;

use super::term::Term;
use super::{Options, Output, kind};
use crate::remote::grep::{Request, Target, scope_for, scope_order};
use crate::remote::token;

const MAX_KEYWORDS: usize = 3;
const MAX_REQUESTS: usize = 6;
const KEYWORD_BYTES: std::ops::RangeInclusive<usize> = 2..=256;

#[derive(Debug, PartialEq, Eq)]
pub(super) struct Local {
    pub(super) repo: Option<PathBuf>,
    pub(super) paths: Vec<String>,
}

#[derive(Debug, Default, PartialEq, Eq)]
pub(super) struct Route {
    pub(super) local: Vec<Local>,
    pub(super) remote: Vec<Request>,
    pub(super) skipped: Vec<String>,
}

pub(super) struct Checkouts<'a> {
    pub(super) exists: &'a dyn Fn(&str) -> bool,
    pub(super) indexed: &'a dyn Fn() -> Vec<(PathBuf, String, String)>,
}

#[derive(Debug, PartialEq, Eq)]
enum PathTarget {
    Local(String),
    GitLab(Target),
}

fn classify(path: &str, exists: &dyn Fn(&str) -> bool) -> PathTarget {
    if let Some(typed) = token::typed(path) {
        if typed.names("Project") {
            return PathTarget::GitLab(Target::ProjectId(typed.id));
        }
        if typed.names("Group") {
            return PathTarget::GitLab(Target::GroupId(typed.id));
        }
    }
    let named = token::is_url(path) || token::is_namespace_path(path);
    match !exists(path) && named {
        true => PathTarget::GitLab(Target::Namespace(path.to_string())),
        false => PathTarget::Local(path.to_string()),
    }
}

pub(super) fn route(
    query: Option<&str>,
    repo: Option<&Path>,
    paths: Vec<String>,
    kinds: &[String],
    options: &Options,
    checkouts: Checkouts<'_>,
) -> Result<Route> {
    let (mut local_paths, mut targets) = (Vec::new(), Vec::new());
    for path in paths {
        match classify(&path, checkouts.exists) {
            PathTarget::Local(path) => local_paths.push(path),
            PathTarget::GitLab(target) => targets.push(target),
        }
    }
    let mut route = Route::default();
    if targets.iter().any(|t| matches!(t, Target::Namespace(_))) {
        let indexed = (checkouts.indexed)();
        for target in &mut targets {
            let Target::Namespace(path) = target else {
                continue;
            };
            if let Some((checkout, _, full_path)) = indexed.iter().find(|(_, host, full_path)| {
                token::namespace(path, host).eq_ignore_ascii_case(full_path)
            }) {
                route.local.push(Local {
                    repo: Some(checkout.clone()),
                    paths: Vec::new(),
                });
                *target = Target::Project(full_path.clone());
            }
        }
    }
    let searchable = searchable(query, kinds, options);
    let remote_only = searchable.is_some() && !kinds.is_empty() && every_kind_hosted(kinds);
    let explicit = !targets.is_empty();
    if targets.is_empty() && local_paths.is_empty() {
        targets.push(Target::Origin(
            repo.map_or_else(|| PathBuf::from("."), Path::to_path_buf),
        ));
    }
    if !local_paths.is_empty() || !explicit {
        route.local.insert(
            0,
            Local {
                repo: repo.map(Path::to_path_buf),
                paths: local_paths,
            },
        );
    }
    if remote_only {
        route.local.clear();
    }
    let Some(keywords) = searchable else {
        let named: Vec<String> = targets
            .iter()
            .filter(|target| target.explicit())
            .map(target_name)
            .collect();
        let reason = format!(
            "GitLab search of {} needs a plain search: literal terms without -l, -c, -q, -v, \
             -g, -t, or a code is: kind",
            named.join(", ")
        );
        anyhow::ensure!(named.is_empty() || !route.local.is_empty(), "{reason}");
        if !named.is_empty() {
            route.skipped.push(reason);
        }
        return Ok(route);
    };
    let kept = targets.len().min(MAX_REQUESTS / keywords.len());
    let dropped: Vec<String> = targets.drain(kept..).map(|t| target_name(&t)).collect();
    if !dropped.is_empty() {
        route.skipped.push(format!(
            "GitLab search sends at most {MAX_REQUESTS} requests, one per term and target; \
             skipped {}",
            dropped.join(", ")
        ));
    }
    for target in targets.drain(..) {
        let scopes = match kinds.is_empty() {
            true => target.default_scopes(),
            false => hosted_scopes(kinds),
        };
        route.remote.push(Request {
            target,
            keywords: keywords.clone(),
            scopes,
        });
    }
    Ok(route)
}

fn target_name(target: &Target) -> String {
    match target {
        Target::Origin(_) => "origin".into(),
        Target::Project(path) | Target::Namespace(path) => path.clone(),
        Target::ProjectId(id) => format!("Project[{id}]"),
        Target::GroupId(id) => format!("Group[{id}]"),
    }
}

fn searchable(query: Option<&str>, kinds: &[String], options: &Options) -> Option<Vec<String>> {
    let filtered =
        !options.globs.is_empty() || !options.types.is_empty() || !options.types_not.is_empty();
    if options.output != Output::Lines || options.invert || filtered {
        return None;
    }
    if !kinds.is_empty() && hosted_scopes(kinds).is_empty() {
        return None;
    }
    let keywords: Vec<String> = query_alternatives(query?)
        .ok()?
        .into_iter()
        .filter(|alternative| options.fixed || !Term::parse(alternative).is_regex())
        .filter(|alternative| KEYWORD_BYTES.contains(&alternative.len()))
        .take(MAX_KEYWORDS)
        .collect();
    (!keywords.is_empty()).then_some(keywords)
}

fn hosted_scopes(kinds: &[String]) -> Vec<&'static str> {
    let mut scopes: Vec<&'static str> = kinds.iter().filter_map(|kind| scope_for(kind)).collect();
    scopes.sort_by_key(|scope| scope_order(scope));
    scopes.dedup();
    scopes
}

fn every_kind_hosted(kinds: &[String]) -> bool {
    let hosted = kind::resolve(kinds, &[])
        .map(|resolved| resolved.hosted)
        .unwrap_or_default();
    kinds.iter().all(|kind| {
        scope_for(kind).is_some() || hosted.iter().any(|node| node.eq_ignore_ascii_case(kind))
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn kinds(names: &[&str]) -> Vec<String> {
        names.iter().map(|name| name.to_string()).collect()
    }

    fn plan(query: &str, kinds: &[String], paths: &[&str], options: &Options) -> Result<Route> {
        route(
            Some(query),
            None,
            paths.iter().map(|path| path.to_string()).collect(),
            kinds,
            options,
            Checkouts {
                exists: &|path| path.starts_with("src"),
                indexed: &|| {
                    vec![(
                        PathBuf::from("/checkouts/gitlab"),
                        "gitlab.com".to_string(),
                        "gitlab-org/gitlab".to_string(),
                    )]
                },
            },
        )
    }

    fn here() -> Local {
        Local {
            repo: None,
            paths: Vec::new(),
        }
    }

    #[test]
    fn plain_searches_add_the_origin_project() {
        let route = plan("run_grep|Context", &[], &[], &Options::default()).unwrap();
        assert_eq!(route.local, [here()]);
        let [request] = route.remote.as_slice() else {
            panic!("expected one request");
        };
        assert_eq!(request.target, Target::Origin(".".into()));
        assert_eq!(request.keywords, ["run_grep", "Context"]);
        assert_eq!(
            request.scopes,
            [
                "work_items",
                "merge_requests",
                "blobs",
                "commits",
                "notes",
                "milestones"
            ]
        );
    }

    #[test]
    fn local_paths_and_code_filters_stay_local() {
        let route = plan("grep", &[], &["src/lib.rs"], &Options::default()).unwrap();
        assert_eq!(
            route.local,
            [Local {
                repo: None,
                paths: vec!["src/lib.rs".into()],
            }]
        );
        assert!(route.remote.is_empty());
        for options in [
            Options {
                output: Output::Files,
                ..Options::default()
            },
            Options {
                invert: true,
                ..Options::default()
            },
            Options {
                globs: vec!["*.rs".into()],
                ..Options::default()
            },
        ] {
            assert!(
                plan("grep", &[], &[], &options).unwrap().remote.is_empty(),
                "{options:?}"
            );
        }
        assert!(
            plan("fn .*grep", &[], &[], &Options::default())
                .unwrap()
                .remote
                .is_empty()
        );
        assert!(
            plan("grep", &kinds(&["function"]), &[], &Options::default())
                .unwrap()
                .remote
                .is_empty()
        );
    }

    #[test]
    fn gitlab_targets_replace_the_checkout() {
        let route = plan(
            "grep",
            &[],
            &[
                "gitlab-org/cli",
                "Group[9]",
                "Project:4",
                "gitlab.com/g/p/-/issues",
            ],
            &Options::default(),
        )
        .unwrap();
        assert!(route.local.is_empty());
        let targets: Vec<&Target> = route.remote.iter().map(|r| &r.target).collect();
        assert_eq!(
            targets,
            [
                &Target::Namespace("gitlab-org/cli".into()),
                &Target::GroupId(9),
                &Target::ProjectId(4),
                &Target::Namespace("gitlab.com/g/p/-/issues".into()),
            ]
        );
        assert!(route.remote[1].scopes.contains(&"projects"));
    }

    #[test]
    fn indexed_checkouts_search_locally_and_remotely() {
        let route = plan(
            "grep",
            &[],
            &["https://gitlab.com/GitLab-org/GitLab/-/tree/main", "src"],
            &Options::default(),
        )
        .unwrap();
        assert_eq!(
            route.local,
            [
                Local {
                    repo: None,
                    paths: vec!["src".into()],
                },
                Local {
                    repo: Some("/checkouts/gitlab".into()),
                    paths: Vec::new(),
                }
            ]
        );
        assert_eq!(
            route.remote[0].target,
            Target::Project("gitlab-org/gitlab".into())
        );
    }

    #[test]
    fn requests_are_capped() {
        let route = plan("aa|bb|cc", &[], &["g/a", "g/b", "g/c"], &Options::default()).unwrap();
        assert_eq!(route.remote.len(), 2);
        assert!(
            route.skipped[0].ends_with("skipped g/c"),
            "{:?}",
            route.skipped
        );
    }

    #[test]
    fn gitlab_targets_need_a_plain_search() {
        assert!(plan("fn .*x", &[], &["gitlab-org/cli"], &Options::default()).is_err());
        let route = plan("fn .*x", &[], &["gitlab-org/gitlab"], &Options::default()).unwrap();
        assert_eq!(route.local.len(), 1);
        assert!(route.skipped[0].starts_with("GitLab search of gitlab-org/gitlab needs"));
    }

    #[test]
    fn hosted_kinds_pick_scopes_and_skip_the_checkout() {
        let only = plan(
            "grep",
            &kinds(&["MergeRequest", "Issue"]),
            &[],
            &Options::default(),
        )
        .unwrap();
        assert!(only.local.is_empty());
        assert_eq!(only.remote[0].scopes, ["work_items", "merge_requests"]);
        let mixed = plan(
            "grep",
            &kinds(&["MergeRequest", "function"]),
            &[],
            &Options::default(),
        )
        .unwrap();
        assert_eq!(mixed.local, [here()]);
        assert_eq!(mixed.remote[0].scopes, ["merge_requests"]);
        let unsearched = plan(
            "grep",
            &kinds(&["MergeRequest", "Pipeline"]),
            &[],
            &Options::default(),
        )
        .unwrap();
        assert!(unsearched.local.is_empty());
        assert_eq!(unsearched.remote[0].scopes, ["merge_requests"]);
    }
}
