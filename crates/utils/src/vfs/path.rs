//! Repository paths: one `/`-joined key per file, resolved lexically, with
//! symlinks followed inside the repository and nowhere else.

use std::io;
use std::path::{Component, Path};

use rustc_hash::FxHashMap;

pub(super) const MAX_LINK_DEPTH: usize = 40;

pub(super) fn is_safe_relative_path(path: &Path) -> bool {
    path.components()
        .all(|component| matches!(component, Component::Normal(_)))
}

pub(super) fn key(path: &Path) -> Option<String> {
    let mut key = String::new();
    for component in path.components() {
        match component {
            Component::Normal(part) => {
                if !key.is_empty() {
                    key.push('/');
                }
                key.push_str(&part.to_string_lossy());
            }
            Component::CurDir => {}
            Component::RootDir => key.clear(),
            Component::ParentDir => {
                if key.is_empty() {
                    return None;
                }
                key.truncate(key.rfind('/').unwrap_or(0));
            }
            Component::Prefix(_) => return None,
        }
    }
    Some(key)
}

pub(super) fn follow_first_link<'a>(
    key: &str,
    links: &'a FxHashMap<String, String>,
) -> Option<(io::Result<String>, Option<&'a str>)> {
    for end in key
        .match_indices('/')
        .map(|(index, _)| index)
        .chain(std::iter::once(key.len()))
    {
        let prefix = &key[..end];
        if let Some(target) = links.get(prefix) {
            let rest = &key[end..];
            let parent = prefix.rsplit_once('/').map_or("", |(parent, _)| parent);
            let resolved = Path::new(parent).join(target);
            return Some((
                self::key(&resolved.join(rest.trim_start_matches('/'))).ok_or_else(not_found),
                rest.is_empty().then_some(target.as_str()),
            ));
        }
    }
    None
}

pub(super) fn not_found() -> io::Error {
    io::Error::new(io::ErrorKind::NotFound, "no such file in the repository")
}
