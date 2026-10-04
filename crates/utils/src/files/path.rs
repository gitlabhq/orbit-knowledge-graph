//! Repository paths: one `/`-joined key per file, resolved lexically, with
//! symlinks followed inside the repository and nowhere else.

use std::io;
use std::path::{Component, Path, PathBuf};

use rustc_hash::FxHashMap;

/// How many links a path may pass through before it is a loop.
pub(super) const MAX_LINK_DEPTH: usize = 40;

/// A repo-relative `/`-joined key; the repository root is `""`. `.` and `..`
/// resolve lexically; `None` if the path climbs above the root.
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

/// Replace the first symlink component of `key` with its target: the rest of
/// the key follows. `None` when no component is a symlink; an error when the
/// target climbs out of the repository.
pub(super) fn follow_first_link(
    key: &str,
    links: &FxHashMap<String, String>,
) -> Option<io::Result<String>> {
    let mut end = 0;
    loop {
        end = match key[end..].find('/') {
            Some(i) => end + i,
            None => key.len(),
        };
        let prefix = &key[..end];
        if let Some(target) = links.get(prefix) {
            let rest = &key[end..];
            let parent = prefix.rsplit_once('/').map_or("", |(parent, _)| parent);
            let resolved = match target.starts_with('/') {
                true => PathBuf::from(target),
                false => Path::new(parent).join(target),
            };
            return Some(
                self::key(&resolved)
                    .map(|k| format!("{k}{rest}"))
                    .ok_or_else(not_found),
            );
        }
        if end == key.len() {
            return None;
        }
        end += 1;
    }
}

pub(super) fn not_found() -> io::Error {
    io::Error::new(io::ErrorKind::NotFound, "no such file in the repository")
}
