//! A checkout on disk. Listing is cheap and reading is not, so every file
//! found is linked into the repository filesystem where it is; the
//! filesystem reads it now only if a pass needs to, and links it either way.

use std::io::ErrorKind;
use std::path::Path;
use std::sync::Mutex;

use ignore::{WalkBuilder, WalkState};
use rayon::prelude::*;
use tracing::warn;

use super::{SourceError, Vfs};

/// Every file below `root` with git's listing semantics: .gitignore,
/// .git/info/exclude and dotfiles honored, ripgrep .ignore and ancestor
/// ignores not, `.git` itself never. The walk is parallel and each file
/// goes into the filesystem as it is found; nothing is collected first.
pub fn discover(root: &Path, vfs: &Vfs) -> Result<(), SourceError> {
    let failed: Mutex<Option<SourceError>> = Mutex::new(None);
    WalkBuilder::new(root)
        .hidden(false)
        .git_ignore(true)
        .git_exclude(true)
        .ignore(false)
        .git_global(false)
        .parents(false)
        .require_git(false)
        .filter_entry(|entry| entry.file_name() != ".git")
        .build_parallel()
        .run(|| {
            Box::new(|entry| {
                let entry = match entry {
                    Ok(entry) => entry,
                    // A live checkout moves under us: something gone since
                    // it was listed is skipped. Anything else (a directory
                    // we may not read, an I/O fault) would make the graph
                    // claim a repository it did not see, so the run fails.
                    Err(e)
                        if e.io_error()
                            .is_some_and(|io| io.kind() == ErrorKind::NotFound) =>
                    {
                        warn!(error = %e, "skipping an entry that vanished during the walk");
                        return WalkState::Continue;
                    }
                    Err(e) => {
                        *failed.lock().unwrap_or_else(|e| e.into_inner()) =
                            Some(SourceError::Io(std::io::Error::other(e)));
                        return WalkState::Quit;
                    }
                };
                let Some(kind) = entry.file_type() else {
                    return WalkState::Continue;
                };
                if !(kind.is_file() || kind.is_symlink()) {
                    return WalkState::Continue;
                }
                let Ok(path) = entry.path().strip_prefix(root) else {
                    return WalkState::Continue;
                };
                match put(root, &path.to_string_lossy(), vfs) {
                    Ok(()) => WalkState::Continue,
                    Err(e) => {
                        *failed.lock().unwrap_or_else(|e| e.into_inner()) = Some(e);
                        WalkState::Quit
                    }
                }
            })
        });
    match failed.into_inner().unwrap_or_else(|e| e.into_inner()) {
        Some(e) => Err(e),
        None => Ok(()),
    }
}

/// The named files below `root`: a change set, where a walk is not wanted.
pub fn discover_paths(root: &Path, paths: Vec<String>, vfs: &Vfs) -> Result<(), SourceError> {
    paths
        .into_par_iter()
        .try_for_each(|path| put(root, &path, vfs))
}

fn put(root: &Path, path: &str, vfs: &Vfs) -> Result<(), SourceError> {
    let on_disk = root.join(path);
    // A live checkout moves under us; a file gone between the listing and
    // here is not a file of the repository, not a failed run.
    let metadata = match on_disk.symlink_metadata() {
        Ok(metadata) => metadata,
        Err(e) => {
            warn!(path, error = %e, "skipping a file that vanished during discovery");
            return Ok(());
        }
    };
    match metadata.is_symlink() {
        true => vfs.list(path, metadata.len(), true),
        false => vfs.link(path, on_disk, metadata.len()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::files::{CapExceeded, Decision, File, Need, Pass, SkipReason};

    /// Drops pngs and symlinks at the header, NUL-bearing files at the
    /// content; wants to see manifests now. The shape of the production
    /// `CodeFilter`.
    struct TestFilter;
    impl Pass for TestFilter {
        fn header(&self, f: &mut File) -> Result<Need, CapExceeded> {
            if f.symlink || f.path.ends_with(".png") {
                f.decision = Decision::ListOnly;
                return Ok(Need::Nothing);
            }
            if f.path.ends_with(".toml") {
                f.decision = Decision::Load;
                return Ok(Need::Bytes);
            }
            Ok(Need::Nothing)
        }
        fn content(&self, f: &mut File, content: &[u8]) {
            if content.contains(&0) {
                f.decision = Decision::ListOnly;
                f.label.skip = Some(SkipReason::Binary);
            }
        }
    }

    fn write(root: &Path, rel: &str, body: &[u8]) {
        let path = root.join(rel);
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(path, body).unwrap();
    }

    #[test]
    fn matches_git_listing_semantics() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        write(root, "src/main.rs", b"fn main(){}");
        write(root, ".git/config", b"[core]\n");
        write(root, ".git/HEAD", b"ref: x\n");
        write(root, ".gitignore", b"build/\n");
        write(root, "build/out.rs", b"compiled\n");
        write(root, ".ignore", b"notes/\n");
        write(root, "notes/x.rs", b"note\n");
        write(root, ".env", b"secret\n");

        let vfs = Vfs::default();
        discover(root, &vfs).unwrap();
        let has = |p: &str| vfs.exists(Path::new(p));

        assert!(
            !has(".git/config") && !has(".git/HEAD"),
            "the .git dir must not be listed"
        );
        assert!(!has("build/out.rs"), ".gitignore must be honored");
        assert!(
            has("notes/x.rs"),
            ".ignore files are not a git concept and must not be honored"
        );
        assert!(
            has(".gitignore") && has(".env"),
            "dotfiles must be included"
        );
        assert!(has("src/main.rs"));
    }

    /// Discovery settles what the header can settle; a file whose fate needs
    /// its bytes stays a parse candidate until the one read at `source`,
    /// where a content pass can still turn it down.
    #[test]
    #[cfg(unix)]
    fn header_settles_at_discovery_and_content_settles_at_source() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        write(root, "src/main.rs", b"fn main() {}");
        write(root, "Cargo.toml", b"[package]");
        write(root, "assets/logo.png", b"\x89PNGdata");
        write(root, "model/weights.bin", b"\x00\x01blob");
        write(root, ".gitignore", b"ignored/\n");
        write(root, "ignored/secret.rs", b"fn secret() {}");
        std::os::unix::fs::symlink("src/main.rs", root.join("link.rs")).unwrap();

        let vfs = Vfs::new(TestFilter, None);
        discover(root, &vfs).unwrap();
        let decision = |p: &str| vfs.file(Path::new(p)).unwrap().decision;

        assert!(!vfs.exists(Path::new("ignored/secret.rs")));
        assert_eq!(decision("assets/logo.png"), Decision::ListOnly);
        assert_eq!(decision("link.rs"), Decision::ListOnly);
        assert_eq!(decision("model/weights.bin"), Decision::Parse);
        assert_eq!(vfs.content_id(Path::new("src/main.rs")), None, "linked");
        assert_eq!(
            decision("Cargo.toml"),
            Decision::Load,
            "decided at discovery"
        );
        assert_eq!(
            vfs.content_id(Path::new("Cargo.toml")),
            None,
            "but not copied"
        );

        assert!(vfs.source(Path::new("src/main.rs")).is_ok());
        assert!(vfs.source(Path::new("model/weights.bin")).is_err());
        assert_eq!(decision("model/weights.bin"), Decision::ListOnly);
        assert_eq!(
            vfs.read_to_string(Path::new("Cargo.toml")).unwrap(),
            "[package]"
        );
    }

    /// A directory the walk may not read is not a smaller repository; it is
    /// a failed run.
    #[test]
    #[cfg(unix)]
    fn an_unreadable_directory_fails_the_walk() {
        use std::os::unix::fs::PermissionsExt;
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        write(root, "src/main.rs", b"fn main() {}");
        write(root, "secret/hidden.rs", b"fn hidden() {}");
        let locked = root.join("secret");
        std::fs::set_permissions(&locked, std::fs::Permissions::from_mode(0o000)).unwrap();
        if std::fs::read_dir(&locked).is_ok() {
            return; // running as root: permissions do not bind, nothing to test
        }

        let result = discover(root, &Vfs::default());

        std::fs::set_permissions(&locked, std::fs::Permissions::from_mode(0o755)).unwrap();
        assert!(result.is_err(), "an unreadable subtree must fail the run");
    }

    #[test]
    fn a_change_set_is_settled_without_a_walk() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        write(root, "a.rs", b"fn a() {}");
        write(root, "b.png", b"\x89PNG");
        write(root, "untouched.rs", b"fn u() {}");

        let vfs = Vfs::new(TestFilter, None);
        let paths = vec!["b.png".into(), "a.rs".into(), "vanished.rs".into()];
        discover_paths(root, paths, &vfs).unwrap();

        let listed: Vec<(String, Decision)> = vfs
            .files()
            .into_iter()
            .map(|f| (f.path, f.decision))
            .collect();
        assert_eq!(
            listed,
            [
                ("a.rs".into(), Decision::Parse),
                ("b.png".into(), Decision::ListOnly)
            ]
        );
    }
}
