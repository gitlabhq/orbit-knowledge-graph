//! A checkout on disk. Listing is cheap and reading is not, so every file
//! found is linked into the repository filesystem where it is; the
//! filesystem reads it now only if a pass needs to, and links it either way.

use std::path::Path;

use ignore::WalkBuilder;
use rayon::prelude::*;

use super::{SourceError, Vfs};

/// Every file below `root` with git's listing semantics: .gitignore,
/// .git/info/exclude and dotfiles honored, ripgrep .ignore and ancestor
/// ignores not, `.git` itself never.
pub fn discover(root: &Path, vfs: &Vfs) -> Result<(), SourceError> {
    let walker = WalkBuilder::new(root)
        .hidden(false)
        .git_ignore(true)
        .git_exclude(true)
        .ignore(false)
        .git_global(false)
        .parents(false)
        .require_git(false)
        .filter_entry(|entry| entry.file_name() != ".git")
        .build();
    let mut paths = Vec::new();
    for entry in walker {
        let entry = entry.map_err(|e| SourceError::Io(std::io::Error::other(e)))?;
        let kind = entry.file_type();
        if !kind.is_some_and(|t| t.is_file() || t.is_symlink()) {
            continue;
        }
        if let Ok(path) = entry.path().strip_prefix(root) {
            paths.push(path.to_string_lossy().into_owned());
        }
    }
    discover_paths(root, paths, vfs)
}

/// The named files below `root`: a change set, where a walk is not wanted.
pub fn discover_paths(root: &Path, paths: Vec<String>, vfs: &Vfs) -> Result<(), SourceError> {
    paths.into_par_iter().try_for_each(|path| {
        let on_disk = root.join(&path);
        let metadata = on_disk.symlink_metadata()?;
        match metadata.is_symlink() {
            true => vfs.list(&path, metadata.len(), true),
            false => vfs.link(&path, on_disk, metadata.len()),
        }
    })
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

    #[test]
    fn a_change_set_is_settled_without_a_walk() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        write(root, "a.rs", b"fn a() {}");
        write(root, "b.png", b"\x89PNG");
        write(root, "untouched.rs", b"fn u() {}");

        let vfs = Vfs::new(TestFilter, None);
        discover_paths(root, vec!["b.png".into(), "a.rs".into()], &vfs).unwrap();

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
