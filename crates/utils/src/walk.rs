//! Directory walk as a [`FileStreamHooks`] source: enumerate a checked-out repo
//! with git's listing semantics and run each file through the hooks. Files are
//! already on disk, so nothing is written — the walk only classifies.

use std::io::Read;
use std::path::Path;

use ignore::WalkBuilder;

use crate::fs_stream::{Decision, FileInventoryEntry, FileStreamHooks, StreamError, step};

/// Walk `root` (honoring `.gitignore`, including dotfiles so resolver inputs
/// survive), running every file through `hooks`. Returns the inventory of
/// recorded files with their [`Decision`]. Paths are relative to `root`.
pub fn walk_dir<H: FileStreamHooks>(
    root: &Path,
    hooks: &mut H,
) -> Result<Vec<FileInventoryEntry>, StreamError> {
    let mut inventory = Vec::new();
    let mut content = Vec::new();

    // git's listing semantics (matching the prior gitalisk listing): .gitignore
    // + .git/info/exclude + dotfiles, but not ripgrep .ignore or global/ancestor
    // ignores, and never `.git` itself (hidden(false) would enumerate it).
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

    for result in walker {
        let dir_entry = result.map_err(|e| StreamError::Io(std::io::Error::other(e)))?;
        let file_type = dir_entry.file_type();
        let is_file = file_type.is_some_and(|t| t.is_file());
        let is_symlink = file_type.is_some_and(|t| t.is_symlink());
        // Directories (and other non-regular, non-symlink entries) are not nodes.
        if !is_file && !is_symlink {
            continue;
        }
        let abs_path = dir_entry.path();
        let Ok(rel_path) = abs_path.strip_prefix(root) else {
            continue;
        };
        let size = abs_path.symlink_metadata().map(|m| m.len()).unwrap_or(0);
        let mut meta = FileInventoryEntry {
            path: rel_path.to_string_lossy().into_owned(),
            size,
            decision: Decision::ListOnly,
        };

        // A symlink has no content to sniff and is never a parse candidate; the
        // hooks settle it (and record why), same as the tar source.
        meta.decision = if is_symlink {
            hooks.on_non_regular(&meta)
        } else {
            step(hooks, &meta, &mut content, |buf| {
                std::fs::File::open(abs_path)?.read_to_end(buf).map(|_| ())
            })?
        };
        if meta.decision != Decision::Drop {
            inventory.push(meta);
        }
    }

    Ok(crate::fs_stream::canonicalize_inventory(inventory))
}

#[cfg(test)]
mod tests {
    use super::*;

    struct TestFilter;
    impl FileStreamHooks for TestFilter {
        fn on_header(&mut self, f: &FileInventoryEntry) -> Option<Decision> {
            (Path::new(&f.path).extension().and_then(|e| e.to_str()) == Some("png"))
                .then_some(Decision::ListOnly)
        }
        fn on_content(&mut self, _f: &FileInventoryEntry, content: &[u8]) -> Decision {
            if content.contains(&0) {
                Decision::ListOnly
            } else {
                Decision::Parse
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

        struct KeepAll;
        impl FileStreamHooks for KeepAll {}
        let inv = walk_dir(root, &mut KeepAll).unwrap();
        let has = |p: &str| inv.iter().any(|e| e.path == p);

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

    #[test]
    fn records_files_with_decisions_and_respects_gitignore() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        write(root, "src/main.rs", b"fn main() {}");
        write(root, "assets/logo.png", b"\x89PNGdata");
        write(root, "model/weights.bin", b"\x00\x01blob");
        write(root, ".gitignore", b"ignored/\n");
        write(root, "ignored/secret.rs", b"fn secret() {}");

        let inv = walk_dir(root, &mut TestFilter).unwrap();
        let by_path = |p: &str| inv.iter().find(|e| e.path == p);

        assert!(
            by_path("ignored/secret.rs").is_none(),
            "gitignored file must be skipped"
        );
        assert_eq!(by_path("src/main.rs").unwrap().decision, Decision::Parse);
        assert_eq!(
            by_path("assets/logo.png").unwrap().decision,
            Decision::ListOnly
        );
        assert_eq!(
            by_path("model/weights.bin").unwrap().decision,
            Decision::ListOnly
        );
        assert!(by_path(".gitignore").is_some(), "dotfiles must be listed");
    }

    #[test]
    #[cfg(unix)]
    fn symlink_is_a_bare_node_not_followed() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        write(root, "src/lib.rs", b"pub fn x() {}");
        std::os::unix::fs::symlink("src/lib.rs", root.join("link.rs")).unwrap();

        let inv = walk_dir(root, &mut TestFilter).unwrap();
        let by_path = |p: &str| inv.iter().find(|e| e.path == p);

        // Routed through on_non_regular (default ListOnly), not read as content —
        // so the `.rs` symlink is a node, not a parse candidate.
        assert_eq!(by_path("link.rs").unwrap().decision, Decision::ListOnly);
        assert_eq!(by_path("src/lib.rs").unwrap().decision, Decision::Parse);
    }
}
