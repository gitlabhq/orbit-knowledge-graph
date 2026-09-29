//! Directory walk as a [`FileStreamHooks`] source. Files are already on disk, so
//! nothing is written — the walk only classifies.

use std::io::Read;
use std::path::Path;

use ignore::WalkBuilder;
use rayon::prelude::*;

use super::inventory::FileInventory;
use super::stream::{Decision, FileInventoryEntry, FileStreamHooks, StreamError, step};

/// Walk `root` (honoring `.gitignore`, including dotfiles so resolver inputs
/// survive), running every file through the hooks. Returns the inventory of
/// recorded files with their [`Decision`]. Paths are relative to `root`.
///
/// Listing is cheap and reading is not, so files are read and classified in
/// parallel, each worker with hooks of its own from `hooks`.
pub fn walk_dir<H: FileStreamHooks>(
    root: &Path,
    hooks: impl Fn() -> H + Sync,
) -> Result<FileInventory, StreamError> {
    let entries = list_files(root)?
        .par_iter()
        .map_init(
            || (hooks(), Vec::new()),
            |(hooks, content), path| classify(root, path, hooks, content),
        )
        .filter_map(Result::transpose)
        .collect::<Result<Vec<_>, _>>()?;
    Ok(FileInventory::new(entries))
}

/// Relative paths of the files and symlinks below `root`, with git's listing
/// semantics: .gitignore + .git/info/exclude + dotfiles, but not ripgrep
/// .ignore or global/ancestor ignores, and never `.git` itself.
fn list_files(root: &Path) -> Result<Vec<String>, StreamError> {
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
    let mut files = Vec::new();
    for result in walker {
        let entry = result.map_err(|e| StreamError::Io(std::io::Error::other(e)))?;
        let kind = entry.file_type();
        if !kind.is_some_and(|t| t.is_file() || t.is_symlink()) {
            continue;
        }
        if let Ok(path) = entry.path().strip_prefix(root) {
            files.push(path.to_string_lossy().into_owned());
        }
    }
    Ok(files)
}

/// The hooks' decision on one file; `None` when they drop it. A symlink has
/// no content to sniff and is never a parse candidate, so the hooks settle
/// it without a read, same as the tar source.
fn classify<H: FileStreamHooks>(
    root: &Path,
    path: &str,
    hooks: &mut H,
    content: &mut Vec<u8>,
) -> Result<Option<FileInventoryEntry>, StreamError> {
    let abs_path = root.join(path);
    let metadata = abs_path.symlink_metadata()?;
    let mut entry = FileInventoryEntry {
        path: path.to_string(),
        size: metadata.len(),
        decision: Decision::ListOnly,
        label: Default::default(),
    };
    let (decision, label) = match metadata.is_symlink() {
        true => hooks.on_non_regular(&entry),
        false => step(hooks, &entry, content, |buf| {
            std::fs::File::open(&abs_path)?.read_to_end(buf).map(drop)
        })?,
    };
    entry.decision = decision;
    entry.label = label;
    Ok((decision != Decision::Drop).then_some(entry))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fs_walk::FileLabel;

    struct TestFilter;
    impl FileStreamHooks for TestFilter {
        fn on_header(&mut self, f: &FileInventoryEntry) -> Option<(Decision, FileLabel)> {
            (Path::new(&f.path).extension().and_then(|e| e.to_str()) == Some("png"))
                .then_some((Decision::ListOnly, FileLabel::default()))
        }
        fn on_content(&mut self, _f: &FileInventoryEntry, content: &[u8]) -> (Decision, FileLabel) {
            if content.contains(&0) {
                (Decision::ListOnly, FileLabel::default())
            } else {
                (Decision::Parse, FileLabel::default())
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
        let inv = walk_dir(root, || KeepAll).unwrap();
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
    fn a_large_tree_is_listed_whole_with_every_decision() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        for i in 0..600 {
            let body: &[u8] = if i % 3 == 0 {
                b"\x00blob"
            } else {
                b"fn f() {}"
            };
            write(root, &format!("src/mod{}/file{i}.rs", i % 7), body);
        }
        write(root, "assets/logo.png", b"\x89PNG");
        write(root, ".gitignore", b"ignored/\n");
        write(root, "ignored/x.rs", b"fn x() {}");

        let inv = walk_dir(root, || TestFilter).unwrap();

        assert_eq!(inv.len(), 602, "600 sources, the png, the .gitignore");
        assert_eq!(inv.by_decision(Decision::Parse).count(), 401);
        assert!(inv.iter().map(|e| &e.path).is_sorted());
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

        let inv = walk_dir(root, || TestFilter).unwrap();
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

        let inv = walk_dir(root, || TestFilter).unwrap();
        let by_path = |p: &str| inv.iter().find(|e| e.path == p);

        assert_eq!(by_path("link.rs").unwrap().decision, Decision::ListOnly);
        assert_eq!(by_path("src/lib.rs").unwrap().decision, Decision::Parse);
    }
}
