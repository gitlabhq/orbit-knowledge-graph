//! Directory walk as a [`FileStreamHooks`] source. Files are already on disk, so
//! nothing is written — the walk only classifies.

use std::io::Read;
use std::path::Path;

use ignore::WalkBuilder;

use super::inventory::FileInventory;
use super::stream::{Decision, FileInventoryEntry, FileStreamHooks, StreamError, step};

/// Walk `root` (honoring `.gitignore`, including dotfiles so resolver inputs
/// survive), running every file through `hooks`. Returns the inventory of
/// recorded files with their [`Decision`]. Paths are relative to `root`.
pub fn walk_dir<H: FileStreamHooks>(
    root: &Path,
    hooks: &mut H,
) -> Result<FileInventory, StreamError> {
    let mut inventory = Vec::new();
    let mut content = Vec::new();
    for entry in list_entries(root)? {
        if let Some(meta) = entry.classify(hooks, &mut content)? {
            inventory.push(meta);
        }
    }
    Ok(FileInventory::new(inventory))
}

/// `walk_dir` with the reading and classifying of files spread over all
/// cores; the directory listing itself is cheap, reading is not. Each
/// worker gets its own hooks, so counters that must be global to the walk
/// (a total-bytes cap) do not apply here.
pub fn walk_dir_parallel<H, F>(root: &Path, hooks: F) -> Result<FileInventory, StreamError>
where
    H: FileStreamHooks,
    F: Fn() -> H + Sync,
{
    use rayon::prelude::*;
    let entries = list_entries(root)?;
    let classified: Vec<Result<Option<FileInventoryEntry>, StreamError>> = entries
        .par_chunks(256)
        .flat_map_iter(|chunk| {
            let mut hooks = hooks();
            let mut content = Vec::new();
            chunk
                .iter()
                .map(|entry| entry.classify(&mut hooks, &mut content))
                .collect::<Vec<_>>()
        })
        .collect();
    let inventory = classified
        .into_iter()
        .filter_map(Result::transpose)
        .collect::<Result<Vec<_>, _>>()?;
    Ok(FileInventory::new(inventory))
}

struct Listed {
    abs_path: std::path::PathBuf,
    meta: FileInventoryEntry,
    is_symlink: bool,
}

impl Listed {
    fn classify<H: FileStreamHooks>(
        &self,
        hooks: &mut H,
        content: &mut Vec<u8>,
    ) -> Result<Option<FileInventoryEntry>, StreamError> {
        // A symlink has no content to sniff and is never a parse candidate; the
        // hooks settle it, same as the tar source.
        let (decision, label) = if self.is_symlink {
            hooks.on_non_regular(&self.meta)
        } else {
            step(hooks, &self.meta, content, |buf| {
                std::fs::File::open(&self.abs_path)?
                    .read_to_end(buf)
                    .map(|_| ())
            })?
        };
        let mut meta = self.meta.clone();
        meta.decision = decision;
        meta.label = label;
        Ok((meta.decision != Decision::Drop).then_some(meta))
    }
}

/// Every file below `root` with git's listing semantics (matching the prior
/// gitalisk listing): .gitignore + .git/info/exclude + dotfiles, but not
/// ripgrep .ignore or global/ancestor ignores, and never `.git` itself
/// (hidden(false) would enumerate it).
fn list_entries(root: &Path) -> Result<Vec<Listed>, StreamError> {
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
    let mut listed = Vec::new();
    for result in walker {
        let dir_entry = result.map_err(|e| StreamError::Io(std::io::Error::other(e)))?;
        let file_type = dir_entry.file_type();
        let is_file = file_type.is_some_and(|t| t.is_file());
        let is_symlink = file_type.is_some_and(|t| t.is_symlink());
        if !is_file && !is_symlink {
            continue;
        }
        let abs_path = dir_entry.path();
        let Ok(rel_path) = abs_path.strip_prefix(root) else {
            continue;
        };
        let size = abs_path.symlink_metadata().map(|m| m.len()).unwrap_or(0);
        listed.push(Listed {
            abs_path: abs_path.to_path_buf(),
            meta: FileInventoryEntry {
                path: rel_path.to_string_lossy().into_owned(),
                size,
                decision: Decision::ListOnly,
                label: Default::default(),
            },
            is_symlink,
        });
    }
    Ok(listed)
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

        assert_eq!(by_path("link.rs").unwrap().decision, Decision::ListOnly);
        assert_eq!(by_path("src/lib.rs").unwrap().decision, Decision::Parse);
    }
}
