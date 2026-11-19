use crate::repository::git::commit::{CommitInfo, get_commits_info, get_current_commit_hash};
use crate::repository::git::current_branch::{get_current_branch, get_current_branch_name};
use crate::repository::git::path_from_bytes;
use crate::repository::git::remote::remote_url;
use crate::repository::git::worktree;
use ignore::{WalkBuilder, gitignore::GitignoreBuilder};
use std::ffi::OsStr;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::mpsc;
use std::thread;

const STATUS_ENTRY_MIN_LENGTH: usize = 4;

#[derive(Clone, Debug)]
pub struct CoreGitaliskRepository {
    pub path: String,
    pub workspace_path: String,
}

/// File information with pre-computed metadata
#[derive(Debug, Clone)]
pub struct FileInfo {
    /// Full file path
    pub path: PathBuf,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum StatusCode {
    Unmodified,  // ' ' (space)
    Modified,    // M
    TypeChanged, // T
    Added,       // A
    Deleted,     // D
    Renamed,     // R
    Copied,      // C
    Unmerged,    // U
    Untracked,   // ?
    Ignored,     // !
    Unknown,     // For any other cases
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct FileStatus {
    pub index: StatusCode,    // X status
    pub worktree: StatusCode, // Y status
}

impl FileStatus {
    pub fn from_status(status: &str) -> Option<Self> {
        if status.len() < 2 {
            return None;
        }

        let index_char = status.chars().nth(0).unwrap();
        let worktree_char = status.chars().nth(1).unwrap();

        Some(Self {
            index: StatusCode::from_char(index_char),
            worktree: StatusCode::from_char(worktree_char),
        })
    }
}

impl StatusCode {
    pub fn from_char(c: char) -> Self {
        match c {
            ' ' => Self::Unmodified,
            'M' => Self::Modified,
            'T' => Self::TypeChanged,
            'A' => Self::Added,
            'D' => Self::Deleted,
            'R' => Self::Renamed,
            'C' => Self::Copied,
            'U' => Self::Unmerged,
            '?' => Self::Untracked,
            '!' => Self::Ignored,
            'X' => Self::Unknown,
            _ => Self::Unknown,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum StagingStatus {
    Staged,     // Changes in index only
    Unstaged,   // Changes in worktree only
    Both,       // Changes in both index and worktree
    Unmodified, // No changes
    Untracked,  // File not tracked by git
    Conflicted, // Merge conflict
}

impl StagingStatus {
    pub fn from_file_status(status: &FileStatus) -> Self {
        match (status.index, status.worktree) {
            (StatusCode::Untracked, _) | (_, StatusCode::Untracked) => StagingStatus::Untracked,
            (StatusCode::Unmerged, _) | (_, StatusCode::Unmerged) => StagingStatus::Conflicted,
            (StatusCode::Unmodified, StatusCode::Unmodified) => StagingStatus::Unmodified,
            (StatusCode::Unmodified, _) => StagingStatus::Unstaged,
            (_, StatusCode::Unmodified) => StagingStatus::Staged,
            _ => StagingStatus::Both,
        }
    }
}

#[derive(Debug, Clone)]
pub struct FileStatusInfo {
    pub path: PathBuf,
    pub original_path: Option<PathBuf>, // Only present for renamed/copied files
    pub status: FileStatus,
    pub staging: StagingStatus,
}

impl FileStatusInfo {
    fn new(path: PathBuf, original_path: Option<PathBuf>, status: FileStatus) -> Self {
        // Determine staging status based on index and worktree status
        let staging = StagingStatus::from_file_status(&status);
        Self {
            path,
            original_path,
            status,
            staging,
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct GetStatusOptions {
    pub include_untracked: bool,
    pub ignore_submodules: bool,
    /// Literal paths, not globs.
    pub pathspecs: Vec<PathBuf>,
}

impl FileInfo {
    pub fn from_path(path: PathBuf) -> Self {
        Self { path }
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn extension(&self) -> &str {
        self.path
            .extension()
            .and_then(|ext| ext.to_str())
            .unwrap_or("")
    }

    pub fn name(&self) -> &str {
        self.path
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or("")
    }
}

pub struct FileInfoIterator {
    receiver: mpsc::Receiver<Result<FileInfo, std::io::Error>>,
    _walker_thread: thread::JoinHandle<()>,
}

impl Iterator for FileInfoIterator {
    type Item = Result<FileInfo, std::io::Error>;

    fn next(&mut self) -> Option<Self::Item> {
        self.receiver.recv().ok()
    }
}

#[derive(Default)]
pub struct IterFileOptions {
    pub include_ignored: bool,
    pub include_hidden: bool,
    pub exclude_patterns: Vec<String>,
}

impl CoreGitaliskRepository {
    /// Whether the untracked file at `new_path` holds exactly what HEAD has
    /// at `deleted_path`: git's own notion of a rename, before staging.
    fn moved_unstaged(&self, deleted_path: &Path, new_path: &Path) -> bool {
        let mut revision_path = std::ffi::OsString::from("HEAD:");
        revision_path.push(deleted_path);
        let committed = Command::new("git")
            .arg("show")
            .arg(revision_path)
            .current_dir(&self.path)
            .output();
        let on_disk = std::fs::read(self.join_repo_path(new_path));
        matches!((committed, on_disk), (Ok(out), Ok(bytes)) if out.status.success() && out.stdout == bytes)
    }

    fn join_repo_path(&self, path: impl AsRef<Path>) -> PathBuf {
        Path::new(&self.path).join(path)
    }

    pub fn new(path: String, workspace_path: String) -> Self {
        Self {
            path,
            workspace_path,
        }
    }

    pub fn get_current_branch(&self) -> Result<String, std::io::Error> {
        get_current_branch(&self.path)
    }

    /// `None` on a detached HEAD.
    pub fn current_branch_name(&self) -> Result<Option<String>, std::io::Error> {
        get_current_branch_name(&self.path)
    }

    pub fn remote_url(&self, name: &str) -> Result<Option<String>, std::io::Error> {
        remote_url(Path::new(&self.path), name)
    }

    pub fn get_current_commit_hash(&self) -> Result<String, std::io::Error> {
        get_current_commit_hash(Path::new(&self.path))
    }

    /// Returns `true` if this repository is a git worktree.
    pub fn is_worktree(&self) -> bool {
        worktree::is_worktree(Path::new(&self.path))
    }

    /// Resolve the parent repository's root path. For worktrees, returns
    /// the parent repo root. For regular repos, returns this repo's path.
    pub fn parent_repo_path(&self) -> Result<PathBuf, std::io::Error> {
        worktree::parent_repo_path(Path::new(&self.path))
    }

    /// The root of this checkout's working tree.
    pub fn toplevel(&self) -> Result<PathBuf, std::io::Error> {
        worktree::toplevel(Path::new(&self.path))
    }

    /// Whether this repository's ignore rules exclude `path`.
    pub fn is_ignored(&self, path: &Path) -> bool {
        worktree::is_ignored(Path::new(&self.path), path)
    }

    /// Files whose committed content differs between `base` and HEAD, in the
    /// shape `get_status` uses: the diff letter is the index status, a rename
    /// carries its old path in `original_path`.
    pub fn changes_since(&self, base: &str) -> Result<Vec<FileStatusInfo>, std::io::Error> {
        let output = Command::new("git")
            .args([
                "diff",
                "--name-status",
                "-z",
                "--end-of-options",
                base,
                "HEAD",
            ])
            .current_dir(&self.path)
            .output()?;
        if !output.status.success() {
            return Err(std::io::Error::other(format!(
                "git diff --name-status {base} HEAD failed: {}",
                String::from_utf8_lossy(&output.stderr).trim()
            )));
        }
        let mut fields = output.stdout.split(|b| *b == 0).filter(|f| !f.is_empty());
        let mut changes = Vec::new();
        while let Some(letters) = fields.next() {
            let Some(path) = fields.next() else { break };
            let letter = letters.first().map_or('X', |b| char::from(*b));
            let status = FileStatus {
                index: StatusCode::from_char(letter),
                worktree: StatusCode::Unmodified,
            };
            let renamed = matches!(letter, 'R' | 'C');
            let (path, original) = match renamed {
                true => match fields.next() {
                    Some(new_path) => (new_path, Some(self.join_repo_path(path_from_bytes(path)))),
                    None => break,
                },
                false => (path, None),
            };
            changes.push(FileStatusInfo::new(
                self.join_repo_path(path_from_bytes(path)),
                original,
                status,
            ));
        }
        Ok(changes)
    }

    pub fn get_commits_info(
        &self,
        commit_hashes: &[&str],
    ) -> Result<Vec<CommitInfo>, std::io::Error> {
        get_commits_info(Path::new(&self.path), commit_hashes)
    }

    // TODO: Implement this
    pub fn list_branches(&self) -> Result<Vec<String>, std::io::Error> {
        let branch_names: Vec<String> = vec!["main".to_string()];
        Ok(branch_names)
    }

    pub fn get_status(&self) -> Result<Vec<FileStatusInfo>, std::io::Error> {
        self.get_status_with(&GetStatusOptions {
            include_untracked: true,
            ..Default::default()
        })
    }

    pub fn get_status_with(
        &self,
        options: &GetStatusOptions,
    ) -> Result<Vec<FileStatusInfo>, std::io::Error> {
        let repo_path = Path::new(&self.path);
        if !repo_path.try_exists().unwrap_or(false) {
            return Err(std::io::Error::other("Repository path does not exist"));
        }

        let mut command = Command::new("git");
        command
            .arg("--literal-pathspecs")
            .args(["status", "--porcelain", "-z"]);
        command.arg(if options.include_untracked {
            "--untracked-files=all"
        } else {
            "--untracked-files=no"
        });
        if options.ignore_submodules {
            command.arg("--ignore-submodules=all");
        }
        let status = command
            .arg("--")
            .args(&options.pathspecs)
            .current_dir(repo_path)
            .output()?;

        if !status.status.success() {
            return Err(std::io::Error::other(format!(
                "git status failed: {}",
                String::from_utf8_lossy(&status.stderr).trim()
            )));
        }

        let entries: Vec<&[u8]> = status
            .stdout
            .split(|b| *b == 0)
            .filter(|s| !s.is_empty())
            .collect();
        let mut status_infos = Vec::new();
        let mut index = 0;
        while index < entries.len() {
            let entry = entries[index];
            index += 1;
            if entry.len() < STATUS_ENTRY_MIN_LENGTH {
                continue;
            }
            let status_str = std::str::from_utf8(&entry[0..2]).unwrap_or("");
            let path = path_from_bytes(&entry[3..]);
            let next = entries.get(index).copied();

            if status_str.contains(['R', 'C'])
                && let Some(original) = next
            {
                index += 1;
                status_infos.push(FileStatusInfo::new(
                    self.join_repo_path(path),
                    Some(self.join_repo_path(path_from_bytes(original))),
                    FileStatus::from_status(status_str).unwrap_or(FileStatus {
                        index: StatusCode::Unknown,
                        worktree: StatusCode::Unknown,
                    }),
                ));
                continue;
            }

            // An unstaged move: a deletion followed by an untracked file with
            // the deleted file's content. Git only detects renames once staged.
            if status_str == " D"
                && let Some(untracked) = next
                && untracked.len() >= STATUS_ENTRY_MIN_LENGTH
                && &untracked[0..2] == b"??"
                && self.moved_unstaged(&path, &path_from_bytes(&untracked[3..]))
            {
                index += 1;
                status_infos.push(FileStatusInfo::new(
                    self.join_repo_path(path_from_bytes(&untracked[3..])),
                    Some(self.join_repo_path(path)),
                    FileStatus {
                        index: StatusCode::Unmodified,
                        worktree: StatusCode::Renamed,
                    },
                ));
                continue;
            }

            status_infos.push(FileStatusInfo::new(
                self.join_repo_path(path),
                None,
                FileStatus::from_status(status_str).unwrap_or(FileStatus {
                    index: StatusCode::Unknown,
                    worktree: StatusCode::Unknown,
                }),
            ));
        }

        Ok(status_infos)
    }

    pub fn tracked_files<S: AsRef<OsStr>>(
        &self,
        pathspecs: &[S],
    ) -> Result<Vec<PathBuf>, std::io::Error> {
        self.list_files(&["--cached"], pathspecs)
    }

    pub fn tracked_ignored_files(&self) -> Result<Vec<PathBuf>, std::io::Error> {
        self.list_files(
            &["--cached", "--ignored", "--exclude-standard"],
            &[] as &[&str],
        )
    }

    fn list_files<S: AsRef<OsStr>>(
        &self,
        flags: &[&str],
        pathspecs: &[S],
    ) -> Result<Vec<PathBuf>, std::io::Error> {
        let output = Command::new("git")
            .arg("--literal-pathspecs")
            .args(["ls-files", "-z"])
            .args(flags)
            .arg("--")
            .args(pathspecs)
            .current_dir(&self.path)
            .output()?;
        if !output.status.success() {
            return Err(std::io::Error::other(format!(
                "git ls-files failed: {}",
                String::from_utf8_lossy(&output.stderr).trim()
            )));
        }
        Ok(output
            .stdout
            .split(|b| *b == 0)
            .filter(|path| !path.is_empty())
            .map(|path| self.join_repo_path(path_from_bytes(path)))
            .collect())
    }

    pub fn iter_repo_files(&self, options: IterFileOptions) -> FileInfoIterator {
        let (sender, receiver) = mpsc::channel();

        let path = self.path.clone();
        // Capture options values to avoid lifetime issues in the closure
        let include_ignored = options.include_ignored;
        let include_hidden = options.include_hidden;
        let exclude_patterns = options.exclude_patterns.clone();

        let walker_thread = thread::spawn(move || {
            // Build custom gitignore matcher from exclude patterns
            let custom_ignores = if !exclude_patterns.is_empty() {
                let mut builder = GitignoreBuilder::new(&path);
                for pattern in &exclude_patterns {
                    if let Err(e) = builder.add_line(None, pattern) {
                        orbit_log_fields::warn!(
                            safe(ERROR_TYPE) = "invalid_exclude_pattern",
                            expanded(ERROR_MESSAGE) = %e,
                            "Failed to add exclude pattern"
                        );
                    }
                }
                builder.build().ok()
            } else {
                None
            };

            let walker = WalkBuilder::new(&path)
                // Enable standard ignore filters when include_ignored is false.
                // When include_ignored=false, we want to respect gitignore, so we enable filters.
                // When include_ignored=true, we want to include ignored files, so we disable filters.
                //
                // This toggles, as a group, all the filters that are enabled by default:
                //
                // - [hidden()](#method.hidden)
                // - [parents()](#method.parents)
                // - [ignore()](#method.ignore)
                // - [git_ignore()](#method.git_ignore)
                // - [git_global()](#method.git_global)
                // - [git_exclude()](#method.git_exclude)
                //
                .standard_filters(!include_ignored)
                // Enables ignoring hidden files.
                // These files include `.gitignore` files, `.DS_store`, etc.
                // By default, we want to include these files to replicate
                // code editors behavior.
                .hidden(!include_hidden)
                // Whether a git repository is required to apply git-related ignore
                // rules (global rules, .gitignore and local exclude rules).
                //
                // When disabled, git-related ignore rules are applied even when searching
                // outside a git repository.
                //
                // We enable this to allow for searching outside of a git repository while
                // still applying git-related ignore rules (which is common in LSP and user
                // code editors).
                .require_git(false)
                // Filter to prevent descending into nested git repositories and .git directories
                .filter_entry(move |entry| {
                    // Skip .git directories and their contents unless explicitly including ignored files
                    if entry.file_name() == ".git" && !include_ignored {
                        return false;
                    }

                    // Skip directories that are nested git repositories
                    // We detect this by checking if a directory (at depth > 0) contains a .git subdirectory
                    if entry.file_type().is_some_and(|ft| ft.is_dir()) && entry.depth() > 0 {
                        let git_dir = entry.path().join(".git");
                        if git_dir.is_dir() {
                            return false;
                        }
                    }

                    // Apply custom exclude patterns using gitignore matching
                    if let Some(ref ignores) = custom_ignores {
                        let entry_path = entry.path();
                        // Strip the base path to get relative path for matching
                        let relative_path = entry_path.strip_prefix(&path).unwrap_or(entry_path);
                        let is_dir = entry.file_type().map(|ft| ft.is_dir()).unwrap_or(false);

                        if let ignore::Match::Ignore(_) = ignores.matched(relative_path, is_dir) {
                            return false;
                        }
                    }

                    true
                })
                .build_parallel();

            walker.run(|| {
                let sender = sender.clone();
                Box::new(move |result| {
                    match result {
                        Ok(entry) => {
                            if entry.file_type().map(|ft| ft.is_file()).unwrap_or(false) {
                                let file_info = FileInfo::from_path(entry.into_path());
                                if sender.send(Ok(file_info)).is_err() {
                                    return ignore::WalkState::Quit;
                                }
                            }
                        }
                        Err(e) => {
                            let io_error = std::io::Error::other(e.to_string());
                            if sender.send(Err(io_error)).is_err() {
                                return ignore::WalkState::Quit;
                            }
                        }
                    }
                    ignore::WalkState::Continue
                })
            });
        });

        FileInfoIterator {
            receiver,
            _walker_thread: walker_thread,
        }
    }

    pub fn process_repo_files<F>(
        &self,
        mut processor: F,
        options: IterFileOptions,
    ) -> Result<(), std::io::Error>
    where
        F: FnMut(FileInfo) -> Result<(), std::io::Error>,
    {
        for result in self.iter_repo_files(options) {
            processor(result?)?;
        }
        Ok(())
    }

    pub fn get_repo_files(
        &self,
        options: IterFileOptions,
    ) -> Result<Vec<FileInfo>, std::io::Error> {
        let mut files = Vec::new();
        self.process_repo_files(
            |file_info| {
                files.push(file_info);
                Ok(())
            },
            options,
        )?;
        Ok(files)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::TempDir;

    fn create_git_repo(path: &std::path::Path) -> std::io::Result<()> {
        let git_dir = path.join(".git");
        fs::create_dir_all(&git_dir)?;
        fs::write(
            git_dir.join("config"),
            "[core]\n\trepositoryformatversion = 0",
        )?;
        Ok(())
    }

    fn create_test_file(path: &std::path::Path, content: &str) -> std::io::Result<()> {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        fs::write(path, content)
    }

    #[test]
    fn test_nested_git_repositories_are_skipped() {
        let temp_dir = TempDir::new().unwrap();
        let root_path = temp_dir.path();

        create_git_repo(root_path).unwrap();

        create_test_file(&root_path.join("root_file.rs"), "// root file").unwrap();
        create_test_file(&root_path.join("src/main.rs"), "fn main() {}").unwrap();

        let nested_repo = root_path.join("nested_repo");
        fs::create_dir_all(&nested_repo).unwrap();
        create_git_repo(&nested_repo).unwrap();

        create_test_file(&nested_repo.join("nested_file.rs"), "// nested file").unwrap();
        create_test_file(&nested_repo.join("src/lib.rs"), "pub fn test() {}").unwrap();

        let deep_nested = nested_repo.join("deep/another_repo");
        fs::create_dir_all(&deep_nested).unwrap();
        create_git_repo(&deep_nested).unwrap();
        create_test_file(&deep_nested.join("deep_file.rs"), "// deep file").unwrap();

        let regular_dir = root_path.join("regular_dir");
        fs::create_dir_all(&regular_dir).unwrap();
        create_test_file(&regular_dir.join("regular_file.rs"), "// regular file").unwrap();

        let repo = CoreGitaliskRepository::new(
            root_path.to_string_lossy().to_string(),
            temp_dir.path().to_string_lossy().to_string(),
        );

        let files_with_ignored = repo
            .get_repo_files(IterFileOptions {
                include_ignored: true,
                include_hidden: true,
                exclude_patterns: vec![],
            })
            .unwrap();
        let file_paths_with_ignored: Vec<String> = files_with_ignored
            .iter()
            .map(|f| f.path.to_string_lossy().to_string())
            .collect();

        assert!(
            file_paths_with_ignored
                .iter()
                .any(|p| p.ends_with("root_file.rs"))
        );
        assert!(
            file_paths_with_ignored
                .iter()
                .any(|p| p.ends_with("src/main.rs"))
        );
        assert!(
            file_paths_with_ignored
                .iter()
                .any(|p| p.ends_with("regular_dir/regular_file.rs"))
        );

        assert!(
            !file_paths_with_ignored
                .iter()
                .any(|p| p.ends_with("nested_file.rs"))
        );
        assert!(
            !file_paths_with_ignored
                .iter()
                .any(|p| p.ends_with("src/lib.rs"))
        );
        assert!(
            !file_paths_with_ignored
                .iter()
                .any(|p| p.ends_with("deep_file.rs"))
        );

        println!("Found files with ignored: {file_paths_with_ignored:?}");

        assert_eq!(
            files_with_ignored.len(),
            4,
            "Expected exactly 4 files from root, regular directory, and .git/config"
        );

        let files_without_ignored = repo
            .get_repo_files(IterFileOptions {
                include_ignored: false,
                include_hidden: false,
                exclude_patterns: vec![],
            })
            .unwrap();
        let file_paths_without_ignored: Vec<String> = files_without_ignored
            .iter()
            .map(|f| f.path.to_string_lossy().to_string())
            .collect();

        assert!(
            file_paths_without_ignored
                .iter()
                .any(|p| p.ends_with("root_file.rs"))
        );
        assert!(
            file_paths_without_ignored
                .iter()
                .any(|p| p.ends_with("src/main.rs"))
        );
        assert!(
            file_paths_without_ignored
                .iter()
                .any(|p| p.ends_with("regular_dir/regular_file.rs"))
        );

        assert!(
            !file_paths_without_ignored
                .iter()
                .any(|p| p.contains(".git"))
        );

        println!("Found files without ignored: {file_paths_without_ignored:?}");

        assert_eq!(
            files_without_ignored.len(),
            3,
            "Expected exactly 3 files from root and regular directory, no .git files"
        );
    }

    #[test]
    fn test_single_repository_all_files_included() {
        let temp_dir = TempDir::new().unwrap();
        let root_path = temp_dir.path();

        create_git_repo(root_path).unwrap();

        create_test_file(&root_path.join("root.rs"), "// root").unwrap();
        create_test_file(&root_path.join("src/main.rs"), "fn main() {}").unwrap();
        create_test_file(&root_path.join("lib/utils.rs"), "pub mod utils;").unwrap();
        create_test_file(
            &root_path.join("tests/integration.rs"),
            "#[test] fn test() {}",
        )
        .unwrap();

        let repo = CoreGitaliskRepository::new(
            root_path.to_string_lossy().to_string(),
            temp_dir.path().to_string_lossy().to_string(),
        );

        let files_with_ignored = repo
            .get_repo_files(IterFileOptions {
                include_ignored: true,
                include_hidden: true,
                exclude_patterns: vec![],
            })
            .unwrap();

        assert_eq!(
            files_with_ignored.len(),
            5,
            "Expected all 5 files to be included (4 source files + .git/config)"
        );

        let file_names_with_ignored: Vec<String> = files_with_ignored
            .iter()
            .map(|f| f.path.file_name().unwrap().to_string_lossy().to_string())
            .collect();

        assert!(file_names_with_ignored.contains(&"root.rs".to_string()));
        assert!(file_names_with_ignored.contains(&"main.rs".to_string()));
        assert!(file_names_with_ignored.contains(&"utils.rs".to_string()));
        assert!(file_names_with_ignored.contains(&"integration.rs".to_string()));
        assert!(file_names_with_ignored.contains(&"config".to_string())); // .git/config

        let files_without_ignored = repo
            .get_repo_files(IterFileOptions {
                include_ignored: false,
                include_hidden: false,
                exclude_patterns: vec![],
            })
            .unwrap();

        assert_eq!(
            files_without_ignored.len(),
            4,
            "Expected 4 source files, no .git files"
        );

        let file_names_without_ignored: Vec<String> = files_without_ignored
            .iter()
            .map(|f| f.path.file_name().unwrap().to_string_lossy().to_string())
            .collect();

        assert!(file_names_without_ignored.contains(&"root.rs".to_string()));
        assert!(file_names_without_ignored.contains(&"main.rs".to_string()));
        assert!(file_names_without_ignored.contains(&"utils.rs".to_string()));
        assert!(file_names_without_ignored.contains(&"integration.rs".to_string()));
        assert!(!file_names_without_ignored.contains(&"config".to_string())); // Should not include .git/config
    }

    #[test]
    fn test_deeply_nested_git_repositories() {
        let temp_dir = TempDir::new().unwrap();
        let root_path = temp_dir.path();

        create_git_repo(root_path).unwrap();
        create_test_file(&root_path.join("root.rs"), "// root").unwrap();

        let level1 = root_path.join("level1");
        fs::create_dir_all(&level1).unwrap();
        create_git_repo(&level1).unwrap();
        create_test_file(&level1.join("level1.rs"), "// level1").unwrap();

        let level2 = level1.join("level2");
        fs::create_dir_all(&level2).unwrap();
        create_git_repo(&level2).unwrap();
        create_test_file(&level2.join("level2.rs"), "// level2").unwrap();

        let level3 = level2.join("level3");
        fs::create_dir_all(&level3).unwrap();
        create_git_repo(&level3).unwrap();
        create_test_file(&level3.join("level3.rs"), "// level3").unwrap();

        let repo = CoreGitaliskRepository::new(
            root_path.to_string_lossy().to_string(),
            temp_dir.path().to_string_lossy().to_string(),
        );

        let files_with_ignored = repo
            .get_repo_files(IterFileOptions {
                include_ignored: true,
                include_hidden: true,
                exclude_patterns: vec![],
            })
            .unwrap();

        assert_eq!(
            files_with_ignored.len(),
            2,
            "Expected root file and .git/config, nested repos should be skipped"
        );

        let file_names_with_ignored: Vec<String> = files_with_ignored
            .iter()
            .map(|f| f.path.file_name().unwrap().to_string_lossy().to_string())
            .collect();

        assert!(file_names_with_ignored.contains(&"root.rs".to_string()));
        assert!(file_names_with_ignored.contains(&"config".to_string())); // .git/config

        let files_without_ignored = repo
            .get_repo_files(IterFileOptions {
                include_ignored: false,
                include_hidden: false,
                exclude_patterns: vec![],
            })
            .unwrap();

        assert_eq!(
            files_without_ignored.len(),
            1,
            "Expected only root file, no .git files or nested repos"
        );

        let file_name = files_without_ignored[0]
            .path
            .file_name()
            .unwrap()
            .to_string_lossy();
        assert_eq!(file_name, "root.rs");
    }

    #[test]
    fn test_exclude_patterns() {
        let temp_dir = TempDir::new().unwrap();
        let root_path = temp_dir.path();

        create_git_repo(root_path).unwrap();

        create_test_file(&root_path.join("include.rs"), "// include").unwrap();
        create_test_file(&root_path.join("exclude.rs"), "// exclude").unwrap();
        create_test_file(&root_path.join("src/main.rs"), "fn main() {}").unwrap();
        create_test_file(&root_path.join("tests/test.rs"), "#[test] fn test() {}").unwrap();

        let repo = CoreGitaliskRepository::new(
            root_path.to_string_lossy().to_string(),
            temp_dir.path().to_string_lossy().to_string(),
        );

        // Test excluding multiple patterns
        let files = repo
            .get_repo_files(IterFileOptions {
                include_ignored: false,
                include_hidden: false,
                exclude_patterns: vec!["tests/".to_string(), "exclude.rs".to_string()],
            })
            .unwrap();

        let file_names: Vec<String> = files
            .iter()
            .map(|f| f.path.file_name().unwrap().to_string_lossy().to_string())
            .collect();

        assert!(file_names.contains(&"include.rs".to_string()));
        assert!(!file_names.contains(&"exclude.rs".to_string()));
        assert!(file_names.contains(&"main.rs".to_string()));
        assert!(!file_names.contains(&"test.rs".to_string()));
    }

    #[test]
    fn test_exclude_patterns_with_globs() {
        let temp_dir = TempDir::new().unwrap();
        let root_path = temp_dir.path();

        create_git_repo(root_path).unwrap();

        create_test_file(&root_path.join("file.rs"), "// file").unwrap();
        create_test_file(&root_path.join("file.test.rs"), "// test").unwrap();
        create_test_file(&root_path.join("file.spec.rs"), "// spec").unwrap();
        create_test_file(&root_path.join("src/lib.rs"), "// lib").unwrap();
        create_test_file(&root_path.join("src/lib.test.rs"), "// lib test").unwrap();

        let repo = CoreGitaliskRepository::new(
            root_path.to_string_lossy().to_string(),
            temp_dir.path().to_string_lossy().to_string(),
        );

        // Test glob pattern to exclude all test files
        let files = repo
            .get_repo_files(IterFileOptions {
                include_ignored: false,
                include_hidden: false,
                exclude_patterns: vec!["*.test.rs".to_string(), "*.spec.rs".to_string()],
            })
            .unwrap();

        let file_names: Vec<String> = files
            .iter()
            .map(|f| f.path.file_name().unwrap().to_string_lossy().to_string())
            .collect();

        assert!(file_names.contains(&"file.rs".to_string()));
        assert!(!file_names.contains(&"file.test.rs".to_string()));
        assert!(!file_names.contains(&"file.spec.rs".to_string()));
        assert!(file_names.contains(&"lib.rs".to_string()));
        assert!(!file_names.contains(&"lib.test.rs".to_string()));
    }
}
