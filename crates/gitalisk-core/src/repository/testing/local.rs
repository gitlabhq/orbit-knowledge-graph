use std::path::{Path, PathBuf};
use std::time::Duration;
use std::{fs, process::Command};
use tempfile::TempDir;

fn copy_dir_all(src: &Path, dst: &Path) -> std::io::Result<()> {
    if !dst.exists() {
        fs::create_dir_all(dst)?;
    }

    for entry in fs::read_dir(src)? {
        let entry = entry?;
        let src_path = entry.path();
        let dst_path = dst.join(entry.file_name());

        if src_path.is_dir() {
            copy_dir_all(&src_path, &dst_path)?;
        } else {
            fs::copy(&src_path, &dst_path)?;
        }
    }

    Ok(())
}

struct LocalGitCommand {
    repo_path: PathBuf,
    command: String,
    args: Vec<String>,
}

impl LocalGitCommand {
    pub fn new(repo_path: &Path, command: &str, args: &[&str]) -> Self {
        Self {
            repo_path: repo_path.to_path_buf(),
            command: command.to_string(),
            args: args.iter().map(|&s| s.to_string()).collect(),
        }
    }

    pub fn execute(self) -> String {
        match Command::new(&self.command)
            .args(&self.args)
            .current_dir(&self.repo_path)
            .output()
        {
            Ok(output) => {
                if !output.status.success() {
                    panic!(
                        "Failed to execute command (command: {}, args: {:?}): {}\n{}",
                        self.command,
                        self.args,
                        output.status,
                        String::from_utf8_lossy(&output.stderr)
                    );
                }
                String::from_utf8_lossy(&output.stdout).to_string()
            }
            Err(e) => {
                panic!("Failed to execute command: {e:?}");
            }
        }
    }
}

pub struct LocalGitRepoFileSystem {
    repo_path: PathBuf,
    sleep_time: Option<Duration>,
}

impl LocalGitRepoFileSystem {
    pub fn new(repo_path: PathBuf, sleep_time: Option<Duration>) -> Self {
        Self {
            repo_path,
            sleep_time,
        }
    }

    fn sleep(&self) {
        if let Some(sleep_time) = self.sleep_time {
            std::thread::sleep(sleep_time);
        }
    }

    fn valid_rel_path(&self, rel_path: &str) -> Result<PathBuf, String> {
        let path = self.repo_path.join(rel_path);
        if !path.exists() {
            return Err(format!("File {path:?} does not exist"));
        }
        Ok(path)
    }

    pub fn file_content(&self, rel_path: &str) -> Result<String, String> {
        let path = self.valid_rel_path(rel_path).unwrap();
        let content = fs::read_to_string(&path).unwrap();
        self.sleep();
        Ok(content)
    }

    fn mut_file(&mut self, rel_path: &str, mut_fn: impl FnOnce(&mut String)) -> &mut Self {
        let path = self.valid_rel_path(rel_path).unwrap();
        let mut content = fs::read_to_string(&path).unwrap();
        mut_fn(&mut content);
        fs::write(&path, content).unwrap();
        self.sleep();
        self
    }

    pub fn replace_file_content(&mut self, rel_path: &str, from: &str, to: &str) -> &mut Self {
        self.mut_file(rel_path, |content| {
            *content = content.replace(from, to);
        });
        self
    }

    pub fn prefix_file_content(&mut self, rel_path: &str, prefix: &str) -> &mut Self {
        self.mut_file(rel_path, |content| {
            *content = format!("{prefix}{content}");
        });
        self
    }

    pub fn append_file_content(&mut self, rel_path: &str, suffix: &str) -> &mut Self {
        self.mut_file(rel_path, |content| {
            *content = format!("{content}{suffix}");
        });
        self
    }

    pub fn create_file(&mut self, rel_path: &str, content: Option<&str>) -> &mut Self {
        let path = self.repo_path.join(rel_path);
        if path.exists() {
            panic!("File {path:?} already exists");
        }

        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).unwrap();
        }
        match content {
            Some(content) => fs::write(&path, content).unwrap(),
            None => fs::write(&path, "").unwrap(),
        }
        self.sleep();
        self
    }

    pub fn delete_file(&mut self, rel_path: &str) -> &mut Self {
        let path = self.valid_rel_path(rel_path).unwrap();
        fs::remove_file(&path).unwrap();
        self.sleep();
        self
    }

    pub fn move_to(&mut self, rel_path: &str, new_rel_path: &str) -> &mut Self {
        let path = self.valid_rel_path(rel_path).unwrap();
        let new_path = self.repo_path.join(new_rel_path);

        if let Some(parent) = new_path.parent() {
            fs::create_dir_all(parent).unwrap();
        }

        fs::rename(&path, &new_path).unwrap();
        self.sleep();
        self
    }

    pub fn rename_to(&mut self, rel_path: &str, new_rel_path: &str) -> &mut Self {
        let path = self.valid_rel_path(rel_path).unwrap();
        let new_path = self.repo_path.join(new_rel_path);

        if let Some(parent) = new_path.parent() {
            fs::create_dir_all(parent).unwrap();
        }

        fs::rename(&path, &new_path).unwrap();
        self.sleep();
        self
    }

    pub fn create_directory(&mut self, rel_path: &str) -> &mut Self {
        let path = self.repo_path.join(rel_path);
        if path.exists() {
            panic!("Directory {path:?} already exists");
        }
        fs::create_dir_all(&path).unwrap();
        self.sleep();
        self
    }
}

pub struct LocalGitRepository {
    _temp_dir: TempDir,
    pub workspace_path: PathBuf,
    pub path: PathBuf,
    pub fs: LocalGitRepoFileSystem,
}

impl Default for LocalGitRepository {
    fn default() -> Self {
        Self::new(None)
    }
}

impl LocalGitRepository {
    pub fn new(sleep_time: Option<Duration>) -> Self {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().to_path_buf();
        let test_repo_path = path.join("test-repo");
        fs::create_dir_all(&test_repo_path).unwrap();
        let mut repo = Self {
            _temp_dir: temp_dir,
            workspace_path: path.clone(),
            path: test_repo_path.clone(),
            fs: LocalGitRepoFileSystem::new(test_repo_path.clone(), sleep_time),
        };
        repo.init().config_author();
        repo
    }

    fn init(&mut self) -> &mut Self {
        LocalGitCommand::new(&self.path, "git", &["init", "-b", "master"]).execute();
        self
    }

    fn config_author(&mut self) -> &mut Self {
        LocalGitCommand::new(
            &self.path,
            "git",
            &["config", "--local", "user.name", "test-gl-user"],
        )
        .execute();
        LocalGitCommand::new(
            &self.path,
            "git",
            &["config", "--local", "user.email", "test-gl-user@gitlab.com"],
        )
        .execute();
        self
    }

    pub fn copy_dir(&mut self, source: &Path) -> &mut Self {
        copy_dir_all(source, &self.path).unwrap();
        self
    }

    pub fn add_all(&mut self) -> &mut Self {
        LocalGitCommand::new(&self.path, "git", &["add", "."]).execute();
        self
    }

    pub fn add_paths(&mut self, paths: &[&str]) -> &mut Self {
        let mut args = vec!["add", "--"];
        args.extend(paths);
        LocalGitCommand::new(&self.path, "git", &args).execute();
        self
    }

    pub fn commit(&mut self, message: &str) -> &mut Self {
        LocalGitCommand::new(&self.path, "git", &["commit", "-m", message]).execute();
        self
    }

    pub fn status(&self) -> String {
        LocalGitCommand::new(&self.path, "git", &["status", "--branch"]).execute()
    }

    pub fn status_porcelain(&self) -> String {
        LocalGitCommand::new(&self.path, "git", &["status", "--porcelain", "-z"]).execute()
    }

    pub fn user_config(&self) -> String {
        LocalGitCommand::new(&self.path, "git", &["config", "--local", "--list"]).execute()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::LazyLock;

    static FIXTURES_PATH: LazyLock<PathBuf> = LazyLock::new(|| {
        Path::new(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .unwrap()
            .parent()
            .unwrap()
            .join("fixtures/ruby/test-repo")
    });

    fn setup_test_repo(log: bool, sleep_time: Option<Duration>) -> LocalGitRepository {
        let mut repo = LocalGitRepository::new(sleep_time);
        assert!(repo.path.exists());
        assert!(FIXTURES_PATH.as_path().exists());
        repo.copy_dir(FIXTURES_PATH.as_path());
        let main_rb_path = repo.path.join("main.rb");
        assert!(main_rb_path.exists());
        if log {
            println!("LocalGitRepository path: {:?}", repo.path);
            println!("FIXTURES_PATH: {FIXTURES_PATH:?}");
            println!("main_rb_path: {main_rb_path:?}");
        }
        repo
    }

    #[test]
    fn basic_init_test() {
        let repo = setup_test_repo(true, None);
        let status = repo.status();
        assert!(status.contains("On branch master"));
    }

    #[test]
    fn basic_add_test() {
        let mut repo = setup_test_repo(false, None);
        repo.add_all();
        let status = repo.status();
        println!("status: {status:?}");
        assert!(status.contains("main.rb"));
        assert!(status.contains("On branch master"));
        repo.commit("Initial commit");
    }

    #[test]
    fn test_file_system_basic() {
        let mut repo = setup_test_repo(false, None);
        repo.fs
            .prefix_file_content("main.rb", "puts 'Hello, World!2'");
        let status: String = repo.status();
        assert!(status.contains("main.rb"));
        assert!(status.contains("On branch master"));

        let content = repo.fs.file_content("main.rb").unwrap();
        assert!(content.contains("Hello, World!2"));

        repo.fs
            .append_file_content("main.rb", "puts 'Hello, World!3'");
        let content = repo.fs.file_content("main.rb").unwrap();
        assert!(content.contains("Hello, World!2"));
        assert!(content.contains("Hello, World!3"));

        repo.fs.delete_file("main.rb");
    }

    #[test]
    fn test_file_system_create_file() {
        let mut repo = setup_test_repo(false, None);
        repo.fs
            .create_file("new_file.rb", Some("puts 'Hello, World!'"));
        let content = repo.fs.file_content("new_file.rb").unwrap();
        assert!(content.contains("Hello, World!"));
    }

    #[test]
    fn test_file_system_delete_file() {
        let mut repo = setup_test_repo(false, None);
        repo.fs.delete_file("main.rb");
        let status = repo.status();
        assert!(!status.contains("main.rb"));
    }

    #[test]
    fn test_file_system_move_file() {
        let mut repo = setup_test_repo(false, None);
        let old_content = repo.fs.file_content("main.rb").unwrap();
        repo.fs.move_to("main.rb", "new_file.rb");
        let new_content = repo.fs.file_content("new_file.rb").unwrap();
        assert!(new_content.contains(old_content.as_str()));
    }

    #[test]
    fn test_file_system_rename_file() {
        let mut repo = setup_test_repo(false, None);
        let old_content = repo.fs.file_content("main.rb").unwrap();
        repo.fs.rename_to("main.rb", "new_file.rb");
        let new_content = repo.fs.file_content("new_file.rb").unwrap();
        assert!(new_content.contains(old_content.as_str()));
    }

    #[test]
    fn test_file_system_create_directory() {
        let mut repo = setup_test_repo(false, None);
        repo.fs.create_directory("new_directory");
        let path = repo.fs.valid_rel_path("new_directory").unwrap();
        assert!(path.exists());

        repo.fs
            .create_directory("new_directory/nested_directory/sub_directory");
        let path = repo
            .fs
            .valid_rel_path("new_directory/nested_directory/sub_directory")
            .unwrap();
        assert!(path.exists());
    }

    #[test]
    fn test_chaining() {
        let mut repo = setup_test_repo(false, None);
        repo.fs
            .create_file("new_file.rb", Some("puts 'Hello, World!'"))
            .delete_file("new_file.rb");
        let status = repo.status();
        assert!(!status.contains("new_file.rb"));
    }

    #[test]
    fn test_comprehensive_file_changes() {
        let mut repo = setup_test_repo(false, None);

        assert!(repo.fs.file_content("app/models/user_model.rb").is_ok());
        assert!(
            repo.fs
                .file_content("lib/authentication/providers.rb")
                .is_ok()
        );

        repo.fs
            .create_file(
                "config.rb",
                Some("# New config file\nmodule Config\n  VERSION = '1.0'\nend"),
            )
            .append_file_content(
                "app/models/base_model.rb",
                "\n  # Added timestamp method\n  def timestamp\n    Time.now\n  end",
            )
            .create_directory("app/utils")
            .create_file(
                "app/utils/string_utils.rb",
                Some(
                    r#"module StringUtils
                            def self.sanitize(str)
                                str.strip.downcase
                            end

                            def self.titleize(str)
                                str.split(' ').map(&:capitalize).join(' ')
                            end
                            end"#,
                ),
            )
            .create_directory("lib/models")
            .move_to("app/models/user_model.rb", "lib/models/user_model.rb")
            .create_file(
                "lib/helper.rb",
                Some("# Helper module\nmodule Helper\n  def self.help\n    'helping'\n  end\nend"),
            )
            .create_directory("app/auth")
            .move_to("lib/authentication/providers.rb", "app/auth/providers.rb")
            .move_to("lib/authentication/tokens.rb", "app/auth/tokens.rb")
            .append_file_content(
                "lib/models/user_model.rb",
                "\n  # Another added method\n  def updated_at\n    Time.now\n  end",
            )
            .delete_file("lib/helper.rb");

        let config_content = repo
            .fs
            .file_content("config.rb")
            .expect("Config file should exist");
        assert!(config_content.contains("module Config"));
        assert!(config_content.contains("VERSION = '1.0'"));

        let base_model_content = repo
            .fs
            .file_content("app/models/base_model.rb")
            .expect("Base model should exist");
        assert!(base_model_content.contains("def timestamp"));

        let utils_content = repo
            .fs
            .file_content("app/utils/string_utils.rb")
            .expect("Utils file should exist");
        assert!(utils_content.contains("module StringUtils"));
        assert!(utils_content.contains("def self.sanitize"));

        assert!(repo.fs.file_content("lib/models/user_model.rb").is_ok());
        assert!(repo.fs.valid_rel_path("app/models/user_model.rb").is_err());

        assert!(repo.fs.file_content("app/auth/providers.rb").is_ok());
        assert!(repo.fs.file_content("app/auth/tokens.rb").is_ok());

        let moved_user_model = repo
            .fs
            .file_content("lib/models/user_model.rb")
            .expect("Moved user model should exist");
        assert!(moved_user_model.contains("def updated_at"));

        assert!(repo.fs.valid_rel_path("lib/helper.rb").is_err());
    }
}
