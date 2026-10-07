use std::path::Path;
use std::process::{Command, Output};

fn run_orbit_in(folder: &Path, args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_orbit"))
        .args(args)
        .current_dir(folder)
        .env("HOME", folder)
        .env("GIT_CEILING_DIRECTORIES", folder.parent().unwrap())
        .env_remove("CLAUDE_CONFIG_DIR")
        .env_remove("GITLAB_ORBIT_DISTRIBUTION")
        .output()
        .unwrap()
}

fn all_output(output: &Output) -> String {
    format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    )
}

#[test]
fn setup_outside_a_repository_succeeds_and_suggests_indexing_by_path() {
    let folder = tempfile::tempdir().unwrap();

    let output = run_orbit_in(folder.path(), &["setup", "claude", "--yes"]);

    let printed = all_output(&output);
    assert!(output.status.success(), "{printed}");
    assert!(folder.path().join(".claude/CLAUDE.md").is_file());
    assert!(printed.contains("not a git repository"), "{printed}");
    assert!(!printed.contains("orbit index ."), "{printed}");
}

#[test]
fn index_outside_a_repository_names_the_resolved_folder_for_scripts() {
    let folder = tempfile::tempdir().unwrap();
    let resolved = dunce::canonicalize(folder.path()).unwrap();

    let output = run_orbit_in(folder.path(), &["index", "."]);

    let printed = all_output(&output);
    assert!(!output.status.success(), "{printed}");
    assert!(
        printed.contains(&format!(
            "no git repository found in {}. Pass",
            resolved.display()
        )),
        "{printed}"
    );
}

fn repository_with_subdirectory(parent: &Path) -> (std::path::PathBuf, std::path::PathBuf) {
    let repository = parent.join("repo");
    let subdirectory = repository.join("src/deep");
    std::fs::create_dir_all(&subdirectory).unwrap();
    std::fs::write(repository.join("src/main.rs"), "fn main() {}\n").unwrap();
    for args in [
        &["init", "-q"][..],
        &["add", "."],
        &[
            "-c",
            "user.name=t",
            "-c",
            "user.email=t@example.com",
            "commit",
            "-qm",
            "init",
        ],
    ] {
        let status = Command::new("git")
            .args(args)
            .current_dir(&repository)
            .status()
            .unwrap();
        assert!(status.success());
    }
    (repository, subdirectory)
}

fn run_orbit_with_home(folder: &Path, home: &Path, args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_orbit"))
        .args(args)
        .current_dir(folder)
        .env("HOME", home)
        .env("GIT_CEILING_DIRECTORIES", home)
        .env_remove("CLAUDE_CONFIG_DIR")
        .env_remove("GITLAB_ORBIT_DISTRIBUTION")
        .output()
        .unwrap()
}

#[test]
fn index_from_a_repository_subdirectory_indexes_the_repository() {
    let home = tempfile::tempdir().unwrap();
    let (repository, subdirectory) = repository_with_subdirectory(home.path());

    let output = run_orbit_with_home(&subdirectory, home.path(), &["index", "."]);

    let printed = all_output(&output);
    assert!(output.status.success(), "{printed}");
    let indexed = dunce::canonicalize(&repository).unwrap();
    assert!(
        printed.contains(&format!("\"path\": {:?}", indexed.display().to_string())),
        "{printed}"
    );
}

#[test]
fn setup_without_indexing_from_a_subdirectory_suggests_a_command_that_works() {
    let home = tempfile::tempdir().unwrap();
    let (_, subdirectory) = repository_with_subdirectory(home.path());

    let setup = run_orbit_with_home(
        &subdirectory,
        home.path(),
        &["setup", "claude", "--yes", "--no-index"],
    );
    let index = run_orbit_with_home(&subdirectory, home.path(), &["index", "."]);

    let printed = all_output(&setup);
    assert!(setup.status.success(), "{printed}");
    assert!(printed.contains("orbit index ."), "{printed}");
    assert!(index.status.success(), "{}", all_output(&index));
}

#[test]
fn setup_with_dir_judges_the_project_root_not_the_working_folder() {
    let home = tempfile::tempdir().unwrap();
    let (repository, _) = repository_with_subdirectory(home.path());
    let elsewhere = tempfile::tempdir().unwrap();

    let output = run_orbit_with_home(
        elsewhere.path(),
        home.path(),
        &[
            "setup",
            "claude",
            "--yes",
            "--no-index",
            "--dir",
            repository.to_str().unwrap(),
        ],
    );

    let printed = all_output(&output);
    assert!(output.status.success(), "{printed}");
    assert!(!printed.contains("not a git repository"), "{printed}");
    assert!(
        printed.contains(&format!("orbit index {}", repository.display())),
        "{printed}"
    );
}
