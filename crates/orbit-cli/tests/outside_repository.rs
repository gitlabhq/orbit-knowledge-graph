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
    assert!(!printed.contains("orbit workspace index ."), "{printed}");
}

#[test]
fn index_outside_a_repository_names_the_resolved_folder_for_scripts() {
    let folder = tempfile::tempdir().unwrap();
    let resolved = dunce::canonicalize(folder.path()).unwrap();

    let output = run_orbit_in(folder.path(), &["workspace", "index", "."]);

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

    let output = run_orbit_with_home(&subdirectory, home.path(), &["workspace", "index", "."]);

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
    let index = run_orbit_with_home(&subdirectory, home.path(), &["workspace", "index", "."]);

    let printed = all_output(&setup);
    assert!(setup.status.success(), "{printed}");
    assert!(printed.contains("orbit workspace index ."), "{printed}");
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
    let project_root = dunce::canonicalize(&repository).unwrap();
    let unwrapped: String = printed
        .chars()
        .filter(|c| !c.is_whitespace() && *c != '│')
        .collect();
    assert!(
        unwrapped.contains(&format!("orbitworkspaceindex{}", project_root.display())),
        "{printed}"
    );
}

#[cfg(unix)]
fn setup_with_claude_version(version: &str, extra_args: &[&str]) -> (tempfile::TempDir, String) {
    use std::os::unix::fs::PermissionsExt;

    let home = tempfile::tempdir().unwrap();
    let bin = home.path().join("shim-bin");
    std::fs::create_dir_all(&bin).unwrap();
    let shim = bin.join("claude");
    std::fs::write(
        &shim,
        format!("#!/bin/sh\necho '{version} (Claude Code)'\n"),
    )
    .unwrap();
    std::fs::set_permissions(&shim, std::fs::Permissions::from_mode(0o755)).unwrap();
    let path = format!(
        "{}:{}",
        bin.display(),
        std::env::var("PATH").unwrap_or_default()
    );
    let mut args = vec!["setup", "claude", "--yes", "--no-index"];
    args.extend_from_slice(extra_args);

    let output = Command::new(env!("CARGO_BIN_EXE_orbit"))
        .args(&args)
        .current_dir(home.path())
        .env("HOME", home.path())
        .env("PATH", path)
        .env("GIT_CEILING_DIRECTORIES", home.path().parent().unwrap())
        .env_remove("CLAUDE_CONFIG_DIR")
        .env_remove("GITLAB_ORBIT_DISTRIBUTION")
        .output()
        .unwrap();
    let printed = all_output(&output);
    assert!(output.status.success(), "{printed}");
    (home, printed)
}

#[cfg(unix)]
fn configured_card(printed: &str) -> &str {
    let start = printed.find("Configured").expect("Configured card");
    &printed[start..]
}

#[cfg(unix)]
#[test]
fn setup_says_when_the_agent_plugin_is_skipped_and_leaves_it_off_the_configured_card() {
    let (home, printed) = setup_with_claude_version("2.1.286", &[]);

    assert!(
        printed.contains("Orbit UI skipped: needs Claude Code"),
        "{printed}"
    );
    assert!(printed.contains("you have 2.1.286"), "{printed}");
    let card = configured_card(&printed);
    assert!(card.contains("instructions, hooks, skill"), "{printed}");
    assert!(!card.contains("agent plugins"), "{printed}");
    assert!(!home.path().join(".claude/orbit").exists());
}

#[cfg(unix)]
#[test]
fn setup_lists_agent_plugins_when_the_plugin_is_installed() {
    let (home, printed) = setup_with_claude_version("99.0.0", &[]);

    assert!(!printed.contains("Orbit UI skipped"), "{printed}");
    assert!(
        configured_card(&printed).contains("skill, agent plugins"),
        "{printed}"
    );
    assert!(home.path().join(".claude/orbit").exists());
}

#[cfg(unix)]
#[test]
fn dry_run_does_not_plan_the_agent_plugin_when_it_would_be_skipped() {
    let (_home, printed) = setup_with_claude_version("2.1.286", &["--dry-run"]);

    assert!(
        printed.contains("Orbit UI skipped: needs Claude Code"),
        "{printed}"
    );
    let plan = &printed[printed.find("Plan").expect("Plan card")..];
    assert!(!plan.contains("claude_plugin"), "{printed}");
    assert!(!plan.contains("agent plugins"), "{printed}");
}

#[cfg(unix)]
#[test]
fn dry_run_plans_the_agent_plugin_when_claude_is_new_enough() {
    let (_home, printed) = setup_with_claude_version("99.0.0", &["--dry-run"]);

    assert!(!printed.contains("Orbit UI skipped"), "{printed}");
    let plan = &printed[printed.find("Plan").expect("Plan card")..];
    assert!(plan.contains("claude_plugin"), "{printed}");
}
