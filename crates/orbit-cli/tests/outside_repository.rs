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
