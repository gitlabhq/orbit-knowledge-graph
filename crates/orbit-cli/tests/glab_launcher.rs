use std::process::{Command, Output};

fn run_orbit(distribution: Option<&str>, args: &[&str]) -> Output {
    let mut command = Command::new(env!("CARGO_BIN_EXE_orbit"));
    command.args(args).env_remove("GITLAB_ORBIT_DISTRIBUTION");
    if let Some(distribution) = distribution {
        command.env("GITLAB_ORBIT_DISTRIBUTION", distribution);
    }
    command.output().unwrap()
}

fn stdout(output: &Output) -> String {
    String::from_utf8_lossy(&output.stdout).into_owned()
}

fn lists_update(help: &str) -> bool {
    help.lines()
        .any(|line| line.trim_start().starts_with("update "))
}

#[test]
fn help_under_glab_lists_update_and_the_glab_usage() {
    let help = stdout(&run_orbit(Some("glab"), &["--help"]));

    assert!(help.contains("Usage: glab orbit <COMMAND>"), "{help}");
    assert!(lists_update(&help), "{help}");
}

#[test]
fn help_without_glab_has_no_update_command() {
    let help = stdout(&run_orbit(None, &["--help"]));

    assert!(help.contains("Usage: orbit <COMMAND>"), "{help}");
    assert!(!lists_update(&help), "{help}");
}

#[test]
fn update_that_reaches_the_binary_points_to_glab() {
    let output = run_orbit(Some("glab"), &["update"]);

    assert!(!output.status.success());
    assert!(
        String::from_utf8_lossy(&output.stderr).contains("run `glab orbit update`"),
        "{output:?}"
    );
}
