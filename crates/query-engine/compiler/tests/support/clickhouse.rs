use std::io::Write;
use std::process::{Command, Stdio};

pub fn execute(sql: &str) -> String {
    let mut child = Command::new("docker")
        .args([
            "run",
            "--rm",
            "-i",
            "clickhouse/clickhouse-server:26.2",
            "clickhouse-local",
            "--multiquery",
        ])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();

    child
        .stdin
        .take()
        .unwrap()
        .write_all(sql.as_bytes())
        .unwrap();
    let output = child.wait_with_output().unwrap();
    assert!(
        output.status.success(),
        "{}\n{sql}",
        String::from_utf8_lossy(&output.stderr)
    );

    String::from_utf8(output.stdout).unwrap()
}
