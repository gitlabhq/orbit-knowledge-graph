fn main() {
    let manifest_dir = std::env::var("CARGO_MANIFEST_DIR").unwrap();

    // Find fixtures via git common dir (main repo's integration-tests-codegraph)
    let common_dir = std::process::Command::new("git")
        .args(["rev-parse", "--git-common-dir"])
        .current_dir(&manifest_dir)
        .output()
        .expect("git rev-parse failed")
        .stdout;
    let common = String::from_utf8(common_dir).unwrap();
    let root = std::path::Path::new(common.trim())
        .parent()
        .unwrap()
        .join("crates/integration-tests-codegraph/fixtures");
    let root = root.to_str().unwrap();

    let mut tests = Vec::new();
    find_yaml(root, root, &mut tests);
    tests.sort();

    for pair in tests.windows(2) {
        if pair[0].0 == pair[1].0 {
            panic!(
                "duplicate test name '{}' from:\n  {}\n  {}",
                pair[0].0, pair[0].1, pair[1].1
            );
        }
    }

    let code: String = tests
        .iter()
        .map(|(name, path)| {
            format!(
                "#[tokio::test]\nasync fn {name}() {{ run_yaml_suite(&std::fs::read_to_string(\"{path}\").expect(\"fixture not found: {path}\")).await; }}\n"
            )
        })
        .collect();

    std::fs::write(
        format!("{}/generated_suites.rs", std::env::var("OUT_DIR").unwrap()),
        code,
    )
    .unwrap();
    println!("cargo::rerun-if-changed=build.rs");
}

fn find_yaml(root: &str, dir: &str, out: &mut Vec<(String, String)>) {
    for entry in std::fs::read_dir(dir).unwrap().flatten() {
        let path = entry.path();
        if path.is_dir() {
            find_yaml(root, path.to_str().unwrap(), out);
        } else if path.extension().is_some_and(|e| e == "yaml") {
            let abs = path.to_str().unwrap().replace('\\', "/");
            let name = abs[root.len() + 1..]
                .replace(['/', '.', '-'], "_")
                .trim_end_matches("_yaml")
                .to_string();
            out.push((name, abs));
        }
    }
}
