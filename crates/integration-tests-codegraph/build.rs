fn main() {
    let root = format!("{}/fixtures", std::env::var("CARGO_MANIFEST_DIR").unwrap());
    let mut tests = Vec::new();
    find_yaml(&root, &root, &mut tests);
    tests.sort();

    let code: String = tests
        .iter()
        .map(|(name, path)| {
            format!("#[tokio::test]\nasync fn {name}() {{ run_yaml_suite(include_str!(\"{path}\")).await; }}\n")
        })
        .collect();

    std::fs::write(
        format!("{}/generated_suites.rs", std::env::var("OUT_DIR").unwrap()),
        code,
    )
    .unwrap();
    println!("cargo::rerun-if-changed=fixtures");
}

fn find_yaml(root: &str, dir: &str, out: &mut Vec<(String, String)>) {
    for entry in std::fs::read_dir(dir).unwrap().flatten() {
        let path = entry.path();
        if path.is_dir() {
            find_yaml(root, path.to_str().unwrap(), out);
        } else if path.extension().is_some_and(|e| e == "yaml") {
            let abs = path.to_str().unwrap().to_string();
            let name = abs[root.len() + 1..]
                .replace(['/', '.', '-'], "_")
                .trim_end_matches("_yaml")
                .to_string();
            out.push((name, abs));
        }
    }
}
