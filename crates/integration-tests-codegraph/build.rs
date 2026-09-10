fn main() {
    let mut tests = Vec::new();
    walk("fixtures", "fixtures", &mut tests);
    tests.sort();

    let code: String = tests
        .iter()
        .map(|(name, path)| {
            format!(
                "#[tokio::test]\nasync fn {name}() {{ run_yaml_suite(include_str!(\"../{path}\")).await; }}\n"
            )
        })
        .collect();

    let out = format!("{}/generated_suites.rs", std::env::var("OUT_DIR").unwrap());
    std::fs::write(out, code).unwrap();
    println!("cargo::rerun-if-changed=fixtures");
}

fn walk(base: &str, dir: &str, out: &mut Vec<(String, String)>) {
    for e in std::fs::read_dir(dir).unwrap().flatten() {
        let p = e.path();
        if p.is_dir() {
            walk(base, p.to_str().unwrap(), out);
        } else if p.extension().is_some_and(|x| x == "yaml") {
            let rel = p.to_str().unwrap().to_string();
            let name = rel[base.len() + 1..]
                .replace('/', "_")
                .replace(".yaml", "")
                .replace('-', "_");
            out.push((name, rel));
        }
    }
}
