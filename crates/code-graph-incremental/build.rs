use std::{env, fs, path::Path};

fn main() {
    let mut code = String::new();
    for (table, dir) in [("RULES", "config/rules"), ("LIBS", "config/lib")] {
        println!("cargo:rerun-if-changed={dir}");
        let mut names: Vec<String> = fs::read_dir(dir)
            .unwrap_or_else(|e| panic!("{dir}: {e}"))
            .filter_map(|e| {
                let path = e.ok()?.path();
                (path.extension()? == "yaml")
                    .then(|| path.file_stem()?.to_str().map(str::to_owned))?
            })
            .collect();
        names.sort();
        code += &format!("const {table}: &[(&str, &str)] = &[\n");
        for name in names {
            code += &format!(
                "    ({name:?}, include_str!(concat!(env!(\"CARGO_MANIFEST_DIR\"), \"/{dir}/{name}.yaml\"))),\n"
            );
        }
        code += "];\n";
    }
    let out = Path::new(&env::var("OUT_DIR").expect("cargo sets OUT_DIR")).join("rule_files.rs");
    fs::write(out, code).expect("OUT_DIR is writable");
}
