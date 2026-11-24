//! The code-indexing pipeline's YAML: pipeline.yaml, the shared rule files
//! and the libraries they import, embedded at build time.

include!(concat!(env!("OUT_DIR"), "/rule_files.rs"));

pub const PIPELINE: &str = include_str!("../config/pipeline.yaml");

/// The YAML at `path` under the crate: read from `CGI_RULES_DIR` when that is
/// set, once per run, so a rules change needs no rebuild; embedded otherwise.
pub fn source(path: &str, embedded: &'static str) -> &'static str {
    static READ: std::sync::LazyLock<
        std::sync::Mutex<std::collections::HashMap<String, &'static str>>,
    > = std::sync::LazyLock::new(Default::default);
    let Some(dir) = std::env::var_os("CGI_RULES_DIR") else {
        return embedded;
    };
    let mut read = READ.lock().expect("rule source cache");
    read.entry(path.to_string()).or_insert_with(|| {
        let full = std::path::Path::new(&dir).join(path);
        let text =
            std::fs::read_to_string(&full).unwrap_or_else(|e| panic!("{}: {e}", full.display()));
        Box::leak(text.into_boxed_str())
    })
}

/// Each rule file of `dir`, as `source` reads it.
pub fn sources(
    dir: &str,
    files: &'static [(&'static str, &'static str)],
) -> &'static [(&'static str, &'static str)] {
    if std::env::var_os("CGI_RULES_DIR").is_none() {
        return files;
    }
    static LEAKED: std::sync::LazyLock<
        std::sync::Mutex<
            std::collections::HashMap<String, &'static [(&'static str, &'static str)]>,
        >,
    > = std::sync::LazyLock::new(Default::default);
    let mut leaked = LEAKED.lock().expect("rule file cache");
    leaked.entry(dir.to_string()).or_insert_with(|| {
        let read: Vec<(&'static str, &'static str)> = files
            .iter()
            .map(|&(name, text)| (name, source(&format!("{dir}/{name}.yaml"), text)))
            .collect();
        Box::leak(read.into_boxed_slice())
    })
}
