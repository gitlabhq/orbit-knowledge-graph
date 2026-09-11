// The embedded ontology is a `rust-embed` folder. Release builds expand to
// `include_bytes!`, which tracks content changes but not file additions or
// removals — this rerun line keeps stale files out of incremental builds.
fn main() {
    println!("cargo:rerun-if-changed=build.rs");
    let dir = std::env::var("ONTOLOGY_DIR")
        .expect("ONTOLOGY_DIR must be set via .cargo/config.toml [env]");
    println!("cargo:rerun-if-changed={dir}");
    let config_dir =
        std::env::var("CONFIG_DIR").expect("CONFIG_DIR must be set via .cargo/config.toml [env]");
    println!("cargo:rerun-if-changed={config_dir}/ontology-archives");
}
