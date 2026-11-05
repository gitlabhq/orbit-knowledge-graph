fn main() {
    println!("cargo:rerun-if-changed=build.rs");
    let config_dir = std::env::var("ORBIT_CONFIG_DIR")
        .expect("ORBIT_CONFIG_DIR must be set via .cargo/config.toml [env]");
    println!("cargo:rerun-if-changed={config_dir}/model");
}
