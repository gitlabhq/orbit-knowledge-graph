fn main() {
    #[cfg(target_os = "macos")]
    println!("cargo:rustc-link-arg-bin=tree-dsl=-Wl,-rpath,@loader_path/deps");
}
