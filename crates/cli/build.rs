fn main() {
    #[cfg(target_os = "macos")]
    {
        println!("cargo:rustc-link-arg-bin=orbit=-Wl,-rpath,@loader_path/deps");
    }
}
