/// libduckdb-sys 2.0 no longer adds an rpath for a downloaded libduckdb, so a debug `bin` run
/// outside Cargo finds the library only through this one. Call it from the build script of a
/// package that depends on libduckdb-sys directly, since only those see the download directory it
/// publishes as DEP_DUCKDB_INCLUDE. Release builds compile DuckDB in, so they must not carry a
/// path to the build machine, and Windows linkers have no rpath.
pub fn link_downloaded_duckdb(bin: &str) {
    println!("cargo:rerun-if-env-changed=DUCKDB_DOWNLOAD_LIB");
    let downloads = std::env::var("DUCKDB_DOWNLOAD_LIB")
        .is_ok_and(|value| matches!(value.to_ascii_lowercase().as_str(), "1" | "true"));
    let unix = std::env::var("CARGO_CFG_TARGET_FAMILY")
        .is_ok_and(|families| families.split(',').any(|family| family == "unix"));
    let debug = std::env::var("PROFILE").is_ok_and(|profile| profile == "debug");
    if !(downloads && unix && debug) {
        return;
    }
    if let Ok(lib_dir) = std::env::var("DEP_DUCKDB_INCLUDE") {
        println!("cargo:rustc-link-arg-bin={bin}=-Wl,-rpath,{lib_dir}");
    }
}
