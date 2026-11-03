use std::sync::OnceLock;

pub fn get() -> &'static str {
    static VERSION: OnceLock<String> = OnceLock::new();
    VERSION.get_or_init(|| {
        option_env!("ORBIT_RELEASE_VERSION")
            .map(str::to_string)
            .or_else(orbit_utils::version::describe_checkout)
            .unwrap_or_else(|| env!("CARGO_PKG_VERSION").to_string())
    })
}
