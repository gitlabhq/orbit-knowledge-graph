pub const TS_EXTENSIONS: &[&str] = &["ts", "tsx", "mts", "cts"];
pub const SFC_EXTENSIONS: &[&str] = &["vue"];
pub const DATA_EXTENSIONS: &[&str] = &["json", "graphql", "gql"];
pub const ASSET_EXTENSIONS: &[&str] = &["svg", "png", "jpg", "jpeg", "gif", "webp", "avif"];

pub const RESOLVER_EXTENSIONS: &[&str] = &[
    "ts", "tsx", "js", "jsx", "mjs", "cjs", "mts", "cts", "vue", "graphql", "gql", "json",
];

pub const RESOLVER_EXTENSIONS_BUN: &[&str] = &[
    "tsx", "jsx", "ts", "mts", "mjs", "js", "cjs", "cts", "vue", "graphql", "gql", "json",
];

pub const EVAL_EXTENSIONS: &[&str] = &[
    "js", "jsx", "cjs", "mjs", "ts", "tsx", "mts", "cts", "json", "graphql", "gql", "vue",
];

pub const BUN_SIGNAL_FILES: &[&str] = &["bun.lock", "bun.lockb", "bunfig.toml"];

pub const WEBPACK_CONFIG_STEM: &str = "webpack.config";

pub const WEBPACK_CONFIG_EXTENSIONS: &[&str] = &["js", "cjs", "mjs", "ts"];

pub fn is_webpack_config_path(relative_path: &str) -> bool {
    let basename = relative_path
        .rsplit(['/', '\\'])
        .next()
        .unwrap_or(relative_path);
    let Some(rest) = basename.strip_prefix(WEBPACK_CONFIG_STEM) else {
        return false;
    };
    let Some(ext) = rest.strip_prefix('.') else {
        return false;
    };
    WEBPACK_CONFIG_EXTENSIONS.contains(&ext)
}

pub const VUE_OPTION_EXECUTABLE_MAPS: &[&str] = &["methods", "computed", "watch"];

pub const VUE_OPTION_EXECUTABLE_FNS: &[&str] = &["data", "setup", "render"];

pub const VUE_OPTION_CONTRACT_KEYS: &[&str] =
    &["props", "emits", "inject", "provide", "components"];

pub const VUE_OPTION_IDENTIFIER_KEYS: &[&str] = &["name"];

pub const VUE_LIFECYCLE_HOOKS: &[&str] = &[
    "beforeCreate",
    "created",
    "beforeMount",
    "mounted",
    "beforeUpdate",
    "updated",
    "beforeDestroy",
    "destroyed",
    "beforeUnmount",
    "unmounted",
    "activated",
    "deactivated",
    "errorCaptured",
    "serverPrefetch",
];

pub fn is_sfc_extension(extension: &str) -> bool {
    SFC_EXTENSIONS.contains(&extension)
}

pub fn is_ts_extension(extension: &str) -> bool {
    TS_EXTENSIONS.contains(&extension)
}

pub fn is_vue_lifecycle_hook(name: &str) -> bool {
    VUE_LIFECYCLE_HOOKS.contains(&name)
}
