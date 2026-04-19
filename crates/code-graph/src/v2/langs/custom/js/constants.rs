//! Shared vocabulary for the JS/TS pipeline.
//!
//! Every token list used by more than one module lives here so adding a
//! new extension, manifest file, or Vue lifecycle hook is a single-site
//! edit.

pub const TS_EXTENSIONS: &[&str] = &["ts", "tsx", "mts", "cts"];
pub const SFC_EXTENSIONS: &[&str] = &["vue", "svelte", "astro"];
pub const DATA_EXTENSIONS: &[&str] = &["json", "graphql", "gql"];
pub const ASSET_EXTENSIONS: &[&str] = &["svg", "png", "jpg", "jpeg", "gif", "webp", "avif"];

/// Extensions the specifier resolver and evaluator probe when resolving a
/// bare specifier. The default order prefers TypeScript, matching the
/// priority tsc/ts-node use.
pub const RESOLVER_EXTENSIONS: &[&str] = &[
    "ts", "tsx", "js", "jsx", "mjs", "cjs", "mts", "cts", "vue", "svelte", "astro", "graphql",
    "gql", "json",
];

/// Same set as `RESOLVER_EXTENSIONS`, ordered to match Bun's loader priority.
pub const RESOLVER_EXTENSIONS_BUN: &[&str] = &[
    "tsx", "jsx", "ts", "mts", "mjs", "js", "cjs", "cts", "vue", "svelte", "astro", "graphql",
    "gql", "json",
];

/// Extensions the webpack-config evaluator probes when resolving a bare
/// specifier that omits its extension. Differs from `RESOLVER_EXTENSIONS`
/// in that it prefers plain `.js` first — webpack configs are almost
/// always JS rather than TS.
pub const EVAL_EXTENSIONS: &[&str] = &[
    "js", "jsx", "cjs", "mjs", "ts", "tsx", "mts", "cts", "json", "graphql", "gql", "vue",
    "svelte", "astro",
];

/// Files at the repo root that JS resolution cares about.
pub const MANIFEST_FILENAMES: &[&str] = &[
    "bun.lock",
    "bun.lockb",
    "bunfig.toml",
    "package.json",
    "pnpm-workspace.yaml",
    "tsconfig.json",
    "jsconfig.json",
];

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
