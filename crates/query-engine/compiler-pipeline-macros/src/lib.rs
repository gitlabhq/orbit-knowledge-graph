//! Proc macros for the compiler pipeline.
//!
//! - `define_compiler_ctx! { .. }` — generates per-pipeline context structs with
//!   per-phase runtime-enforced field access grants

use proc_macro::TokenStream;

mod ssot;

fn to_pascal(field_name: &str) -> String {
    field_name
        .split('_')
        .map(|seg| {
            let mut chars = seg.chars();
            match chars.next() {
                Some(c) => c.to_uppercase().to_string() + chars.as_str(),
                None => String::new(),
            }
        })
        .collect()
}

/// Generate per-pipeline context structs and per-phase runtime-enforced
/// field access grants.
///
/// See `compiler/src/config.rs` for the full DSL syntax.
#[proc_macro]
pub fn define_compiler_ctx(input: TokenStream) -> TokenStream {
    ssot::generate(input)
}
