//! Declarative macros for defining env and state capability traits.
//!
//! - `define_env_capabilities!`   — one getter trait per field
//! - `define_state_capabilities!` — get/set/take traits + `Seal*` types per field
//!
//! The corresponding `#[derive(PipelineEnv)]` and `#[derive(PipelineState)]`
//! proc macros live in the `pipeline-macros` crate and generate the impls.

/// Define env capability traits. Call once per crate.
///
/// ```ignore
/// define_env_capabilities! {
///     pub ontology: Arc<Ontology>,
///     pub security_ctx: SecurityContext,
/// }
/// // Generates:
/// //   pub trait HasOntology { fn ontology(&self) -> &Arc<Ontology>; }
/// //   pub trait HasSecurityCtx { fn security_ctx(&self) -> &SecurityContext; }
/// ```
#[macro_export]
macro_rules! define_env_capabilities {
    ( $( $vis:vis $field:ident : $ty:ty ),* $(,)? ) => {
        $(
            ::pastey::paste! {
                $vis trait [<Has $field:camel>] {
                    fn $field(&self) -> &$ty;
                }
            }
        )*
    };
}

/// Define state capability traits and seal types. Call once per crate.
///
/// ```ignore
/// define_state_capabilities! {
///     pub json: String,
///     pub input: Input,
///     pub node: Node,
/// }
/// // Generates per field:
/// //   pub trait HasJson { fn json() -> Result<&String>; fn json_mut(); fn set_json(); fn take_json(); }
/// //   pub struct SealJson;
/// //   impl<S: PipelineState + HasJson> Seal<S> for SealJson { ... }
/// ```
#[macro_export]
macro_rules! define_state_capabilities {
    ( $( $vis:vis $field:ident : $ty:ty ),* $(,)? ) => {
        $(
            ::pastey::paste! {
                $vis trait [<Has $field:camel>] {
                    fn $field(&self) -> $crate::error::Result<&$ty>;
                    fn [<$field _mut>](&mut self) -> $crate::error::Result<&mut $ty>;
                    fn [<set_ $field>](&mut self, value: $ty);
                    fn [<take_ $field>](&mut self) -> $crate::error::Result<$ty>;
                }

                $vis struct [<Seal $field:camel>];

                impl<S: $crate::pipeline::PipelineState + [<Has $field:camel>]> $crate::pipeline::Seal<S> for [<Seal $field:camel>] {
                    fn seal(&self, state: &mut S) {
                        let _ = state.[<take_ $field>]();
                    }
                }
            }
        )*
    };
}
