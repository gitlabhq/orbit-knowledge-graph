//! The three stage roles: [`Extractor`] (producer), [`Transform`] (processor),
//! and the [`write`] functions (consumer). The runner in [`super`] composes
//! them per page.

mod extract;
mod transform;
pub(in crate::modules::sdlc) mod write;

pub(in crate::modules::sdlc) use extract::Extractor;
pub(in crate::modules::sdlc) use transform::Transform;
