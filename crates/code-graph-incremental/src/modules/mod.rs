pub mod core;
pub mod ssa;
pub mod text;

use crate::engine::Module;

pub static ALL: &[&Module] = &[&core::MODULE, &ssa::MODULE, &text::MODULE];
