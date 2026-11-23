use crate::engine::{Function, Module};
use crate::modules::function;

/// What a name reads, bound per phase.
pub struct Core;

#[derive(Default)]
pub struct State;

impl Module for Core {
    const NAME: &'static str = "core";
    type State = State;

    fn functions() -> &'static [Function<Self>] {
        FUNCTIONS
    }
}

static FUNCTIONS: &[Function<Core>] = &[function!(required Flow, "read", [one])];
