//! What a name reads: a hook each phase that reads names binds to a
//! provider of its own.

use crate::engine::{Function, Module};
use crate::modules::function;

pub struct Names;

#[derive(Default)]
pub struct State;

impl Module for Names {
    const NAME: &'static str = "names";
    type State = State;

    fn functions() -> &'static [Function<Self>] {
        FUNCTIONS
    }
}

static FUNCTIONS: &[Function<Names>] = &[function!(required Flow, "read", [one])];
