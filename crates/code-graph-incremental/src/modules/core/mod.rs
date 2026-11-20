use crate::engine::{Cx, Function, Module, Store, Val, Vals, one};
use crate::modules::function;

/// What a name reads, bound per phase, and the def the rule walk is inside.
pub struct Core;

#[derive(Default)]
pub struct State;

impl<S: Store> Cx<'_, '_, Core, S> {
    fn node(&self, node: u32) -> Val {
        Val::Node(self.loc(node))
    }
}

impl Module for Core {
    const NAME: &'static str = "core";
    type State = State;

    fn functions() -> &'static [Function<Self>] {
        FUNCTIONS
    }
}

static FUNCTIONS: &[Function<Core>] = &[
    function!(required Flow, "read", [one]),
    function!(Flow, "enclosing", [], |cx, _| {
        let node = cx.enclosing();
        one(cx.node(node))
    }),
    function!(Flow, "enclosing_def", [], |cx, _| {
        let def = cx.enclosing_def();
        def.map_or_else(Vals::new, |d| one(cx.node(d)))
    }),
];
