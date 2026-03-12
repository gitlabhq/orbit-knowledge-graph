use std::future::Future;
use std::pin::Pin;

use crate::error::Result;
use crate::runner::TestState;

pub type HandlerFn =
    for<'a> fn(&'a mut TestState, &'a kdl::KdlNode) -> Pin<Box<dyn Future<Output = Result> + 'a>>;

pub struct CommandDef {
    pub name: &'static str,
    pub handler: HandlerFn,
}

inventory::collect!(CommandDef);

pub fn lookup(name: &str) -> Option<&'static CommandDef> {
    inventory::iter::<CommandDef>
        .into_iter()
        .find(|c| c.name == name)
}

#[macro_export]
macro_rules! register_command {
    ($name:literal, $handler:path) => {
        inventory::submit! {
            $crate::registry::CommandDef {
                name: $name,
                handler: |state, node| Box::pin($handler(state, node)),
            }
        }
    };
}
