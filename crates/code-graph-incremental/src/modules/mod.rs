pub mod core;
pub mod names;
pub mod ssa;
pub mod text;

use crate::engine::{Cursor, Cx, Module, Registration, Store, Vals, register};

/// The default catalog: every module an `Env` mounts unless told otherwise.
pub static ALL: &[Registration] = &[
    register::<core::Core>(),
    register::<names::Names>(),
    register::<ssa::Ssa>(),
    register::<text::Text>(),
    crate::engine::register_backend::<crate::backend::tables::Rows>(),
];

fn node<'t, M: Module, S: Store>(cx: &Cx<'_, 't, M, S>, args: &[Vals], at: usize) -> Cursor<'t, S> {
    cx.jump(args[at][0].loc().expect("this argument is a node"))
}

fn top_level<S: Store>(
    root: Cursor<'_, S>,
    source_file: u16,
) -> impl Iterator<Item = Cursor<'_, S>> {
    std::iter::once(root).chain(root.children().filter(move |c| c.is(source_file)))
}

macro_rules! function {
    (required $kind:ident, $name:literal, [$($arg:ident),*]) => {
        $crate::engine::Function {
            name: $name,
            sig: &[$(function!(@arg $arg)),*],
            kind: $crate::engine::Kind::$kind,
            key: &[],
            uses: &[],
            required: true,
            call: $crate::engine::Calls {
                compact: |_, _| unreachable!("each phase binds a provider to a required function"),
                mutable: |_, _| unreachable!("each phase binds a provider to a required function"),
            },
        }
    };
    (
        $kind:ident, $name:literal, [$($arg:ident),*],
        $(@key [$($key:expr),*],)?
        $(@uses [$($module:literal . $used:literal),*],)?
        $body:expr
    ) => {
        $crate::engine::Function {
            name: $name,
            sig: &[$(function!(@arg $arg)),*],
            kind: $crate::engine::Kind::$kind,
            key: &[$($($key),*)?],
            uses: &[$($(($module, $used)),*)?],
            required: false,
            call: $crate::engine::Calls {
                compact: $body,
                mutable: $body,
            },
        }
    };
    (@arg one) => { $crate::engine::Arg::One };
    (@arg set) => { $crate::engine::Arg::Set };
    (@arg edge) => { $crate::engine::Arg::Edge };
    (@arg pattern) => { $crate::engine::Arg::Pattern };
    (@arg rest) => { $crate::engine::Arg::Rest };
    (@arg shape) => { $crate::engine::Arg::Shape };
}
pub(crate) use function;
