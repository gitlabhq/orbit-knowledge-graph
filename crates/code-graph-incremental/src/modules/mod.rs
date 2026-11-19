pub mod ssa;
pub mod text;

use crate::engine::{Cursor, Cx, Module, Registration, Vals, register};

pub static ALL: &[Registration] = &[
    register::<core::Core>(),
    register::<ssa::Ssa>(),
    register::<text::Text>(),
];

fn node<'t, M: Module>(cx: &Cx<'_, 't, M>, args: &[Vals], at: usize) -> Cursor<'t> {
    cx.jump(args[at][0].loc().expect("this argument is a node"))
}

fn arg<'t, M: Module>(cx: &Cx<'_, 't, M>, args: &[Vals], at: usize) -> Option<Cursor<'t>> {
    args[at]
        .first()
        .and_then(|v| v.loc())
        .map(|loc| cx.jump(loc))
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
            call: |_, _| unreachable!("each phase binds a provider to a required function"),
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
            call: $body,
        }
    };
    (@arg one) => { $crate::engine::Arg::One };
    (@arg set) => { $crate::engine::Arg::Set };
}
use function;
