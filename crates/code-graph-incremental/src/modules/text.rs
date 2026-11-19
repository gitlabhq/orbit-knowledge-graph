use crate::engine::{Arg, Function, Kind, Module, Val, Vals};

pub static MODULE: Module = Module {
    name: "text",
    functions: &[Function {
        name: "lowercase",
        sig: &[Arg::One],
        kind: Kind::Pure,
        call: |cx, a| {
            let syms = &cx.env.lang.syms;
            a[0].iter()
                .filter_map(|v| match v {
                    Val::Sym(sym) => {
                        Some(Val::Sym(syms.intern(&syms.resolve(*sym).to_lowercase())))
                    }
                    _ => None,
                })
                .collect::<Vals>()
        },
    }],
};
