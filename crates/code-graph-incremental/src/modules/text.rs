use crate::engine::{Function, Module, Val, Vals};
use crate::modules::function;

pub struct Text;

impl Module for Text {
    const NAME: &'static str = "text";
    type State = ();

    fn functions() -> &'static [Function<Self>] {
        FUNCTIONS
    }
}

static FUNCTIONS: &[Function<Text>] = &[function!(Pure, "lowercase", [one], |cx, a| {
    let syms = &cx.env().lang.syms;
    a[0].iter()
        .filter_map(|v| match v {
            Val::Sym(sym) => Some(Val::Sym(syms.intern(&syms.resolve(*sym).to_lowercase()))),
            _ => None,
        })
        .collect::<Vals>()
})];
