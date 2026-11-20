use crate::canonical::Canonical as C;
use crate::engine::{Cursor, Store};

pub(super) fn producer<S: Store>(binding: Cursor<'_, S>) -> u32 {
    let call = binding.child(C::Rhs).and_then(Cursor::rhs_call);
    let member_call = call
        .filter(|_| !binding.has(C::SsaTyped))
        .filter(|c| c.child(C::Callee).is_some_and(|k| k.has(C::Member)));
    member_call.map_or(binding.index(), |c| c.index())
}
