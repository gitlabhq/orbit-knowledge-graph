use super::{Core, Ref};
use crate::canonical::Canonical as C;
use crate::engine::{Cursor, Cx, Store, Val, Vals, of, one};

pub(super) fn producer<S: Store>(binding: Cursor<'_, S>) -> u32 {
    let call = binding.child(C::Rhs).and_then(Cursor::rhs_call);
    let member_call = call
        .filter(|_| !binding.has(C::SsaTyped))
        .filter(|c| c.child(C::Callee).is_some_and(|k| k.has(C::Member)));
    member_call.map_or(binding.index(), |c| c.index())
}

pub(super) fn declared_type<S: Store>(v: Cursor<'_, S>) -> Option<Cursor<'_, S>> {
    match v.is(C::Binding) {
        true => v.typed(),
        false => v.child(C::Callee).filter(|_| v.is(C::Call)),
    }
}

impl<'t, S: Store> Cx<'_, 't, Core, S> {
    fn value(&mut self, t: Cursor<'t, S>) -> Vals {
        if C::try_from_u16(t.kind()).is_some_and(C::is_opaque_value) {
            return one(Val::Opaque);
        }
        if t.is(C::Call) {
            return of([t]);
        }
        if t.is(C::Ivar) {
            let found = self.ivar(t);
            return match found.is_empty() {
                true => one(Val::Opaque),
                false => found,
            };
        }
        Vals::new()
    }

    pub(super) fn value_of(&mut self, c: Cursor<'t, S>) -> Vals {
        if c.is(C::SsaArm) {
            let mut found = self.value(c.tail_expr());
            found.retain(|v| *v != Val::Opaque);
            return found;
        }
        if !c.is(C::Binding) {
            return self.value(c);
        }
        let tail = c.child(C::Rhs).map(Cursor::tail_expr);
        let reads_import = c.bare_rhs().is_some_and(|rhs| {
            let found = self.read(rhs);
            found
                .iter()
                .any(|v| matches!(self.view(*v), Some(Ref::Import)))
        });
        let untracked = tail.is_some_and(|t| t.is(C::Member) && self.read(t).is_empty());
        if c.has(C::SsaTyped) || reads_import || untracked {
            return of([c]);
        }
        match tail {
            Some(t) => self.value(t),
            None => one(Val::Opaque),
        }
    }

    pub(super) fn valued(&mut self, arm: Cursor<'t, S>) -> bool {
        let tail = arm.tail_expr();
        if tail.is(C::SsaBranch) || tail.is(C::SsaReturn) {
            return false;
        }
        !self.value_of(arm).is_empty()
            || self.value(tail).is_empty() && self.read(tail).iter().any(|v| *v != Val::Opaque)
    }

    pub(super) fn receiver_calls(&mut self, member: Cursor<'t, S>) -> Vals {
        let receiver = member.child(C::Dispatch).and_then(|d| d.child(C::Object));
        let Some(receiver) = receiver else {
            return Vals::new();
        };
        let mut found = self.read(receiver);
        found.retain(|v| matches!(self.view(*v), Some(Ref::Call(_))));
        found
    }
}
