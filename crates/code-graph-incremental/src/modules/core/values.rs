use super::{Core, Ref};
use crate::canonical::Canonical as C;
use crate::engine::{Cursor, Cx, Store, Val, Vals, of, one};
use crate::tree::is_name_text;

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

pub(super) fn field_value<S: Store>(def: Cursor<'_, S>) -> Option<Val> {
    let binding = def.child(C::Binding).filter(|b| b.typed().is_some())?;
    let stored = def.has(C::FieldDef) || def.has(C::Property);
    stored.then(|| Val::Node(def.jump(def.fi(), producer(binding)).into()))
}

pub(super) fn sink_step<S: Store>(
    c: Cursor<'_, S>,
) -> Result<Cursor<'_, S>, Option<Cursor<'_, S>>> {
    if c.is(C::SsaArm) {
        let tail = c.tail_expr();
        if tail.is(C::SsaBranch) || tail.is(C::SsaReturn) {
            return Err(None);
        }
        return c.parent().filter(|p| p.is(C::SsaBranch)).ok_or(None);
    }
    if !c.is(C::SsaBranch) {
        return Err(None);
    }
    let first = c.parent().ok_or(None)?;
    if first.is(C::Rhs) {
        let direct = first
            .child(C::SsaBranch)
            .is_some_and(|b| b.index() == c.index());
        return Err(first.parent().filter(|b| direct && b.is(C::Binding)));
    }
    let (mut up, mut below) = (first, c);
    while !up.is(C::SsaArm) {
        let tail = up.last_named().filter(|t| up.stands_for(*t));
        if tail.is_none_or(|t| t.index() != below.index()) {
            return Err(None);
        }
        (below, up) = (up, up.parent().ok_or(None)?);
    }
    match up.tail_expr().index() == c.index() {
        true => up.parent().filter(|p| p.is(C::SsaBranch)).ok_or(None),
        false => Err(None),
    }
}

pub(super) fn binding_site<S: Store>(c: Cursor<'_, S>) -> Option<Cursor<'_, S>> {
    c.ancestors()
        .take_while(|up| !up.is(C::Def))
        .filter(|up| up.is(C::Binding))
        .find(|binding| {
            binding.child(C::Rhs).is_some_and(|rhs| {
                rhs.child(C::SsaBranch).is_none() && rhs.value_child(C::Member).is_some()
            })
        })
}

impl<'t, S: Store> Cx<'_, 't, Core, S> {
    pub(super) fn source(&mut self, c: Cursor<'t, S>) -> Cursor<'t, S> {
        let t = c.tail_expr();
        if let Some(record) = Some(t).filter(|t| t.is(C::Obj)).or_else(|| t.child(C::Obj)) {
            return record;
        }
        let class = t
            .child(C::Callee)
            .is_some_and(|callee| self.is_class_ref(callee));
        t.child(C::Args).filter(|_| class).unwrap_or(t)
    }

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
                .any(|v| matches!(self.view(*v), Some(Ref::Import(_))))
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
        !self.value_of(arm).is_empty()
            || self.value(tail).is_empty() && self.read(tail).iter().any(|v| *v != Val::Opaque)
    }

    pub(super) fn reference(&mut self, c: Cursor<'t, S>) -> Vals {
        let found = self.type_of(c);
        let parent = c.parent().map_or(c.index(), Cursor::index);
        match found.as_slice() {
            [v] if matches!(self.view(*v), Some(Ref::Def(t) | Ref::Import(t)) if t != parent) => {
                found
            }
            _ => Vals::new(),
        }
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

    pub(super) fn targets(&mut self, call: Cursor<'t, S>) -> Vals {
        let Some(callee) = call.child(C::Callee).filter(|c| c.sym_opt().is_some()) else {
            return Vals::new();
        };
        let bound = self.read(callee);
        let implicit = call.has_tag(self.tags().implicit_self);
        let read_any = !bound.is_empty();
        let named: Vals = bound
            .into_iter()
            .filter(|v| *v != Val::Opaque)
            .filter(|v| !implicit || matches!(self.view(*v), Some(Ref::Def(_) | Ref::Import(_))))
            .collect();
        let bound: Vals = named
            .into_iter()
            .map(|v| self.member_for(v, call))
            .collect();
        if read_any || !implicit {
            return bound;
        }
        match self.enclosing_class() {
            Some(class) => of(self.members(class, callee.sym(), call.role())),
            None => Vals::new(),
        }
    }

    pub(super) fn unresolved(&mut self, call: Cursor<'t, S>) -> bool {
        let Some(callee) = call.child(C::Callee).filter(|c| c.sym_opt().is_some()) else {
            return false;
        };
        if !is_name_text(self.lang().syms.resolve(callee.sym())) {
            return false;
        }
        let implicit = call.has_tag(self.tags().implicit_self);
        self.read(callee).is_empty() && (!implicit || self.targets(call).is_empty())
    }
}
