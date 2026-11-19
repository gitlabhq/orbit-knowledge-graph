use smallvec::SmallVec;

use super::ops::{self, Chain, Members, Parts, Types};
use super::role;
use crate::canonical::Canonical as C;
use crate::engine::{Cursor, Cx, Loc, Val, Vals, of};
use crate::modules::ssa::{Value, to_vals};
use crate::tags::ReservedTags;

type Values = SmallVec<[Value; 2]>;

impl<'t> Types<'t> for Cx<'t> {
    fn parts(&mut self, owner: Cursor<'t>) -> Parts<'t> {
        if self.family.is_some() {
            return self.family_parts(owner);
        }
        let dn = owner.index();
        let same_name = owner
            .child_sym(C::DefName)
            .and_then(|name| self.ssa_defs_named(name))
            .unwrap_or_default();
        let same_name = same_name
            .into_iter()
            .filter(|&d| d != dn && (owner.has(C::ImplBlock) || self.at(d).has(C::ImplBlock)));
        std::iter::once(owner)
            .chain(same_name.map(|d| self.at(d)))
            .collect()
    }

    fn supers(&mut self, part: Cursor<'t>) -> Parts<'t> {
        if self.family.is_some() {
            return self.family_supers(part);
        }
        self.supers_of(part.index())
            .into_iter()
            .map(|d| self.at(d))
            .collect()
    }

    fn members_index(&self) -> (&Members, &ReservedTags) {
        (self.members_index(), &self.tags)
    }
}

impl<'t> Chain<'t> for Cx<'t> {
    type Out = Values;

    fn early(&mut self, c: Cursor<'t>) -> Option<Values> {
        let call = c.value_child(C::Call)?;
        if call.has(C::Property) && self.chain_is_class(c.reference()) {
            return None;
        }
        Some(SmallVec::from_elem(Value::Call(call.index()), 1))
    }

    fn name(&mut self, c: Cursor<'t>) -> Values {
        if c.has(C::Qualifier) {
            return self.lookup_static(c.sym()).into_iter().collect();
        }
        let ivar = c.value_child(C::Ivar);
        let sym = ivar.map_or(c.sym(), |iv| iv.sym());
        let mut bound = match ivar {
            Some(_) => Vec::new(),
            None => self.lookup(sym),
        };
        if bound.is_empty() {
            let field = self
                .enclosing_class(self.enclosing())
                .and_then(|cls| self.ivar_type(cls, sym));
            bound.extend(field.map(|f| Value::Call(Self::producer_of(f))));
        }
        bound
            .into_iter()
            .map(|r| match self.member_for(r, c) {
                Value::LocalDef(d) => self.field_value(d).unwrap_or(Value::LocalDef(d)),
                r => r,
            })
            .collect()
    }

    fn member(&mut self, c: Cursor<'t>, member: Cursor<'t>, object: Cursor<'t>) -> Values {
        let name = object.reference();
        let bare = object.value_child(C::Call).is_none()
            && !name.has(C::Object)
            && !name.has(C::Qualifier)
            && name.value_child(C::Member).is_none()
            && name.value_child(C::Ivar).is_none();
        if !bare {
            return self.member_of_object(c, member, object);
        }
        self.ssa.site = Some((c.index(), member.index(), object.index()));
        let key = [
            SmallVec::from_elem(Val::Sym(name.sym()), 1),
            SmallVec::from_elem(Val::Sym(member.sym()), 1),
        ];
        let found = self.call(crate::fn_id!("core", "named_member"), &key);
        self.values(&found)
    }
}

impl<'t> Cx<'t> {
    pub(crate) fn named_member(&mut self) -> Vals {
        let Some((c, member, object)) = self.ssa.site.take() else {
            return Vals::new();
        };
        let found = self.member_of_object(self.at(c), self.at(member), self.at(object));
        to_vals(&found)
    }

    fn member_of_object(
        &mut self,
        c: Cursor<'t>,
        member: Cursor<'t>,
        object: Cursor<'t>,
    ) -> Values {
        let mut targets = Values::new();
        for r in self.lookup_chain(object) {
            match r {
                Value::Type(node) => targets.push(Value::LocalDef(node)),
                Value::Call(n) if let Some(ty) = self.binding_type(n) => {
                    targets.extend(self.lookup_chain(ty));
                    targets.push(r);
                }
                _ => targets.push(r),
            }
        }
        let role = c.role();
        let mut found = Values::new();
        for r in targets {
            match r {
                Value::LocalDef(d) => {
                    let members = ops::members(self, self.at(d), member.sym(), role);
                    found.extend(members.into_iter().map(|m| Value::LocalDef(m.index())));
                }
                Value::ImportRef(_) => found.push(r),
                _ => {}
            }
        }
        found
    }

    pub(crate) fn member(&mut self, value: Val, member: Cursor<'t>) -> Vals {
        match self.value(value) {
            Some(Value::Type(n) | Value::LocalDef(n)) => {
                of(ops::members(self, self.at(n), member.sym(), role(member)))
            }
            Some(Value::Call(n)) if let Some(ty) = self.binding_type(n) => {
                let types = self.call1(crate::fn_id!("core", "type"), Val::Node(ty.into()));
                let mut out = Vals::new();
                for t in types {
                    let found = self.call(
                        crate::fn_id!("core", "member"),
                        &[
                            SmallVec::from_elem(t, 1),
                            SmallVec::from_elem(Val::Node(member.into()), 1),
                        ],
                    );
                    out.extend(
                        found
                            .into_iter()
                            .filter(|f| !out.contains(f))
                            .collect::<Vals>(),
                    );
                }
                out
            }
            _ => Vals::new(),
        }
    }

    pub(crate) fn unmatched(&mut self, value: Val, member: Cursor<'t>) -> Vals {
        let keep = match self.value(value) {
            Some(Value::Type(_)) | None => false,
            Some(Value::LocalDef(n)) => {
                ops::members(self, self.at(n), member.sym(), role(member)).is_empty()
            }
            Some(_) => true,
        };
        match keep {
            true => SmallVec::from_elem(value, 1),
            false => Vals::new(),
        }
    }

    pub(crate) fn reference(&mut self, reference: Cursor<'t>) -> Vals {
        let found = self.lookup_chain(reference);
        let parent = reference.parent().map_or(reference.index(), Cursor::index);
        match found.as_slice() {
            [Value::LocalDef(target) | Value::ImportRef(target)] if *target != parent => {
                of([Loc {
                    fi: self.fi,
                    node: *target,
                }])
            }
            _ => Vals::new(),
        }
    }

    pub(crate) fn standalone(&self, member: Cursor<'t>) -> bool {
        let in_chain = member.parent().is_some_and(|parent| {
            parent.is(C::Callee)
                || parent.is(C::Object) && parent.parent().is_some_and(|owner| owner.is(C::Member))
        });
        !in_chain && self.field_slot(member).is_none()
    }

    pub(crate) fn callee_targets(&mut self, call: Cursor<'t>) -> Vals {
        let Some(sym) = call.child(C::Callee).and_then(|callee| callee.sym_opt()) else {
            return Vals::new();
        };
        let targets = match call.has_tag(self.tags.implicit_self) {
            true => self.implicit_targets(sym, call),
            false => self.name_targets(sym, call, false),
        };
        to_vals(&targets)
    }
}
