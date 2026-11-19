use rustc_hash::FxHashSet;
use smallvec::SmallVec;

use super::{Core, Ref, role_code, use_role, values};
use crate::canonical::Canonical as C;
use crate::engine::{Cursor, Cx, Loc, Store, Val, Vals, of, one};
use crate::tree::Role;

type Parts<'t, S> = SmallVec<[Cursor<'t, S>; 4]>;

impl<'t, S: Store> Cx<'_, 't, Core, S> {
    pub(super) fn chain(&mut self, c: Cursor<'t, S>) -> Vals {
        if let Some(call) = c.value_child(C::Call)
            && !(call.has(C::Property) && self.is_class_ref(c.reference()))
        {
            return of([call]);
        }
        let c = c.reference();
        let member = Some(c)
            .filter(|c| c.has(C::Object))
            .or_else(|| c.value_child(C::Member));
        let Some(member) = member else {
            return self.named(c);
        };
        let Some(object) = member.child(C::Object) else {
            return Vals::new();
        };
        let name = object.reference();
        let bare = object.value_child(C::Call).is_none()
            && !name.has(C::Object)
            && !name.has(C::Qualifier)
            && name.value_child(C::Member).is_none()
            && name.value_child(C::Ivar).is_none();
        match bare {
            true => {
                let key = [one(Val::Node(member.into())), one(Val::Node(c.into()))];
                self.call(crate::fn_id!("core", "named_member"), &key)
            }
            false => self.member_of_object(c, member),
        }
    }

    fn named(&mut self, c: Cursor<'t, S>) -> Vals {
        if c.has(C::Qualifier) {
            return self.read(c);
        }
        let ivar = c.value_child(C::Ivar);
        let mut bound = match ivar {
            Some(_) => Vals::new(),
            None => self.read(c),
        };
        if bound.is_empty() {
            bound = self.ivar(ivar.unwrap_or(c));
        }
        bound
            .into_iter()
            .map(|v| {
                let v = self.member_for(v, c);
                match self.view(v) {
                    Some(Ref::Def(d)) => values::field_value(self.at(d)).unwrap_or(v),
                    _ => v,
                }
            })
            .collect()
    }

    pub(super) fn ivar(&mut self, c: Cursor<'t, S>) -> Vals {
        let Some(class) = self.enclosing_class() else {
            return Vals::new();
        };
        let args = [one(Val::Node(class.into())), one(Val::Sym(c.sym()))];
        self.call(crate::fn_id!("core", "ivar"), &args)
    }

    pub(super) fn is_class_ref(&mut self, c: Cursor<'t, S>) -> bool {
        let found = self.type_of(c);
        found
            .iter()
            .any(|v| matches!(self.view(*v), Some(Ref::Def(n)) if self.at(n).is_class()))
    }

    pub(super) fn member_of_object(&mut self, c: Cursor<'t, S>, member: Cursor<'t, S>) -> Vals {
        let Some(object) = member.child(C::Object) else {
            return Vals::new();
        };
        let mut held = Vals::new();
        for v in self.type_of(object) {
            match self.view(v) {
                Some(Ref::Type(n)) => held.push(self.node(n)),
                Some(Ref::Call(n)) if let Some(ty) = values::declared_type(self.at(n)) => {
                    held.extend(self.type_of(ty));
                    held.push(v);
                }
                _ => held.push(v),
            }
        }
        let role = c.role();
        let mut found = Vals::new();
        for v in held {
            match self.view(v) {
                Some(Ref::Def(d)) => found.extend(of(self.members(self.at(d), member.sym(), role))),
                Some(Ref::Import(_)) => found.push(v),
                _ => {}
            }
        }
        found
    }

    pub(super) fn member(&mut self, value: Val, member: Cursor<'t, S>) -> Vals {
        match self.view(value) {
            Some(Ref::Type(n) | Ref::Def(n)) => {
                of(self.members(self.at(n), member.sym(), use_role(member)))
            }
            Some(Ref::Call(n)) if let Some(ty) = values::declared_type(self.at(n)) => {
                let mut out = Vals::new();
                for t in self.type_of(ty) {
                    let args = [one(t), one(Val::Node(member.into()))];
                    for found in self.call(crate::fn_id!("core", "member"), &args) {
                        if !out.contains(&found) {
                            out.push(found);
                        }
                    }
                }
                out
            }
            _ => Vals::new(),
        }
    }

    pub(super) fn unmatched(&mut self, value: Val, member: Cursor<'t, S>) -> Vals {
        let keep = match self.view(value) {
            Some(Ref::Type(_)) | None => false,
            Some(Ref::Def(n)) => self
                .members(self.at(n), member.sym(), use_role(member))
                .is_empty(),
            Some(_) => true,
        };
        match keep {
            true => one(value),
            false => Vals::new(),
        }
    }

    fn parts(&mut self, owner: Cursor<'t, S>) -> Parts<'t, S> {
        let same_name = owner
            .child_sym(C::DefName)
            .and_then(|name| self.state().defs.get(&name).cloned())
            .unwrap_or_default();
        let own = owner.index();
        let mut parts: Parts<'t, S> = SmallVec::from_elem(owner, 1);
        for d in same_name {
            if d != own && (owner.has(C::ImplBlock) || self.at(d).has(C::ImplBlock)) {
                parts.push(self.at(d));
            }
        }
        parts
    }

    fn declared(&mut self, owner: Cursor<'t, S>, name: u32, role: Role) -> Option<Cursor<'t, S>> {
        let args = [
            one(Val::Node(owner.into())),
            one(Val::Sym(name)),
            one(Val::Sym(role_code(role))),
        ];
        let found = self.call(crate::fn_id!("core", "declared"), &args);
        found
            .first()
            .and_then(|v| v.loc())
            .map(|loc| self.jump(loc))
    }

    pub(super) fn members(
        &mut self,
        owner: Cursor<'t, S>,
        name: u32,
        role: Role,
    ) -> Vec<Cursor<'t, S>> {
        let mut seen: FxHashSet<Loc> = FxHashSet::from_iter([owner.into()]);
        let mut level: Parts<'t, S> = SmallVec::from_elem(owner, 1);
        while !level.is_empty() {
            let mut found: Vec<Cursor<'t, S>> = Vec::new();
            let mut parts: Parts<'t, S> = SmallVec::new();
            for &owner in &level {
                let own = self.parts(owner);
                let members: Parts<'t, S> = match self.declared(own[0], name, role) {
                    Some(member) => SmallVec::from_elem(member, 1),
                    None => own[1..]
                        .iter()
                        .filter_map(|&p| self.declared(p, name, role))
                        .collect(),
                };
                for member in members {
                    if !found.iter().any(|f| f.index() == member.index()) {
                        found.push(member);
                    }
                }
                parts.extend(own);
            }
            if !found.is_empty() {
                return found;
            }
            let mut next = Parts::new();
            for part in parts {
                let supers = self
                    .state()
                    .supers
                    .get(&part.index())
                    .cloned()
                    .unwrap_or_default();
                next.extend(
                    supers
                        .into_iter()
                        .map(|d| self.at(d))
                        .filter(|s| seen.insert((*s).into())),
                );
            }
            level = next;
        }
        Vec::new()
    }

    pub(super) fn member_for(&mut self, value: Val, site: Cursor<'t, S>) -> Val {
        let Some(Ref::Def(def)) = self.view(value) else {
            return value;
        };
        let target = self.at(def);
        let tags = self.tags();
        let both = |d: Cursor<'t, S>| d.has_tag(tags.value) && d.has_tag(tags.callable);
        let role = site.role();
        let filled = match role {
            Role::Value => target.has_tag(tags.value),
            Role::Callable => target.has_tag(tags.callable) && !both(target),
            Role::Type => target.has_tag(tags.declares_type),
        };
        let Some(name) = target.child_sym(C::DefName).filter(|_| !filled) else {
            return value;
        };
        let declaring = |up: Cursor<'t, S>| {
            up.index() == def
                || up.is(C::Binding) && up.has(C::Declaration) && up.sym_opt() == Some(name)
        };
        let class = target
            .ancestors()
            .filter(|n| n.is(C::Def))
            .find(|n| n.is_class())
            .filter(|class| target.is_member_of(*class) || site.ancestors().any(declaring));
        let member = class
            .and_then(|class| self.members(class, name, role).first().copied())
            .filter(|&member| !(both(target) && both(member)));
        member.map_or(value, |m| Val::Node(m.into()))
    }

    pub(super) fn callable(&mut self, target: Val) -> Vals {
        match self.view(target) {
            Some(Ref::Type(n)) => {
                let class = self.at(n);
                match class.child_sym(C::Callable) {
                    Some(name) => of(self.members(class, name, Role::Callable)),
                    None => of([class]),
                }
            }
            Some(Ref::Def(n)) if self.at(n).has_tag(self.tags().callable) => one(target),
            _ => Vals::new(),
        }
    }
}
