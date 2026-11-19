use rustc_hash::FxHashSet;
use smallvec::SmallVec;

use super::{Core, Ref, role_code, site_role, values};
use crate::canonical::Canonical as C;
use crate::engine::{Cursor, Cx, Loc, Store, Val, Vals, of, one};
use crate::tree::Role;

type Parts<'t, S> = SmallVec<[Cursor<'t, S>; 4]>;

impl<'t, S: Store> Cx<'_, 't, Core, S> {
    pub(super) fn lookup(&mut self, name: u32, role: Role, values: &[Val]) -> Vals {
        let mut found = Vals::new();
        for &v in values {
            let hits = match self.view(v) {
                Some(Ref::Def(n) | Ref::Type(n)) => of(self.members(self.at(n), name, role)),
                Some(Ref::Import(_)) => one(v),
                _ => continue,
            };
            for hit in hits {
                if !found.contains(&hit) {
                    found.push(hit);
                }
            }
        }
        found
    }

    pub(super) fn unmatched(&mut self, value: Val, member: Cursor<'t, S>) -> Vals {
        let keep = match self.view(value) {
            Some(Ref::Type(_)) | None => false,
            Some(Ref::Def(n)) => {
                let role = site_role(member);
                self.members(self.at(n), member.sym(), role).is_empty()
            }
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

    pub(super) fn meaning(&mut self, value: Val, site: Cursor<'t, S>) -> Vals {
        let value = self.member_for(value, site);
        match self.view(value) {
            Some(Ref::Def(d)) => one(values::field_value(self.at(d)).unwrap_or(value)),
            _ => one(value),
        }
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
