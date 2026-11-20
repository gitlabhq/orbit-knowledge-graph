use rustc_hash::FxHashSet;
use smallvec::SmallVec;

use super::{Core, Ref, Roles, role_code, site_role, values};
use crate::canonical::Canonical as C;
use crate::engine::{Cursor, Cx, Loc, Store, Val, Vals, of, one};
use crate::tree::Role;

type Parts<'t, S> = SmallVec<[Cursor<'t, S>; 4]>;

impl<'t, S: Store> Cx<'_, 't, Core, S> {
    pub(super) fn lookup(&mut self, name: u32, role: Role, roles: &Roles, values: &[Val]) -> Vals {
        let mut found = Vals::new();
        for &v in values {
            let hits = match self.view(v) {
                Some(Ref::Def(n) | Ref::Type(n)) => {
                    of(self.members(self.jump(n), name, role, roles))
                }
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

    pub(super) fn unmatched(&mut self, value: Val, member: Cursor<'t, S>, roles: &Roles) -> Vals {
        let keep = match self.view(value) {
            Some(Ref::Type(_)) | None => false,
            Some(Ref::Def(n)) => {
                let role = site_role(member);
                self.members(self.jump(n), member.sym(), role, roles)
                    .is_empty()
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
            .map(|name| self.named_defs(name))
            .unwrap_or_default();
        let own = Loc::from(owner);
        let mut parts: Parts<'t, S> = SmallVec::from_elem(owner, 1);
        for d in same_name {
            if d != own && (owner.has(C::ImplBlock) || self.jump(d).has(C::ImplBlock)) {
                parts.push(self.jump(d));
            }
        }
        parts
    }

    fn declared(
        &mut self,
        owner: Cursor<'t, S>,
        name: u32,
        role: Role,
        roles: &Roles,
    ) -> Option<Cursor<'t, S>> {
        let _ = roles;
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
        roles: &Roles,
    ) -> Vec<Cursor<'t, S>> {
        let mut seen: FxHashSet<Loc> = FxHashSet::from_iter([owner.into()]);
        let mut level: Parts<'t, S> = SmallVec::from_elem(owner, 1);
        while !level.is_empty() {
            let mut found: Vec<Cursor<'t, S>> = Vec::new();
            let mut parts: Parts<'t, S> = SmallVec::new();
            for &owner in &level {
                let own = self.parts(owner);
                let members: Parts<'t, S> = match self.declared(own[0], name, role, roles) {
                    Some(member) => SmallVec::from_elem(member, 1),
                    None => own[1..]
                        .iter()
                        .filter_map(|&p| self.declared(p, name, role, roles))
                        .collect(),
                };
                for member in members {
                    if !found.iter().any(|f| Loc::from(*f) == Loc::from(member)) {
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
                for s in self.supers(part.into()) {
                    if seen.insert(s) {
                        next.push(self.jump(s));
                    }
                }
            }
            level = next;
        }
        Vec::new()
    }

    pub(super) fn meaning(&mut self, value: Val, site: Cursor<'t, S>, roles: &Roles) -> Vals {
        let value = self.member_for(value, site, roles);
        match self.view(value) {
            Some(Ref::Def(d)) => one(values::field_value(self.jump(d)).unwrap_or(value)),
            _ => one(value),
        }
    }

    pub(super) fn member_for(&mut self, value: Val, site: Cursor<'t, S>, roles: &Roles) -> Val {
        let Some(Ref::Def(def)) = self.view(value) else {
            return value;
        };
        let target = self.jump(def);
        let both = |d: Cursor<'t, S>| roles.both(d);
        let role = site.role();
        let filled = roles.has(target, role) && !(role == Role::Callable && both(target));
        let Some(name) = target.child_sym(C::DefName).filter(|_| !filled) else {
            return value;
        };
        let declaring = |up: Cursor<'t, S>| {
            Loc::from(up) == def
                || up.is(C::Binding) && up.has(C::Declaration) && up.sym_opt() == Some(name)
        };
        let class = target
            .ancestors()
            .filter(|n| n.is(C::Def))
            .find(|n| n.is_class())
            .filter(|class| target.is_member_of(*class) || site.ancestors().any(declaring));
        let member = class
            .and_then(|class| self.members(class, name, role, roles).first().copied())
            .filter(|&member| !(both(target) && both(member)));
        member.map_or(value, |m| Val::Node(m.into()))
    }

    pub(super) fn callable(&mut self, target: Val, roles: &Roles) -> Vals {
        match self.view(target) {
            Some(Ref::Type(n)) => {
                let class = self.jump(n);
                match class.child_sym(C::Callable) {
                    Some(name) => of(self.members(class, name, Role::Callable, roles)),
                    None => of([class]),
                }
            }
            Some(Ref::Def(n)) if roles.has(self.jump(n), Role::Callable) => one(target),
            _ => Vals::new(),
        }
    }

    fn ancestors(&mut self, class: Loc) -> FxHashSet<Loc> {
        let mut seen = FxHashSet::from_iter([class]);
        let mut stack = vec![class];
        while let Some(at) = stack.pop() {
            for part in self.parts(self.jump(at)) {
                for s in self.supers(part.into()) {
                    if seen.insert(s) {
                        stack.push(s);
                    }
                }
            }
        }
        seen
    }

    pub(super) fn lub(&mut self, values: &[Val]) -> Vals {
        let mut common: Option<FxHashSet<Loc>> = None;
        for &v in values {
            let Some(Ref::Def(class) | Ref::Type(class)) = self.view(v) else {
                return Vals::new();
            };
            if !self.jump(class).is_class() {
                return Vals::new();
            }
            let above = self.ancestors(class);
            match &mut common {
                Some(common) => common.retain(|c| above.contains(c)),
                None => common = Some(above),
            }
        }
        let common: Vec<Loc> = common.into_iter().flatten().collect();
        let above: Vec<FxHashSet<Loc>> = common.iter().map(|&c| self.ancestors(c)).collect();
        let least: Vec<Loc> = common
            .iter()
            .filter(|t| {
                !common
                    .iter()
                    .zip(&above)
                    .any(|(u, a)| u != *t && a.contains(*t))
            })
            .copied()
            .collect();
        match least.as_slice() {
            [one_class] => one(Val::Node(*one_class)),
            _ => Vals::new(),
        }
    }
}
