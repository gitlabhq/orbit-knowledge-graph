use super::ops::{self, Chain, Members, Parts, Types};
use super::{Core, Ref, binding_type, field_value, role};
use crate::canonical::Canonical as C;
use crate::engine::{Cursor, Cx, Val, Vals, of, one};
use crate::tags::ReservedTags;
use crate::tree::{Role, is_name_text};

impl<'t> Types<'t> for Cx<'_, 't, Core> {
    fn parts(&mut self, owner: Cursor<'t>) -> Parts<'t> {
        let dn = owner.index();
        let same_name = owner
            .child_sym(C::DefName)
            .and_then(|name| self.state().defs.get(&name).cloned())
            .unwrap_or_default();
        let same_name = same_name
            .into_iter()
            .filter(|&d| d != dn && (owner.has(C::ImplBlock) || self.at(d).has(C::ImplBlock)));
        std::iter::once(owner)
            .chain(same_name.map(|d| self.at(d)))
            .collect()
    }

    fn supers(&mut self, part: Cursor<'t>) -> Parts<'t> {
        let supers = self.state().supers.get(&part.index()).cloned();
        supers.into_iter().flatten().map(|d| self.at(d)).collect()
    }

    fn members_index(&mut self) -> (&Members, ReservedTags) {
        let tags = self.tags();
        (&self.state().members, tags)
    }
}

impl<'t> Chain<'t> for Cx<'_, 't, Core> {
    type Out = Vals;

    fn early(&mut self, c: Cursor<'t>) -> Option<Vals> {
        let call = c.value_child(C::Call)?;
        if call.has(C::Property) && self.is_class_ref(c.reference()) {
            return None;
        }
        Some(of([call]))
    }

    fn name(&mut self, c: Cursor<'t>) -> Vals {
        if c.has(C::Qualifier) {
            let r = self.ssa(crate::fn_id!("ssa", "read_static"), c);
            return r;
        }
        let ivar = c.value_child(C::Ivar);
        let mut bound = match ivar {
            Some(_) => Vals::new(),
            None => self.ssa(crate::fn_id!("ssa", "read"), c),
        };
        if bound.is_empty() {
            bound = self.ssa(crate::fn_id!("core", "ivar"), ivar.unwrap_or(c));
        }
        bound
            .into_iter()
            .map(|r| {
                let r = self.member_for(r, c);
                match self.view(r) {
                    Some(Ref::Def(d)) => field_value(self.at(d)).unwrap_or(r),
                    _ => r,
                }
            })
            .collect()
    }

    fn member(&mut self, c: Cursor<'t>, member: Cursor<'t>, object: Cursor<'t>) -> Vals {
        let name = object.reference();
        let bare = object.value_child(C::Call).is_none()
            && !name.has(C::Object)
            && !name.has(C::Qualifier)
            && name.value_child(C::Member).is_none()
            && name.value_child(C::Ivar).is_none();
        if !bare {
            return self.member_of_object(c, member, object);
        }
        let key = [
            one(Val::Sym(name.sym())),
            one(Val::Sym(member.sym())),
            one(Val::Node(c.into())),
            one(Val::Node(member.into())),
            one(Val::Node(object.into())),
        ];
        self.call(crate::fn_id!("core", "named_member"), &key)
    }
}

impl<'t> Cx<'_, 't, Core> {
    pub(super) fn is_class_ref(&mut self, c: Cursor<'t>) -> bool {
        let found = self.ssa(crate::fn_id!("core", "type"), c);
        found
            .iter()
            .any(|v| matches!(self.view(*v), Some(Ref::Def(n)) if self.at(n).is_class()))
    }

    pub(super) fn member_of_object(
        &mut self,
        c: Cursor<'t>,
        member: Cursor<'t>,
        object: Cursor<'t>,
    ) -> Vals {
        let mut targets = Vals::new();
        for r in self.ssa(crate::fn_id!("core", "type"), object) {
            match self.view(r) {
                Some(Ref::Type(node)) => targets.push(Val::Node(self.loc(node))),
                Some(Ref::Call(n)) if let Some(ty) = binding_type(self.at(n)) => {
                    targets.extend(self.ssa(crate::fn_id!("core", "type"), ty));
                    targets.push(r);
                }
                _ => targets.push(r),
            }
        }
        let role = c.role();
        let mut found = Vals::new();
        for r in targets {
            match self.view(r) {
                Some(Ref::Def(d)) => {
                    found.extend(of(ops::members(self, self.at(d), member.sym(), role)));
                }
                Some(Ref::Import(_)) => found.push(r),
                _ => {}
            }
        }
        found
    }

    pub(super) fn member(&mut self, value: Val, member: Cursor<'t>) -> Vals {
        match self.view(value) {
            Some(Ref::Type(n) | Ref::Def(n)) => {
                of(ops::members(self, self.at(n), member.sym(), role(member)))
            }
            Some(Ref::Call(n)) if let Some(ty) = binding_type(self.at(n)) => {
                let mut out = Vals::new();
                for t in self.ssa(crate::fn_id!("core", "type"), ty) {
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

    pub(super) fn unmatched(&mut self, value: Val, member: Cursor<'t>) -> Vals {
        let keep = match self.view(value) {
            Some(Ref::Type(_)) | None => false,
            Some(Ref::Def(n)) => {
                ops::members(self, self.at(n), member.sym(), role(member)).is_empty()
            }
            Some(_) => true,
        };
        match keep {
            true => one(value),
            false => Vals::new(),
        }
    }

    pub(super) fn reference(&mut self, reference: Cursor<'t>) -> Vals {
        let found = self.ssa(crate::fn_id!("core", "type"), reference);
        let parent = reference.parent().map_or(reference.index(), Cursor::index);
        match found.as_slice() {
            [v] if matches!(self.view(*v), Some(Ref::Def(t) | Ref::Import(t)) if t != parent) => {
                found
            }
            _ => Vals::new(),
        }
    }

    /// The class member a bare name means at `site`, when the name is a member
    /// of the enclosing class that the site cannot mean as written.
    fn member_for(&mut self, value: Val, site: Cursor<'t>) -> Val {
        let Some(Ref::Def(def)) = self.view(value) else {
            return value;
        };
        let target = self.at(def);
        let tags = self.tags();
        let both = |d: Cursor<'t>| d.has_tag(tags.value) && d.has_tag(tags.callable);
        let role = site.role();
        let filled = match role {
            Role::Value => target.has_tag(tags.value),
            Role::Callable => target.has_tag(tags.callable) && !both(target),
            Role::Type => target.has_tag(tags.declares_type),
        };
        let Some(name) = target.child_sym(C::DefName).filter(|_| !filled) else {
            return value;
        };
        let declaring = |up: Cursor<'t>| {
            up.index() == def
                || up.is(C::Binding) && up.has(C::Declaration) && up.sym_opt() == Some(name)
        };
        let class = target
            .ancestors()
            .filter(|node| node.is(C::Def))
            .find(|node| node.is_class())
            .filter(|class| target.is_member_of(*class) || site.ancestors().any(declaring));
        let member = class
            .and_then(|class| ops::members(self, class, name, role).first().copied())
            .filter(|&member| !(both(target) && both(member)));
        member.map_or(value, |m| Val::Node(m.into()))
    }

    pub(super) fn callee_targets(&mut self, call: Cursor<'t>) -> Vals {
        let Some(callee) = call.child(C::Callee).filter(|c| c.sym_opt().is_some()) else {
            return Vals::new();
        };
        match call.has_tag(self.tags().implicit_self) {
            true => self.implicit_targets(callee, call),
            false => self.name_targets(callee, call, false),
        }
    }

    fn implicit_targets(&mut self, callee: Cursor<'t>, site: Cursor<'t>) -> Vals {
        if !self.ssa(crate::fn_id!("ssa", "defined"), callee).is_empty() {
            let bound = self.ssa(crate::fn_id!("ssa", "read"), callee);
            if !bound.is_empty() {
                let named: Vals = bound
                    .into_iter()
                    .filter(|v| matches!(self.view(*v), Some(Ref::Def(_) | Ref::Import(_))))
                    .collect();
                return named
                    .into_iter()
                    .map(|r| self.member_for(r, site))
                    .collect();
            }
        }
        if let Some(class) = self.enclosing_class() {
            let members = ops::members(self, class, callee.sym(), site.role());
            if !members.is_empty() {
                return of(members);
            }
        }
        self.name_targets(callee, site, true)
    }

    fn name_targets(&mut self, callee: Cursor<'t>, site: Cursor<'t>, callable_only: bool) -> Vals {
        let targets: Vals = self
            .ssa(crate::fn_id!("ssa", "read"), callee)
            .into_iter()
            .map(|r| self.member_for(r, site))
            .collect();
        if !targets.is_empty() || !is_name_text(self.env().lang.syms.resolve(callee.sym())) {
            return targets;
        }
        let callable = self.tags().callable;
        let mut wild = self.call(crate::fn_id!("ssa", "wildcards"), &[]);
        wild.retain(|v| {
            !callable_only
                || v.loc()
                    .and_then(|loc| self.jump(loc).parent())
                    .is_some_and(|import| import.has_tag(callable))
        });
        wild
    }
}
