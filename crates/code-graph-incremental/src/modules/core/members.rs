use rustc_hash::FxHashSet;
use smallvec::SmallVec;

use super::{Core, Ref, role_code, values};
use crate::canonical::Canonical as C;
use crate::engine::{Cursor, Cx, Loc, Store, Val, Vals, one};
use crate::tree::Role;

type Parts<'t, S> = SmallVec<[Cursor<'t, S>; 4]>;

impl<'t, S: Store> Cx<'_, 't, Core, S> {
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

    pub(super) fn members(
        &mut self,
        owner: Cursor<'t, S>,
        name: u32,
        role: Role,
    ) -> Vec<Cursor<'t, S>> {
        let args = [
            one(Val::Node(owner.into())),
            one(Val::Sym(name)),
            one(Val::Sym(role_code(self.lang(), role))),
        ];
        let found = self.call(crate::fn_id!("core", "members"), &args);
        found
            .iter()
            .filter_map(|v| v.loc())
            .map(|loc| self.jump(loc))
            .collect()
    }

    pub(super) fn meaning(&mut self, value: Val, site: Cursor<'t, S>) -> Vals {
        let value = self.member_for(value, site);
        match self.view(value) {
            Some(Ref::Def(d)) => one(values::field_value(self.jump(d)).unwrap_or(value)),
            _ => one(value),
        }
    }

    pub(super) fn member_for(&mut self, value: Val, site: Cursor<'t, S>) -> Val {
        let args = [one(value), one(Val::Node(site.into()))];
        let found = self.call(crate::fn_id!("core", "member_for"), &args);
        found.first().copied().unwrap_or(value)
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
