use smallvec::SmallVec;

use super::Scope;
use super::paths::join;
use crate::canonical::Canonical as C;
use crate::constants::WILDCARD;
use crate::engine::{Cursor, Cx, Loc, Store, Val, Vals, of};
use crate::tree::EdgeKind;

struct Bound {
    local: u32,
    imported: Option<u32>,
}

impl<'t, S: Store> Cx<'_, 't, Scope, S> {
    fn wildcard(&self, sym: u32) -> bool {
        self.str(sym) == WILDCARD
    }

    fn bound(&self, name: Cursor<'t, S>) -> Bound {
        let written = name.child_sym(C::SsaHint).unwrap_or(name.sym());
        let imported = (!self.wildcard(written)).then_some(written);
        Bound {
            local: name.child_sym(C::Alias).unwrap_or(written),
            imported,
        }
    }

    fn import_names(
        &self,
        owner: Cursor<'t, S>,
        nested: bool,
    ) -> impl Iterator<Item = Cursor<'t, S>> {
        owner
            .descendants_pruned(move |n| !nested && n.is(C::Def))
            .filter(|n| n.is(C::Name))
            .filter(|n| {
                n.parent()
                    .is_some_and(|i| i.is(C::Import) || i.is(C::ImportType))
            })
    }

    fn imported(&self, name: Cursor<'t, S>) -> Vals {
        of(self
            .targets(EdgeKind::Imports, name.into())
            .iter()
            .map(|&l| self.jump(l)))
    }

    fn scope_of(&self, c: Cursor<'t, S>) -> Cursor<'t, S> {
        c.ancestors()
            .find(|a| a.is(C::Def) || a.parent().is_none())
            .unwrap_or(c)
    }

    pub(super) fn read(&mut self, c: Cursor<'t, S>) -> Vals {
        let Some(sym) = c.sym_opt() else {
            return Vals::new();
        };
        let scope = self.scope_of(c);
        self.call(
            crate::fn_id!("scope", "lookup"),
            &[of([scope]), crate::engine::one(Val::Sym(sym))],
        )
    }

    pub(super) fn lookup(&mut self, scope: Cursor<'t, S>, name: u32) -> Vals {
        let root = scope.parent().is_none();
        let types_only = !root && scope.is_class();
        let defs: Vals = of(scope
            .descendants_pruned(|n| n.is(C::Def))
            .filter(|d| d.is(C::Def) && !d.has(C::ImplBlock))
            .filter(|d| d.child_sym(C::DefName) == Some(name))
            .filter(|d| !types_only || d.is_class()));
        if !defs.is_empty() {
            return defs;
        }
        let names: SmallVec<[Cursor<'t, S>; 2]> = self
            .import_names(scope, false)
            .filter(|n| !n.ancestors().any(|a| a.is(C::ModuleExport)))
            .filter(|n| self.bound(*n).local == name)
            .collect();
        if !names.is_empty() {
            return names.into_iter().flat_map(|n| self.imported(n)).collect();
        }
        if !root {
            let parent = self.scope_of(scope);
            return self.call(
                crate::fn_id!("scope", "lookup"),
                &[of([parent]), crate::engine::one(Val::Sym(name))],
            );
        }
        let wild: SmallVec<[Cursor<'t, S>; 2]> = self
            .import_names(scope, true)
            .filter(|n| {
                let bound = self.bound(*n);
                bound.imported.is_none() && n.child(C::Alias).is_none()
            })
            .collect();
        let targets: Vals = wild.into_iter().flat_map(|n| self.imported(n)).collect();
        let mut found = Vals::new();
        for target in targets {
            found.extend(self.exported_by(target, name));
        }
        found
    }

    fn exported_by(&mut self, target: Val, name: u32) -> Vals {
        self.call(
            crate::fn_id!("scope", "exported"),
            &[
                crate::engine::one(target),
                crate::engine::one(Val::Sym(name)),
            ],
        )
    }

    pub(super) fn exported(&mut self, file: Cursor<'t, S>, name: u32) -> Vals {
        let exports = self.tags().exports;
        let offers = |d: Cursor<'t, S>| {
            let nested = d.is_class()
                && d.ancestors()
                    .find(|a| a.is(C::Def))
                    .is_some_and(|a| a.is_class());
            (!nested && d.child_sym(C::DefName) == Some(name))
                || d.child_sym(C::DefaultExport) == Some(name)
                || d.tag(exports) == Some(name)
        };
        let defs: SmallVec<[Cursor<'t, S>; 2]> = file
            .descendants()
            .filter(|d| d.is(C::Def) && !d.has(C::ImplBlock) && offers(*d))
            .collect();
        let classes: Vals = of(defs.iter().copied().filter(|d| d.is_class()));
        if !classes.is_empty() {
            return classes;
        }
        if !defs.is_empty() {
            return of(defs);
        }
        let listed = self.listed(file, name);
        if !listed.is_empty() {
            return listed;
        }
        let names: SmallVec<[Cursor<'t, S>; 2]> = self
            .import_names(file, true)
            .filter(|n| self.bound(*n).local == name)
            .collect();
        let reexported: Vals = names.into_iter().flat_map(|n| self.imported(n)).collect();
        if !reexported.is_empty() {
            return reexported;
        }
        let wild: SmallVec<[Cursor<'t, S>; 2]> = self
            .import_names(file, true)
            .filter(|n| self.bound(*n).imported.is_none() && n.child(C::Alias).is_none())
            .collect();
        let targets: Vals = wild.into_iter().flat_map(|n| self.imported(n)).collect();
        let mut found = Vals::new();
        for target in targets {
            for v in self.exported_by(target, name) {
                if !found.contains(&v) {
                    found.push(v);
                }
            }
        }
        found
    }

    fn listed(&self, file: Cursor<'t, S>, name: u32) -> Vals {
        let file = super::top(file);
        let top = |local: u32| {
            file.children()
                .find(|d| d.is(C::Def) && d.child_sym(C::DefName) == Some(local))
        };
        let mut found = Vals::new();
        for export in file.children().filter(|n| n.is(C::ModuleExport)) {
            let locals = export
                .names()
                .filter(|n| n.child_sym(C::Alias).unwrap_or(n.sym()) == name)
                .map(|n| n.sym())
                .chain((export.child_sym(C::DefaultExport) == Some(name)).then(|| export.sym()));
            found.extend(of(locals.filter_map(top)));
        }
        found
    }

    pub(super) fn export(&mut self, module: Cursor<'t, S>, name: Val) -> Vals {
        let name = match name {
            Val::Node(loc) => {
                let node = self.jump(loc);
                match node.is(C::Name) {
                    true => self.bound(node).imported,
                    false => node.sym_opt(),
                }
            }
            other => self.text(other),
        };
        let Some(name) = name else {
            return of([module]);
        };
        let found = self.exported_by(Val::Node(module.into()), name);
        if !found.is_empty() {
            return found;
        }
        let mut found = Vals::new();
        for dir in self.package_dirs(module) {
            let path = join(&dir, self.str(name));
            found.extend(self.files_at(&path));
        }
        found
    }

    pub(super) fn foreign(&self, v: Val) -> bool {
        v.loc().is_some_and(|l| l.fi != self.fi())
    }

    pub(super) fn module(&self, v: Val) -> bool {
        matches!(v, Val::Node(Loc { node: 0, .. }))
    }
}
