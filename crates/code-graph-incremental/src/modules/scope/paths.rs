use smallvec::SmallVec;

use super::Scope;
use crate::canonical::Canonical as C;
use crate::constants::PATH_SEP;
use crate::engine::{Cursor, Cx, Store, Val, Vals, one};

pub(super) fn join(prefix: &str, path: &str) -> String {
    match (prefix.is_empty(), path.is_empty()) {
        (true, _) => path.to_owned(),
        (_, true) => prefix.to_owned(),
        _ => format!("{prefix}{PATH_SEP}{path}"),
    }
}

fn dir(key: &str) -> &str {
    key.rsplit_once(PATH_SEP).map_or("", |(d, _)| d)
}

fn stem(path: &str) -> &str {
    let (head, last) = path.rsplit_once(PATH_SEP).unwrap_or(("", path));
    match last.rfind('.') {
        Some(at) if at > 0 => &path[..head.len() + usize::from(!head.is_empty()) + at],
        _ => path,
    }
}

pub(super) fn apply_alias(path: &str, pattern: &str, replacement: &str) -> Option<String> {
    if let (Some(prefix), Some(target)) = (pattern.strip_suffix('*'), replacement.strip_suffix('*'))
    {
        return path
            .strip_prefix(prefix)
            .map(|rest| format!("{target}{rest}"));
    }
    let rest = path.strip_prefix(pattern)?;
    (rest.is_empty() || rest.starts_with(PATH_SEP)).then(|| format!("{replacement}{rest}"))
}

fn qualified_path(name: &str) -> String {
    name.split("::")
        .flat_map(|s| s.split(['.', '\\']))
        .filter(|s| !s.is_empty())
        .collect::<Vec<_>>()
        .join(PATH_SEP)
}

impl<'t, S: Store> Cx<'_, 't, Scope, S> {
    pub(super) fn str(&self, sym: u32) -> &'t str {
        self.lang().syms.resolve(sym)
    }

    pub(super) fn intern(&self, text: &str) -> Val {
        Val::Sym(self.lang().syms.intern(text))
    }

    pub(super) fn path_of(&self, c: Cursor<'t, S>) -> String {
        let mut parts: SmallVec<[&str; 8]> = std::iter::once(c)
            .chain(c.ancestors())
            .take_while(|n| !n.is(C::Root))
            .map(|n| self.str(n.sym()))
            .collect();
        parts.reverse();
        parts.join(PATH_SEP)
    }

    fn project_nodes(&self, kind: C) -> Vals {
        let Some(root) = self.project() else {
            return Vals::new();
        };
        root.descendants()
            .filter(|n| n.is(kind))
            .map(|n| Val::Node(n.into()))
            .collect()
    }

    pub(super) fn roots(&self) -> Vals {
        let mut roots = Vals::new();
        for v in self.project_nodes(C::SourceRoot) {
            let marker = self.jump(v.loc().expect("project nodes are nodes"));
            if let Some(dir) = marker.parent().filter(|p| p.is(C::Dir)) {
                let path = self.intern(&self.path_of(dir));
                if !roots.contains(&path) {
                    roots.push(path);
                }
            }
        }
        roots
    }

    pub(super) fn aliases(&self) -> Vals {
        let mut aliases: Vec<(usize, Val)> = self
            .project_nodes(C::Alias)
            .into_iter()
            .filter_map(|v| {
                let alias = self.jump(v.loc()?);
                alias.child_sym(C::Str)?;
                Some((self.str(alias.sym()).len(), v))
            })
            .collect();
        aliases.sort_by_key(|&(len, _)| std::cmp::Reverse(len));
        aliases.into_iter().map(|(_, v)| v).collect()
    }

    pub(super) fn alias(&mut self, import: Cursor<'t, S>, scope: Option<u32>) -> Vals {
        let Some(source) = self.source(import) else {
            return Vals::new();
        };
        let source = self.str(source);
        let aliases = self.call(crate::fn_id!("scope", "aliases"), &[]);
        for alias in aliases.iter().filter_map(|v| v.loc()) {
            let alias = self.jump(alias);
            if alias.child_sym(C::Scope).is_some_and(|s| Some(s) != scope) {
                continue;
            }
            let replacement = alias.child_sym(C::Str).map_or("", |s| self.str(s));
            let Some(path) = apply_alias(source, self.str(alias.sym()), replacement) else {
                continue;
            };
            let path = self.intern(&path);
            if !self.call1(crate::fn_id!("scope", "files"), path).is_empty() {
                return one(path);
            }
        }
        Vals::new()
    }

    pub(super) fn source(&self, import: Cursor<'t, S>) -> Option<u32> {
        import.child_sym(C::SourcePath)
    }

    fn entries(&self) -> SmallVec<[(String, u32); 4]> {
        self.project_nodes(C::SourcePath)
            .into_iter()
            .filter_map(|v| {
                let entry = self.jump(v.loc()?);
                let dir = entry.parent().filter(|p| p.is(C::Dir) || p.is(C::Root))?;
                Some((self.path_of(dir), entry.sym_opt()?))
            })
            .collect()
    }

    pub(super) fn keys(&self, file: Cursor<'t, S>) -> Vals {
        let label = file.tree().label.as_str();
        let stem = stem(label);
        let mut keys: Vals = SmallVec::new();
        let add = |keys: &mut Vals, key: &str| {
            let key = self.intern(key);
            if !keys.contains(&key) {
                keys.push(key);
            }
        };
        add(&mut keys, label);
        add(&mut keys, stem);
        for (dir, path) in self.entries() {
            let path = self.str(path);
            if path == label || path == stem {
                add(&mut keys, &dir);
            }
        }
        let file = super::top(file);
        let package = file
            .child_sym(C::Package)
            .map_or(String::new(), |p| qualified_path(self.str(p)));
        for def in file.children().filter(|d| d.is(C::Def)) {
            if let Some(name) = def.child_sym(C::DefName) {
                add(&mut keys, &join(&package, &qualified_path(self.str(name))));
            }
        }
        keys
    }

    fn file_keys(&mut self, fi: u32) -> Vals {
        self.call1(
            crate::fn_id!("scope", "keys"),
            Val::Node(crate::engine::Loc { fi, node: 0 }),
        )
    }

    pub(super) fn files_at(&mut self, path: &str) -> Vals {
        let roots = self.call(crate::fn_id!("scope", "roots"), &[]);
        let prefixes: SmallVec<[&str; 4]> = std::iter::once("")
            .chain(
                roots
                    .iter()
                    .filter_map(|&r| self.text(r))
                    .map(|s| self.str(s)),
            )
            .collect();
        let glob = path.strip_suffix("/*").or((path == "*").then_some(""));
        let mut found = Vals::new();
        for prefix in prefixes {
            for fi in 0..self.files() {
                if self
                    .jump(crate::engine::Loc { fi, node: 0 })
                    .children()
                    .next()
                    .is_none()
                {
                    continue;
                }
                let keys = self.file_keys(fi);
                let hit = keys.iter().filter_map(|&k| self.text(k)).any(|k| {
                    let key = self.str(k);
                    match glob {
                        Some(d) => dir(key) == join(prefix, d),
                        None => key == join(prefix, path),
                    }
                });
                if hit {
                    found.push(Val::Node(crate::engine::Loc { fi, node: 0 }));
                }
            }
            if glob.is_none() && !found.is_empty() {
                break;
            }
        }
        found.sort_unstable_by_key(|v| v.loc());
        found.dedup();
        found
    }

    pub(super) fn package_dirs(&self, file: Cursor<'t, S>) -> SmallVec<[String; 1]> {
        let label = file.tree().label.as_str();
        let stem = stem(label);
        self.entries()
            .into_iter()
            .filter(|(_, path)| {
                let path = self.str(*path);
                path == label || path == stem
            })
            .map(|(dir, _)| dir)
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::{apply_alias, qualified_path, stem};

    #[test]
    fn an_alias_matches_whole_path_segments_only() {
        assert_eq!(
            apply_alias("app", "app", "src/app").as_deref(),
            Some("src/app")
        );
        assert_eq!(
            apply_alias("app/foo", "app", "src/app").as_deref(),
            Some("src/app/foo")
        );
        assert_eq!(apply_alias("application/foo", "app", "src/app"), None);
        assert_eq!(
            apply_alias("@/x/y", "@/*", "src/*").as_deref(),
            Some("src/x/y")
        );
    }

    #[test]
    fn a_stem_drops_only_the_last_extension() {
        assert_eq!(stem("a/b.py"), "a/b");
        assert_eq!(stem("a/b.d.ts"), "a/b.d");
        assert_eq!(stem("a.b/Dockerfile"), "a.b/Dockerfile");
        assert_eq!(stem(".eslintrc"), ".eslintrc");
        assert_eq!(qualified_path("a::b.c\\d"), "a/b/c/d");
    }
}
