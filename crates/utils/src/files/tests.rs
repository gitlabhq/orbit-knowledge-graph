//! The contract of the repository filesystem, exercised through `Vfs::load`
//! and the public verbs only.

use std::io::{ErrorKind, Write};
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use flate2::write::GzEncoder;

use super::sources::{Archive, Changed, Checkout, Memory};
use super::{Decision, File, Kind, Limits, Loading, Options, Pass, Put, Source, SourceError, Vfs};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
enum Role {
    Source,
    #[default]
    Input,
}

/// The shape of a code filter: pngs are listed at the header, `.rs` is a
/// source, a `.toml` is sniffed first, and a NUL in the bytes lists the file.
struct CodeFilter;

impl Pass for CodeFilter {
    type Tag = Role;

    fn header(&self, file: &mut File<Role>) {
        if file.path.ends_with(".png") {
            file.decide(Decision::List("excluded_extension"));
        } else if file.path.ends_with(".rs") {
            file.decide(Decision::Keep(Role::Source));
        }
    }

    fn content(&self, file: &mut File<Role>, bytes: &[u8]) {
        if bytes.contains(&0) {
            file.decide(Decision::List("binary"));
        }
    }
}

struct DropLogs;

impl Pass for DropLogs {
    type Tag = Role;

    fn header(&self, file: &mut File<Role>) {
        if file.path.ends_with(".log") {
            file.decide(Decision::Drop("log"));
        }
    }
}

fn memory(files: &[(&str, &[u8])]) -> Memory {
    Memory(
        files
            .iter()
            .map(|(path, bytes)| (path.to_string(), bytes.to_vec()))
            .collect(),
    )
}

fn load<T: super::Tag>(
    source: impl Source,
    passes: impl Pass<Tag = T> + 'static,
    limits: Limits,
) -> Vfs<T> {
    Vfs::load(source, passes, limits, Options::default()).unwrap()
}

fn rows<T: super::Tag>(vfs: &Vfs<T>) -> Vec<(String, Decision<T>)> {
    vfs.files()
        .map(|file| (file.path.clone(), file.decision()))
        .collect()
}

fn text<T: super::Tag>(vfs: &Vfs<T>, path: &str) -> String {
    String::from_utf8(vfs.read(Path::new(path)).unwrap().to_vec()).unwrap()
}

fn read_error<T: super::Tag>(vfs: &Vfs<T>, path: &str) -> ErrorKind {
    vfs.read(Path::new(path)).unwrap_err().kind()
}

fn write(root: &Path, relative: &str, body: &[u8]) {
    let path = root.join(relative);
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(path, body).unwrap();
}

#[test]
fn a_chain_lets_the_later_pass_refine_the_earlier_decision() {
    struct Rescue;
    impl Pass for Rescue {
        type Tag = Role;
        fn header(&self, file: &mut File<Role>) {
            if file.path.ends_with(".keep.png") {
                file.decide(Decision::Keep(Role::Input));
            }
        }
    }
    let passes = CodeFilter.then(Rescue);

    let mut listed = File::new("a.png".into(), 1);
    passes.header(&mut listed);
    assert_eq!(listed.decision(), Decision::List("excluded_extension"));

    let mut rescued = File::new("a.keep.png".into(), 1);
    passes.header(&mut rescued);
    assert_eq!(rescued.decision(), Decision::Keep(Role::Input));
    passes.content(&mut rescued, b"\x00");
    assert_eq!(rescued.decision(), Decision::List("binary"));
}

#[test]
fn behaves_like_a_filesystem_rooted_at_the_repository() {
    let vfs = load(
        memory(&[
            ("src/main.rs", b"fn main() {}"),
            ("/src/lib/mod.rs", b"pub mod a;"),
            ("./README.md", b"# hi"),
        ]),
        (),
        Limits::default(),
    );

    assert_eq!(text(&vfs, "src/main.rs"), "fn main() {}");
    assert_eq!(text(&vfs, "/src/main.rs"), "fn main() {}");
    assert_eq!(
        text(&vfs, "src/lib/../main.rs"),
        "fn main() {}",
        "`..` inside the root is a path"
    );
    let stat = vfs.stat(Path::new("src/main.rs")).unwrap();
    assert_eq!(
        (stat.path.as_path(), stat.kind, stat.len, stat.link),
        (Path::new("/src/main.rs"), Kind::File, 12, None)
    );
    assert_eq!(vfs.stat(Path::new("src")).unwrap().kind, Kind::Dir);
    assert_eq!(vfs.stat(Path::new("/")).unwrap().kind, Kind::Dir);
    for missing in ["src/missing.rs", "../escape.rs", "src/../../escape.rs"] {
        assert_eq!(
            vfs.stat(Path::new(missing)).unwrap_err().kind(),
            ErrorKind::NotFound,
            "{missing}"
        );
    }
    assert_eq!(vfs.read_dir(Path::new("/")).unwrap(), ["README.md", "src"]);
    assert_eq!(vfs.read_dir(Path::new("src")).unwrap(), ["lib", "main.rs"]);
    assert_eq!(read_error(&vfs, "src"), ErrorKind::IsADirectory);
    assert_eq!(read_error(&vfs, "nope"), ErrorKind::NotFound);
    assert_eq!(
        vfs.read_dir(Path::new("README.md")).unwrap_err().kind(),
        ErrorKind::NotADirectory
    );
    let below_src: Vec<&str> = vfs
        .subtree(Path::new("src"))
        .map(|f| f.path.as_str())
        .collect();
    assert_eq!(below_src, ["src/lib/mod.rs", "src/main.rs"]);
}

#[test]
fn the_passes_decide_what_is_kept_listed_and_dropped() {
    let vfs = load(
        memory(&[
            ("src/main.rs", b"fn main() {}"),
            ("Cargo.toml", b"[package]"),
            ("assets/logo.png", b"\x89PNG"),
            ("model/weights.bin", b"\x00\x01"),
            ("build.log", b"noise"),
        ]),
        CodeFilter.then(DropLogs),
        Limits::default(),
    );

    assert_eq!(
        rows(&vfs),
        [
            ("Cargo.toml".into(), Decision::Keep(Role::Input)),
            (
                "assets/logo.png".into(),
                Decision::List("excluded_extension")
            ),
            ("model/weights.bin".into(), Decision::List("binary")),
            ("src/main.rs".into(), Decision::Keep(Role::Source)),
        ],
        "a sniffed file no pass objected to keeps the default tag"
    );
    assert_eq!(
        read_error(&vfs, "model/weights.bin"),
        ErrorKind::Unsupported
    );
    assert_eq!(
        vfs.stat(Path::new("assets/logo.png")).unwrap().decision,
        Some(Decision::List("excluded_extension"))
    );
    assert_eq!(
        vfs.stat(Path::new("build.log")).unwrap_err().kind(),
        ErrorKind::NotFound
    );
    let usage = vfs.usage();
    assert_eq!((usage.files, usage.bytes, usage.kept), (4, 32, 21));
    assert_eq!(usage.resident, 21, "only kept files cost bytes");
}

#[test]
fn a_drop_decided_by_the_content_leaves_no_trace_either() {
    struct DropBinaries;
    impl Pass for DropBinaries {
        type Tag = ();
        fn content(&self, file: &mut File<()>, bytes: &[u8]) {
            if bytes.contains(&0) {
                file.decide(Decision::Drop("binary"));
            }
        }
    }
    let dir = tempfile::tempdir().unwrap();
    write(dir.path(), "blob.bin", b"\x00");
    write(dir.path(), "text.rs", b"fn t() {}");
    let from_bytes = load(
        memory(&[("blob.bin", b"\x00"), ("text.rs", b"fn t() {}")]),
        DropBinaries,
        Limits::default(),
    );
    let from_disk = load(Checkout(dir.path()), DropBinaries, Limits::default());

    for vfs in [&from_bytes, &from_disk] {
        let paths: Vec<&str> = vfs.files().map(|f| f.path.as_str()).collect();
        assert_eq!(paths, ["text.rs"]);
        assert_eq!(vfs.read_dir(Path::new("/")).unwrap(), ["text.rs"]);
        assert_eq!(
            vfs.stat(Path::new("blob.bin")).unwrap_err().kind(),
            ErrorKind::NotFound
        );
    }
}

#[test]
fn caps_are_charged_for_every_file_before_any_decision() {
    let three = || memory(&[("a.log", b"1"), ("b.log", b"22"), ("c.log", b"333")]);
    let files = Limits {
        files: Some(2),
        ..Limits::default()
    };
    let total = Limits {
        total_bytes: Some(5),
        ..Limits::default()
    };
    for limits in [files, total] {
        let failed = Vfs::load(three(), DropLogs, limits, Options::default());
        assert!(
            matches!(failed, Err(SourceError::Cap(_))),
            "dropped files still count: {limits:?}"
        );
    }

    let oversize = Limits {
        file_bytes: Some(4),
        ..Limits::default()
    };
    let vfs = load(
        memory(&[("big.rs", b"12345"), ("ok.rs", b"1")]),
        CodeFilter,
        oversize,
    );
    assert_eq!(
        rows(&vfs),
        [
            ("big.rs".into(), Decision::List("oversize")),
            ("ok.rs".into(), Decision::Keep(Role::Source)),
        ],
        "the store lists an oversize file before the passes see it"
    );
}

/// A source that offers each file lazily and counts how often it was asked
/// to produce bytes.
struct Lazy<'a> {
    files: Vec<(&'static str, &'static [u8])>,
    reads: &'a AtomicUsize,
}

impl Source for Lazy<'_> {
    fn fill<T: super::Tag>(self, into: &Loading<T>) -> Result<(), SourceError> {
        for (path, bytes) in self.files {
            let read = Box::new(|| {
                self.reads.fetch_add(1, Ordering::SeqCst);
                Ok(bytes.to_vec())
            });
            into.put(
                path,
                Put::Lazy {
                    size: bytes.len() as u64,
                    read,
                },
            )?;
        }
        Ok(())
    }
}

#[test]
fn a_lazy_file_is_produced_once_and_never_when_the_header_settled_it() {
    let reads = AtomicUsize::new(0);
    let vfs = load(
        Lazy {
            files: vec![
                ("src/main.rs", b"fn main() {}"),
                ("Cargo.toml", b"[package]"),
                ("logo.png", b"\x89PNG"),
                ("build.log", b"noise"),
            ],
            reads: &reads,
        },
        CodeFilter.then(DropLogs),
        Limits::default(),
    );

    assert_eq!(
        reads.load(Ordering::SeqCst),
        2,
        "only the kept and the sniffed file"
    );
    assert_eq!(text(&vfs, "Cargo.toml"), "[package]");
    assert_eq!(text(&vfs, "src/main.rs"), "fn main() {}");
    assert_eq!(reads.load(Ordering::SeqCst), 2, "reads come from the store");
}

#[test]
fn identical_content_at_many_paths_is_stored_once_and_stays_many_files() {
    let body = vec![b'x'; 80];
    let vfs = load(
        Memory(
            ["a/one.js", "b/two.js", "c/three.js"]
                .map(|path| (path.to_string(), body.clone()))
                .into(),
        ),
        (),
        Limits {
            resident_bytes: Some(100),
            ..Limits::default()
        },
    );

    let usage = vfs.usage();
    assert_eq!((usage.files, usage.kept), (3, 240));
    assert_eq!(
        (usage.resident, usage.spilled, usage.deduped_bytes),
        (80, 0, 160),
        "240 shared bytes fit a budget of 100"
    );
    assert_eq!(vfs.read_dir(Path::new("b")).unwrap(), ["two.js"]);
    assert_eq!(&*vfs.read(Path::new("c/three.js")).unwrap(), &body[..]);
}

#[test]
fn bytes_past_the_budget_spill_and_read_back_identically() {
    let files = [
        ("small.rs", b"fits".as_slice()),
        ("big.rs", &[b'b'; 64]),
        ("later.rs", b"also spilled"),
    ];
    let budgets = [
        (Some(10), "a small budget"),
        (Some(0), "a zero budget spills everything"),
    ];
    for (resident_bytes, case) in budgets {
        for compress_spill in [false, true] {
            let vfs = Vfs::load(
                memory(&files),
                (),
                Limits {
                    resident_bytes,
                    ..Limits::default()
                },
                Options {
                    compress_spill,
                    ..Options::default()
                },
            )
            .unwrap();
            let usage = vfs.usage();
            assert!(usage.spilled > 0, "{case}");
            assert_eq!(
                usage.resident,
                if resident_bytes == Some(0) { 0 } else { 4 }
            );
            for (path, body) in files {
                assert_eq!(
                    &*vfs.read(Path::new(path)).unwrap(),
                    body,
                    "{case} compressed={compress_spill}"
                );
            }
            assert_eq!(vfs.stat(Path::new("big.rs")).unwrap().len, 64);
        }
    }
}

#[test]
fn compressed_spill_takes_less_disk_and_a_disk_cap_fails_the_load() {
    let compressible = vec![b'a'; 4096];
    let spilled_with = |compress_spill: bool| {
        Vfs::load(
            memory(&[("a.txt", &compressible)]),
            (),
            Limits {
                resident_bytes: Some(0),
                ..Limits::default()
            },
            Options {
                compress_spill,
                ..Options::default()
            },
        )
        .unwrap()
        .usage()
        .spilled
    };
    assert_eq!(spilled_with(false), 4096);
    assert!(
        spilled_with(true) < 100,
        "4096 identical bytes compress to a few"
    );

    let capped = Vfs::load(
        memory(&[("a.txt", &compressible)]),
        (),
        Limits {
            resident_bytes: Some(0),
            spilled_bytes: Some(1024),
            ..Limits::default()
        },
        Options::default(),
    );
    assert!(matches!(
        capped,
        Err(SourceError::Cap(cap)) if cap.metric == "spilled_bytes"
    ));
}

/// Eight workers putting the same 80 bytes at once; a budget of 100 leaves
/// no room for a double charge.
struct Workers(Vec<u8>);

impl Source for Workers {
    fn fill<T: super::Tag>(self, into: &Loading<T>) -> Result<(), SourceError> {
        std::thread::scope(|scope| {
            for worker in 0..8 {
                let body = &self.0;
                scope.spawn(move || {
                    into.put(&format!("w{worker}.js"), Put::Bytes(body.clone()))
                        .unwrap();
                    for i in 0..200 {
                        let unique = format!("worker {worker} file {i}").into_bytes();
                        into.put(&format!("w{worker}/f{i}.txt"), Put::Bytes(unique))
                            .unwrap();
                    }
                });
            }
        });
        Ok(())
    }
}

#[test]
fn concurrent_puts_share_the_store_and_charge_shared_content_once() {
    let vfs = load(
        Workers(vec![b'x'; 80]),
        (),
        Limits {
            resident_bytes: Some(100_000),
            ..Limits::default()
        },
    );

    let usage = vfs.usage();
    assert_eq!(usage.files, 8 + 1_600);
    assert_eq!(
        usage.deduped_bytes,
        7 * 80,
        "the shared content was charged once"
    );
    assert_eq!(usage.spilled, 0);
    assert_eq!(text(&vfs, "w3/f7.txt"), "worker 3 file 7");
    assert_eq!(vfs.read_dir(Path::new("w5")).unwrap().len(), 200);
}

#[test]
fn symlinks_are_listed_followed_inside_the_repository_and_never_escape() {
    struct Linked;
    impl Source for Linked {
        fn fill<T: super::Tag>(self, into: &Loading<T>) -> Result<(), SourceError> {
            into.put("shared/skills/a/SKILL.md", Put::Bytes(b"# a".to_vec()))?;
            into.put("Gemfile", Put::Bytes(b"source 'x'".to_vec()))?;
            into.put("Gemfile.next", Put::Symlink("Gemfile".into()))?;
            into.put(".agents/skills", Put::Symlink("../shared/skills".into()))?;
            into.put("bin/run", Put::Symlink("../Gemfile.next".into()))?;
            into.put("abs", Put::Symlink("/Gemfile".into()))?;
            into.put("etc", Put::Symlink("../../etc/passwd".into()))?;
            into.put("root_etc", Put::Symlink("/etc/passwd".into()))?;
            into.put("dangling", Put::Symlink("nowhere".into()))?;
            into.put("ping", Put::Symlink("pong".into()))?;
            into.put("pong", Put::Symlink("ping".into()))
        }
    }
    let vfs = load(Linked, CodeFilter, Limits::default());

    assert_eq!(text(&vfs, "Gemfile.next"), "source 'x'");
    assert_eq!(text(&vfs, "bin/run"), "source 'x'", "a chain");
    assert_eq!(text(&vfs, "abs"), "source 'x'", "a rooted target");
    assert_eq!(
        text(&vfs, "/.agents/skills/a/SKILL.md"),
        "# a",
        "a path through a symlinked directory"
    );
    assert_eq!(
        vfs.stat(Path::new(".agents/skills")).unwrap().kind,
        Kind::Dir
    );
    assert_eq!(vfs.read_dir(Path::new(".agents/skills")).unwrap(), ["a"]);
    let run = vfs.stat(Path::new("./bin/../bin/run")).unwrap();
    assert_eq!(run.path, Path::new("/Gemfile"), "canonical");
    assert_eq!(run.link.as_deref(), Some(Path::new("../Gemfile.next")));
    assert_eq!(
        run.decision,
        Some(Decision::Keep(Role::Input)),
        "the decision of the file reached"
    );
    assert_eq!(
        vfs.files()
            .find(|f| f.path == "Gemfile.next")
            .unwrap()
            .decision(),
        Decision::List("symlink"),
        "the link itself is never parsed"
    );

    for path in ["etc", "root_etc", "dangling"] {
        assert_eq!(read_error(&vfs, path), ErrorKind::NotFound, "{path}");
        assert_eq!(
            vfs.stat(Path::new(path)).unwrap_err().kind(),
            ErrorKind::NotFound,
            "{path} leads nowhere"
        );
    }
    assert!(vfs.read(Path::new("ping")).is_err(), "a loop ends");
    assert!(vfs.read(Path::new("ping/deeper")).is_err());
}

#[test]
fn two_entries_for_one_path_keep_the_later_one() {
    let vfs = load(
        memory(&[("a.rs", b"first"), ("b.rs", b"b"), ("a.rs", b"second")]),
        (),
        Limits::default(),
    );
    assert_eq!(text(&vfs, "a.rs"), "second");
    assert_eq!((vfs.usage().files, vfs.usage().duplicate_paths), (2, 1));
}

#[test]
fn a_cancelled_load_stops_at_the_next_file() {
    let flag = AtomicBool::new(true);
    let cancelled = Vfs::load(
        memory(&[("a.rs", b"a")]),
        (),
        Limits::default(),
        Options {
            cancelled: Some(Box::new(move || flag.load(Ordering::SeqCst))),
            ..Options::default()
        },
    );
    assert!(matches!(cancelled, Err(SourceError::Cancelled)));
}

#[test]
fn a_checkout_lists_what_git_lists() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path();
    write(root, "src/main.rs", b"fn main(){}");
    write(root, ".git/config", b"[core]\n");
    write(root, ".gitignore", b"build/\n");
    write(root, "build/out.rs", b"compiled\n");
    write(root, ".ignore", b"notes/\n");
    write(root, "notes/x.rs", b"note\n");
    write(root, ".env", b"secret\n");

    let vfs = load(Checkout(root), (), Limits::default());
    let paths: Vec<&str> = vfs.files().map(|f| f.path.as_str()).collect();

    assert_eq!(
        paths,
        [".env", ".gitignore", ".ignore", "notes/x.rs", "src/main.rs"],
        ".git never, .gitignore honored, .ignore is not a git concept, dotfiles included"
    );
}

/// A checkout is linked, never copied. The header settles what it can at
/// discovery; a file whose fate needs its bytes is read then for the
/// decision only; the rest are checked on the first read, where a content
/// pass can still turn them down, and the inventory records it.
#[test]
#[cfg(unix)]
fn a_linked_checkout_is_checked_on_first_read() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path();
    write(root, "src/main.rs", b"fn main() {}");
    write(root, "Cargo.toml", b"[package]");
    write(root, "assets/logo.png", b"\x89PNGdata");
    write(root, "model/blob.rs", b"\x00\x01 not rust");
    std::os::unix::fs::symlink("src/main.rs", root.join("link.rs")).unwrap();

    let vfs = load(Checkout(root), CodeFilter, Limits::default());
    let decision = |path: &str| vfs.stat(Path::new(path)).unwrap().decision.unwrap();

    assert_eq!(vfs.usage().resident, 0, "nothing copied");
    assert_eq!(
        decision("Cargo.toml"),
        Decision::Keep(Role::Input),
        "sniffed at discovery"
    );
    assert_eq!(
        decision("assets/logo.png"),
        Decision::List("excluded_extension")
    );
    assert_eq!(
        decision("model/blob.rs"),
        Decision::Keep(Role::Source),
        "not yet read"
    );
    assert_eq!(
        text(&vfs, "link.rs"),
        "fn main() {}",
        "a symlink reads as its target"
    );

    assert_eq!(text(&vfs, "src/main.rs"), "fn main() {}");
    assert_eq!(text(&vfs, "Cargo.toml"), "[package]");
    assert_eq!(read_error(&vfs, "model/blob.rs"), ErrorKind::Unsupported);
    assert_eq!(
        decision("model/blob.rs"),
        Decision::List("binary"),
        "the refusal is recorded"
    );
    assert_eq!(
        rows(&vfs)
            .iter()
            .find(|(p, _)| p == "model/blob.rs")
            .unwrap()
            .1,
        Decision::List("binary"),
        "and the inventory shows it"
    );
}

#[test]
#[cfg(unix)]
fn an_unreadable_directory_fails_the_walk() {
    use std::os::unix::fs::PermissionsExt;
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path();
    write(root, "src/main.rs", b"fn main() {}");
    write(root, "secret/hidden.rs", b"fn hidden() {}");
    let locked = root.join("secret");
    std::fs::set_permissions(&locked, std::fs::Permissions::from_mode(0o000)).unwrap();
    if std::fs::read_dir(&locked).is_ok() {
        return; // running as root: permissions do not bind, nothing to test
    }

    let result = Vfs::load(Checkout(root), (), Limits::default(), Options::default());

    std::fs::set_permissions(&locked, std::fs::Permissions::from_mode(0o755)).unwrap();
    assert!(
        result.is_err(),
        "an unreadable subtree is not a smaller repository"
    );
}

#[test]
fn a_change_set_is_settled_without_a_walk_and_a_vanished_file_is_skipped() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path();
    write(root, "a.rs", b"fn a() {}");
    write(root, "b.png", b"\x89PNG");
    write(root, "untouched.rs", b"fn u() {}");

    let vfs = load(
        Changed {
            root,
            paths: vec!["b.png".into(), "a.rs".into(), "vanished.rs".into()],
        },
        CodeFilter,
        Limits::default(),
    );

    assert_eq!(
        rows(&vfs),
        [
            ("a.rs".into(), Decision::Keep(Role::Source)),
            ("b.png".into(), Decision::List("excluded_extension")),
        ]
    );
}

enum Entry<'a> {
    File(&'a str, &'a [u8]),
    Symlink(&'a str, &'a str),
    Raw(tar::EntryType, &'a str, &'a [u8]),
}

fn archive(entries: &[Entry]) -> Vec<u8> {
    let mut builder = tar::Builder::new(Vec::new());
    for entry in entries {
        let mut header = tar::Header::new_gnu();
        header.set_mode(0o644);
        match entry {
            Entry::File(path, body) => {
                header.set_size(body.len() as u64);
                builder.append_data(&mut header, path, *body).unwrap();
            }
            Entry::Symlink(path, target) => {
                header.set_entry_type(tar::EntryType::Symlink);
                header.set_size(0);
                builder.append_link(&mut header, *path, *target).unwrap();
            }
            Entry::Raw(kind, path, body) => {
                header.set_entry_type(*kind);
                header.set_size(body.len() as u64);
                builder.append_data(&mut header, path, *body).unwrap();
            }
        }
    }
    gzip(&builder.into_inner().unwrap())
}

fn gzip(tar: &[u8]) -> Vec<u8> {
    let mut encoder = GzEncoder::new(Vec::new(), flate2::Compression::fast());
    encoder.write_all(tar).unwrap();
    encoder.finish().unwrap()
}

#[test]
fn an_archive_is_read_below_its_root_and_only_its_root() {
    let long = format!("root/{}/{}.rs", "d".repeat(300), "f".repeat(300));
    let vfs = load(
        Archive(
            &archive(&[
                Entry::Raw(
                    tar::EntryType::XGlobalHeader,
                    "pax_global_header",
                    b"comment=x\n",
                ),
                Entry::Raw(
                    tar::EntryType::XHeader,
                    "PaxHeader/main.rs",
                    b"path=root/src/main.rs\n",
                ),
                Entry::File("root/src/main.rs", b"fn main() {}"),
                Entry::File("other-root/file.rs", b"elsewhere"),
                Entry::File(&long, b"fn f() {}"),
                Entry::Symlink("root/bin/run", "../src/main.rs"),
                Entry::Symlink("root/escape", "/etc/passwd"),
            ])[..],
        ),
        (),
        Limits::default(),
    );

    let paths: Vec<&str> = vfs.files().map(|f| f.path.as_str()).collect();
    assert_eq!(
        paths,
        ["bin/run", &long["root/".len()..], "escape", "src/main.rs"],
        "PAX headers and a foreign root are skipped; names too long for a disk are fine here"
    );
    assert_eq!(text(&vfs, "src/main.rs"), "fn main() {}");
    assert_eq!(text(&vfs, "bin/run"), "fn main() {}");
    assert_eq!(read_error(&vfs, "escape"), ErrorKind::NotFound);
    assert_eq!(
        vfs.stat(Path::new("root")).unwrap_err().kind(),
        ErrorKind::NotFound
    );
}

#[test]
fn an_archive_that_climbs_out_of_its_root_is_a_fault() {
    let mut builder = tar::Builder::new(Vec::new());
    let mut header = tar::Header::new_gnu();
    header.set_size(9);
    header.set_mode(0o644);
    header.set_entry_type(tar::EntryType::Regular);
    let path = b"root/../../escape.txt";
    header.as_mut_bytes()[..path.len()].copy_from_slice(path);
    header.set_cksum();
    builder.append(&header, &b"malicious"[..]).unwrap();
    let data = gzip(&builder.into_inner().unwrap());

    let failed = Vfs::load(
        Archive(&data[..]),
        (),
        Limits::default(),
        Options::default(),
    );
    let error = failed.err().unwrap().to_string();
    assert!(error.contains("path traversal"), "{error}");
}

#[test]
fn empty_and_truncated_archives_are_empty_not_faults() {
    let empty = Vfs::<()>::load(Archive(&[][..]), (), Limits::default(), Options::default());
    assert!(matches!(empty, Err(SourceError::Empty)));

    let full = archive(&[Entry::File("root/src/main.rs", b"fn main() {}")]);
    let truncated = Vfs::<()>::load(
        Archive(&full[..full.len() / 2]),
        (),
        Limits::default(),
        Options::default(),
    );
    assert!(matches!(truncated, Err(SourceError::Empty)));
}

/// An entry can carry its real size in a PAX record with the base header
/// size left at zero. The cap must see the real size, or the body is
/// inflated into memory before anything can refuse it.
#[test]
fn an_oversize_entry_is_listed_when_its_size_comes_from_a_pax_record() {
    let body = vec![b'a'; 4096];
    let mut builder = tar::Builder::new(Vec::new());
    let mut pax = tar::Header::new_gnu();
    pax.set_entry_type(tar::EntryType::XHeader);
    pax.set_size(13);
    pax.set_mode(0o644);
    builder
        .append_data(&mut pax, "PaxHeaders/size", &b"13 size=4096\n"[..])
        .unwrap();
    let mut header = tar::Header::new_gnu();
    header.set_size(0);
    header.set_mode(0o644);
    builder
        .append_data(&mut header, "root/big.txt", &body[..])
        .unwrap();
    let data = gzip(&builder.into_inner().unwrap());

    let vfs = load(
        Archive(&data[..]),
        (),
        Limits {
            file_bytes: Some(64),
            ..Limits::default()
        },
    );

    let big = vfs.files().next().unwrap();
    assert_eq!(
        (big.path.as_str(), big.size, big.decision()),
        ("big.txt", 4096, Decision::List("oversize"))
    );
    assert_eq!(vfs.usage().resident, 0, "never inflated");
}

#[test]
#[cfg(unix)]
fn the_same_repository_reads_the_same_from_a_checkout_and_an_archive() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path();
    let fixture: [(&str, &[u8]); 4] = [
        ("src/main.rs", b"fn main() {}"),
        ("Cargo.toml", b"[package]"),
        ("assets/logo.png", b"\x89PNGdata"),
        ("model/blob.rs", b"\x00\x01 not rust"),
    ];
    for (path, body) in fixture {
        write(root, path, body);
    }
    std::os::unix::fs::symlink("src/main.rs", root.join("link.rs")).unwrap();
    let mut entries: Vec<Entry> = fixture
        .iter()
        .map(|(path, body)| Entry::File(Box::leak(format!("repo/{path}").into_boxed_str()), body))
        .collect();
    entries.push(Entry::Symlink("repo/link.rs", "src/main.rs"));
    let data = archive(&entries);

    let from_checkout = load(Checkout(root), CodeFilter, Limits::default());
    let from_archive = load(Archive(&data[..]), CodeFilter, Limits::default());
    type Seen = (String, u64, Decision<Role>, Option<Vec<u8>>);
    let read_all = |vfs: &Vfs<Role>| -> Vec<Seen> {
        vfs.files()
            .map(|f| {
                let bytes = vfs.read(Path::new(&f.path)).ok().map(|b| b.to_vec());
                (f.path.clone(), f.size, f.decision(), bytes)
            })
            .collect()
    };

    assert_eq!(read_all(&from_checkout), read_all(&from_archive));
    assert_eq!(
        from_checkout.usage().kept,
        from_archive.usage().kept,
        "after the reads, the checkout's late verdicts match the archive's early ones"
    );
}
