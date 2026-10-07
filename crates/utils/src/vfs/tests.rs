use std::io::{self, ErrorKind};
use std::path::Path;
use std::sync::{
    Arc, Barrier,
    atomic::{AtomicUsize, Ordering::SeqCst},
};

use super::{
    Decision, File, Kind, LimitKind, Limits, Loading, Options, Pass, Put, Source, SourceError, Tag,
    Vfs,
};

fn load<T: Tag>(source: impl Source, pass: impl Pass<Tag = T> + 'static) -> Vfs<T> {
    Vfs::load(source, pass, Limits::default(), Options::default()).unwrap()
}

fn rejected(size: u64) -> Put<'static> {
    read_and_store(size, || panic!("rejected reader"))
}

fn read_and_store(size: u64, read: impl FnOnce() -> io::Result<Vec<u8>> + 'static) -> Put<'static> {
    Put::ReadAndStore {
        size,
        read: Box::new(read),
    }
}

fn read_on_demand(
    size: u64,
    read: impl Fn(u64) -> io::Result<Vec<u8>> + Send + Sync + 'static,
) -> Put<'static> {
    Put::ReadOnDemand {
        size,
        read: Arc::new(read),
    }
}

struct Filter(Arc<AtomicUsize>);
impl Pass for Filter {
    type Tag = ();
    fn metadata(&self, file: &File<'_, ()>) -> Decision<()> {
        assert!(file.bytes().is_none());
        if file.path.ends_with(".skip") {
            Decision::List("excluded")
        } else {
            Decision::Keep(())
        }
    }
    fn content(&self, file: &File<'_, ()>) -> Decision<()> {
        self.0.fetch_add(1, SeqCst);
        if file.bytes().unwrap().contains(&0) {
            Decision::List("binary")
        } else {
            file.decision()
        }
    }
}

#[test]
fn catalog_preserves_empty_rejected_and_replaced_files() {
    let vfs = load(
        [
            ("src/file", Put::Bytes(b"old".to_vec())),
            ("src/empty", Put::Bytes(vec![])),
            ("src/file", Put::Bytes(b"new".to_vec())),
            ("binary", Put::Bytes(vec![0])),
            ("ignored.skip", rejected(3)),
        ],
        Filter(Arc::default()),
    );
    assert_eq!(
        vfs.files()
            .map(|file| file.path.as_ref())
            .collect::<Vec<_>>(),
        ["binary", "ignored.skip", "src/empty", "src/file"]
    );
    assert_eq!(&*vfs.read("/src/../src/file").unwrap(), b"new");
    assert!(vfs.read("src/empty").unwrap().is_empty());
    assert_eq!(
        vfs.read_dir("/").unwrap(),
        ["binary", "ignored.skip", "src"]
    );
    assert_eq!(vfs.subtree("src").count(), 2);
    assert_eq!(vfs.subtree("../").count(), 0);
    assert_eq!(vfs.stat("src").unwrap().kind, Kind::Dir);
    for (path, expected) in [
        ("binary", ErrorKind::Unsupported),
        ("ignored.skip", ErrorKind::Unsupported),
        ("missing", ErrorKind::NotFound),
        ("../escape", ErrorKind::NotFound),
        ("src", ErrorKind::IsADirectory),
    ] {
        assert_eq!(vfs.read(path).unwrap_err().kind(), expected);
    }
    assert_eq!(
        vfs.read_dir("binary").unwrap_err().kind(),
        ErrorKind::NotADirectory
    );
    assert_eq!(
        vfs.stat("binary").unwrap().decision,
        Some(Decision::List("binary"))
    );
    let usage = vfs.usage();
    assert_eq!((usage.files, usage.duplicate_paths, usage.kept), (4, 1, 3));
}

#[test]
fn virtual_links_resolve_only_inside_the_catalog() {
    let mut inputs = vec![("dir/file", Put::Bytes(b"data".to_vec()))];
    inputs.extend(
        [
            ("alias", "dir"),
            ("chain", "/alias"),
            ("dir/link", "file"),
            ("escape", "../../outside"),
            ("host", "/etc/passwd"),
            ("dangling", "absent"),
            ("ping", "pong"),
            ("pong", "ping"),
        ]
        .map(|(path, target)| (path, Put::Symlink(target.into()))),
    );
    let vfs = load(inputs, ());
    assert_eq!(&*vfs.read("chain/link").unwrap(), b"data");
    assert_eq!(vfs.read_dir("alias").unwrap(), ["file", "link"]);
    assert_eq!(vfs.subtree("chain").count(), 2);
    let stat = vfs.stat("chain/link").unwrap();
    assert_eq!(stat.path, Path::new("/dir/file"));
    assert_eq!(stat.link.as_deref(), Some(Path::new("file")));
    assert!(vfs.stat("alias/file").unwrap().link.is_none());
    for path in ["escape", "escape/file", "host", "dangling"] {
        assert_eq!(vfs.stat(path).unwrap_err().kind(), ErrorKind::NotFound);
    }
    assert_eq!(vfs.read("ping").unwrap_err().kind(), ErrorKind::Other);
}

#[test]
fn large_unsorted_catalog_keeps_the_last_version_of_each_path() {
    let source = [b"old", b"new"].into_iter().flat_map(|version| {
        (0..10_000)
            .rev()
            .map(move |index| (format!("dir/{index:05}"), Put::Bytes(version.to_vec())))
    });
    let vfs = load(source, ());
    assert_eq!(vfs.usage().files, 10_000);
    assert_eq!(vfs.usage().duplicate_paths, 10_000);
    for (index, file) in vfs.files().enumerate() {
        assert_eq!(file.path, format!("dir/{index:05}"));
        assert_eq!(&*vfs.read(file.path.as_ref()).unwrap(), b"new");
    }
    assert_eq!(vfs.subtree("dir").count(), 10_000);
    assert_eq!(vfs.read_dir("dir").unwrap().len(), 10_000);
}

#[test]
fn storage_modes_share_content_and_enforce_exact_budgets() {
    let body = vec![b'x'; 4096];
    for (resident, compress) in [(4096, false), (0, false), (0, true)] {
        let vfs = Vfs::load(
            [
                ("a", Put::Bytes(body.clone())),
                ("b", Put::Bytes(body.clone())),
                ("empty", Put::Bytes(vec![])),
            ],
            (),
            Limits {
                resident_bytes: Some(resident),
                spilled_bytes: Some(4096),
                ..Limits::default()
            },
            Options {
                compress_spill: compress,
                ..Options::default()
            },
        )
        .unwrap();
        let a = vfs.read("a").unwrap();
        let b = vfs.read("b").unwrap();
        assert_eq!(&*a, body);
        assert_eq!(&*b, body);
        assert_eq!(vfs.usage().deduped_bytes, 4096);
        assert_eq!(vfs.usage().resident, resident);
        if resident > 0 {
            assert!(Arc::ptr_eq(&a, &b));
        } else if compress {
            assert!(vfs.usage().spilled < 4096);
        } else {
            assert_eq!(vfs.usage().spilled, 4096);
        }
        drop(vfs);
        assert_eq!(&*a, body);
    }
    for metric in [
        LimitKind::Files,
        LimitKind::TotalBytes,
        LimitKind::SpilledBytes,
    ] {
        let mut limits = Limits::default();
        match metric {
            LimitKind::Files => limits.files = Some(0),
            LimitKind::TotalBytes => limits.total_bytes = Some(0),
            LimitKind::SpilledBytes => {
                limits.resident_bytes = Some(0);
                limits.spilled_bytes = Some(0);
            }
            LimitKind::ResidentBytes => {
                unreachable!("resident overflow spills instead of failing loading")
            }
        }
        let result = Vfs::load([("a", Put::Bytes(vec![1]))], (), limits, Options::default());
        assert!(matches!(result, Err(SourceError::Cap(cap)) if cap.metric == metric));
    }
    for size in [2, u64::MAX] {
        let result = Vfs::load(
            [
                ("big", rejected(size)),
                ("empty", Put::Bytes(vec![])),
                ("extra", Put::Bytes(vec![1])),
            ],
            (),
            Limits {
                file_bytes: Some(0),
                total_bytes: Some(size.saturating_add(1)),
                files: Some(3),
                ..Limits::default()
            },
            Options::default(),
        );
        if size == u64::MAX {
            let Err(SourceError::Cap(cap)) = result else {
                panic!("expected total byte cap");
            };
            assert_eq!(cap.metric, LimitKind::TotalBytes);
            assert_eq!(
                cap.to_string(),
                format!("total_bytes cap exceeded ({} > {})", u64::MAX, u64::MAX)
            );
        } else {
            let vfs = result.unwrap();
            assert_eq!(
                vfs.stat("big").unwrap().decision,
                Some(Decision::List("oversize"))
            );
            assert!(vfs.read("empty").unwrap().is_empty());
        }
    }
}

#[test]
fn policy_composition_preserves_laziness_and_borrowed_content() {
    struct Stage(Arc<AtomicUsize>);
    impl Pass for Stage {
        type Tag = u8;
        fn metadata(&self, file: &File<'_, u8>) -> Decision<u8> {
            assert!(file.bytes().is_none());
            match file.decision() {
                Decision::Pending => Decision::Keep(1),
                Decision::Keep(tag) => Decision::Keep(tag + 1),
                decision => panic!("unexpected decision: {decision:?}"),
            }
        }
        fn content(&self, file: &File<'_, u8>) -> Decision<u8> {
            assert_eq!(file.bytes(), Some([].as_slice()));
            self.0.fetch_add(1, SeqCst);
            let Decision::Keep(tag) = file.decision() else {
                panic!("metadata must keep the file");
            };
            Decision::Keep(tag + 1)
        }
    }
    for deferred in [false, true] {
        let reads = Arc::new(AtomicUsize::new(0));
        let calls = Arc::new(AtomicUsize::new(0));
        let counter = reads.clone();
        let input = if deferred {
            read_on_demand(0, move |max| {
                assert_eq!(max, 0);
                counter.fetch_add(1, SeqCst);
                Ok(vec![])
            })
        } else {
            read_and_store(0, move || {
                counter.fetch_add(1, SeqCst);
                Ok(vec![])
            })
        };
        let vfs = Vfs::load(
            [("empty", input)],
            Stage(calls.clone()).then(Stage(calls.clone()).then(Stage(calls.clone()))),
            Limits {
                file_bytes: Some(0),
                ..Limits::default()
            },
            Options::default(),
        )
        .unwrap();
        assert_eq!(reads.load(SeqCst), usize::from(!deferred));
        for _ in 0..2 {
            assert!(vfs.read("empty").unwrap().is_empty());
        }
        assert_eq!(reads.load(SeqCst), if deferred { 2 } else { 1 });
        assert_eq!(calls.load(SeqCst), 3);
        let file = vfs.files().next().unwrap();
        assert!(file.bytes().is_none());
        assert_eq!(file.decision(), Decision::Keep(6));
    }
}

#[test]
fn reader_failures_remain_cataloged_and_later_failures_are_retryable() {
    let attempts = Arc::new(AtomicUsize::new(0));
    let reads = attempts.clone();
    let calls = Arc::new(AtomicUsize::new(0));
    let vfs = Vfs::load(
        [
            (
                "failed",
                read_and_store(4, || {
                    Err(io::Error::new(ErrorKind::PermissionDenied, "denied"))
                }),
            ),
            ("mismatch", read_and_store(4, || Ok(vec![]))),
            (
                "retry",
                read_on_demand(4, move |_| match reads.fetch_add(1, SeqCst) {
                    0 => Err(ErrorKind::NotFound.into()),
                    1 | 2 => Ok(b"data".to_vec()),
                    _ => Ok(b"large".to_vec()),
                }),
            ),
        ],
        Filter(calls.clone()),
        Limits {
            file_bytes: Some(4),
            ..Limits::default()
        },
        Options::default(),
    )
    .unwrap();
    assert_eq!(vfs.usage().files, 3);
    for (path, kind) in [
        ("failed", ErrorKind::PermissionDenied),
        ("mismatch", ErrorKind::InvalidData),
        ("retry", ErrorKind::NotFound),
    ] {
        assert_eq!(vfs.read(path).unwrap_err().kind(), kind);
    }
    assert_eq!(vfs.read("failed").unwrap_err().to_string(), "denied");
    assert_eq!(calls.load(SeqCst), 0);
    for _ in 0..2 {
        assert_eq!(&*vfs.read("retry").unwrap(), b"data");
    }
    assert_eq!(calls.load(SeqCst), 1);
    assert_eq!(
        vfs.read("retry").unwrap_err().kind(),
        ErrorKind::FileTooLarge
    );
    assert_eq!(vfs.usage().kept, 4);
    let result = Vfs::load(
        [("file", Put::Bytes(vec![]))],
        (),
        Limits::default(),
        Options {
            cancelled: Some(Box::new(|| true)),
            ..Options::default()
        },
    );
    assert!(matches!(result, Err(SourceError::Cancelled)));
}

#[test]
fn concurrent_loading_and_first_reads_preserve_content_and_classify_once() {
    struct Workers;
    impl Source for Workers {
        fn fill<T: Tag>(self, into: &Loading<T>) -> Result<(), SourceError> {
            std::thread::scope(|scope| {
                for worker in 0..8 {
                    scope.spawn(move || {
                        into.put(&format!("{worker}"), Put::Bytes(vec![b'x'; 128]))
                            .unwrap()
                    });
                }
            });
            into.put("lazy", read_on_demand(1, |_| Ok(vec![0])))
        }
    }
    let calls = Arc::new(AtomicUsize::new(0));
    let vfs = Vfs::load(
        Workers,
        Filter(calls.clone()),
        Limits {
            resident_bytes: Some(0),
            spilled_bytes: Some(128),
            ..Limits::default()
        },
        Options::default(),
    )
    .unwrap();
    let usage = vfs.usage();
    assert_eq!(
        (usage.files, usage.spilled, usage.deduped_bytes),
        (9, 128, 896)
    );
    let start = Barrier::new(8);
    std::thread::scope(|scope| {
        for worker in 0..8 {
            let (vfs, start) = (&vfs, &start);
            scope.spawn(move || {
                start.wait();
                assert_eq!(vfs.read("lazy").unwrap_err().kind(), ErrorKind::Unsupported);
                assert_eq!(&*vfs.read(format!("{worker}")).unwrap(), vec![b'x'; 128]);
            });
        }
    });
    assert_eq!(calls.load(SeqCst), 9);
    assert_eq!(
        vfs.stat("lazy").unwrap().decision,
        Some(Decision::List("binary"))
    );
}
