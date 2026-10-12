use orbit_utils::vfs::{Decision, File, Limits, Options, Pass, Put, Vfs};

#[test]
fn loading_leaves_the_global_rayon_pool_unbuilt() {
    struct Keep;
    impl Pass for Keep {
        type Tag = ();
        fn metadata(&self, _: &File<'_, ()>) -> Decision<()> {
            Decision::Keep(())
        }
    }
    let files = (0..10_000)
        .rev()
        .map(|i| (format!("src/{i}.rs"), Put::Bytes(vec![b'x'])));
    let vfs = Vfs::load(files, Keep, Limits::default(), Options::default()).unwrap();
    assert_eq!(vfs.usage().files, 10_000);
    assert!(rayon::ThreadPoolBuilder::new().build_global().is_ok());
}
