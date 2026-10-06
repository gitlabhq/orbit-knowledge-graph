#[path = "vfs/archive.rs"]
mod archive;
#[path = "vfs/native.rs"]
mod native;

macro_rules! suite {
    ($name:ident) => {
        #[test]
        fn $name() {
            integration_testkit::vfs::run(include_str!(concat!(
                "vfs/fixtures/",
                stringify!($name),
                ".yaml"
            )));
        }
    };
}

suite!(archive);
suite!(directory);
suite!(filesystem);
suite!(limits);
suite!(policy);
suite!(storage);
