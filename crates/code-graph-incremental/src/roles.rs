/// Declares a table of role ids and the role names pipeline.yaml binds.
#[macro_export]
macro_rules! roles {
    ($name:ident: $id:ty { $($role:ident),* $(,)? }) => {
        #[derive(Clone, Copy, Default)]
        pub struct $name {
            $(pub $role: $id,)*
        }

        impl $name {
            pub(crate) const NAMES: &'static [&'static str] = &[$(stringify!($role)),*];

            pub(crate) fn bound(ids: &[$id]) -> Self {
                let mut ids = ids.iter().copied();
                Self { $($role: ids.next().unwrap_or_default(),)* }
            }
        }
    };
}
