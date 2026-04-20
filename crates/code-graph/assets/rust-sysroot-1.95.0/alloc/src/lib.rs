#![allow(unused)]

extern crate core;

pub mod borrow {
    pub trait ToOwned {
        type Owned;

        fn to_owned(&self) -> Self::Owned;
    }
}

pub mod boxed {
    pub struct Box<T: ?Sized>(pub *const T);
}

pub mod string {
    pub struct String;
}

pub mod vec {
    pub struct Vec<T>(pub core::marker::PhantomData<T>);
}
