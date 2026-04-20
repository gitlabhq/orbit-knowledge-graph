#![allow(unused)]

extern crate alloc;
extern crate core;
extern crate proc_macro;

pub mod alloc_ {
    pub use alloc::*;
}

pub mod borrow {
    pub use alloc::borrow::*;
}

pub mod boxed {
    pub use alloc::boxed::*;
}

pub mod convert {
    pub use core::convert::*;
}

pub mod future {
    pub use core::future::*;
}

pub mod ops {
    pub use core::ops::*;
}

pub mod option {
    pub use core::option::*;
}

pub mod pin {
    pub use core::pin::*;
}

pub mod proc_macro_ {
    pub use proc_macro::*;
}

pub mod result {
    pub use core::result::*;
}

pub mod string {
    pub use alloc::string::*;
}

pub mod task {
    pub use core::task::*;
}

pub mod vec {
    pub use alloc::vec::*;
}
