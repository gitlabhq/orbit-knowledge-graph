#![allow(unused)]
#![feature(lang_items, no_core)]
#![no_core]

pub mod clone {
    pub trait Clone {
        fn clone(&self) -> Self;
    }
}

pub mod convert {
    pub trait From<T> {
        fn from(value: T) -> Self;
    }

    impl<T> From<T> for T {
        fn from(value: T) -> Self {
            value
        }
    }
}

pub mod marker {
    pub struct PhantomData<T: ?Sized>;
}

pub mod option {
    pub enum Option<T> {
        Some(T),
        None,
    }
}

pub mod result {
    pub enum Result<T, E> {
        Ok(T),
        Err(E),
    }
}

pub mod pin {
    pub struct Pin<P>(pub P);
}

pub mod task {
    pub enum Poll<T> {
        #[lang = "Ready"]
        Ready(T),
        #[lang = "Pending"]
        Pending,
    }

    pub struct Context<'a> {
        _marker: &'a (),
    }
}

pub mod ops {
    pub enum Infallible {}

    pub enum ControlFlow<B, C = ()> {
        #[lang = "Continue"]
        Continue(C),
        #[lang = "Break"]
        Break(B),
    }

    #[lang = "deref"]
    pub trait Deref {
        #[lang = "deref_target"]
        type Target: ?Sized;
        fn deref(&self) -> &Self::Target;
    }

    #[lang = "deref_mut"]
    pub trait DerefMut: Deref {
        fn deref_mut(&mut self) -> &mut Self::Target;
    }

    #[lang = "index"]
    pub trait Index<Idx: ?Sized> {
        type Output: ?Sized;
        fn index(&self, index: Idx) -> &Self::Output;
    }

    #[lang = "index_mut"]
    pub trait IndexMut<Idx: ?Sized>: Index<Idx> {
        fn index_mut(&mut self, index: Idx) -> &mut Self::Output;
    }

    #[lang = "add"]
    pub trait Add<Rhs = Self> {
        type Output;
        fn add(self, rhs: Rhs) -> Self::Output;
    }

    #[lang = "not"]
    pub trait Not {
        type Output;
        fn not(self) -> Self::Output;
    }

    #[lang = "neg"]
    pub trait Neg {
        type Output;
        fn neg(self) -> Self::Output;
    }

    pub trait FromResidual<R = <Self as Try>::Residual> {
        #[lang = "from_residual"]
        fn from_residual(residual: R) -> Self;
    }

    pub trait Residual<O>: Sized {
        type TryType: Try<Output = O, Residual = Self>;
    }

    #[lang = "Try"]
    pub trait Try: FromResidual<Self::Residual> {
        type Output;
        type Residual;

        #[lang = "from_output"]
        fn from_output(output: Self::Output) -> Self;

        #[lang = "branch"]
        fn branch(self) -> ControlFlow<Self::Residual, Self::Output>;
    }
}

pub mod future {
    use crate::pin::Pin;
    use crate::task::{Context, Poll};

    #[lang = "future_trait"]
    pub trait Future {
        #[lang = "future_output"]
        type Output;

        #[lang = "poll"]
        fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output>;
    }

    pub trait IntoFuture {
        type Output;
        type IntoFuture: Future<Output = Self::Output>;

        #[lang = "into_future"]
        fn into_future(self) -> Self::IntoFuture;
    }

    impl<F: Future> IntoFuture for F {
        type Output = F::Output;
        type IntoFuture = F;

        fn into_future(self) -> Self::IntoFuture {
            self
        }
    }
}

use crate::convert::From;
use crate::ops::{ControlFlow, FromResidual, Infallible, Residual, Try};
use crate::option::Option;
use crate::option::Option::{None, Some};
use crate::result::Result;
use crate::result::Result::{Err, Ok};

impl<T> Try for Option<T> {
    type Output = T;
    type Residual = Option<Infallible>;

    fn from_output(output: Self::Output) -> Self {
        Some(output)
    }

    fn branch(self) -> ControlFlow<Self::Residual, Self::Output> {
        match self {
            Some(value) => ControlFlow::Continue(value),
            None => ControlFlow::Break(None),
        }
    }
}

impl<T> FromResidual<Option<Infallible>> for Option<T> {
    fn from_residual(residual: Option<Infallible>) -> Self {
        match residual {
            None => None,
            Some(_) => loop {},
        }
    }
}

impl<T> Residual<T> for Option<Infallible> {
    type TryType = Option<T>;
}

impl<T, E> Try for Result<T, E> {
    type Output = T;
    type Residual = Result<Infallible, E>;

    fn from_output(output: Self::Output) -> Self {
        Ok(output)
    }

    fn branch(self) -> ControlFlow<Self::Residual, Self::Output> {
        match self {
            Ok(value) => ControlFlow::Continue(value),
            Err(error) => ControlFlow::Break(Err(error)),
        }
    }
}

impl<T, E, F: From<E>> FromResidual<Result<Infallible, E>> for Result<T, F> {
    fn from_residual(residual: Result<Infallible, E>) -> Self {
        match residual {
            Err(error) => Err(F::from(error)),
            Ok(_) => loop {},
        }
    }
}

impl<T, E> Residual<T> for Result<Infallible, E> {
    type TryType = Result<T, E>;
}
