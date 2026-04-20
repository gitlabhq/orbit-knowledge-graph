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

    #[lang = "sub"]
    pub trait Sub<Rhs = Self> {
        type Output;
        fn sub(self, rhs: Rhs) -> Self::Output;
    }

    #[lang = "mul"]
    pub trait Mul<Rhs = Self> {
        type Output;
        fn mul(self, rhs: Rhs) -> Self::Output;
    }

    #[lang = "div"]
    pub trait Div<Rhs = Self> {
        type Output;
        fn div(self, rhs: Rhs) -> Self::Output;
    }

    #[lang = "rem"]
    pub trait Rem<Rhs = Self> {
        type Output;
        fn rem(self, rhs: Rhs) -> Self::Output;
    }

    #[lang = "bitand"]
    pub trait BitAnd<Rhs = Self> {
        type Output;
        fn bitand(self, rhs: Rhs) -> Self::Output;
    }

    #[lang = "bitor"]
    pub trait BitOr<Rhs = Self> {
        type Output;
        fn bitor(self, rhs: Rhs) -> Self::Output;
    }

    #[lang = "bitxor"]
    pub trait BitXor<Rhs = Self> {
        type Output;
        fn bitxor(self, rhs: Rhs) -> Self::Output;
    }

    #[lang = "shl"]
    pub trait Shl<Rhs = Self> {
        type Output;
        fn shl(self, rhs: Rhs) -> Self::Output;
    }

    #[lang = "shr"]
    pub trait Shr<Rhs = Self> {
        type Output;
        fn shr(self, rhs: Rhs) -> Self::Output;
    }

    #[lang = "add_assign"]
    pub trait AddAssign<Rhs = Self> {
        fn add_assign(&mut self, rhs: Rhs);
    }

    #[lang = "sub_assign"]
    pub trait SubAssign<Rhs = Self> {
        fn sub_assign(&mut self, rhs: Rhs);
    }

    #[lang = "mul_assign"]
    pub trait MulAssign<Rhs = Self> {
        fn mul_assign(&mut self, rhs: Rhs);
    }

    #[lang = "div_assign"]
    pub trait DivAssign<Rhs = Self> {
        fn div_assign(&mut self, rhs: Rhs);
    }

    #[lang = "rem_assign"]
    pub trait RemAssign<Rhs = Self> {
        fn rem_assign(&mut self, rhs: Rhs);
    }

    #[lang = "bitand_assign"]
    pub trait BitAndAssign<Rhs = Self> {
        fn bitand_assign(&mut self, rhs: Rhs);
    }

    #[lang = "bitor_assign"]
    pub trait BitOrAssign<Rhs = Self> {
        fn bitor_assign(&mut self, rhs: Rhs);
    }

    #[lang = "bitxor_assign"]
    pub trait BitXorAssign<Rhs = Self> {
        fn bitxor_assign(&mut self, rhs: Rhs);
    }

    #[lang = "shl_assign"]
    pub trait ShlAssign<Rhs = Self> {
        fn shl_assign(&mut self, rhs: Rhs);
    }

    #[lang = "shr_assign"]
    pub trait ShrAssign<Rhs = Self> {
        fn shr_assign(&mut self, rhs: Rhs);
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
