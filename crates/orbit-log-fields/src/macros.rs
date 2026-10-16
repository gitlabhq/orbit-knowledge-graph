//! Log and span macros that name fields with `safe(NAME)` and `expanded(NAME)`.
//!
//! Each macro rewrites `safe(NAME)` to `{ $crate::safe::NAME }` and
//! `expanded(NAME)` to `{ $crate::expanded::NAME }`, and passes every other
//! token to `tracing` unchanged.

#[doc(hidden)]
pub mod __private {
    pub use tracing;
}

/// Rewrites the field names one token at a time, then calls `$crate::__private::tracing::$mac!`
/// with `$prefix` and the rewritten tokens.
#[doc(hidden)]
#[macro_export]
macro_rules! __rewrite {
    ($mac:ident [$($prefix:tt)*] [$($out:tt)*] safe($name:ident) $($rest:tt)*) => {
        $crate::__rewrite!($mac [$($prefix)*] [$($out)* { $crate::safe::$name }] $($rest)*)
    };
    ($mac:ident [$($prefix:tt)*] [$($out:tt)*] expanded($name:ident) $($rest:tt)*) => {
        $crate::__rewrite!($mac [$($prefix)*] [$($out)* { $crate::expanded::$name }] $($rest)*)
    };
    ($mac:ident [$($prefix:tt)*] [$($out:tt)*] $token:tt $($rest:tt)*) => {
        $crate::__rewrite!($mac [$($prefix)*] [$($out)* $token] $($rest)*)
    };
    ($mac:ident [$($prefix:tt)*] [$($out:tt)*]) => {
        $crate::__private::tracing::$mac!($($prefix)* $($out)*)
    };
}

/// `tracing::info!` and the other level macros read `{ NAME } = value` right
/// after `target:` as a block of fields, so every level goes through `event!`.
#[doc(hidden)]
#[macro_export]
macro_rules! __event {
    ($level:ident, target: $target:expr, $($args:tt)*) => {
        $crate::__rewrite!(event [target: $target, $crate::__private::tracing::Level::$level,] [] $($args)*)
    };
    ($level:ident, $($args:tt)*) => {
        $crate::__rewrite!(event [$crate::__private::tracing::Level::$level,] [] $($args)*)
    };
}

/// `tracing::trace!` with `safe(NAME)` and `expanded(NAME)` field names.
#[macro_export]
macro_rules! trace {
    ($($args:tt)*) => { $crate::__event!(TRACE, $($args)*) };
}

/// `tracing::debug!` with `safe(NAME)` and `expanded(NAME)` field names.
#[macro_export]
macro_rules! debug {
    ($($args:tt)*) => { $crate::__event!(DEBUG, $($args)*) };
}

/// `tracing::info!` with `safe(NAME)` and `expanded(NAME)` field names.
#[macro_export]
macro_rules! info {
    ($($args:tt)*) => { $crate::__event!(INFO, $($args)*) };
}

/// `tracing::warn!` with `safe(NAME)` and `expanded(NAME)` field names.
#[macro_export]
macro_rules! warn {
    ($($args:tt)*) => { $crate::__event!(WARN, $($args)*) };
}

/// `tracing::error!` with `safe(NAME)` and `expanded(NAME)` field names.
#[macro_export]
macro_rules! error {
    ($($args:tt)*) => { $crate::__event!(ERROR, $($args)*) };
}

/// `tracing::trace_span!` with `safe(NAME)` and `expanded(NAME)` field names.
#[macro_export]
macro_rules! trace_span {
    ($($args:tt)*) => { $crate::__rewrite!(trace_span [] [] $($args)*) };
}

/// `tracing::debug_span!` with `safe(NAME)` and `expanded(NAME)` field names.
#[macro_export]
macro_rules! debug_span {
    ($($args:tt)*) => { $crate::__rewrite!(debug_span [] [] $($args)*) };
}

/// `tracing::info_span!` with `safe(NAME)` and `expanded(NAME)` field names.
#[macro_export]
macro_rules! info_span {
    ($($args:tt)*) => { $crate::__rewrite!(info_span [] [] $($args)*) };
}

/// `tracing::warn_span!` with `safe(NAME)` and `expanded(NAME)` field names.
#[macro_export]
macro_rules! warn_span {
    ($($args:tt)*) => { $crate::__rewrite!(warn_span [] [] $($args)*) };
}

/// `tracing::error_span!` with `safe(NAME)` and `expanded(NAME)` field names.
#[macro_export]
macro_rules! error_span {
    ($($args:tt)*) => { $crate::__rewrite!(error_span [] [] $($args)*) };
}
