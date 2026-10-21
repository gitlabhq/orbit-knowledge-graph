/// ```compile_fail
/// orbit_log_fields::info!(safe(KIND) = ?Some("x"), "rejected");
/// ```
///
/// ```compile_fail
/// orbit_log_fields::info!(safe(KIND) = %"x", "rejected");
/// ```
#[doc(hidden)]
#[macro_export]
macro_rules! __rewrite {
    ($mac:ident $prefix:tt [$($out:tt)*] safe($name:ident) = ? $($rest:tt)*) => {
        compile_error!(concat!("safe field ", stringify!($name), " takes a plain value, not `?`"))
    };
    ($mac:ident $prefix:tt [$($out:tt)*] safe($name:ident) = % $($rest:tt)*) => {
        compile_error!(concat!("safe field ", stringify!($name), " takes a plain value, not `%`"))
    };
    ($mac:ident $prefix:tt [$($out:tt)*] safe($name:ident) = $($rest:tt)*) => {
        $crate::__value!($mac $prefix [$($out)* { $crate::safe::$name } =] $($rest)*)
    };
    ($mac:ident $prefix:tt [$($out:tt)*] expanded($name:ident) = $($rest:tt)*) => {
        $crate::__value!($mac $prefix [$($out)* { $crate::expanded::$name } =] $($rest)*)
    };
    ($mac:ident $prefix:tt [$($out:tt)*] $token:tt $($rest:tt)*) => {
        $crate::__rewrite!($mac $prefix [$($out)* $token] $($rest)*)
    };
    ($mac:ident [$($prefix:tt)*] [$($out:tt)*]) => {
        $crate::__tracing::$mac!($($prefix)* $($out)*)
    };
}

#[doc(hidden)]
#[macro_export]
macro_rules! __value {
    ($mac:ident $prefix:tt [$($out:tt)*] ? $value:expr $(, $($rest:tt)*)?) => {
        $crate::__rewrite!($mac $prefix [$($out)* ?$value,] $($($rest)*)?)
    };
    ($mac:ident $prefix:tt [$($out:tt)*] % $value:expr $(, $($rest:tt)*)?) => {
        $crate::__rewrite!($mac $prefix [$($out)* %$value,] $($($rest)*)?)
    };
    ($mac:ident $prefix:tt [$($out:tt)*] $value:expr $(, $($rest:tt)*)?) => {
        $crate::__rewrite!($mac $prefix [$($out)* $value,] $($($rest)*)?)
    };
}

/// `tracing::info!` and the other level macros read `{ NAME } = value` right
/// after `target:` as a block of fields, so every level goes through `event!`.
#[doc(hidden)]
#[macro_export]
macro_rules! __event {
    ($level:ident, target: $target:expr, $($args:tt)*) => {
        $crate::__rewrite!(event [target: $target, $crate::__tracing::Level::$level,] [] $($args)*)
    };
    ($level:ident, $($args:tt)*) => {
        $crate::__rewrite!(event [$crate::__tracing::Level::$level,] [] $($args)*)
    };
}

#[macro_export]
macro_rules! trace {
    ($($args:tt)*) => { $crate::__event!(TRACE, $($args)*) };
}

#[macro_export]
macro_rules! debug {
    ($($args:tt)*) => { $crate::__event!(DEBUG, $($args)*) };
}

#[macro_export]
macro_rules! info {
    ($($args:tt)*) => { $crate::__event!(INFO, $($args)*) };
}

#[macro_export]
macro_rules! warn {
    ($($args:tt)*) => { $crate::__event!(WARN, $($args)*) };
}

#[macro_export]
macro_rules! error {
    ($($args:tt)*) => { $crate::__event!(ERROR, $($args)*) };
}

#[macro_export]
macro_rules! trace_span {
    ($($args:tt)*) => { $crate::__rewrite!(trace_span [] [] $($args)*) };
}

#[macro_export]
macro_rules! debug_span {
    ($($args:tt)*) => { $crate::__rewrite!(debug_span [] [] $($args)*) };
}

#[macro_export]
macro_rules! info_span {
    ($($args:tt)*) => { $crate::__rewrite!(info_span [] [] $($args)*) };
}

#[macro_export]
macro_rules! warn_span {
    ($($args:tt)*) => { $crate::__rewrite!(warn_span [] [] $($args)*) };
}

#[macro_export]
macro_rules! error_span {
    ($($args:tt)*) => { $crate::__rewrite!(error_span [] [] $($args)*) };
}
