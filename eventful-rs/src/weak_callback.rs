//! Weak callbacks whose closure arity matches the setter they are passed to.

/// Create a callback that holds a weak reference to a [`ShardRc`](crate::ShardRc)
/// and skips calls once the value has been collected.
///
/// ```text
/// weak_callback!(name => |arg1, arg2, …| body)
/// weak_callback!(name => || body)
/// ```
///
/// `name` is a variable holding a `ShardRc<T>` or a `&ShardRc<T>`. The macro
/// downgrades it and expands to a `move` closure taking `arg1, arg2, …`, so the
/// closure goes straight into a Slint `on_*` setter or any other function
/// expecting `impl Fn*(A1, A2, …)`, and the argument types are inferred from it.
/// There is no limit on the number of arguments. While the value is alive, `body`
/// runs with `name` rebound to the upgraded `&ShardRc<T>` for the duration of the
/// call, so the body cannot capture the strong reference. The body must evaluate
/// to `()`; use [`weak_callback_or_else!`](crate::weak_callback_or_else!) for
/// callbacks that return a value.
///
/// Only a plain variable name is accepted before `=>`. To use a field, bind it
/// first, for example `let app = &self.app;`.
///
/// Arguments are identifiers, `_`, or other single-token patterns such as
/// `(a, b)`, each with an optional `: Type` annotation.
///
/// ```rust
/// use eventful_rs::*;
/// use std::cell::Cell;
/// declare_shard!(Ui, runtime = main);
///
/// #[eventful(shard = Ui)]
/// struct App {
///     keys: Cell<u32>,
/// }
///
/// // Stands in for a generated Slint component.
/// fn on_key(callback: impl FnMut(String, bool) + 'static) -> impl FnMut(String, bool) {
///     callback
/// }
///
/// Ui::shard().run_main(|| async {
///     let app = App::bind_local(App {
///         keys: Cell::new(0),
///         events: Default::default(),
///     })?;
///     let mut key = on_key(weak_callback!(app => |_text, ctrl| {
///         if !ctrl {
///             app.keys.set(app.keys.get() + 1);
///         }
///     }));
///     key("a".into(), false);
///     key("b".into(), true);
///     assert_eq!(app.keys.get(), 1);
///     Ok::<(), InvokeError>(())
/// })?;
/// # Ok::<(), InvokeError>(())
/// ```
#[macro_export]
macro_rules! weak_callback {
    (self => $($rest:tt)*) => {
        ::core::compile_error!(
            "`weak_callback!` takes the variable holding the `ShardRc` before `=>`, not `self`; bind it first, e.g. `let app = &self.app;`"
        )
    };
    ($owner:ident => || $body:expr $(,)?) => {
        $crate::__weak_callback! { @fresh [skip] [$owner] [$body] [] }
    };
    ($owner:ident => |$($arg:tt $(: $arg_ty:ty)?),*| $body:expr $(,)?) => {
        $crate::__weak_callback! {
            @fresh [skip] [$owner] [$body] [] $([$arg $(: $arg_ty)?])*
        }
    };
    ($($input:tt)*) => {
        ::core::compile_error!(
            "expected `weak_callback!(name => |args| body)`, where `name` is a variable holding a `ShardRc`; bind an owner expression first, e.g. `let app = &self.app;`"
        )
    };
}

/// Create a value-returning callback that holds a weak reference to a
/// [`ShardRc`](crate::ShardRc) and calls `fallback` once the value has been collected.
///
/// ```text
/// weak_callback_or_else!(name => |arg1, arg2, …| body, |arg1, arg2, …| fallback)
/// weak_callback_or_else!(name => || body, || fallback)
/// ```
///
/// This works like [`weak_callback!`](crate::weak_callback!), except that `body`
/// may return a value. The fallback closure receives the same arguments and must
/// produce the same type. Its parameters accept the same forms as the callback's,
/// so `|_, _| 0` ignores them. A fallback taking a different number of arguments
/// is a compile error.
///
/// ```rust
/// use eventful_rs::*;
/// declare_shard!(Ui, runtime = main);
///
/// #[eventful(shard = Ui)]
/// struct App {
///     offset: i32,
/// }
///
/// // Stands in for a generated Slint component.
/// fn on_shift(callback: impl FnMut(i32) -> i32 + 'static) -> impl FnMut(i32) -> i32 {
///     callback
/// }
///
/// Ui::shard().run_main(|| async {
///     let app = App::bind_local(App {
///         offset: 10,
///         events: Default::default(),
///     })?;
///     let mut shift = on_shift(weak_callback_or_else!(app => |a| a + app.offset, |a| a));
///     assert_eq!(shift(1), 11);
///     Ok::<(), InvokeError>(())
/// })?;
/// # Ok::<(), InvokeError>(())
/// ```
#[macro_export]
macro_rules! weak_callback_or_else {
    (self => $($rest:tt)*) => {
        ::core::compile_error!(
            "`weak_callback_or_else!` takes the variable holding the `ShardRc` before `=>`, not `self`; bind it first, e.g. `let app = &self.app;`"
        )
    };
    ($owner:ident => || $body:expr, || $fallback:expr $(,)?) => {
        $crate::__weak_callback! { @fresh [fallback [] [] [$fallback]] [$owner] [$body] [] }
    };
    (
        $owner:ident => || $body:expr,
        |$($fallback_arg:tt $(: $fallback_ty:ty)?),*| $fallback:expr $(,)?
    ) => {
        $crate::__weak_callback! {
            @fresh
            [fallback [$([$fallback_arg $(: $fallback_ty)?])*] [] [$fallback]]
            [$owner] [$body] []
        }
    };
    (
        $owner:ident => |$($arg:tt $(: $arg_ty:ty)?),*| $body:expr,
        || $fallback:expr $(,)?
    ) => {
        $crate::__weak_callback! {
            @fresh [fallback [] [] [$fallback]] [$owner] [$body] []
            $([$arg $(: $arg_ty)?])*
        }
    };
    (
        $owner:ident => |$($arg:tt $(: $arg_ty:ty)?),*| $body:expr,
        |$($fallback_arg:tt $(: $fallback_ty:ty)?),*| $fallback:expr $(,)?
    ) => {
        $crate::__weak_callback! {
            @fresh
            [fallback [$([$fallback_arg $(: $fallback_ty)?])*] [] [$fallback]]
            [$owner] [$body] []
            $([$arg $(: $arg_ty)?])*
        }
    };
    ($($input:tt)*) => {
        ::core::compile_error!(
            "expected `weak_callback_or_else!(name => |args| body, |args| fallback)`, where `name` is a variable holding a `ShardRc`; bind an owner expression first, e.g. `let app = &self.app;`"
        )
    };
}

/// Implementation detail of `weak_callback!` and `weak_callback_or_else!`.
///
/// Each `@fresh` step introduces one `__eventful_arg` identifier. Identifiers
/// written by different macro expansions are distinct bindings, so every closure
/// parameter gets its own name, and the user's patterns, which may be `_` or
/// destructure the value, are bound from them in both branches. In fallback mode,
/// each step also pairs one fallback parameter with the callback's argument.
#[doc(hidden)]
#[macro_export]
macro_rules! __weak_callback {
    (
        @fresh [skip] $owner:tt $body:tt [$($done:tt)*]
        [$arg:tt $(: $arg_ty:ty)?] $($rest:tt)*
    ) => {
        $crate::__weak_callback! {
            @fresh [skip] $owner $body
            [$($done)* [__eventful_arg $arg $(: $arg_ty)?]] $($rest)*
        }
    };
    (
        @fresh
        [fallback [[$fallback_arg:tt $(: $fallback_ty:ty)?] $($pending:tt)*] [$($paired:tt)*] $fallback:tt]
        $owner:tt $body:tt [$($done:tt)*]
        [$arg:tt $(: $arg_ty:ty)?] $($rest:tt)*
    ) => {
        $crate::__weak_callback! {
            @fresh
            [fallback [$($pending)*] [$($paired)* [$fallback_arg $(: $fallback_ty)?]] $fallback]
            $owner $body
            [$($done)* [__eventful_arg $arg $(: $arg_ty)?]] $($rest)*
        }
    };
    (@fresh [fallback [] $paired:tt $fallback:tt] $owner:tt $body:tt $done:tt $arg:tt $($rest:tt)*) => {
        ::core::compile_error!(
            "the `weak_callback_or_else!` fallback takes fewer arguments than the callback"
        )
    };
    (@fresh [fallback [$($pending:tt)+] $paired:tt $fallback:tt] $owner:tt $body:tt $done:tt) => {
        ::core::compile_error!(
            "the `weak_callback_or_else!` fallback takes more arguments than the callback"
        )
    };
    (
        @fresh [skip] [$owner:ident] [$body:expr]
        [$([$fresh:ident $arg:tt $(: $arg_ty:ty)?])*]
    ) => {{
        let __eventful_weak = $crate::ShardRc::downgrade(&$owner);
        move |$($fresh $(: $arg_ty)?),*| {
            if let ::core::option::Option::Some(__eventful_value) = __eventful_weak.upgrade() {
                let $owner = &__eventful_value;
                $(let $arg = $fresh;)*
                $body
            }
        }
    }};
    (
        @fresh
        [fallback [] [$([$fallback_arg:tt $(: $fallback_ty:ty)?])*] [$fallback:expr]]
        [$owner:ident] [$body:expr]
        [$([$fresh:ident $arg:tt $(: $arg_ty:ty)?])*]
    ) => {{
        let __eventful_weak = $crate::ShardRc::downgrade(&$owner);
        move |$($fresh $(: $arg_ty)?),*| match __eventful_weak.upgrade() {
            ::core::option::Option::Some(__eventful_value) => {
                let $owner = &__eventful_value;
                $(let $arg = $fresh;)*
                $body
            }
            ::core::option::Option::None => {
                $(let $fallback_arg $(: $fallback_ty)? = $fresh;)*
                $fallback
            }
        }
    }};
}
