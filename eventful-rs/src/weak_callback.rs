//! Weak callbacks whose closure arity matches the setter they are passed to.

/// Create a callback that holds a weak reference to a [`ShardRc`](crate::ShardRc)
/// and skips calls once the value has been collected.
///
/// ```text
/// weak_callback!(owner, |receiver, arg1, arg2, …| body)
/// ```
///
/// `owner` is an expression of type `ShardRc<T>` (or a reference to one). The
/// macro downgrades it and expands to a `move` closure taking `arg1, arg2, …`,
/// so the closure goes straight into a Slint `on_*` setter or any other function
/// expecting `impl Fn*(A1, A2, …)`, and the argument types are inferred from it.
/// There is no limit on the number of arguments. While the value is alive,
/// `receiver` is bound to a `&ShardRc<T>` for the duration of the call and
/// `body` runs; it must evaluate to `()`. Use [`weak_callback_or_else!`](crate::weak_callback_or_else!) for
/// callbacks that return a value.
///
/// Arguments are identifiers, `_`, or other single-token patterns such as
/// `(a, b)`, each with an optional `: Type` annotation. The receiver accepts the
/// same forms.
///
/// The closure is `move`. Name the receiver like the owner so the body cannot
/// capture the strong reference, which would keep the value alive.
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
///     let mut key = on_key(weak_callback!(app, |app, _text, ctrl| {
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
    (
        $owner:expr,
        |$receiver:tt $(: $receiver_ty:ty)? $(, $arg:tt $(: $arg_ty:ty)?)*| $body:expr $(,)?
    ) => {
        $crate::__weak_callback! {
            @fresh [skip] [$owner] [$receiver $(: $receiver_ty)?] [$body] []
            $([$arg $(: $arg_ty)?])*
        }
    };
}

/// Create a value-returning callback that holds a weak reference to a
/// [`ShardRc`](crate::ShardRc) and calls `fallback` once the value has been collected.
///
/// ```text
/// weak_callback_or_else!(owner, |receiver, arg1, arg2, …| body, |arg1, arg2, …| fallback)
/// ```
///
/// This works like [`weak_callback!`](crate::weak_callback!), except that `body` may return a value. The
/// fallback closure receives the same arguments and must produce the same type.
/// Its parameters accept the same forms as the callback's, so `|_, _| 0` ignores
/// them; write `|| value` when there are no arguments.
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
///     let mut shift = on_shift(weak_callback_or_else!(app, |app, a| a + app.offset, |a| a));
///     assert_eq!(shift(1), 11);
///     Ok::<(), InvokeError>(())
/// })?;
/// # Ok::<(), InvokeError>(())
/// ```
#[macro_export]
macro_rules! weak_callback_or_else {
    (
        $owner:expr,
        |$receiver:tt $(: $receiver_ty:ty)? $(, $arg:tt $(: $arg_ty:ty)?)*| $body:expr,
        || $fallback:expr $(,)?
    ) => {
        $crate::__weak_callback! {
            @fresh [fallback [] [] [$fallback]] [$owner] [$receiver $(: $receiver_ty)?] [$body] []
            $([$arg $(: $arg_ty)?])*
        }
    };
    (
        $owner:expr,
        |$receiver:tt $(: $receiver_ty:ty)? $(, $arg:tt $(: $arg_ty:ty)?)*| $body:expr,
        |$($fallback_arg:tt $(: $fallback_ty:ty)?),*| $fallback:expr $(,)?
    ) => {
        $crate::__weak_callback! {
            @fresh
            [fallback [$([$fallback_arg $(: $fallback_ty)?])*] [] [$fallback]]
            [$owner] [$receiver $(: $receiver_ty)?] [$body] []
            $([$arg $(: $arg_ty)?])*
        }
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
        @fresh [skip] $owner:tt $receiver:tt $body:tt [$($done:tt)*]
        [$arg:tt $(: $arg_ty:ty)?] $($rest:tt)*
    ) => {
        $crate::__weak_callback! {
            @fresh [skip] $owner $receiver $body
            [$($done)* [__eventful_arg $arg $(: $arg_ty)?]] $($rest)*
        }
    };
    (
        @fresh
        [fallback [[$fallback_arg:tt $(: $fallback_ty:ty)?] $($pending:tt)*] [$($paired:tt)*] $fallback:tt]
        $owner:tt $receiver:tt $body:tt [$($done:tt)*]
        [$arg:tt $(: $arg_ty:ty)?] $($rest:tt)*
    ) => {
        $crate::__weak_callback! {
            @fresh
            [fallback [$($pending)*] [$($paired)* [$fallback_arg $(: $fallback_ty)?]] $fallback]
            $owner $receiver $body
            [$($done)* [__eventful_arg $arg $(: $arg_ty)?]] $($rest)*
        }
    };
    (@fresh [fallback [] $paired:tt $fallback:tt] $owner:tt $receiver:tt $body:tt $done:tt $arg:tt $($rest:tt)*) => {
        ::core::compile_error!(
            "the `weak_callback_or_else!` fallback takes fewer arguments than the callback"
        )
    };
    (@fresh [fallback [$($pending:tt)+] $paired:tt $fallback:tt] $owner:tt $receiver:tt $body:tt $done:tt) => {
        ::core::compile_error!(
            "the `weak_callback_or_else!` fallback takes more arguments than the callback"
        )
    };
    (
        @fresh [skip] [$owner:expr] [$receiver:tt $(: $receiver_ty:ty)?] [$body:expr]
        [$([$fresh:ident $arg:tt $(: $arg_ty:ty)?])*]
    ) => {{
        let __eventful_weak = $crate::ShardRc::downgrade(&$owner);
        move |$($fresh $(: $arg_ty)?),*| {
            if let ::core::option::Option::Some(__eventful_value) = __eventful_weak.upgrade() {
                let $receiver $(: $receiver_ty)? = &__eventful_value;
                $(let $arg = $fresh;)*
                $body
            }
        }
    }};
    (
        @fresh
        [fallback [] [$([$fallback_arg:tt $(: $fallback_ty:ty)?])*] [$fallback:expr]]
        [$owner:expr] [$receiver:tt $(: $receiver_ty:ty)?] [$body:expr]
        [$([$fresh:ident $arg:tt $(: $arg_ty:ty)?])*]
    ) => {{
        let __eventful_weak = $crate::ShardRc::downgrade(&$owner);
        move |$($fresh $(: $arg_ty)?),*| match __eventful_weak.upgrade() {
            ::core::option::Option::Some(__eventful_value) => {
                let $receiver $(: $receiver_ty)? = &__eventful_value;
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
