//! Named shard declarations and compile-time or dynamic affinity.
use crate::{Eventful, HasEvents, InvokeError, ShardEventHandle, ShardId, ShardRc};
use std::future::Future;

/// Affinity used to validate binding through a runtime shard handle.
/// Named bindings return one stable shard ID. DynamicShard accepts any shard.
pub trait ShardAffinity: 'static {
    /// Return the required shard identity, or `None` for dynamic affinity.
    fn shard_id() -> Option<ShardId>;
}

/// Explicit opt-in to selecting a shard at runtime with bind/bind_async.
/// Such a value's actual destination is carried by its handle, not its type.
///
/// ```
/// use eventful_rs::*;
/// #[eventful(shard = DynamicShard)]
/// struct Session;
/// let worker = shard::Shard::new("session-worker");
/// let session = futures::executor::block_on(worker.bind_async(|bind| {
///     bind(Session { events: Default::default() }).as_handle()
/// })).unwrap();
/// drop(session);
/// worker.join().unwrap();
/// ```
pub enum DynamicShard {}
impl ShardAffinity for DynamicShard {
    /// Return the required shard identity, or `None` for dynamic affinity.
    fn shard_id() -> Option<ShardId> {
        None
    }
}

/// A named singleton shard. Implementations must always return the same shard.
/// Prefer declare_shard! to implementing this trait manually.
pub trait ShardBinding: 'static {
    /// Access the singleton submission handle, initializing its backend if needed.
    fn handle() -> ShardEventHandle;

    /// Bind values of this affinity. A mismatched eventful type fails to compile.
    fn bind_async<F, R, T>(f: F) -> impl Future<Output = Result<R, InvokeError>> + Send + 'static
    where
        Self: Sized,
        T: Eventful<Shard = Self> + HasEvents<T::EventSetType> + 'static,
        F: FnOnce(&dyn Fn(T) -> ShardRc<T>) -> R + Send + 'static,
        R: Send + 'static,
    {
        Self::handle().bind_async(f)
    }
}
impl<S: ShardBinding> ShardAffinity for S {
    /// Return the required shard identity, or `None` for dynamic affinity.
    fn shard_id() -> Option<ShardId> {
        use crate::EventLoopHandle;
        Some(Self::handle().shard_id())
    }
}

/// Declare a shard marker and its lazily initialized singleton.
///
/// ```
/// use eventful_rs::*;
/// declare_shard!(pub Worker, runtime = std);
/// #[eventful(shard = Worker)]
/// struct State;
/// let value = futures::executor::block_on(State::spawn(|| State {
///     events: Default::default(),
/// })).unwrap();
/// Worker::shard().join().unwrap();
/// ```
///
/// Runtimes: std, tokio, main, tokio_main, slint. Tokio and Slint require their
/// respective features. Initialize calling-thread/UI shards on their owner
/// thread via Marker::shard() before submitting work from other threads.
/// This macro does not select a default for surrounding eventful declarations.
#[macro_export]
macro_rules! declare_shard {
    ($vis:vis $name:ident, runtime = std) => {
        $crate::declare_shard!(@impl $vis $name, $crate::shard::Shard,
            $crate::shard::Shard::new(stringify!($name)));
    };
    ($vis:vis $name:ident, runtime = tokio) => {
        $crate::declare_shard!(@impl $vis $name, $crate::tokio::TokioShard,
            $crate::tokio::TokioShard::new(stringify!($name)));
    };
    ($vis:vis $name:ident, runtime = main) => {
        $crate::declare_shard!(@impl $vis $name, $crate::local::LocalShard,
            $crate::local::LocalShard::new());
    };
    ($vis:vis $name:ident, runtime = tokio_main) => {
        $crate::declare_shard!(@impl $vis $name, $crate::tokio_local::TokioLocalShard,
            $crate::tokio_local::TokioLocalShard::new(stringify!($name)));
    };
    ($vis:vis $name:ident, runtime = slint) => {
        $crate::declare_shard!(@impl $vis $name, $crate::slint::SlintShard,
            $crate::slint::SlintShard::new());
    };
    (@impl $vis:vis $name:ident, $backend:ty, $init:expr) => {
        /// Marker for a lazily initialized singleton shard.
        $vis enum $name {}
        impl $name {
            /// Access the singleton backend, initializing it on first use.
            $vis fn shard() -> &'static $backend {
                static SHARD: ::std::sync::LazyLock<$backend> =
                    ::std::sync::LazyLock::new(|| $init);
                &SHARD
            }
        }
        impl $crate::ShardBinding for $name {
            /// Access the singleton submission handle, initializing its backend if needed.
    fn handle() -> $crate::ShardEventHandle {
                $crate::EventLoop::handle(Self::shard())
            }
        }
    };
}

/// Select the default shard for eventful types in the current module/file.
///
/// ```
/// use eventful_rs::*;
/// declare_shard!(pub Worker, runtime = std);
/// file_scope!(shard = Worker);
///
/// #[eventful]
/// struct Counter;
///
/// fn on_worker<T: Eventful<Shard = Worker>>() {}
/// fn main() {
///     on_worker::<Counter>();
/// }
/// ```
///
/// Declare once per module, anywhere at module level. Explicit `eventful` shard
/// arguments and enclosing `#[scope]` selections take precedence. Child modules
/// do not automatically inherit this selection. Normal Rust imports apply:
/// `use super::*` can import the alias; a local file_scope! overrides that import.
/// Use `#[scope]` for explicit recursive inline-module selection.
/// This declares the reserved module-local alias `__EventfulFileShard`.
#[macro_export]
macro_rules! file_scope {
    (shard = $shard:path $(,)?) => {
        #[doc(hidden)]
        type __EventfulFileShard = $shard;
    };
}
