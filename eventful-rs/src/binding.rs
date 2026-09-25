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
///     bind(Session { events: Default::default() }).to_handle()
/// }))?;
/// drop(session);
/// worker.join()?;
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
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
