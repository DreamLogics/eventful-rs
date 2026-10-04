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
/// let worker = std_rt::Shard::new("session-worker");
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

    /// Spawn a future on this shard's thread from code already running there.
    /// The future need not be `Send` and may capture `Rc`, `RefCell`, or
    /// [`ShardRc`]. The shard's driver polls it, first on a later turn rather
    /// than during this call, and it interleaves with other work at each
    /// `.await`. Panics are reported like callback panics. Shutdown aborts
    /// local tasks instead of waiting for them.
    ///
    /// Futures see the shard's runtime: Tokio timers and I/O need a Tokio shard.
    /// Capturing a strong reference keeps that value alive until the task ends;
    /// prefer [`ShardRc::spawn_owned`] or a [`crate::ShardWeak`] for loops.
    ///
    /// ```
    /// use eventful_rs::*;
    /// use std::{cell::Cell, rc::Rc};
    /// declare_shard!(Ui, runtime = main);
    ///
    /// Ui::shard().run_main(|| async {
    ///     let ticks = Rc::new(Cell::new(0));
    ///     let counter = ticks.clone();
    ///     let task = Ui::spawn_local(async move {
    ///         counter.set(counter.get() + 1);
    ///         counter.get()
    ///     })?;
    ///     // Awaiting the task from this shard yields its output.
    ///     assert_eq!(task.await?, 1);
    ///     assert_eq!(ticks.get(), 1);
    ///     Ok::<(), InvokeError>(())
    /// })?;
    /// # Ok::<(), InvokeError>(())
    /// ```
    ///
    /// # Errors
    /// Returns [`InvokeError::WrongShard`] when not called on this shard's
    /// thread, or [`InvokeError::Closed`] once the shard is shutting down.
    fn spawn_local<F>(future: F) -> Result<crate::LocalTask<F::Output>, InvokeError>
    where
        Self: Sized,
        F: Future + 'static,
        F::Output: 'static,
    {
        crate::local_task::spawn(&Self::handle(), future)
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
/// use_shard!(shard = Worker);
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
/// arguments and enclosing `#[sharded]` selections take precedence. Child modules
/// do not automatically inherit this selection. Normal Rust imports apply:
/// `use super::*` can import the alias; a local use_shard! overrides that import.
/// Use `#[sharded]` for explicit recursive inline-module selection.
/// This declares the reserved module-local alias `__EventfulFileShard`.
#[macro_export]
macro_rules! use_shard {
    (shard = $shard:path $(,)?) => {
        #[doc(hidden)]
        type __EventfulFileShard = $shard;
    };
}
