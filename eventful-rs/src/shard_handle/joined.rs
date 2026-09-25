use super::{ShardHandleInternal, ShardRcHandle};
use crate::{Eventful, HasEvents, InvokeError};
use futures::{FutureExt, channel::oneshot};
use std::panic::AssertUnwindSafe;

/// Strong handles validated to belong to the same shard.
///
/// Construct with [`ShardRcHandle::join`]. Groups of two through eight handles
/// support synchronous, asynchronous, and deferred upgrades. Chained joins
/// produce flat tuples. Handles are retained until the submitted job completes.
///
/// Joining borrows its inputs and clones the handles only when their shards match.
/// The returned group owns its handles. Only the built-in shard handle is supported.
#[derive(Clone, Debug)]
pub struct JoinedHandles<H> {
    handles: H,
}

impl<T> ShardRcHandle<T>
where
    T: Eventful + HasEvents<T::EventSetType> + 'static,
{
    /// Join two strong handles, returning `None` if their shards differ.
    ///
    /// Borrows both inputs and clones them only on success. This checks shard
    /// identity, not whether the shard is still running.
    ///
    /// ```
    /// use eventful_rs::*;
    /// use std::cell::Cell;
    /// declare_shard!(pub Counters, runtime = std);
    ///
    /// #[eventful(shard = Counters)]
    /// struct Counter { value: Cell<usize> }
    ///
    /// let (foo, bar) = Counters::shard().bind(|bind| {
    ///     let make = || bind(Counter {
    ///         value: Cell::new(0), events: Default::default(),
    ///     }).as_handle();
    ///     (make(), make())
    /// });
    /// let joined = foo.join(&bar).expect("same shard");
    /// joined.upgrade_in_shard(|(foo, bar)| {
    ///     foo.value.set(1);
    ///     bar.value.set(2);
    /// });
    /// let sum = futures::executor::block_on(
    ///     joined.deferred_upgrade_in_shard(async |(foo, bar)| {
    ///         foo.value.get() + bar.value.get()
    ///     }),
    /// );
    /// assert_eq!(sum, 3);
    /// Counters::shard().join().unwrap();
    /// ```
    pub fn join<U>(
        &self,
        other: &ShardRcHandle<U>,
    ) -> Option<JoinedHandles<(Self, ShardRcHandle<U>)>>
    where
        U: Eventful + HasEvents<U::EventSetType> + 'static,
    {
        (self.shard_id() == other.shard_id()).then(|| JoinedHandles {
            handles: (self.clone(), other.clone()),
        })
    }
}

macro_rules! joined_upgrades {
    ($($ty:ident : $value:ident),+) => {
        impl<$($ty),+> JoinedHandles<($(ShardRcHandle<$ty>,)+)>
        where
            $($ty: Eventful
                + HasEvents<$ty::EventSetType> + 'static,)+
        {
            /// Run a callback with all values on their shared shard.
            /// A stopped shard silently skips the callback, as for a single handle.
            pub fn upgrade_in_shard<F>(&self, task: F)
            where
                F: FnOnce(($(&$ty,)+)) + Send + 'static,
            {
                let handles = self.handles.clone();
                let _ = self.handles.0.shard_handle.post(Box::new(move |store| {
                    Box::pin(async move {
                        let ($($value,)+) = &handles;
                        $(let $value = { store.borrow().get::<super::ShardValue<$ty>>($value.id()) };)+
                        if let ($(Some($value),)+) = ($($value,)+) {
                            task(($(&$value,)+));
                        }
                        drop(handles);
                    })
                }));
            }

            /// Run an async callback with all values on their shared shard.
            /// Other jobs may run while the callback is suspended.
            pub fn upgrade_in_shard_async<F>(&self, task: F)
            where
                F: AsyncFnOnce(($(&$ty,)+)) -> () + Send + 'static,
            {
                let handles = self.handles.clone();
                let _ = self.handles.0.shard_handle.post(Box::new(move |store| {
                    Box::pin(async move {
                        let ($($value,)+) = &handles;
                        $(let $value = { store.borrow().get::<super::ShardValue<$ty>>($value.id()) };)+
                        if let ($(Some($value),)+) = ($($value,)+) {
                            task(($(&$value,)+)).await;
                        }
                        drop(handles);
                    })
                }));
            }

            /// Run an async callback and return its result, panicking on failure.
            pub async fn deferred_upgrade_in_shard<F, R>(&self, task: F) -> R
            where
                F: AsyncFnOnce(($(&$ty,)+)) -> R + Send + 'static,
                R: Send + 'static,
            {
                self.try_deferred_upgrade_in_shard(task)
                    .await.expect("deferred shard invocation failed")
            }

            /// Submit immediately and observe completion, including delivery failures.
            /// Dropping the returned future does not cancel the submitted job.
            pub fn try_deferred_upgrade_in_shard<F, R>(
                &self,
                task: F,
            ) -> futures::future::BoxFuture<'static, Result<R, InvokeError>>
            where
                F: AsyncFnOnce(($(&$ty,)+)) -> R + Send + 'static,
                R: Send + 'static,
            {
                let handles = self.handles.clone();
                let (tx, rx) = oneshot::channel();
                let posted = self.handles.0.shard_handle.post(Box::new(move |store| {
                    Box::pin(async move {
                        let result = AssertUnwindSafe(async move {
                            let ($($value,)+) = &handles;
                            $(let $value = { store.borrow().get::<super::ShardValue<$ty>>($value.id()) }
                                .ok_or(InvokeError::ValueMissing)?;)+
                            let result = task(($(&$value,)+)).await;
                            drop(handles);
                            Ok(result)
                        }).catch_unwind().await.unwrap_or(Err(InvokeError::Panicked));
                        let _ = tx.send(result);
                    })
                }));
                Box::pin(async move {
                    posted?;
                    rx.await.map_err(|_| InvokeError::Canceled)?
                })
            }
        }
    };
}

macro_rules! joined_extend {
    ($($ty:ident : $value:ident),+) => {
        impl<$($ty),+> JoinedHandles<($(ShardRcHandle<$ty>,)+)>
        where
            $($ty: Eventful
                + HasEvents<$ty::EventSetType> + 'static,)+
        {
            /// Append a strong handle to the flat tuple if it shares this shard.
            /// Borrows both inputs and clones their handles only on success.
            pub fn join<U>(&self, other: &ShardRcHandle<U>)
                -> Option<JoinedHandles<($(ShardRcHandle<$ty>,)+ ShardRcHandle<U>)>>
            where
                U: Eventful
                    + HasEvents<U::EventSetType> + 'static,
            {
                if self.handles.0.shard_id() != other.shard_id() {
                    return None;
                }
                let ($($value,)+) = &self.handles;
                Some(JoinedHandles { handles: ($($value.clone(),)+ other.clone()) })
            }
        }
    };
}

macro_rules! joined_arities {
    ($first:ident : $first_value:ident, $($ty:ident : $value:ident),+) => {
        joined_upgrades!($first: $first_value, $($ty: $value),+);
        joined_arities!(@extend $first: $first_value; $($ty: $value),+);
    };
    (@extend $($done:ident : $done_value:ident),+; $next:ident : $next_value:ident, $($rest:ident : $rest_value:ident),+) => {
        joined_upgrades!($($done: $done_value,)+ $next: $next_value);
        joined_extend!($($done: $done_value,)+ $next: $next_value);
        joined_arities!(@extend $($done: $done_value,)+ $next: $next_value; $($rest: $rest_value),+);
    };
    (@extend $($done:ident : $done_value:ident),+; $last:ident : $last_value:ident) => {};
}

joined_arities!(A: a, B: b, C: c, D: d, E: e, G: g, H: h, I: i);
