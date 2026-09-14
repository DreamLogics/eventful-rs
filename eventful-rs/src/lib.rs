#![forbid(unsafe_code)]
#![doc = include_str!("../README.md")]

pub use eventful_rs_macros::{action, asynced, asynchronize, eventful, events, sharded_main};

mod engine;
pub use engine::{InvokeError, ShardEventHandle};

mod shard_handle;
pub use shard_handle::*;

pub mod local;

pub mod shard;

#[cfg(feature = "tokio")]
pub mod tokio;

#[cfg(feature = "tokio")]
pub mod tokio_local;

#[cfg(feature = "slint")]
pub mod slint;

use std::sync::{Arc, Mutex};

type JoinCallback = Box<dyn Fn() -> Result<(), ShardError> + Send + Sync + 'static>;
static SHARD_REGISTRY: Mutex<Vec<(ShardId, JoinCallback)>> = Mutex::new(Vec::new());

/// Stop and join registered background shards without holding the registry lock.
/// Call from synchronous code, after producers have finished submitting work.
pub fn join_all_shards() -> Result<(), ShardError> {
    if engine::on_shard_thread() {
        return Err(ShardError::JoinError(
            "join all shards from outside their event loops".into(),
            None,
        ));
    }
    #[cfg(feature = "tokio")]
    if ::tokio::runtime::Handle::try_current().is_ok() {
        return Err(ShardError::JoinError(
            "use join_all_shards_async() from Tokio".into(),
            None,
        ));
    }
    join_all_shards_blocking()
}

fn join_all_shards_blocking() -> Result<(), ShardError> {
    let entries = std::mem::take(&mut *SHARD_REGISTRY.lock().unwrap());
    let mut errors = Vec::new();
    let mut retry = Vec::new();
    for (id, join) in entries {
        if let Err(error) = join() {
            errors.push(error.to_string());
            retry.push((id, join));
        }
    }
    SHARD_REGISTRY.lock().unwrap().extend(retry);
    if errors.is_empty() {
        Ok(())
    } else {
        Err(ShardError::JoinError(errors.join("; "), None))
    }
}

/// Join background shards without blocking a Tokio worker.
#[cfg(feature = "tokio")]
pub async fn join_all_shards_async() -> Result<(), ShardError> {
    if engine::on_shard_thread() {
        return Err(ShardError::JoinError(
            "cannot join all shards from a shard thread".into(),
            None,
        ));
    }
    ::tokio::task::spawn_blocking(join_all_shards_blocking)
        .await
        .map_err(|e| ShardError::JoinError(e.to_string(), None))?
}

fn register_shard(shard_id: ShardId, join: JoinCallback) {
    SHARD_REGISTRY.lock().unwrap().push((shard_id, join));
}

#[derive(Debug)]
pub enum ShardError {
    JoinError(
        String,
        Option<Box<dyn std::error::Error + Send + Sync + 'static>>,
    ),
    PostError(
        String,
        Option<Box<dyn std::error::Error + Send + Sync + 'static>>,
    ),
}

impl std::fmt::Display for ShardError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ShardError::JoinError(msg, original_err) => {
                write!(f, "join error: {msg}")?;

                if let Some(err) = original_err {
                    write!(f, ": {err}")?;
                }

                Ok(())
            }

            ShardError::PostError(msg, original_err) => {
                write!(f, "post error: {msg}")?;

                if let Some(err) = original_err {
                    write!(f, ": {err}")?;
                }

                Ok(())
            }
        }
    }
}

impl std::error::Error for ShardError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            ShardError::JoinError(_, Some(err)) | ShardError::PostError(_, Some(err)) => {
                Some(err.as_ref())
            }

            _ => None,
        }
    }
}

static LAST_SHARD_ID: Mutex<usize> = Mutex::new(0);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ShardId(pub usize);

impl ShardId {
    pub fn new() -> Self {
        let mut last_id = LAST_SHARD_ID.lock().unwrap();
        *last_id += 1;
        ShardId(*last_id)
    }
}

impl Default for ShardId {
    fn default() -> Self {
        ShardId::new()
    }
}

pub trait EventLoopHandle: Clone + Send + Sync + 'static {
    fn shard_id(&self) -> ShardId;
    fn invoke<F>(&self, task: F)
    where
        F: FnOnce() + Send + 'static;
    fn invoke_async<F>(&self, f: F)
    where
        F: AsyncFnOnce() -> () + Send + 'static;
    fn invoke_with_handle<T, H, F>(&self, handle: H, f: F)
    where
        T: Eventful<EventLoopHandleType = Self> + HasEvents<T::EventSetType> + Sized + 'static,
        H: ShardHandle<T>,
        F: FnOnce(&T) + Send + 'static;
    fn invoke_with_handle_async<T, H, F>(&self, handle: H, f: F)
    where
        T: Eventful<EventLoopHandleType = Self> + HasEvents<T::EventSetType> + Sized + 'static,
        H: ShardHandle<T>,
        F: AsyncFnOnce(&T) -> () + Send + 'static;

    fn deferred_invoke<T, H, F, R>(
        &self,
        handle: H,
        task: F,
    ) -> futures::future::BoxFuture<'static, R>
    where
        T: Eventful<EventLoopHandleType = Self> + HasEvents<T::EventSetType> + Sized + 'static,
        H: ShardHandle<T>,
        F: AsyncFnOnce(&T) -> R + Send + 'static,
        R: Send + 'static;

    fn spawn<F>(&self, f: F)
    where
        F: Future + Send + 'static;
}

pub trait Eventful {
    type EventSetType: ?Sized + Send + Sync + 'static;
    type EventLoopHandleType: EventLoopHandle;
    /// Default destination used by the convenience From conversion.
    fn default_handle() -> Self::EventLoopHandleType {
        panic!("this type has no default shard; construct it with bind/bind_async")
    }
}

pub trait HasEvents<E>
where
    E: ?Sized + Send + 'static,
{
    fn events(&self) -> &Arc<E>;
}

pub trait EventLoop {
    type HandleType: EventLoopHandle;
    fn handle(&self) -> Self::HandleType;
    /// Spawn objects bound to this event loop.
    fn bind<F, R, T>(&self, f: F) -> R
    where
        F: FnOnce(&dyn Fn(T) -> ShardRc<T>) -> R + Send + 'static,
        R: Send + 'static,
        T: Eventful<EventLoopHandleType = Self::HandleType>
            + HasEvents<T::EventSetType>
            + Sized
            + 'static;
    fn spawn<F>(&self, f: F)
    where
        F: Future + Send + 'static,
    {
        self.handle().spawn(f);
    }
    fn join(&self) -> Result<(), ShardError>;
}

type TaskFn<Args> = dyn Fn(Args) + Send + Sync + 'static;

/// Type-erased connection storage for one signal signature.
pub struct Event<Args> {
    connections: Mutex<Vec<Arc<TaskFn<Args>>>>,
}

impl<Args> Default for Event<Args> {
    fn default() -> Self {
        Self {
            connections: Mutex::new(Vec::new()),
        }
    }
}

impl<Args> Event<Args>
where
    Args: Clone + Send + 'static,
{
    pub fn add_connection<F>(&self, connection: F)
    where
        F: Fn(Args) + Send + Sync + 'static,
    {
        self.connections.lock().unwrap().push(Arc::new(connection));
    }

    pub fn emit(&self, args: Args) {
        let snapshot = self.connections.lock().unwrap().clone();
        for connection in snapshot {
            connection(args.clone());
        }
    }

    pub fn connection_count(&self) -> usize {
        self.connections.lock().unwrap().len()
    }
}

#[macro_export]
macro_rules! use_shard {
    ($name:path) => {
        type DefaultShardHandleType = $crate::ShardEventHandle;
        fn default_shard() -> &'static impl $crate::EventLoop<HandleType = $crate::ShardEventHandle>
        {
            &$name
        }
    };
}

mod background;
mod main_loop;
