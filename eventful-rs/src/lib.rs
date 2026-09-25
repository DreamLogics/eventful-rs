#![doc = include_str!("../GUIDE.md")]
#![forbid(unsafe_code)]
#![deny(missing_docs)]

pub use eventful_rs_macros::{
    action, asynced, asynchronize, eventful, events, scope, sharded_main,
};

mod binding;
pub use binding::*;

mod engine;
pub use engine::{InvokeError, ShardEventHandle};

/// A tracked event handler failed to complete. Uses the same failure reasons as
/// deferred shard invocations.
pub type DeliveryError = InvokeError;

mod shard_handle;
pub use shard_handle::*;
mod connection;
pub use connection::*;

pub mod local;

pub mod shard;

pub mod task;

#[cfg(feature = "tokio")]
pub mod tokio;

#[cfg(feature = "tokio")]
pub mod tokio_local;

#[cfg(feature = "slint")]
pub mod slint;

mod error;
pub use error::{ShardError, ShardId};
mod event;
pub use event::{Event, EventLabel};
mod event_loop;
pub use event_loop::{EventLoop, EventLoopHandle};
mod eventful;
pub use eventful::{Eventful, HasEvents};
mod registry;
pub use registry::join_all_shards;
#[cfg(feature = "tokio")]
pub use registry::join_all_shards_async;
pub(crate) use registry::register_shard;
mod background;
mod main_loop;
