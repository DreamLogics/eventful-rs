#![doc = include_str!("../GUIDE.md")]
#![forbid(unsafe_code)]
#![deny(missing_docs, missing_debug_implementations)]

extern crate self as eventful_rs;

pub use eventful_rs_macros::{
    action, asynced, asynchronize, declare_shard, eventful, events, sharded, sharded_main,
    slint_events,
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
mod local_task;
pub use local_task::{LocalTask, TaskHandle};

pub mod local_rt;

pub mod std_rt;

pub mod task;

#[cfg(feature = "tokio")]
pub mod tokio_rt;

#[cfg(feature = "tokio")]
pub mod tokio_local_rt;

#[cfg(feature = "slint")]
pub mod slint_rt;

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

#[doc(hidden)]
#[path = "builders.rs"]
pub mod __builders;
