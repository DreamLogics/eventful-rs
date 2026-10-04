//! Private implementation contract for built-in value handles.
use crate::{Eventful, HasEvents};
/// Internal identity access, inaccessible to downstream implementors.
pub trait Sealed<T>
where
    T: Eventful + HasEvents<T::EventSetType> + Sized + 'static,
{
    /// Key in the owner store.
    fn id(&self) -> usize;
    /// Identity of the owner shard.
    fn shard_id(&self) -> crate::ShardId;
    /// What a queued job needs to reach the value, without cloning the handle.
    fn target(&self) -> Target;
}

/// Store key of a queued job's value. For strong handles, also a lifetime token
/// that keeps the value alive until the job has run, like the handle would.
#[derive(Debug)]
pub struct Target {
    /// Key in the owner store.
    pub(crate) key: usize,
    /// Strong-handle token released when the job finishes; `None` for weak handles.
    pub(crate) _retain: Option<super::ShardRcId>,
}
