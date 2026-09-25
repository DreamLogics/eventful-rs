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
}
