//! Registration and coordinated joining of background shards.
use crate::{ShardError, ShardId, engine};
use std::sync::Mutex;

/// Thread-safe callback that requests shutdown and observes thread completion.
type JoinCallback = Box<dyn Fn() -> Result<(), ShardError> + Send + Sync + 'static>;
/// Registered background joins; callbacks run after releasing this mutex.
static SHARD_REGISTRY: Mutex<Vec<(ShardId, JoinCallback)>> = Mutex::new(Vec::new());

/// Stop and join registered background shards without holding the registry lock.
/// Call from synchronous code, after producers have finished submitting work.
///
/// # Errors
/// Returns [`ShardError::JoinError`] on a shard thread, inside Tokio, or when
/// any background join fails. Failed joins remain registered for retry.
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

/// Take a registry snapshot, join outside the lock, and retain failed joins for retry.
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
///
/// # Errors
/// Returns [`ShardError::JoinError`] on a shard thread or if a background join
/// or the Tokio blocking task fails.
///
/// # Panics
/// Panics when called outside a Tokio runtime.
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

/// Retain a background join callback until coordinated shutdown.
pub(crate) fn register_shard(shard_id: ShardId, join: JoinCallback) {
    SHARD_REGISTRY.lock().unwrap().push((shard_id, join));
}

/// Forget a shard that has been joined successfully.
/// A no-op while `join_all_shards` holds the entry; it is not retained on success.
pub(crate) fn unregister_shard(shard_id: ShardId) {
    let removed: Vec<_> = {
        let mut registry = SHARD_REGISTRY.lock().unwrap_or_else(|e| e.into_inner());
        let (removed, kept) = std::mem::take(&mut *registry)
            .into_iter()
            .partition(|(id, _)| *id == shard_id);
        *registry = kept;
        removed
    };
    // Release callback captures outside the registry lock.
    drop(removed);
}

#[cfg(test)]
mod tests {
    use super::SHARD_REGISTRY;
    use crate::EventLoop;

    fn registered(id: crate::ShardId) -> bool {
        SHARD_REGISTRY
            .lock()
            .unwrap()
            .iter()
            .any(|(registered, _)| *registered == id)
    }

    #[test]
    fn joined_shards_leave_the_registry() {
        let shard = crate::std_rt::Shard::new("registry-join");
        let id = shard.shard_id();
        assert!(registered(id));
        shard.join().unwrap();
        assert!(!registered(id));
        shard.join().unwrap(); // Repeated joins still report the cached outcome.
    }
}
