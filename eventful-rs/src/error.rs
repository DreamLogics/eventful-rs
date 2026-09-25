//! Runtime lifecycle errors and shard identities.
use std::sync::Mutex;

/// Failure to join a shard or post work to a backend.
#[derive(Debug)]
#[non_exhaustive]
pub enum ShardError {
    /// Joining failed, with context and an optional underlying error.
    JoinError(
        String,
        Option<Box<dyn std::error::Error + Send + Sync + 'static>>,
    ),
    /// Submission failed, with context and an optional underlying error.
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

/// Monotonic process-wide allocator for shard identities.
static LAST_SHARD_ID: Mutex<usize> = Mutex::new(0);

/// Opaque process-local shard identity, allocated by [`ShardId::new`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ShardId(
    /// Numeric identity allocated by the library.
    usize,
);

impl ShardId {
    /// Allocate a fresh process-local identity.
    ///
    /// # Panics
    /// Panics if all process-local identities have been allocated.
    pub fn new() -> Self {
        let mut last_id = LAST_SHARD_ID.lock().unwrap_or_else(|e| e.into_inner());
        *last_id = last_id
            .checked_add(1)
            .expect("shard identity space exhausted");
        ShardId(*last_id)
    }
}

impl Default for ShardId {
    fn default() -> Self {
        ShardId::new()
    }
}
