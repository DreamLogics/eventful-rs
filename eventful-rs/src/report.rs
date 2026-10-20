//! Process-wide reporting of panics isolated by shard drivers.
use crate::ShardId;
use std::{
    any::Any,
    fmt,
    panic::AssertUnwindSafe,
    sync::{Arc, PoisonError, RwLock},
};

/// An installed reporter, shared so it can be called without holding the lock.
type Reporter = Arc<dyn Fn(&CallbackPanic<'_>) + Send + Sync + 'static>;

/// Reporter used instead of the default stderr message, if any.
static REPORTER: RwLock<Option<Reporter>> = RwLock::new(None);

/// A shard callback, job, or local task panicked. The driver isolated the
/// panic and the shard keeps running. Mutations made before the panic are not
/// rolled back.
#[non_exhaustive]
pub struct CallbackPanic<'a> {
    /// Shard whose driver caught the panic.
    shard_id: ShardId,
    /// Payload passed to `panic!` or `resume_unwind`.
    payload: &'a (dyn Any + Send),
}
impl CallbackPanic<'_> {
    /// Shard whose driver caught the panic.
    pub fn shard_id(&self) -> ShardId {
        self.shard_id
    }
    /// The panic payload, for downcasting application-specific values.
    pub fn payload(&self) -> &(dyn Any + Send) {
        self.payload
    }
    /// The panic message, when the payload is a `&str` or `String`.
    pub fn message(&self) -> Option<&str> {
        self.payload
            .downcast_ref::<&str>()
            .copied()
            .or_else(|| self.payload.downcast_ref::<String>().map(String::as_str))
    }
}
impl fmt::Debug for CallbackPanic<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CallbackPanic")
            .field("shard_id", &self.shard_id)
            .field("message", &self.message())
            .finish_non_exhaustive()
    }
}
impl fmt::Display for CallbackPanic<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("shard callback panicked")?;
        match self.message() {
            Some(message) => write!(f, ": {message}"),
            None => Ok(()),
        }
    }
}

/// Replace how shard drivers report isolated callback panics, for every
/// runtime and every shard in the process. By default each panic prints
/// `eventful-rs: shard callback panicked` to stderr; applications that own the
/// terminal, such as TUIs, can route reports to a log instead.
///
/// The reporter runs on the panicking shard's thread after unwinding has
/// stopped, so it should return quickly. A panic inside the reporter is
/// discarded. Tracked emissions and deferred calls still receive
/// [`crate::InvokeError::Panicked`] independently of reporting.
///
/// This only replaces the library's own message. The standard panic hook
/// still runs at the panic site; use [`std::panic::set_hook`] to change it.
pub fn set_panic_reporter<F>(reporter: F)
where
    F: Fn(&CallbackPanic<'_>) + Send + Sync + 'static,
{
    let reporter: Reporter = Arc::new(reporter);
    let previous = REPORTER
        .write()
        .unwrap_or_else(PoisonError::into_inner)
        .replace(reporter);
    drop(previous); // Release captures outside the lock.
}

/// Report an isolated panic through the installed reporter or to stderr.
pub(crate) fn report(shard_id: ShardId, payload: &(dyn Any + Send)) {
    // Clone out of the lock so a reporter may itself install a reporter.
    let reporter = REPORTER
        .read()
        .unwrap_or_else(PoisonError::into_inner)
        .clone();
    let Some(reporter) = reporter else {
        eprintln!("eventful-rs: shard callback panicked");
        return;
    };
    let panic = CallbackPanic { shard_id, payload };
    // The driver must survive a faulty reporter.
    let _ = std::panic::catch_unwind(AssertUnwindSafe(|| reporter(&panic)));
}
