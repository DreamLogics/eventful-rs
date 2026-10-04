//! Owner-thread value storage and strong-handle lifetime tokens.
//!
//! Collection is event driven: releasing the last strong token queues the
//! value's key on its store's [`CollectSignal`] and wakes the shard driver.
//! Values whose last token is gone but which an in-flight callback still
//! borrows become orphans and are rechecked after later work completes.
use futures::task::AtomicWaker;
use std::{
    any::Any,
    collections::HashMap,
    rc::Rc,
    sync::{
        Arc, Mutex, Weak,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    task::{Context, Poll},
};

/// Thread-safe queue of keys that may have become collectable.
pub(crate) struct CollectSignal {
    /// Keys whose strong-token count reached zero since the last pass.
    candidates: Mutex<Vec<usize>>,
    /// The owner shard's driver, woken when a candidate is queued.
    waker: AtomicWaker,
    /// Set once the store is gone; later releases need not be queued.
    closed: AtomicBool,
}

impl CollectSignal {
    /// Queue a candidate key and wake the driver.
    fn notify(&self, key: usize) {
        if self.closed.load(Ordering::Acquire) {
            return;
        }
        self.candidates
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .push(key);
        self.waker.wake();
    }

    /// Ready when candidates are queued; registers the driver for later wakeups.
    pub(crate) fn poll_candidates(&self, cx: &mut Context<'_>) -> Poll<()> {
        self.waker.register(cx.waker());
        let queued = !self
            .candidates
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .is_empty();
        if queued {
            Poll::Ready(())
        } else {
            Poll::Pending
        }
    }

    /// Take every queued candidate.
    fn drain(&self) -> Vec<usize> {
        std::mem::take(&mut *self.candidates.lock().unwrap_or_else(|e| e.into_inner()))
    }
}

/// Store entry identity shared by strong tokens and weak local references.
pub(crate) struct StoreEntry {
    /// Numeric store key for the value.
    key: usize,
    /// Live [`ShardRcId`] tokens; the store's own reference is not counted.
    tokens: AtomicUsize,
    /// Collection queue of the owning store.
    signal: Arc<CollectSignal>,
}

/// Counted token retaining a store entry while local or remote strong handles exist.
pub(crate) struct ShardRcId {
    /// Shared entry identity; counted in `tokens` for as long as this token lives.
    entry: Arc<StoreEntry>,
}

impl ShardRcId {
    /// Count a new token for an entry.
    fn acquire(entry: Arc<StoreEntry>) -> Self {
        entry.tokens.fetch_add(1, Ordering::Relaxed);
        Self { entry }
    }

    /// Numeric store key shared by all strong handles for one value.
    pub(crate) fn key(&self) -> usize {
        self.entry.key
    }

    /// Reference the entry without retaining the value.
    pub(crate) fn downgrade(&self) -> Weak<StoreEntry> {
        Arc::downgrade(&self.entry)
    }

    /// Create a token if the entry has not been collected.
    pub(crate) fn upgrade(entry: &Weak<StoreEntry>) -> Option<Self> {
        entry.upgrade().map(Self::acquire)
    }
}

impl Clone for ShardRcId {
    fn clone(&self) -> Self {
        Self::acquire(self.entry.clone())
    }
}

impl Drop for ShardRcId {
    fn drop(&mut self) {
        // New tokens are only cloned from live ones or created on the owner
        // thread, so a count reaching zero is observed exactly once per release.
        if self.entry.tokens.fetch_sub(1, Ordering::AcqRel) == 1 {
            self.entry.signal.notify(self.entry.key);
        }
    }
}

impl std::fmt::Debug for ShardRcId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("ShardRcId").field(&self.key()).finish()
    }
}

/// One registered allocation.
struct Stored {
    /// Type-erased `ShardValue<T>` allocation.
    value: Rc<dyn Any>,
    /// Token-counted identity shared with strong handles.
    entry: Arc<StoreEntry>,
    /// Address of the inner `T`, the key of `ShardRcStore::addresses`.
    address: usize,
}

/// Type-erased values owned and collected exclusively on their shard thread.
pub(crate) struct ShardRcStore {
    /// Registered allocations by store key.
    values: HashMap<usize, Stored>,
    /// Store keys by the address of the stored `T`, for reference lookups.
    /// Stored allocations are pinned by their `Rc`, so addresses are unique and stable.
    addresses: HashMap<usize, usize>,
    /// Last allocated store key; keys are never reused within a store.
    last_id: usize,
    /// Queue of keys released by strong tokens on any thread.
    signal: Arc<CollectSignal>,
    /// Token-free keys still borrowed by in-flight callbacks.
    orphans: Vec<usize>,
}

impl ShardRcStore {
    /// Create an empty owner-thread value store.
    pub(crate) fn new() -> Self {
        Self {
            values: HashMap::new(),
            addresses: HashMap::new(),
            last_id: 0,
            signal: Arc::new(CollectSignal {
                candidates: Mutex::new(Vec::new()),
                waker: AtomicWaker::new(),
                closed: AtomicBool::new(false),
            }),
            orphans: Vec::new(),
        }
    }

    /// Collection queue the driver waits on.
    pub(crate) fn signal(&self) -> Arc<CollectSignal> {
        self.signal.clone()
    }

    /// Whether a completed callback may have released an orphaned value.
    pub(crate) fn has_orphans(&self) -> bool {
        !self.orphans.is_empty()
    }

    /// Register an allocation and return its strong-handle lifetime token.
    pub(crate) fn insert<T>(&mut self, value: Rc<super::ShardValue<T>>) -> ShardRcId
    where
        T: 'static,
    {
        self.last_id += 1;
        let key = self.last_id;
        let entry = Arc::new(StoreEntry {
            key,
            tokens: AtomicUsize::new(0),
            signal: self.signal.clone(),
        });
        let token = ShardRcId::acquire(entry.clone());
        let address = std::ptr::from_ref::<T>(&value).addr();
        self.addresses.insert(address, key);
        self.values.insert(
            key,
            Stored {
                value,
                entry,
                address,
            },
        );
        token
    }

    /// Look up a local allocation; absent keys or incorrect types return None.
    pub(crate) fn get<T>(&self, id: usize) -> Option<Rc<T>>
    where
        T: 'static,
    {
        self.values
            .get(&id)
            .and_then(|stored| stored.value.clone().downcast::<T>().ok())
    }

    /// Find a stored value by identity and issue a token for its existing entry.
    pub(crate) fn find_value<T: 'static>(
        &self,
        value: &T,
    ) -> Option<(ShardRcId, Rc<super::ShardValue<T>>)> {
        let key = self.addresses.get(&std::ptr::from_ref(value).addr())?;
        let stored = self.values.get(key)?;
        let found = stored
            .value
            .clone()
            .downcast::<super::ShardValue<T>>()
            .ok()?;
        // The address map is an index; identity is still checked by pointer and type.
        std::ptr::eq::<T>(&**found, value)
            .then(|| (ShardRcId::acquire(stored.entry.clone()), found))
    }

    /// Remove released, unreferenced entries and return allocations for
    /// dropping outside the borrow. Only queued candidates and orphans are checked.
    pub(crate) fn take_garbage(&mut self) -> Vec<Rc<dyn Any>> {
        let mut keys = std::mem::take(&mut self.orphans);
        keys.extend(self.signal.drain());
        keys.sort_unstable();
        keys.dedup();
        let mut retired = Vec::new();
        for key in keys {
            let Some(stored) = self.values.get(&key) else {
                continue;
            };
            if stored.entry.tokens.load(Ordering::Acquire) != 0 {
                // Re-acquired; releasing the new token queues the key again.
                continue;
            }
            if Rc::strong_count(&stored.value) == 1 {
                if let Some(stored) = self.values.remove(&key) {
                    self.addresses.remove(&stored.address);
                    retired.push(stored.value);
                }
            } else {
                self.orphans.push(key);
            }
        }
        retired
    }
}

impl Default for ShardRcStore {
    fn default() -> Self {
        Self::new()
    }
}

impl Drop for ShardRcStore {
    fn drop(&mut self) {
        // Values drop after this; their releases have no store to collect from.
        self.signal.closed.store(true, Ordering::Release);
    }
}
