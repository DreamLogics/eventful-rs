//! Owner-thread value storage and strong-handle lifetime tokens.
use std::{any::Any, collections::HashMap, rc::Rc, sync::Arc};

/// Reference-counted token retaining a store entry while remote handles exist.
#[derive(Debug, Clone)]
pub(crate) struct ShardRcId {
    /// Numeric store key shared by all strong handles for one value.
    pub(crate) id: Arc<usize>,
}

/// Type-erased values owned and collected exclusively on their shard thread.
pub(crate) struct ShardRcStore {
    /// Registered allocations paired with their strong-handle lifetime tokens.
    values: HashMap<usize, (Rc<dyn Any>, ShardRcId)>,
    /// Last allocated store key; keys are never reused within a store.
    last_id: usize,
}

impl ShardRcStore {
    /// Create an empty owner-thread value store.
    pub(crate) fn new() -> Self {
        Self {
            values: HashMap::new(),
            last_id: 0,
        }
    }

    /// Register an allocation and return its strong-handle lifetime token.
    pub(crate) fn insert<T>(&mut self, value: Rc<T>) -> ShardRcId
    where
        T: 'static,
    {
        self.last_id += 1;
        let id = self.last_id;
        let sid = ShardRcId { id: Arc::new(id) };
        self.values.insert(id, (value, sid.clone()));
        sid
    }

    /// Look up a local allocation; absent keys or incorrect types return None.
    pub(crate) fn get<T>(&self, id: usize) -> Option<Rc<T>>
    where
        T: 'static,
    {
        self.values
            .get(&id)
            .and_then(|(value, _)| value.clone().downcast::<T>().ok())
    }

    /// Remove unreferenced entries and return allocations for dropping outside the borrow.
    pub(crate) fn take_garbage(&mut self) -> Vec<Rc<dyn Any>> {
        let ids_to_remove: Vec<usize> = self
            .values
            .iter()
            .filter_map(|(&id, (value, sid))| {
                if Arc::strong_count(&sid.id) == 1 && Rc::strong_count(value) == 1 {
                    Some(id)
                } else {
                    None
                }
            })
            .collect();

        ids_to_remove
            .into_iter()
            .filter_map(|id| self.values.remove(&id).map(|(value, _)| value))
            .collect()
    }
}

impl Default for ShardRcStore {
    fn default() -> Self {
        Self::new()
    }
}
