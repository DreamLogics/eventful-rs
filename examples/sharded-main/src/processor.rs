//! A worker that normalizes imported product names and reports each accepted row.
use eventful_rs::*;
use std::cell::Cell;

declare_shard!(pub ImportShard, runtime = std);
file_scope!(shard = ImportShard);

/// Progress updates for a product import.
#[events]
pub trait ImportEvents {
    /// One validated product name is ready for display.
    fn imported(&self, name: String);
}

/// Import state remains on the worker thread.
#[eventful(ImportEvents)]
pub struct Importer {
    /// Number of accepted products, retained across batches.
    count: Cell<usize>,
}

#[asynchronize]
impl Importer {
    /// Construct local state on the declared worker.
    pub async fn new() -> Result<ShardRcHandle<Self>, InvokeError> {
        Self::spawn(|| Self {
            count: Cell::new(0),
            events: Default::default(),
        })
        .await
    }

    /// Normalize a batch and await each listener before reporting success.
    /// Count mutations are retained if a later delivery fails.
    #[asynced]
    pub async fn import(&self, rows: Vec<String>) -> Result<usize, DeliveryError> {
        let mut imported = 0;
        for row in rows {
            let name = row.trim();
            if name.is_empty() {
                continue;
            }
            self.count.set(self.count.get() + 1);
            self.emit_imported_tracked(name.to_owned()).await?;
            imported += 1;
        }
        Ok(imported)
    }

    /// Queue a reset from either synchronous or async callers.
    #[action]
    pub fn reset(&self) {
        self.count.set(0);
    }

    /// Read the number of imported products.
    #[asynced]
    pub fn count(&self) -> usize {
        self.count.get()
    }

    /// Local-only reporting is accessible through handle callbacks too.
    pub fn report(&self) -> String {
        format!("{} products imported", self.count.get())
    }
}
