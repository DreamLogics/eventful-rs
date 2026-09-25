//! Individual and grouped subscription lifetimes.
use std::sync::Arc;

use crate::event::EventInternal;

/// A Connection instance represents a connection between an event
/// and a listener. It can be used to disconnect the listener from the event.
/// Dropping a Connection instance will not automatically disconnect the listener,
/// you may use the `scoped` method to create a ScopedConnection if this is desired.
/// `Label` is the signal routing type; it defaults to `()` for unlabelled events.
pub struct Connection<Args, Label = ()> {
    /// Stable key identifying this entry within its owner storage.
    id: usize,
    /// Shared subscription storage for a signal signature and label type.
    event: Arc<EventInternal<Args, Label>>,
}

impl<Args, Label> Connection<Args, Label> {
    /// Create a token for an already registered subscription.
    pub(crate) fn new(id: usize, event: Arc<EventInternal<Args, Label>>) -> Self {
        Self { id, event }
    }

    /// Remove this subscription from future emission snapshots.
    /// Deliveries already captured by an emission may still run.
    pub fn disconnect(self) {
        self.event.disconnect(self.id);
    }

    /// Creates a ScopedConnection instance that will automatically disconnect
    /// the listener from the event when it is dropped.
    pub fn scoped(self) -> ScopedConnection<Args, Label> {
        ScopedConnection::new(self.id, self.event.clone())
    }
}

/// A ScopedConnection instance represents a connection between an event
/// and a listener. It can be used to disconnect the listener from the event.
/// Dropping a ScopedConnection instance will automatically disconnect
/// the listener from the event.
pub struct ScopedConnection<Args, Label = ()> {
    /// Stable key identifying this entry within its owner storage.
    id: usize,
    /// Shared subscription storage for a signal signature and label type.
    event: Arc<EventInternal<Args, Label>>,
}

impl<Args, Label> ScopedConnection<Args, Label> {
    /// Create a token for an already registered subscription.
    pub(crate) fn new(id: usize, event: Arc<EventInternal<Args, Label>>) -> Self {
        Self { id, event }
    }
}

impl<Args, Label> Drop for ScopedConnection<Args, Label> {
    fn drop(&mut self) {
        self.event.disconnect(self.id);
    }
}

/// Connections for all signals of one event interface.
/// Dropping the group keeps subscriptions active. Call disconnect() or use
/// scoped() to disconnect every subscription. Groups created by ShardRc::connect
/// also disconnect when their owning value is destroyed. Targets remain weak.
#[derive(Clone, Default)]
pub struct ConnectionGroup {
    /// Type-erased removers for the individual signal subscriptions.
    disconnectors: Vec<Arc<dyn Fn() + Send + Sync>>,
}

impl ConnectionGroup {
    /// Add an individual connection to the group.
    pub fn push<Args: 'static, Label: Send + Sync + 'static>(
        &mut self,
        connection: Connection<Args, Label>,
    ) {
        self.disconnectors.push(Arc::new(move || {
            connection.event.disconnect(connection.id);
        }));
    }

    /// Disconnect every subscription. Already queued deliveries may still run.
    /// Removal is per signal, not an atomic operation across the event interface.
    pub fn disconnect(self) {
        self.disconnect_all();
    }

    /// Remove each subscription independently; queued snapshots remain valid.
    fn disconnect_all(&self) {
        for disconnect in &self.disconnectors {
            disconnect();
        }
    }

    /// Disconnect the whole group when the returned guard is dropped.
    pub fn scoped(self) -> ScopedConnectionGroup {
        ScopedConnectionGroup(self)
    }
}

/// A group that disconnects on drop. Dropping any clone disconnects the group,
/// matching ScopedConnection semantics.
#[derive(Clone)]
pub struct ScopedConnectionGroup(ConnectionGroup);

impl Drop for ScopedConnectionGroup {
    fn drop(&mut self) {
        self.0.disconnect_all();
    }
}

/// Implemented by generated event sets for compatible listener types.
pub trait ConnectEvents<T>
where
    T: crate::Eventful + crate::HasEvents<T::EventSetType> + 'static,
{
    /// Subscribe the target to every signal; the target is held weakly.
    fn connect_events<S: crate::Sharded<T>>(&self, target: &S) -> ConnectionGroup;
}

impl<Args, Label> Clone for Connection<Args, Label> {
    fn clone(&self) -> Self {
        Self {
            id: self.id,
            event: self.event.clone(),
        }
    }
}

impl<Args, Label> Clone for ScopedConnection<Args, Label> {
    fn clone(&self) -> Self {
        Self {
            id: self.id,
            event: self.event.clone(),
        }
    }
}
