use std::sync::Arc;

use crate::EventInternal;

/// A Connection instance represents a connection between an event
/// and a listener. It can be used to disconnect the listener from the event.
/// Dropping a Connection instance will not automatically disconnect the listener,
/// you may use the `scoped` method to create a ScopedConnection if this is desired.
#[derive(Clone)]
pub struct Connection<Args> {
    id: usize,
    event: Arc<EventInternal<Args>>,
}

impl<Args> Connection<Args> {
    pub(crate) fn new(id: usize, event: Arc<EventInternal<Args>>) -> Self {
        Self { id, event }
    }

    /// Disconnects the listener from the event. After calling this method,
    /// the listener will no longer receive events.
    pub fn disconnect(self) {
        let mut connections = self.event.connections.lock().unwrap();
        connections.retain(|c| c.id != self.id);
    }

    /// Creates a ScopedConnection instance that will automatically disconnect
    /// the listener from the event when it is dropped.
    pub fn scoped(self) -> ScopedConnection<Args> {
        ScopedConnection::new(self.id, self.event.clone())
    }
}

/// A ScopedConnection instance represents a connection between an event
/// and a listener. It can be used to disconnect the listener from the event.
/// Dropping a ScopedConnection instance will automatically disconnect
/// the listener from the event.
#[derive(Clone)]
pub struct ScopedConnection<Args> {
    id: usize,
    event: Arc<EventInternal<Args>>,
}

impl<Args> ScopedConnection<Args> {
    pub(crate) fn new(id: usize, event: Arc<EventInternal<Args>>) -> Self {
        Self { id, event }
    }
}

impl<Args> Drop for ScopedConnection<Args> {
    fn drop(&mut self) {
        let mut connections = self.event.connections.lock().unwrap();
        connections.retain(|c| c.id != self.id);
    }
}

/// Connections for all signals of one event interface.
/// Dropping the group keeps subscriptions active. Call disconnect() or use
/// scoped() to disconnect every subscription. Groups created by ShardRc::connect
/// also disconnect when their owning value is destroyed. Targets remain weak.
#[derive(Clone, Default)]
pub struct ConnectionGroup {
    disconnectors: Vec<Arc<dyn Fn() + Send + Sync>>,
}

impl ConnectionGroup {
    /// Add an individual connection to the group.
    pub fn push<Args: 'static>(&mut self, connection: Connection<Args>) {
        self.disconnectors.push(Arc::new(move || {
            let mut connections = connection.event.connections.lock().unwrap();
            connections.retain(|c| c.id != connection.id);
        }));
    }

    /// Disconnect every subscription. Already queued deliveries may still run.
    /// Removal is per signal, not an atomic operation across the event interface.
    pub fn disconnect(self) {
        self.disconnect_all();
    }

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
    fn connect_events<S: crate::Sharded<T>>(&self, target: &S) -> ConnectionGroup;
}
