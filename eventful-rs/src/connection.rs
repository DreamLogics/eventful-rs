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
