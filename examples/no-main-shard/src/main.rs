//! Monitor a device from a synchronous application using two background shards.
use std::thread;

use eventful_rs::events;
use eventful_rs::{EventLoop, ShardHandle, Sharded, declare_shard, eventful};

use crate::monitor::MonitorShard;

declare_shard!(pub DeviceShard, runtime = std);

/// Updates emitted by a background telemetry source.
#[events]
trait TelemetryEvents {
    /// Announce a connected device.
    fn connected(&self, name: String);
    /// Report the latest device coordinates.
    fn on_position(&self, x: f32, y: f32);
}

/// Device state owned by the telemetry worker.
#[eventful(TelemetryEvents, shard = DeviceShard)]
struct Device {
    /// Human-readable device name.
    name: String,
}

impl Device {
    /// Create the device state and its event storage.
    fn new(name: String) -> Self {
        Self {
            name,
            events: Default::default(),
        }
    }

    /// Publish a sample device status and position.
    fn publish_status(&self) {
        self.emit_connected(format!("Device online: {}", self.name));
        self.emit_on_position(12.0, 34.0);
    }
}

/// Display-side listener living on its own background shard.
mod monitor {

    use super::*;
    declare_shard!(pub MonitorShard, runtime = std);

    /// Display device updates on the monitor shard.
    #[eventful(shard = MonitorShard)]
    pub struct Monitor;

    impl Monitor {
        /// Create a listener for device updates.
        pub fn new() -> Self {
            Monitor {
                events: Default::default(),
            }
        }
    }

    impl TelemetryEvents for Monitor {
        fn connected(&self, name: String) {
            println!("Monitor received on {:?}: {name}", thread::current().name());
        }

        fn on_position(&self, x: f32, y: f32) {
            println!(
                "Monitor moved to ({x}, {y}) on {:?}",
                thread::current().name()
            );
        }
    }
}

/// Publish three samples and stop the producer before draining its listener.
fn main() -> Result<(), eventful_rs::ShardError> {
    let source = DeviceShard::shard()
        .bind(|sharded| sharded(Device::new("Warehouse scanner".to_owned())).to_handle());
    let monitor =
        MonitorShard::shard().bind(|sharded| sharded(monitor::Monitor::new()).to_handle());

    // Subscribe the listener to every event in TelemetryEvents.
    source.connect(&monitor);

    source.upgrade_in_shard(|source| {
        for _ in 0..3 {
            source.publish_status();
        }
    });

    DeviceShard::shard().join()?;
    monitor::MonitorShard::shard().join()?;
    Ok(())
}
