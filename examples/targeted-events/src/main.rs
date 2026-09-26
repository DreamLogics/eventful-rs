//! Route business notifications to topic subscribers across shards.
//!
//! An operations dashboard subscribes to orders and shipping; billing has its own listener. The library
//! checks that rule before queuing a callback; receivers never filter payloads.
eventful_rs::declare_shard!(pub Main, runtime = main);
use eventful_rs::*;

/// Business areas used for routing subscriptions.
#[derive(Debug, Clone, Copy)]
enum Topic {
    /// Order lifecycle notifications.
    Orders,
    /// Billing notifications.
    Billing,
    /// Shipment notifications.
    Shipping,
}

/// One routing label can describe several topics without multi-label API support.
struct Topics(u8);

impl Topics {
    /// Subscribe to order events.
    const ORDERS: Self = Self(1);
    /// Subscribe to billing events.
    const BILLING: Self = Self(2);
    /// Subscribe to shipment events.
    const SHIPPING: Self = Self(4);

    /// Combine business topics into a routing mask.
    fn from_topics(topics: &[Topic]) -> Self {
        Self(topics.iter().fold(0, |mask, topic| {
            mask | match topic {
                Topic::Orders => Self::ORDERS.0,
                Topic::Billing => Self::BILLING.0,
                Topic::Shipping => Self::SHIPPING.0,
            }
        }))
    }
}

impl EventLabel for Topics {
    /// Accept any overlap. Applications could instead require every subscribed bit.
    fn matches(&self, emitted: &Self) -> bool {
        self.0 & emitted.0 != 0
    }
}

/// Business update routed to interested subscribers.
#[derive(Debug, Clone)]
struct Notification {
    /// Business message displayed by matching subscribers.
    message: String,
    /// Business topics attached to the notification.
    topics: Vec<Topic>,
}

/// Listener interface for published business notifications.
#[events]
trait NotificationEvents {
    /// Receive a notification selected by the routing layer.
    #[with_label(Topics)]
    fn on_publish(&self, notification: Notification);
}

#[eventful(NotificationEvents, shard = Main)]
struct NotificationBus;

impl NotificationBus {
    /// The label is supplied at emission and is not part of the handler signature.
    async fn publish(&self, topics: Vec<Topic>, message: String) -> Result<(), DeliveryError> {
        let label = Topics::from_topics(&topics);
        self.emit_on_publish_tracked(label, Notification { topics, message })
            .await
    }
}

/// Subscribers with state confined to a background shard.
#[scope(shard = NotificationShard)]
mod notifications {
    use std::cell::RefCell;

    use super::*;
    declare_shard!(pub NotificationShard, runtime = std);

    /// Collect notifications accepted by an external subscription.
    #[eventful]
    pub struct Subscriber {
        /// Label printed when this subscriber receives an update.
        name: &'static str,
        /// Notifications delivered to this subscriber.
        received_notifications: RefCell<Vec<Notification>>,
    }

    #[asynchronize(pub)]
    impl Subscriber {
        /// Create a subscriber with an empty notification history.
        pub async fn new(name: &'static str) -> Result<ShardRcHandle<Self>, InvokeError> {
            Self::spawn(move || Self {
                name,
                received_notifications: RefCell::new(Vec::new()),
                events: Default::default(),
            })
            .await
        }

        /// Read how many notifications reached this subscriber.
        #[asynced]
        pub fn total_received(&self) -> usize {
            self.received_notifications.borrow().len()
        }
    }

    impl NotificationEvents for Subscriber {
        fn on_publish(&self, notification: Notification) {
            // No topic check: only matching emissions reach this handler.
            println!(
                "{} received {:?}: {}",
                self.name, notification.topics, notification.message
            );
            self.received_notifications.borrow_mut().push(notification);
        }
    }
}

/// Run the scenario and verify its observable results before shutting down.
#[sharded_main(Main)]
async fn main() -> Result<(), InvokeError> {
    use notifications::*;
    let orders = Subscriber::new("Orders").await?;
    let billing = Subscriber::new("Billing").await?;
    let shipping = Subscriber::new("Shipping").await?;
    let orders_or_shipping = Subscriber::new("Orders or shipping").await?;
    let observer = Subscriber::new("All notifications").await?;
    let bus: ShardRc<NotificationBus> = ShardRc::try_bind(NotificationBus {
        events: Default::default(),
    })?;

    bus.on_publish().connect_labelled(&orders, Topics::ORDERS);
    bus.on_publish().connect_labelled(&billing, Topics::BILLING);
    bus.on_publish()
        .connect_labelled(&shipping, Topics::SHIPPING);
    bus.on_publish().connect_labelled(
        &orders_or_shipping,
        Topics(Topics::ORDERS.0 | Topics::SHIPPING.0),
    );
    // An ordinary connection is a wildcard, including for an empty topic mask.
    bus.on_publish().connect(&observer);

    // Deterministic cases show overlap, a nonmatch, and an empty label. An orders/shipping
    // notification reaches the combined subscriber once, even though both bits match.
    for (topics, message) in [
        (vec![Topic::Orders], "Order 1042 accepted"),
        (vec![Topic::Billing], "Invoice 1042 paid"),
        (vec![Topic::Shipping], "Parcel 1042 dispatched"),
        (vec![Topic::Orders, Topic::Shipping], "Order 1042 delivered"),
        (
            vec![Topic::Orders, Topic::Billing, Topic::Shipping],
            "Order 1042 closed",
        ),
        (vec![], "System heartbeat"),
    ] {
        // Tracking waits for every selected receiver before continuing.
        bus.publish(topics, message.into()).await?;
    }

    let totals = (
        orders.total_received().await,
        billing.total_received().await,
        shipping.total_received().await,
        orders_or_shipping.total_received().await,
        observer.total_received().await,
    );
    assert_eq!(totals, (3, 2, 3, 4, 6));
    println!(
        "Totals: Orders: {}, Billing: {}, Shipping: {}, Orders or shipping: {}, All: {}",
        totals.0, totals.1, totals.2, totals.3, totals.4,
    );
    Ok(())
}
