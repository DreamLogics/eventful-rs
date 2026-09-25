//! Route marbles across shards using application-defined bitmask labels.
//!
//! A subscription accepts any marble whose colors overlap its mask. The library
//! checks that rule before queuing a callback; receivers never filter payloads.
eventful_rs::declare_shard!(pub Main, runtime = main);
use eventful_rs::*;

#[derive(Debug, Clone, Copy)]
enum Color {
    Red,
    Green,
    Blue,
}

/// One routing label can describe several colors without multi-label API support.
struct ColorMask(u8);

impl ColorMask {
    const RED: Self = Self(1);
    const GREEN: Self = Self(2);
    const BLUE: Self = Self(4);

    fn from_colors(colors: &[Color]) -> Self {
        Self(colors.iter().fold(0, |mask, color| {
            mask | match color {
                Color::Red => Self::RED.0,
                Color::Green => Self::GREEN.0,
                Color::Blue => Self::BLUE.0,
            }
        }))
    }
}

impl EventLabel for ColorMask {
    /// Accept any overlap. Applications could instead require every subscribed bit.
    fn matches(&self, emitted: &Self) -> bool {
        self.0 & emitted.0 != 0
    }
}

#[derive(Debug, Clone)]
struct Marble {
    colors: Vec<Color>,
}

#[events]
trait MarbleCreatorEvents {
    #[with_label(ColorMask)]
    fn on_create_marble(&self, marble: Marble);
}

#[eventful(MarbleCreatorEvents, shard = Main)]
struct MarbleCreator;

impl MarbleCreator {
    /// The label is supplied at emission and is not part of the handler signature.
    async fn create_marble(&self, colors: Vec<Color>) -> Result<(), DeliveryError> {
        let label = ColorMask::from_colors(&colors);
        self.emit_on_create_marble_tracked(label, Marble { colors })
            .await
    }
}

#[scope(shard = MarbleShard)]
mod marbles {
    use std::cell::RefCell;

    use super::*;
    declare_shard!(pub MarbleShard, runtime = std);

    #[eventful]
    pub struct MarbleReceiver {
        name: &'static str,
        received_marbles: RefCell<Vec<Marble>>,
    }

    #[asynchronize]
    impl MarbleReceiver {
        pub async fn new(name: &'static str) -> Result<ShardRcHandle<Self>, InvokeError> {
            Self::spawn(move || Self {
                name,
                received_marbles: RefCell::new(Vec::new()),
                events: Default::default(),
            })
            .await
        }

        #[asynced]
        pub fn total_received(&self) -> usize {
            self.received_marbles.borrow().len()
        }
    }

    impl MarbleCreatorEvents for MarbleReceiver {
        fn on_create_marble(&self, marble: Marble) {
            // No color check: only matching emissions reach this handler.
            println!("{} received {:?}", self.name, marble.colors);
            self.received_marbles.borrow_mut().push(marble);
        }
    }
}

#[sharded_main(Main)]
async fn main() {
    use marbles::*;
    let red = MarbleReceiver::new("Red").await.unwrap();
    let green = MarbleReceiver::new("Green").await.unwrap();
    let blue = MarbleReceiver::new("Blue").await.unwrap();
    let red_or_blue = MarbleReceiver::new("Red or blue").await.unwrap();
    let observer = MarbleReceiver::new("All marbles").await.unwrap();
    let creator: ShardRc<MarbleCreator> = MarbleCreator {
        events: Default::default(),
    }
    .into();

    creator
        .on_create_marble()
        .connect_labelled(&red, ColorMask::RED);
    creator
        .on_create_marble()
        .connect_labelled(&green, ColorMask::GREEN);
    creator
        .on_create_marble()
        .connect_labelled(&blue, ColorMask::BLUE);
    creator.on_create_marble().connect_labelled(
        &red_or_blue,
        ColorMask(ColorMask::RED.0 | ColorMask::BLUE.0),
    );
    // An ordinary connection is a wildcard, including for an empty color mask.
    creator.on_create_marble().connect(&observer);

    // Deterministic cases show overlap, a nonmatch, and an empty label. A red/blue
    // marble reaches the combined subscriber once, even though both bits match.
    for colors in [
        vec![Color::Red],
        vec![Color::Green],
        vec![Color::Blue],
        vec![Color::Red, Color::Blue],
        vec![Color::Red, Color::Green, Color::Blue],
        vec![],
    ] {
        // Tracking waits for every selected receiver before continuing.
        creator.create_marble(colors).await.unwrap();
    }

    let totals = (
        red.total_received().await,
        green.total_received().await,
        blue.total_received().await,
        red_or_blue.total_received().await,
        observer.total_received().await,
    );
    assert_eq!(totals, (3, 2, 3, 4, 6));
    println!(
        "Totals: Red: {}, Green: {}, Blue: {}, Red or blue: {}, All: {}",
        totals.0, totals.1, totals.2, totals.3, totals.4,
    );
}
