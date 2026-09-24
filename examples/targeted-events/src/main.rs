use eventful_rs::*;
use rand::prelude::*;

#[derive(Debug, Clone, Copy, PartialEq)]
enum Color {
    Red,
    Green,
    Blue,
}

#[derive(Debug, Clone)]
struct Marble {
    pub colors: Vec<Color>,
}

trait ColoredListener {
    fn my_color(&self) -> Color;
}

#[events(ColoredListener)]
trait MarbleCreatorEvents {
    fn on_create_marble(&self, marble: Marble);
}

#[eventful(MarbleCreatorEvents)]
struct MarbleCreator;

impl MarbleCreator {
    pub fn new() -> ShardRc<Self> {
        Self {
            events: Default::default(),
        }
        .into()
    }

    pub fn create_marble(&self) {
        // Create a marble with a random number of colors
        let mut rng = rand::rng();
        let r: u8 = rng.random();
        let nr_of_colors = match r {
            0..100 => 1,
            100..160 => 2,
            160..200 => 3,
            200..220 => 4,
            220..230 => 5,
            _ => 6,
        };

        let mut colors: Vec<Color> = Vec::new();
        for _ in 0..nr_of_colors {
            let color = match rng.random::<u8>() % 3 {
                0 => Color::Red,
                1 => Color::Green,
                _ => Color::Blue,
            };
            colors.push(color);
        }

        let marble = Marble { colors };
        self.emit_on_create_marble(marble);
    }
}

mod marbles {
    use std::cell::RefCell;

    use super::*;
    shard_std!(MARBLE_SHARD);

    #[eventful]
    pub struct MarbleReceiver {
        color: Color,
        received_marbles: RefCell<Vec<Marble>>,
    }

    #[asynchronize]
    impl MarbleReceiver {
        pub fn new(color: Color) -> ShardRcHandle<Self> {
            Self {
                color,
                received_marbles: RefCell::new(Vec::new()),
                events: Default::default(),
            }
            .into()
        }

        #[asynced]
        pub fn total_received(&self) -> usize {
            self.received_marbles.borrow().len()
        }
    }

    impl MarbleCreatorEvents for MarbleReceiver {
        fn on_create_marble(&self, marble: Marble) {
            if marble.colors.contains(&self.color) {
                println!(
                    "Receiver {:?} received marble with colors: {:?}",
                    self.color, marble.colors
                );
                self.received_marbles.borrow_mut().push(marble);
            }
        }
    }

    impl ColoredListener for MarbleReceiver {
        fn my_color(&self) -> Color {
            self.color
        }
    }
}

#[sharded_main]
async fn main() {
    use marbles::*;
    let red_receiver = MarbleReceiver::new(Color::Red);
    let green_receiver = MarbleReceiver::new(Color::Green);
    let blue_receiver = MarbleReceiver::new(Color::Blue);

    let creator = MarbleCreator::new();

    creator
        .on_create_marble()
        .connect_filtered(&red_receiver, |t| t.my_color() == Color::Red);
    creator
        .on_create_marble()
        .connect_filtered(&green_receiver, |t| t.my_color() == Color::Green);
    creator
        .on_create_marble()
        .connect_filtered(&blue_receiver, |t| t.my_color() == Color::Blue);

    for _ in 0..100 {
        creator.create_marble();
    }

    let total_received_red = red_receiver.total_received().await;
    let total_received_green = green_receiver.total_received().await;
    let total_received_blue = blue_receiver.total_received().await;

    println!(
        "Total received marbles: Red: {}, Green: {}, Blue: {}",
        total_received_red, total_received_green, total_received_blue
    );
}
