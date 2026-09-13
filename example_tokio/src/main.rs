use eventful_rs::*;

use crate::web::*;

mod web;

shard_local!(MAIN_SHARD);

#[eventful]
struct App {
    web_client: ShardRcHandle<web::WebClient>,
}

#[asynchronize]
impl App {
    fn new() -> ShardRcHandle<Self> {
        let web_client = web::WebClient::new();
        let wch = web_client.clone();
        let app = MAIN_SHARD.spawn(move |sharded| {
            sharded(Self {
                web_client,
                events: Default::default(),
            })
            .as_handle()
        });

        wch.on_response().connect(&app);

        app
    }

    #[action]
    fn run(&self) {
        let url = "https://www.rust-lang.org";
        self.web_client.fetch(url);
        // self.web_client.
    }
}

impl WebClientEvents for App {
    fn on_response(&self, content: String) {
        println!("Received response: {}", content);
        MAIN_SHARD.exit();
    }
}

fn main() {
    let app = App::new();
    app.run();

    MAIN_SHARD.run_event_loop();
    TOKIO_WEB.join();
}
