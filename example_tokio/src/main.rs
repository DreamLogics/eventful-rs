use eventful_rs::*;

use crate::web::*;
mod web;

shard_main!(MAIN_SHARD);

#[eventful]
struct App {
    web_client: ShardRcHandle<web::WebClient>,
}

#[asynchronize]
impl App {
    fn new() -> ShardRcHandle<Self> {
        let web_client = web::WebClient::new();
        let wch = web_client.clone();
        let app: ShardRcHandle<Self> = Self {
            web_client,
            events: Default::default(),
        }
        .into();

        wch.on_response().connect(&app);

        app
    }

    #[action]
    fn run(&self) {
        let url = "https://www.rust-lang.org";
        self.web_client.fetch(url);
        // self.web_client.
    }

    #[asynced]
    async fn what_did_we_do(&self) {
        if let Some(url) = self.web_client.last_url().await {
            println!("Last fetched URL: {}", url);
        } else {
            println!("No URL fetched yet.");
        }
    }
}

impl WebClientEvents for App {
    fn on_response(&self, content: String) {
        println!("Received response: {}", content);
        MAIN_SHARD.exit();
    }
}

fn main() {
    MAIN_SHARD.spawn(async {
        let app = App::new();
        app.run();
        app.what_did_we_do().await;
    });

    MAIN_SHARD.run_event_loop();
    TOKIO_WEB.join();
}
