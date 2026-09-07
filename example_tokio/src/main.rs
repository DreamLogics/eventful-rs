use eventful_rs::{Erc, EventLoop, eventful, shard_std};

use crate::web::*;

mod web;

shard_std!(MAIN_SHARD);

#[eventful(MAIN_SHARD)]
struct App {
    web_client: Erc<web::WebClient>,
}

impl App {
    fn new() -> Erc<Self> {
        let web_client = web::WebClient::new();
        let wch = web_client.clone();
        let app = MAIN_SHARD.bind(App { web_client });

        wch.on_response().connect(&app);

        app
    }

    fn run(&self) {
        let url = "https://www.rust-lang.org";
        self.web_client.fetch(url);
    }
}

impl WebClientEvents for App {
    fn on_response(&self, content: String) {
        println!("Received response: {}", content);
    }
}

fn main() {
    let app = App::new();
}
