use eventful_rs::*;

use crate::web::*;

mod expanded;
mod web;

shard_std!(MAIN_SHARD);

#[eventful]
struct App {
    web_client: Erc<web::WebClient>,
}

#[with_actions]
impl App {
    fn new() -> Erc<Self> {
        let web_client = web::WebClient::new();
        let wch = web_client.clone();
        let app = erc!(App { web_client });

        wch.on_response().connect(&app);

        app
    }

    #[action]
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
    app.run();

    TOKIO_WEB.join();
    MAIN_SHARD.join();
}
