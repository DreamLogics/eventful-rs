use std::cell::RefCell;

use eventful_rs::*;
use reqwest::IntoUrl;

shard_tokio!(TOKIO_WEB);

#[events]
pub trait WebClientEvents {
    fn on_response(&self, content: String);
}

// #[actions]
// pub trait WebClientActions {
//     fn fetch<T>(&self, url: T)
//     where
//         T: IntoUrl + Send + 'static;
// }

#[eventful(WebClientEvents)]
pub struct WebClient {
    client: reqwest::Client,
    last_url: RefCell<Option<String>>,
}

#[asynchronize]
impl WebClient {
    pub fn new() -> ShardRcHandle<Self> {
        TOKIO_WEB.spawn(|sharded| {
            let client = reqwest::Client::new();
            let last_url = RefCell::new(None);
            sharded(WebClient {
                client,
                last_url,
                events: Default::default(),
            })
            .as_handle()
        })
    }

    #[action]
    pub async fn fetch<T>(&self, url: T)
    where
        T: IntoUrl + Send + 'static,
    {
        println!("fetch called");
        let response = self.client.get(url).send().await;
        match response {
            Ok(resp) => {
                self.last_url.replace(resp.url().to_string().into());
                if let Ok(text) = resp.text().await {
                    println!("received text, emitting...");
                    self.emit_on_response(text);
                }
            }
            Err(e) => {
                eprintln!("Error fetching URL: {}", e);
            }
        }
    }

    #[asynced]
    pub fn last_url(&self) -> Option<String> {
        self.last_url.borrow().clone()
    }
}
