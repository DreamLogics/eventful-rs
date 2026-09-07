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
}

#[with_actions]
impl WebClient {
    pub fn new() -> Erc<Self> {
        erc!(WebClient {
            client: reqwest::Client::new(),
            web_client_events: Default::default(),
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
}
