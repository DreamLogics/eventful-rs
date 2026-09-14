use eventful_rs::*;
use std::cell::RefCell;
shard_tokio!(TOKIO_WEB);

#[events]
pub trait WebClientEvents {
    fn on_response(&self, content: String);
}

#[eventful(WebClientEvents)]
pub struct WebClient {
    client: reqwest::Client,
    last_url: RefCell<Option<String>>,
}
#[asynchronize]
impl WebClient {
    pub fn new() -> ShardRcHandle<Self> {
        // The factory executes on TOKIO_WEB, including construction of the client.
        TOKIO_WEB.bind(|bind| {
            bind(Self {
                client: reqwest::Client::new(),
                last_url: RefCell::new(None),
                events: Default::default(),
            })
            .as_handle()
        })
    }
    #[asynced]
    pub async fn fetch(&self, url: String) -> Result<(), String> {
        let response = self
            .client
            .get(&url)
            .send()
            .await
            .map_err(|e| e.to_string())?
            .error_for_status()
            .map_err(|e| e.to_string())?;
        let content = response.text().await.map_err(|e| e.to_string())?;
        self.last_url.replace(Some(url));
        self.emit_on_response(content);
        Ok(())
    }
    #[asynced]
    pub fn last_url(&self) -> Option<String> {
        self.last_url.borrow().clone()
    }
}
