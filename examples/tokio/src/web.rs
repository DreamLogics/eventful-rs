use eventful_rs::*;
use std::cell::RefCell;
declare_shard!(pub WebShard, runtime = tokio);

#[events]
pub trait WebClientEvents {
    fn on_response(&self, content: String);
}

#[eventful(WebClientEvents, shard = WebShard)]
pub struct WebClient {
    client: reqwest::Client,
    last_url: RefCell<Option<String>>,
}
#[asynchronize]
impl WebClient {
    pub async fn new() -> Result<ShardRcHandle<Self>, InvokeError> {
        // The factory constructs the HTTP client on WebClient::Shard.
        Self::spawn(|| Self {
            client: reqwest::Client::new(),
            last_url: RefCell::new(None),
            events: Default::default(),
        })
        .await
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
