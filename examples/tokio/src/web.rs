use eventful_rs::*;
use std::cell::RefCell;
declare_shard!(pub WebShard, runtime = tokio);
file_scope!(shard = WebShard);

/// Responses available to application listeners.
#[events]
pub trait WebClientEvents {
    /// Receive the downloaded response body.
    fn on_response(&self, content: String);
}

/// Reusable HTTP client with shard-local request history.
#[eventful(WebClientEvents)]
pub struct WebClient {
    /// Connection-pooling HTTP client owned by the web shard.
    client: reqwest::Client,
    /// Last successfully downloaded URL.
    last_url: RefCell<Option<String>>,
}
#[asynchronize]
impl WebClient {
    /// Construct this example value and initialize its event storage.
    pub async fn new() -> Result<ShardRcHandle<Self>, InvokeError> {
        // The factory constructs the HTTP client on WebClient::Shard.
        Self::spawn(|| Self {
            client: reqwest::Client::new(),
            last_url: RefCell::new(None),
            events: Default::default(),
        })
        .await
    }
    /// Download a URL, record it, and await delivery to response listeners.
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
        self.emit_on_response_tracked(content)
            .await
            .map_err(|e| e.to_string())?;
        Ok(())
    }
    /// Read the last successfully downloaded URL.
    #[asynced]
    pub fn last_url(&self) -> Option<String> {
        self.last_url.borrow().clone()
    }
}
