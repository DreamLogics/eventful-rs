use crate::web::*;
use eventful_rs::*;
use std::io::{Read, Write};
/// Tokio-backed HTTP client and its typed response signal.
mod web;
declare_shard!(pub Main, runtime = main);

/// Main-thread listener coordinating HTTP work.
#[eventful(shard = Main)]
struct App {
    /// Strong handle retaining the background HTTP client.
    web_client: ShardRcHandle<WebClient>,
}
#[asynchronize]
impl App {
    /// Construct this example value and initialize its event storage.
    async fn new() -> Result<ShardRcHandle<Self>, InvokeError> {
        let web_client = WebClient::new().await?;
        let client = web_client.clone();
        let app = Self::spawn(move || Self {
            web_client: client,
            events: Default::default(),
        })
        .await?;
        web_client.on_response().connect(&app);
        Ok(app)
    }
    /// Request a download and wait for its response event.
    #[asynced]
    async fn run(&self, url: String) -> Result<(), String> {
        self.web_client.fetch(url).await
    }
    /// Read the last successfully downloaded URL.
    #[asynced]
    async fn last_url(&self) -> Option<String> {
        self.web_client.last_url().await
    }
}
impl WebClientEvents for App {
    fn on_response(&self, content: String) {
        println!("Received response: {content}");
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // A real HTTP request, served locally: deterministic and usable offline.
    let listener = std::net::TcpListener::bind("127.0.0.1:0")?;
    let url = format!("http://{}/", listener.local_addr()?);
    let server = std::thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        stream
            .set_read_timeout(Some(std::time::Duration::from_secs(5)))
            .unwrap();
        let mut request = [0; 4096];
        let _ = stream.read(&mut request).unwrap();
        let body = "hello from the local server";
        write!(
            stream,
            "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
            body.len()
        )
        .unwrap();
    });
    Main::shard().run_main(move || async move {
        let app = App::new().await.unwrap();
        app.run(url.clone()).await.unwrap();
        assert_eq!(app.last_url().await, Some(url));
        println!("HTTP example complete");
    });
    WebShard::shard().join()?;
    server.join().unwrap();
    Ok(())
}
