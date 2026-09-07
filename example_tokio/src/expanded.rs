use eventful_rs::*;

mod web {
    use eventful_rs::*;
    use reqwest::IntoUrl;
    pub static TOKIO_WEB: ::std::sync::LazyLock<::eventful_rs::tokio::TokioShard> =
        ::std::sync::LazyLock::new(|| ::eventful_rs::tokio::TokioShard::new());
    fn default_shard() -> &'static ::eventful_rs::tokio::TokioShard {
        &TOKIO_WEB
    }
    pub trait WebClientEvents: Send + Sync + 'static {
        fn on_response(&self, content: String);
    }
    pub struct WebClientEventsOnResponseSignal {
        inner: ::eventful_rs::Event<(String,)>,
    }
    impl ::core::default::Default for WebClientEventsOnResponseSignal {
        fn default() -> Self {
            Self {
                inner: ::eventful_rs::Event::default(),
            }
        }
    }
    impl WebClientEventsOnResponseSignal {
        pub fn connect<T>(&self, target: &::eventful_rs::Erc<T>)
        where
            T: WebClientEvents + ::eventful_rs::EventTarget,
            String: Clone + Send + 'static,
        {
            let weak = target.weak();
            let event_loop = target.event_loop();
            self.inner.add_connection(move |(content,)| {
                let weak = weak.clone();
                let event_loop = event_loop.clone();
                let _ = event_loop.invoke(move || {
                    if let Some(target) = weak.upgrade() {
                        target.on_response(content);
                    }
                });
            });
        }
        pub fn emit(&self, content: String)
        where
            String: Clone + Send + 'static,
        {
            self.inner.emit((content,));
        }
    }
    pub struct WebClientEventsEventSet {
        pub on_response: WebClientEventsOnResponseSignal,
    }
    #[automatically_derived]
    impl ::core::default::Default for WebClientEventsEventSet {
        #[inline]
        fn default() -> WebClientEventsEventSet {
            WebClientEventsEventSet {
                on_response: ::core::default::Default::default(),
            }
        }
    }
    pub trait HasWebClientEventsEvents {
        fn web_client_events(&self) -> &WebClientEventsEventSet;
    }
    pub trait WebClientEventsSignalsExt: HasWebClientEventsEvents {
        fn on_response(&self) -> &WebClientEventsOnResponseSignal {
            &self.web_client_events().on_response
        }
    }
    trait WebClientEventsEmittersExt: HasWebClientEventsEvents {
        fn emit_on_response(&self, content: String)
        where
            String: Clone + Send + 'static,
        {
            self.web_client_events().on_response.emit(content);
        }
    }
    impl<T> WebClientEventsSignalsExt for ::eventful_rs::Erc<T>
    where
        T: ::eventful_rs::EventTarget + ?Sized,
        ::eventful_rs::Erc<T>: HasWebClientEventsEvents,
    {
    }
    impl<T: HasWebClientEventsEvents + ?Sized> WebClientEventsEmittersExt for T {}
    pub struct WebClient {
        client: reqwest::Client,
        web_client_events: WebClientEventsEventSet,
    }
    impl ::eventful_rs::EventTarget for WebClient {
        fn event_loop(&self) -> impl ::eventful_rs::EventLoopHandle {
            default_shard().handle()
        }
    }
    impl HasWebClientEventsEvents for WebClient {
        fn web_client_events(&self) -> &WebClientEventsEventSet {
            &self.web_client_events
        }
    }
    impl HasWebClientEventsEvents for ::eventful_rs::Erc<WebClient> {
        fn web_client_events(&self) -> &WebClientEventsEventSet {
            &self.get().web_client_events
        }
    }
    impl WebClient {
        pub fn new() -> Erc<Self> {
            default_shard().bind(WebClient {
                client: reqwest::Client::new(),
                web_client_events: Default::default(),
            })
        }
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
    pub trait WebClientActions {
        fn fetch<T>(&self, url: T)
        where
            T: IntoUrl + Send + 'static;
    }
    impl WebClientActions for ::eventful_rs::Erc<WebClient> {
        fn fetch<T>(&self, url: T)
        where
            T: IntoUrl + Send + 'static,
        {
            let weak = self.weak().clone();
            self.event_loop().invoke_async(async move || {
                if let Some(target) = weak.upgrade() {
                    target.fetch(url).await;
                }
            });
        }
    }
}
pub static MAIN_SHARD: ::std::sync::LazyLock<::eventful_rs::shard::Shard> =
    ::std::sync::LazyLock::new(|| ::eventful_rs::shard::Shard::new("$name"));
fn default_shard() -> &'static ::eventful_rs::shard::Shard {
    &MAIN_SHARD
}
struct App {
    web_client: Erc<web::WebClient>,
}
impl ::eventful_rs::EventTarget for App {
    fn event_loop(&self) -> impl ::eventful_rs::EventLoopHandle {
        default_shard().handle()
    }
}
impl App {
    fn new() -> Erc<Self> {
        let web_client = web::WebClient::new();
        let wch = web_client.clone();
        let app = default_shard().bind(App { web_client });
        wch.on_response().connect(&app);
        app
    }
    fn run(&self) {
        let url = "https://www.rust-lang.org";
        self.web_client.fetch(url);
    }
}
pub trait AppActions {
    fn run(&self);
}
impl AppActions for ::eventful_rs::Erc<App> {
    fn run(&self) {
        let weak = self.weak().clone();
        self.event_loop().invoke(move || {
            if let Some(target) = weak.upgrade() {
                target.run();
            }
        });
    }
}
impl web::WebClientEvents for App {
    fn on_response(&self, content: String) {
        {
            println!("Received response: {}", content);
        };
    }
}
fn main() {
    let app = App::new();
    app.run();
    TOKIO_WEB.join();
    MAIN_SHARD.join();
}
