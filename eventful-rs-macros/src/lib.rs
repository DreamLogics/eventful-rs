#![doc = include_str!("../README.md")]
#![forbid(unsafe_code)]
#![deny(missing_docs)]

use proc_macro::TokenStream;
use quote::format_ident;
mod affinity;
mod attributes;
mod declaration;
mod dispatch;
mod entry;
mod events;

/// Define a typed event interface with synchronous `&self` methods.
/// Generated signal-access and emitter extension traits inherit the interface
/// visibility, so private interfaces can use private payload types.
///
/// Concrete argument types such as `Vec<String>` and `Option<Vec<String>>` are
/// supported. Traits and methods cannot declare generic parameters (`<T>`).
///
/// Attach it to a source with `#[eventful(Interface)]` and implement it on
/// listeners. `source.event_name().connect(&listener)` dispatches callbacks on
/// each listener's shard. Ordinary emissions do not wait for listeners; tracked
/// emissions return a future that observes completion and delivery errors.
///
/// Mark individual methods with `#[with_label(LabelType)]` to enable routing.
/// The type must implement `eventful_rs::EventLabel`. Generated emitters take
/// the label first, followed by the declared arguments; handler signatures stay
/// unchanged. `signal.connect_labelled(&listener, subscription)` delivers only
/// when `subscription.matches(&emitted)` returns true. Ordinary `connect` and
/// bulk connections subscribe to every label. Matching occurs on the emitting
/// thread before cloning arguments or submitting work to the receiver's shard.
#[proc_macro_attribute]
pub fn events(attr: TokenStream, item: TokenStream) -> TokenStream {
    events::events(attr, item)
}

/// Generate handle wrappers for annotated methods in an inherent `impl`.
///
/// The generated extension trait is private by default. Pass `pub`, `pub(crate)`,
/// or another Rust visibility to export it: `#[asynchronize(pub)]`.
///
/// Mark methods with `#[asynced]` to await their results, or `#[action]` to queue
/// work from synchronous or async code without waiting. Original methods
/// keep their signatures. Unannotated methods remain accessible through references to shard-local
/// values, including inside `upgrade_in_shard` callbacks.
/// Dispatched methods require `&self` and `Send + 'static` arguments and results.
#[proc_macro_attribute]
pub fn asynchronize(_attr: TokenStream, item: TokenStream) -> TokenStream {
    dispatch::asynchronize(_attr, item)
}

/// Select a shard for eventful types in an inline module, including nested inline
/// modules. Paths are relative to the annotated module (e.g. super::Worker).
/// Explicit eventful selections and nested scopes override the inherited choice.
/// Out-of-line modules must select their own shard in their source file.
#[proc_macro_attribute]
pub fn scope(attr: TokenStream, item: TokenStream) -> TokenStream {
    affinity::scope(attr, item)
}

/// Add an `events` field and trait implementations needed for shard binding.
///
/// Pass an event interface, as in `#[eventful(ProducerEvents)]`, to let instances
/// of the annotated struct emit those events. Select its shard with `shard = Marker`
/// or an enclosing `#[scope(shard = Marker)]` attribute. Otherwise, the current
/// module must declare `file_scope!(shard = Marker);`.
/// Initialize the generated field with `events: Default::default()`.
#[proc_macro_attribute]
pub fn eventful(attr: TokenStream, item: TokenStream) -> TokenStream {
    affinity::eventful(attr, item)
}

/// Mark a method in an `#[asynchronize]` impl for fire-and-forget dispatch.
///
/// The method must return `()`, and can be synchronous or async. Its handle
/// wrapper is a normal function: it queues work and returns immediately, so no
/// async caller or `.await` is needed. The shard awaits an async original method;
/// returning from the handle call does not signal completion.
#[proc_macro_attribute]
pub fn action(_attr: TokenStream, item: TokenStream) -> TokenStream {
    item
}

/// Mark a method in an `#[asynchronize]` impl for an async handle wrapper.
///
/// The original method may be synchronous or async. Polling the handle wrapper
/// queues the call on the value's shard; awaiting it returns the method's result.
/// This does not wait for untracked events emitted by the method. Await tracked
/// emissions inside the method when listener completion is part of its work.
#[proc_macro_attribute]
pub fn asynced(_attr: TokenStream, item: TokenStream) -> TokenStream {
    item
}

/// Run an async main function on a main-thread shard and join background shards.
///
/// Pass an explicitly declared main-thread shard marker: `#[sharded_main(Main)]`.
/// The shard continues processing callbacks while the main future awaits work,
/// allowing background producers to deliver events to main-thread listeners.
/// Declare the marker with runtime = main or tokio_main.
/// The main function's return type is preserved.
#[proc_macro_attribute]
pub fn sharded_main(attr: TokenStream, item: TokenStream) -> TokenStream {
    entry::sharded_main(attr, item)
}

/// Convert snake-case method names into generated signal type suffixes.
fn pascal(value: &str) -> String {
    let value = value.strip_prefix("r#").unwrap_or(value);
    value
        .split('_')
        .filter(|s| !s.is_empty())
        .map(|part| {
            let mut chars = part.chars();
            chars
                .next()
                .map(|c| c.to_uppercase().collect::<String>() + chars.as_str())
                .unwrap_or_default()
        })
        .collect()
}

/// Choose a generated callback binding that cannot shadow an argument.
fn fresh_target(arguments: &[syn::Ident]) -> syn::Ident {
    let mut name = "__eventful_target".to_owned();
    while arguments.iter().any(|a| a == &name) {
        name.push('_');
    }
    format_ident!("{}", name)
}

/// Resolve the runtime dependency using the downstream manifest, including aliases.
fn runtime_path() -> syn::Path {
    match proc_macro_crate::crate_name("eventful-rs") {
        Ok(proc_macro_crate::FoundCrate::Name(name)) => {
            let ident = format_ident!("{}", name);
            syn::parse_quote!(::#ident)
        }
        // Integration tests and doctests use the library through its extern name.
        _ => syn::parse_quote!(::eventful_rs),
    }
}

/// Declare a named singleton: `declare_shard!(pub Worker, runtime = std);`.
///
/// Supports outer attributes and Rust visibility, including inside functions.
/// Runtimes are `std`, `main`, `tokio`, `tokio_main`, and `slint`; the last three
/// require their corresponding runtime features. Calling-thread and UI markers
/// must first be initialized on their owner thread.
/// See the [runtime guide](https://docs.rs/eventful-rs/latest/eventful_rs/#quick-start)
/// for a complete example.
#[proc_macro]
pub fn declare_shard(input: TokenStream) -> TokenStream {
    declaration::declare_shard(input)
}
