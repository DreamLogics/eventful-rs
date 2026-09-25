//! Expansion of the main macro family.
use proc_macro::TokenStream;
use quote::{format_ident, quote};
use syn::parse_macro_input;

/// Run an async main function on a main-thread shard and join background shards.
///
/// Pass an explicitly declared main-thread shard marker: `#[sharded_main(Main)]`.
/// The shard continues processing callbacks while the main future awaits work,
/// allowing background producers to deliver events to main-thread listeners.
/// Declare the marker with runtime = main or tokio_main.
/// The main function's return type is preserved.
pub(crate) fn sharded_main(attr: TokenStream, item: TokenStream) -> TokenStream {
    let mut item = parse_macro_input!(item as syn::ItemFn);

    if item.sig.asyncness.is_none() {
        return syn::Error::new_spanned(item.sig.fn_token, "sharded_main function must be async")
            .to_compile_error()
            .into();
    }

    if !item.sig.inputs.is_empty() || !item.sig.generics.params.is_empty() {
        return syn::Error::new_spanned(&item.sig, "sharded_main takes no arguments or generics")
            .to_compile_error()
            .into();
    }
    let shard = parse_macro_input!(attr as syn::Path);
    let mut sig_orig = item.sig.clone();
    sig_orig.asyncness = None;

    let new_ident = format_ident!("{}_sharded_main", item.sig.ident);
    item.sig.ident = new_ident.clone();

    quote! {
        #item

        #sig_orig {
            let result = #shard::shard().run_main(#new_ident);
            if let Err(e) = ::eventful_rs::join_all_shards() {
                panic!("failed to join background shards: {}", e);
            }
            result
        }
    }
    .into()
}
