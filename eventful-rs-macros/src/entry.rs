//! Expansion of the main macro family.
use proc_macro::TokenStream;
use quote::quote;
use syn::parse_macro_input;

/// Lint attributes that also apply to the moved body.
const LINTS: &[&str] = &["allow", "warn", "deny", "forbid", "expect"];

/// Run an async main function on a main-thread shard and join background shards.
///
/// Pass an explicitly declared main-thread shard marker: `#[sharded_main(Main)]`.
/// The shard continues processing callbacks while the main future awaits work,
/// allowing background producers to deliver events to main-thread listeners.
/// Declare the marker with runtime = main or tokio_main.
/// The main function's return type is preserved.
pub(crate) fn sharded_main(attr: TokenStream, item: TokenStream) -> TokenStream {
    let runtime = crate::runtime_path();
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

    // The wrapper keeps the user's attributes, except `expect`, which belongs to
    // the body and would be unfulfilled on the wrapper.
    let (inner_lints, attributes): (Vec<_>, Vec<_>) = item
        .attrs
        .drain(..)
        .partition(|attr| attr.path().is_ident("expect"));
    let inner_lints = inner_lints
        .into_iter()
        .chain(
            attributes
                .iter()
                .filter(|attr| LINTS.iter().any(|name| attr.path().is_ident(name)))
                .cloned(),
        )
        .collect::<Vec<_>>();
    let visibility = std::mem::replace(&mut item.vis, syn::Visibility::Inherited);
    let mut sig_orig = item.sig.clone();
    sig_orig.asyncness = None;

    // Nesting the body keeps it private and avoids sibling-name collisions; the
    // wrapper's cfg attributes already cover it.
    let new_ident = quote::format_ident!("__eventful_sharded_main");
    item.sig.ident = new_ident.clone();
    item.attrs = inner_lints;

    quote! {
        #(#attributes)*
        #visibility #sig_orig {
            #item
            let result = #shard::shard().run_main(#new_ident);
            if let ::core::result::Result::Err(e) = #runtime::join_all_shards() {
                ::core::panic!("failed to join background shards: {}", e);
            }
            result
        }
    }
    .into()
}
