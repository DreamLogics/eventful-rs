//! Expansion of named singleton shard declarations.
use proc_macro::TokenStream;
use quote::quote;
use syn::{
    Attribute, Ident, Token, Visibility,
    parse::{Parse, ParseStream},
    parse_macro_input,
};

/// A marker declaration and its selected backend.
struct Declaration {
    /// Outer attributes on the marker.
    attrs: Vec<Attribute>,
    /// Visibility shared by the marker and backend accessor.
    visibility: Visibility,
    /// Application's marker name.
    name: Ident,
    /// Built-in backend selector.
    runtime: Ident,
}
impl Parse for Declaration {
    fn parse(input: ParseStream<'_>) -> syn::Result<Self> {
        let attrs = input.call(Attribute::parse_outer)?;
        let visibility = input.parse()?;
        let name = input.parse()?;
        input.parse::<Token![,]>()?;
        let key: Ident = input.parse()?;
        if key != "runtime" {
            return Err(syn::Error::new_spanned(key, "expected `runtime`"));
        }
        input.parse::<Token![=]>()?;
        let runtime = input.parse()?;
        Ok(Self {
            attrs,
            visibility,
            name,
            runtime,
        })
    }
}

/// Generate a marker and conditionally compiled singleton implementations.
pub(crate) fn declare_shard(input: TokenStream) -> TokenStream {
    let Declaration {
        attrs,
        visibility,
        name,
        runtime: backend,
    } = parse_macro_input!(input as Declaration);
    let runtime = crate::runtime_path();
    let conditions = match crate::attributes::conditions(&attrs) {
        Ok(conditions) => conditions,
        Err(error) => return error.to_compile_error().into(),
    };
    let (ty, init) = match backend.to_string().as_str() {
        "std" => (
            quote!(#runtime::std_rt::Shard),
            quote!(#runtime::std_rt::Shard::new(stringify!(#name))),
        ),
        "tokio" => (
            quote!(#runtime::tokio_rt::TokioShard),
            quote!(#runtime::tokio_rt::TokioShard::new(stringify!(#name))),
        ),
        "main" => (
            quote!(#runtime::local_rt::LocalShard),
            quote!(#runtime::local_rt::LocalShard::new()),
        ),
        "tokio_main" => (
            quote!(#runtime::tokio_local_rt::TokioLocalShard),
            quote!(#runtime::tokio_local_rt::TokioLocalShard::new(stringify!(#name))),
        ),
        "slint" => (
            quote!(#runtime::slint_rt::SlintShard),
            quote!(#runtime::slint_rt::SlintShard::new()),
        ),
        _ => {
            return syn::Error::new_spanned(
                backend,
                "expected std, tokio, main, tokio_main, or slint",
            )
            .to_compile_error()
            .into();
        }
    };
    quote! {
        /// Marker for a lazily initialized singleton shard.
        #(#attrs)*
        #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
        #visibility enum #name {}
        #(#conditions)*
        impl #name {
            /// Access the singleton backend, initializing it on first use.
            ///
            /// # Panics
            /// Panics if backend initialization fails.
            #visibility fn shard() -> &'static #ty {
                static SHARD: ::std::sync::LazyLock<#ty> = ::std::sync::LazyLock::new(|| #init);
                &SHARD
            }
        }
        #(#conditions)*
        impl #runtime::ShardBinding for #name {
            fn handle() -> #runtime::ShardEventHandle {
                #runtime::EventLoop::handle(Self::shard())
            }
        }
    }
    .into()
}
