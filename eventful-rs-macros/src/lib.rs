#![forbid(unsafe_code)]

use proc_macro::TokenStream;
use quote::{format_ident, quote};
use syn::{
    FnArg, ItemImpl, ItemStruct, ItemTrait, Pat, TraitItem, Type, parse_macro_input, parse_quote,
};

#[proc_macro_attribute]
pub fn events(_attr: TokenStream, item: TokenStream) -> TokenStream {
    let trait_item = parse_macro_input!(item as ItemTrait);
    if !trait_item.generics.params.is_empty() {
        return syn::Error::new_spanned(
            &trait_item.generics,
            "generic event traits are not supported",
        )
        .to_compile_error()
        .into();
    }
    for item in &trait_item.items {
        let TraitItem::Fn(method) = item else {
            return syn::Error::new_spanned(item, "event traits may only contain methods")
                .to_compile_error()
                .into();
        };
        let sig = &method.sig;
        if sig.asyncness.is_some()
            || !sig.generics.params.is_empty()
            || !matches!(sig.receiver(), Some(r) if r.reference.is_some() && r.mutability.is_none())
            || !matches!(&sig.output, syn::ReturnType::Default)
        {
            return syn::Error::new_spanned(
                sig,
                "events require a non-generic synchronous &self method with no return type",
            )
            .to_compile_error()
            .into();
        }
    }
    let trait_name = &trait_item.ident;
    let set_name = format_ident!("{}EventSet", trait_name);
    let ext_signals_name = format_ident!("{}SignalsExt", trait_name);
    let ext_emitter_name = format_ident!("{}EmittersExt", trait_name);

    let methods = trait_item
        .items
        .iter()
        .filter_map(|item| {
            let TraitItem::Fn(method) = item else {
                return None;
            };
            let method_name = &method.sig.ident;
            let signal_name =
                format_ident!("{}{}Signal", trait_name, pascal(&method_name.to_string()));
            //let emit_name = format_ident!("emit_{}", method_name);

            let mut names = Vec::new();
            let mut types = Vec::<Type>::new();
            for input in &method.sig.inputs {
                match input {
                    FnArg::Receiver(_) => {}
                    FnArg::Typed(arg) => {
                        let Pat::Ident(pat) = arg.pat.as_ref() else {
                            return Some(Err(syn::Error::new_spanned(
                                &arg.pat,
                                "event arguments must be simple identifiers",
                            )));
                        };
                        names.push(pat.ident.clone());
                        types.push((*arg.ty).clone());
                    }
                }
            }
            Some(Ok((
                method_name.clone(),
                signal_name,
                //emit_name,
                names,
                types,
            )))
        })
        .collect::<Result<Vec<_>, syn::Error>>();

    let methods = match methods {
        Ok(methods) => methods,
        Err(error) => return error.to_compile_error().into(),
    };

    let mut signal_defs = Vec::new();
    let mut set_fields = Vec::new();
    let mut extension_signal_methods = Vec::new();
    let mut extension_emitter_methods = Vec::new();

    for (method_name, signal_name, /*emit_name, */ arg_names, arg_types) in &methods {
        let target_ident = fresh_target(arg_names);
        let emit_tracked_name = format_ident!("emit_{}_tracked", method_name);
        let emit_name = format_ident!("emit_{}", method_name);
        signal_defs.push(quote! {
            pub struct #signal_name {
                inner: ::eventful_rs::Event<(#(#arg_types,)*)>,
            }

            impl ::core::default::Default for #signal_name {
                fn default() -> Self {
                    Self { inner: ::eventful_rs::Event::default() }
                }
            }

            impl #signal_name {
                pub fn connect<T, S>(&self, target: &S)
                where
                    T: ::eventful_rs::Eventful
                            + ::eventful_rs::HasEvents<T::EventSetType>
                            + #trait_name
                            + Sized
                            + 'static,
                    S: ::eventful_rs::Sharded<T>,
                    #(#arg_types: Clone + Send + 'static,)*
                {
                    let handle = ::eventful_rs::ShardHandle::downgrade(&::eventful_rs::Sharded::as_handle(target));
                    let tracked_handle = handle.clone();
                    self.inner.add_tracked_connection(move |(#(#arg_names,)*)| {
                        ::eventful_rs::ShardHandle::upgrade_in_shard(&handle, move |#target_ident| {
                            #target_ident.#method_name(#(#arg_names),*);
                        });
                    }, move |(#(#arg_names,)*)| {
                        tracked_handle.try_deferred_upgrade_in_shard(async move |#target_ident| {
                            #target_ident.#method_name(#(#arg_names),*);
                        })
                    });
                }

                pub fn emit(&self, #(#arg_names: #arg_types),*)
                where
                    #(#arg_types: Clone + Send + 'static,)*
                {
                    self.inner.emit((#(#arg_names,)*));
                }

                pub fn emit_tracked(&self, #(#arg_names: #arg_types),*)
                    -> impl ::core::future::Future<Output = Result<(), ::eventful_rs::DeliveryError>> + Send + 'static + use<>
                where
                    #(#arg_types: Clone + Send + 'static,)*
                {
                    self.inner.emit_tracked((#(#arg_names,)*))
                }
            }
        });
        set_fields.push(quote!(pub #method_name: #signal_name));
        extension_signal_methods.push(quote! {
            fn #method_name(&self) -> &#signal_name {
                &self.events().#method_name
            }
        });
        extension_emitter_methods.push(quote! {
            fn #emit_name(&self, #(#arg_names: #arg_types),*)
            where
                #(#arg_types: Clone + Send + 'static,)*
            {
                self.events().#method_name.emit(#(#arg_names),*);
            }

            fn #emit_tracked_name(&self, #(#arg_names: #arg_types),*)
                -> impl ::core::future::Future<Output = Result<(), ::eventful_rs::DeliveryError>> + Send + 'static
            where
                #(#arg_types: Clone + Send + 'static,)*
            {
                self.events().#method_name.emit_tracked(#(#arg_names),*)
            }
        });
    }

    quote! {
        #trait_item

        #(#signal_defs)*

        #[derive(Default)]
        pub struct #set_name {
            #(#set_fields,)*
        }

        pub trait #ext_signals_name: ::eventful_rs::HasEvents<#set_name> {
            #(#extension_signal_methods)*
        }

        pub trait #ext_emitter_name: ::eventful_rs::HasEvents<#set_name>{
            #(#extension_emitter_methods)*
        }

        impl<T> #ext_signals_name for ::eventful_rs::ShardRc<T>
        where
            T: ::eventful_rs::Eventful<EventSetType = #set_name> + ::eventful_rs::HasEvents<#set_name> + Sized + 'static,
            ::eventful_rs::ShardRc<T>: ::eventful_rs::HasEvents<#set_name>,
        {}

        impl<T> #ext_signals_name for ::eventful_rs::ShardRcHandle<T>
        where
            T: ::eventful_rs::Eventful<EventSetType = #set_name> + ::eventful_rs::HasEvents<#set_name> + Sized + 'static,
            ::eventful_rs::ShardRcHandle<T>: ::eventful_rs::HasEvents<#set_name>,
        {}

        impl<T> #ext_signals_name for ::eventful_rs::ShardWeakHandle<T>
        where
            T: ::eventful_rs::Eventful<EventSetType = #set_name> + ::eventful_rs::HasEvents<#set_name> + Sized + 'static,
            ::eventful_rs::ShardWeakHandle<T>: ::eventful_rs::HasEvents<#set_name>,
        {}

        impl<T: ::eventful_rs::HasEvents<#set_name> + ?Sized> #ext_emitter_name for T {}
    }
    .into()
}

#[proc_macro_attribute]
pub fn asynchronize(_attr: TokenStream, item: TokenStream) -> TokenStream {
    let mut item = parse_macro_input!(item as ItemImpl);
    if item.trait_.is_some() || !item.generics.params.is_empty() {
        return syn::Error::new_spanned(&item, "asynchronize requires a non-generic inherent impl")
            .to_compile_error()
            .into();
    }
    let struct_type = &item.self_ty;
    let struct_name = if let Type::Path(type_path) = &**struct_type {
        type_path.path.segments.last().unwrap().ident.clone()
    } else {
        return syn::Error::new_spanned(&item.self_ty, "expected a struct type for the impl block")
            .to_compile_error()
            .into();
    };
    let trait_name = format_ident!("{}Async", struct_name);

    let (impl_generics, type_generics, where_clause) = item.generics.split_for_impl();

    let mut action_method_signature = Vec::new();
    let mut action_method_implementation = Vec::new();

    let mut async_method_signature = Vec::new();
    let mut async_method_implementation = Vec::new();

    for method in item.items.iter() {
        if let syn::ImplItem::Fn(method) = method {
            let annotated = method.attrs.iter().any(|a| {
                a.path()
                    .segments
                    .last()
                    .is_some_and(|s| s.ident == "action" || s.ident == "asynced")
            });
            if annotated
                && (!matches!(method.sig.receiver(), Some(r) if r.reference.is_some() && r.mutability.is_none())
                    || method.sig.unsafety.is_some()
                    || method.sig.constness.is_some())
            {
                return syn::Error::new_spanned(
                    &method.sig,
                    "dispatched methods require a safe, non-const &self receiver",
                )
                .to_compile_error()
                .into();
            }
            for attr in &method.attrs {
                if attr
                    .path()
                    .segments
                    .last()
                    .is_some_and(|s| s.ident == "action")
                {
                    let sig = &method.sig;

                    if let syn::ReturnType::Type(_, ty) = &sig.output {
                        if let Type::Tuple(tuple) = &**ty {
                            if !tuple.elems.is_empty() {
                                return syn::Error::new_spanned(
                                    &sig.output,
                                    "action methods must return `()`",
                                )
                                .to_compile_error()
                                .into();
                            }
                        } else {
                            return syn::Error::new_spanned(
                                &sig.output,
                                "action methods must return `()`",
                            )
                            .to_compile_error()
                            .into();
                        }
                    }

                    let method_name = &sig.ident;
                    let mut method_arg_names = Vec::new();

                    for input in &sig.inputs {
                        match input {
                            FnArg::Receiver(_) => {}
                            FnArg::Typed(arg) => {
                                let Pat::Ident(pat) = arg.pat.as_ref() else {
                                    return syn::Error::new_spanned(
                                        &arg.pat,
                                        "action arguments must be simple identifiers",
                                    )
                                    .to_compile_error()
                                    .into();
                                };
                                method_arg_names.push(pat.ident.clone());
                            }
                        }
                    }

                    let target_ident = fresh_target(&method_arg_names);
                    let is_async = sig.asyncness.is_some();
                    let mut trait_sig = sig.clone();
                    trait_sig.asyncness = None;
                    action_method_signature.push(trait_sig.clone());

                    let method_impl = if is_async {
                        quote! {
                            #trait_sig {
                                ::eventful_rs::ShardHandle::upgrade_in_shard_async(self, async move |#target_ident| {
                                    #target_ident.#method_name(#(#method_arg_names,)*).await;
                                });
                            }
                        }
                    } else {
                        quote! {
                            #trait_sig {
                                ::eventful_rs::ShardHandle::upgrade_in_shard(self, move |#target_ident| {
                                    #target_ident.#method_name(#(#method_arg_names,)*);
                                });
                            }
                        }
                    };
                    action_method_implementation.push(method_impl);
                } else if attr
                    .path()
                    .segments
                    .last()
                    .is_some_and(|s| s.ident == "asynced")
                {
                    let sig = &method.sig;

                    let method_name = &sig.ident;
                    let mut method_arg_names = Vec::new();

                    for input in &sig.inputs {
                        match input {
                            FnArg::Receiver(_) => {}
                            FnArg::Typed(arg) => {
                                let Pat::Ident(pat) = arg.pat.as_ref() else {
                                    return syn::Error::new_spanned(
                                        &arg.pat,
                                        "action arguments must be simple identifiers",
                                    )
                                    .to_compile_error()
                                    .into();
                                };
                                method_arg_names.push(pat.ident.clone());
                            }
                        }
                    }

                    let target_ident = fresh_target(&method_arg_names);
                    let is_async = sig.asyncness.is_some();
                    let mut trait_sig = sig.clone();
                    trait_sig.asyncness = Some(syn::token::Async::default());
                    async_method_signature.push(trait_sig.clone());

                    let method_impl = if is_async {
                        quote! {
                            #trait_sig {
                                ::eventful_rs::ShardHandle::deferred_upgrade_in_shard(self, async move |#target_ident| {
                                    #target_ident.#method_name(#(#method_arg_names,)*).await
                                }).await
                            }
                        }
                    } else {
                        quote! {
                            #trait_sig {
                                ::eventful_rs::ShardHandle::deferred_upgrade_in_shard(self, async move |#target_ident| {
                                    #target_ident.#method_name(#(#method_arg_names,)*)
                                }).await
                            }
                        }
                    };
                    async_method_implementation.push(method_impl);
                }
            }
        }
    }

    for member in &mut item.items {
        if let syn::ImplItem::Fn(method) = member {
            method.attrs.retain(|a| {
                !a.path()
                    .segments
                    .last()
                    .is_some_and(|s| s.ident == "action" || s.ident == "asynced")
            });
        }
    }
    if action_method_signature.is_empty() && async_method_signature.is_empty() {
        return quote! {
            #item
        }
        .into();
    }

    quote! {
        #item

        #[allow(async_fn_in_trait)]
        pub trait #trait_name {
            #(#action_method_signature;)*

            #(#async_method_signature;)*
        }

        impl #impl_generics #trait_name for ::eventful_rs::ShardWeakHandle<#struct_name #type_generics> #where_clause {
            #(#action_method_implementation)*

            #(#async_method_implementation)*
        }

        impl #impl_generics #trait_name for ::eventful_rs::ShardRcHandle<#struct_name #type_generics> #where_clause {
            #(#action_method_implementation)*

            #(#async_method_implementation)*
        }
    }.into()
}

#[proc_macro_attribute]
pub fn eventful(attr: TokenStream, item: TokenStream) -> TokenStream {
    let mut item = parse_macro_input!(item as ItemStruct);
    if !item.generics.params.is_empty() {
        return syn::Error::new_spanned(
            &item.generics,
            "generic eventful structs are not supported",
        )
        .to_compile_error()
        .into();
    }
    let struct_name = &item.ident;
    let needs_to_gen_trait = attr.is_empty();
    let trait_name = if attr.is_empty() {
        let gen_trait_name = format_ident!("{}Events", struct_name);
        parse_quote!(#gen_trait_name)
    } else {
        parse_macro_input!(attr as syn::Path)
    };
    let (impl_generics, type_generics, where_clause) = item.generics.split_for_impl();

    let Some(trait_ident) = trait_name
        .segments
        .last()
        .map(|segment| segment.ident.clone())
    else {
        return syn::Error::new_spanned(trait_name, "expected an event trait")
            .to_compile_error()
            .into();
    };

    let mut set_name = trait_name.clone();
    set_name.segments.last_mut().unwrap().ident = format_ident!("{}EventSet", trait_ident);
    let set_field = format_ident!("events");

    match &mut item.fields {
        syn::Fields::Named(fields) => {
            fields.named.push(syn::parse_quote! {
                #set_field: std::sync::Arc<#set_name>
            });
        }

        syn::Fields::Unit => {
            let fields: syn::FieldsNamed = syn::parse_quote!({
                #set_field: std::sync::Arc<#set_name>
            });

            item.fields = syn::Fields::Named(fields);
            item.semi_token = None;
        }

        syn::Fields::Unnamed(_) => {
            return syn::Error::new_spanned(item, "tuple structs are not supported")
                .to_compile_error()
                .into();
        }
    }

    let gen_trait = if needs_to_gen_trait {
        let trait_ident_set = format_ident!("{}EventsEventSet", struct_name);
        quote! {
            pub trait #trait_ident {}

            pub struct #trait_ident_set {}

            impl Default for #trait_ident_set {
                fn default() -> Self {
                    #trait_ident_set {}
                }
            }
        }
    } else {
        quote!()
    };

    quote! {
        #gen_trait

        #item


        impl #impl_generics ::eventful_rs::HasEvents<#set_name>
            for #struct_name #type_generics
            #where_clause
        {
            fn #set_field(&self) -> &std::sync::Arc<#set_name> {
                &self.#set_field
            }
        }

        impl #impl_generics ::eventful_rs::Eventful
            for #struct_name #type_generics #where_clause
        {
            type EventSetType = #set_name;
            type EventLoopHandleType = DefaultShardHandleType;
            fn default_handle() -> Self::EventLoopHandleType {
                ::eventful_rs::EventLoop::handle(default_shard())
            }
        }

    }
    .into()
}

#[proc_macro_attribute]
pub fn action(_attr: TokenStream, item: TokenStream) -> TokenStream {
    item
}

#[proc_macro_attribute]
pub fn asynced(_attr: TokenStream, item: TokenStream) -> TokenStream {
    item
}

#[proc_macro_attribute]
pub fn sharded_main(attr: TokenStream, item: TokenStream) -> TokenStream {
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
    let declaration = if attr.is_empty() {
        quote!(::eventful_rs::shard_main!(MAIN_SHARD);)
    } else if attr.to_string() == "tokio" {
        quote!(::eventful_rs::shard_tokio_main!(MAIN_SHARD);)
    } else {
        return syn::Error::new(
            proc_macro2::Span::call_site(),
            "expected #[sharded_main] or #[sharded_main(tokio)]",
        )
        .to_compile_error()
        .into();
    };
    let mut sig_orig = item.sig.clone();
    sig_orig.asyncness = None;

    let new_ident = format_ident!("{}_sharded_main", item.sig.ident);
    item.sig.ident = new_ident.clone();

    quote! {
        #declaration

        #item

        #sig_orig {
            let result = MAIN_SHARD.run_main(#new_ident);
            if let Err(e) = ::eventful_rs::join_all_shards() {
                panic!("failed to join background shards: {}", e);
            }
            result
        }
    }
    .into()
}

fn pascal(value: &str) -> String {
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

fn fresh_target(arguments: &[syn::Ident]) -> syn::Ident {
    let mut name = "__eventful_target".to_owned();
    while arguments.iter().any(|a| a == &name) {
        name.push('_');
    }
    format_ident!("{}", name)
}
