//! Expansion of the dispatch macro family.
use proc_macro::TokenStream;
use quote::{format_ident, quote};
use syn::{FnArg, ItemImpl, Pat, Safety, Type, parse_macro_input};

use crate::fresh_target;
/// Generate handle wrappers for annotated methods in an inherent `impl`.
///
/// Mark methods with `#[asynced]` to await their results, or `#[action]` to queue
/// work from synchronous or async code without waiting. Original methods
/// keep their signatures. Unannotated methods remain accessible through references to shard-local
/// values, including inside `upgrade_in_shard` callbacks.
/// Dispatched methods require `&self` and `Send + 'static` arguments and results.
pub(crate) fn asynchronize(_attr: TokenStream, item: TokenStream) -> TokenStream {
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
                && (!matches!(method.sig.receiver(), Some(r) if matches!(r.kind, syn::ReceiverKind::Reference(_, _, None)))
                    || matches!(method.sig.safety, Safety::Unsafe(_))
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
                    let docs: Vec<_> = method
                        .attrs
                        .iter()
                        .filter(|a| a.path().is_ident("doc"))
                        .collect();
                    action_method_signature.push(quote! {
                        /// Queue this method on the value's shard without waiting.
                        #(#docs)*
                        #trait_sig
                    });

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
                    let docs: Vec<_> = method
                        .attrs
                        .iter()
                        .filter(|a| a.path().is_ident("doc"))
                        .collect();
                    async_method_signature.push(quote! {
                        /// Await this method on the value's shard; panics on dispatch failure.
                        #(#docs)*
                        #trait_sig
                    });

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

        /// Generated methods for dispatching calls through strong or weak handles.
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
