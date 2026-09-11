use proc_macro::TokenStream;
use quote::{format_ident, quote};
use syn::{
    FnArg, ItemImpl, ItemStruct, ItemTrait, Pat, TraitItem, Type, parse_macro_input, parse_quote,
};

fn snake(name: &str) -> String {
    let mut out = String::new();
    for (i, ch) in name.chars().enumerate() {
        if ch.is_uppercase() {
            if i != 0 {
                out.push('_');
            }
            out.extend(ch.to_lowercase());
        } else {
            out.push(ch);
        }
    }
    out
}

#[proc_macro_attribute]
pub fn events(_attr: TokenStream, item: TokenStream) -> TokenStream {
    let mut trait_item = parse_macro_input!(item as ItemTrait);
    let trait_name = &trait_item.ident;
    let set_name = format_ident!("{}EventSet", trait_name);
    //let has_name = format_ident!("Has{}Events", trait_name);
    let ext_signals_name = format_ident!("{}SignalsExt", trait_name);
    let ext_emitter_name = format_ident!("{}EmittersExt", trait_name);
    //let set_field = format_ident!("{}", snake(&trait_name.to_string()));

    // set our supertraits to: `Send + Sync + 'static`

    trait_item.supertraits.clear();
    trait_item.supertraits.push(syn::parse_quote!(Send));
    trait_item.supertraits.push(syn::parse_quote!(Sync));
    trait_item.supertraits.push(syn::parse_quote!('static));

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
            let emit_name = format_ident!("emit_{}", method_name);

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
                emit_name,
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

    for (method_name, signal_name, emit_name, arg_names, arg_types) in &methods {
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
                pub fn connect<T, H>(&self, target: & ::eventful_rs::ShardHandle<T, H, #set_name>)
                where
                    T: ::eventful_rs::HasEvents<#set_name> + #trait_name + ?Sized + 'static,
                    H: ::eventful_rs::EventLoopHandle,
                    #(#arg_types: Clone + Send + 'static,)*
                {
                    // let weak = target.weak();
                    // let event_loop = target.event_loop();
                    let handle = target.clone();
                    self.inner.add_connection(move |(#(#arg_names,)*)| {
                        // let weak = weak.clone();
                        // let event_loop = event_loop.clone();
                        let _ = handle.in_shard(move |target| {
                            target.#method_name(#(#arg_names),*);
                        });
                    });
                }

                pub fn emit(&self, #(#arg_names: #arg_types),*)
                where
                    #(#arg_types: Clone + Send + 'static,)*
                {
                    self.inner.emit((#(#arg_names,)*));
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

        trait #ext_emitter_name: ::eventful_rs::HasEvents<#set_name>{
            #(#extension_emitter_methods)*
        }

        impl<T, H> #ext_signals_name for ::eventful_rs::ShardHandle<T, H, #set_name>
        where
            T: ::eventful_rs::HasEvents<#set_name> + ?Sized + 'static,
            H: EventLoopHandle,
            ::eventful_rs::ShardHandle<T, H, #set_name>: ::eventful_rs::HasEvents<#set_name>,
        {}

        impl<T: ::eventful_rs::HasEvents<#set_name> + ?Sized> #ext_emitter_name for T {}
    }
    .into()
}

#[proc_macro_attribute]
pub fn asynchronize(_attr: TokenStream, item: TokenStream) -> TokenStream {
    let item = parse_macro_input!(item as ItemImpl);
    let struct_type = &item.self_ty;
    let struct_name = if let Type::Path(type_path) = &**struct_type {
        type_path.path.segments.last().unwrap().ident.clone()
    } else {
        return syn::Error::new_spanned(&item.self_ty, "expected a struct type for the impl block")
            .to_compile_error()
            .into();
    };
    let trait_name = format_ident!("{}Async", struct_name);
    let mut generics = item.generics.clone();

    generics
        .params
        .push(syn::parse_quote!(__H: EventLoopHandle));
    generics
        .params
        .push(syn::parse_quote!(__S: ?Sized + Send + 'static));

    let (impl_generics, _, where_clause) = generics.split_for_impl();
    let (_, type_generics, _) = item.generics.split_for_impl();

    let mut action_method_signature = Vec::new();
    let mut action_method_implementation = Vec::new();

    for method in item.items.iter() {
        if let syn::ImplItem::Fn(method) = method {
            for attr in &method.attrs {
                if attr.path().is_ident("action") {
                    // generate a signature for the method in the trait
                    // this signature is stripped of the `#[action]` attribute and the method body
                    // as well as the `async` keyword, if present

                    let sig = &method.sig;

                    // error if sig has a return type that is not `-> ()`
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
                    // let mut method_generic_arg_names = Vec::new();

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

                    // for generic in &sig.generics.params {
                    //     if let syn::GenericParam::Type(type_param) = generic {
                    //         method_generic_arg_names.push(type_param.ident.clone());
                    //     }
                    // }

                    let is_async = sig.asyncness.is_some();
                    let mut trait_sig = sig.clone();
                    trait_sig.asyncness = None;
                    action_method_signature.push(trait_sig.clone());

                    // generate an implementation for the method in the impl block
                    let method_impl = if is_async {
                        quote! {
                            #trait_sig {
                                let weak = self.weak().clone();
                                self.event_loop().invoke_async(async move || {
                                    if let Some(target) = weak.upgrade() {
                                        target.#method_name(#(#method_arg_names,)*).await;
                                    }
                                });
                            }
                        }
                    } else {
                        quote! {
                            #trait_sig {
                                let weak = self.weak().clone();
                                self.event_loop().invoke(move || {
                                    if let Some(target) = weak.upgrade() {
                                        target.#method_name(#(#method_arg_names,)*);
                                    }
                                });
                            }
                        }
                    };
                    action_method_implementation.push(method_impl);
                } else if attr.path().is_ident("asynchronize") {
                    // convert method access to an async call
                    // call is invoked on the shard that owns this target
                    // then the result is sent back to the shard from where the call originated from
                    let sig = &method.sig;

                    let method_name = &sig.ident;
                    let mut method_arg_names = Vec::new();
                    // let mut method_generic_arg_names = Vec::new();

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

                    // for generic in &sig.generics.params {
                    //     if let syn::GenericParam::Type(type_param) = generic {
                    //         method_generic_arg_names.push(type_param.ident.clone());
                    //     }
                    // }

                    let is_async = sig.asyncness.is_some();
                    let mut trait_sig = sig.clone();
                    trait_sig.asyncness = None;
                    action_method_signature.push(trait_sig.clone());

                    // generate an implementation for the method in the impl block
                    let method_impl = if is_async {
                        quote! {
                            #trait_sig {
                                let weak = self.weak().clone();
                                self.event_loop().invoke_async(async move || {
                                    if let Some(target) = weak.upgrade() {
                                        target.#method_name(#(#method_arg_names,)*).await;
                                    }
                                });
                            }
                        }
                    } else {
                        quote! {
                            #trait_sig {
                                let weak = self.weak().clone();
                                self.event_loop().invoke(move || {
                                    if let Some(target) = weak.upgrade() {
                                        target.#method_name(#(#method_arg_names,)*);
                                    }
                                });
                            }
                        }
                    };
                    action_method_implementation.push(method_impl);
                }
            }
        }
    }

    if action_method_signature.is_empty() {
        return quote! {
            #item
        }
        .into();
    }

    quote! {
        #item

        pub trait #trait_name {
            #(#action_method_signature;)*
        }

        impl #impl_generics #trait_name for ::eventful_rs::ShardHandle<#struct_name #type_generics, __H, __S> #where_clause {
            #(#action_method_implementation)*
        }
    }.into()
}

#[proc_macro_attribute]
pub fn eventful(attr: TokenStream, item: TokenStream) -> TokenStream {
    // if attr.is_empty() {
    //     let item = parse_macro_input!(item as ItemStruct);
    //     let struct_name = &item.ident;
    //     let (impl_generics, type_generics, where_clause) = item.generics.split_for_impl();

    //     return quote! {
    //         #item

    //         impl #impl_generics ::eventful_rs::EventTarget for #struct_name #type_generics #where_clause {
    //             fn event_loop(&self) -> impl ::eventful_rs::EventLoopHandle {
    //                 default_shard().handle()
    //             }
    //         }
    //     }.into();
    // }

    let mut item = parse_macro_input!(item as ItemStruct);
    let struct_name = &item.ident;
    let needs_to_gen_trait = attr.is_empty();
    let trait_name = if attr.is_empty() {
        let gen_trait_name = format_ident!("{}Events", struct_name);
        parse_quote!(#gen_trait_name)
    } else {
        parse_macro_input!(attr as syn::Path)
    };
    // let (impl_generics, type_generics, where_clause) = item.generics.split_for_impl();
    let mut generics = item.generics.clone();

    generics
        .params
        .push(syn::parse_quote!(__H: EventLoopHandle));
    generics
        .params
        .push(syn::parse_quote!(__E: ?Sized + Send + 'static));

    let (impl_generics_handle, _, where_clause_handle) = generics.split_for_impl();
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

    let set_name = format_ident!("{}EventSet", trait_ident);
    //let has_name = format_ident!("HasEvents");
    let set_field = format_ident!("events");

    match &mut item.fields {
        syn::Fields::Named(fields) => {
            fields.named.push(syn::parse_quote! {
                #set_field: Arc<#set_name>
            });
        }

        syn::Fields::Unit => {
            let fields: syn::FieldsNamed = syn::parse_quote!({
                #set_field: Arc<#set_name>
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

        impl #impl_generics ::eventful_rs::EventTarget for #struct_name #type_generics #where_clause {
            fn event_loop(&self) -> impl ::eventful_rs::EventLoopHandle {
                default_shard().handle()
            }
        }

        impl #impl_generics ::eventful_rs::HasEvents<#set_name>
            for #struct_name #type_generics
            #where_clause
        {
            fn #set_field(&self) -> &Arc<#set_name> {
                &self.#set_field
            }
        }

    }
    .into()
}

#[proc_macro_attribute]
pub fn action(_attr: TokenStream, item: TokenStream) -> TokenStream {
    item
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
