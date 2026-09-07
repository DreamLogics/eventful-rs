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
    let has_name = format_ident!("Has{}Events", trait_name);
    let ext_signals_name = format_ident!("{}SignalsExt", trait_name);
    let ext_emitter_name = format_ident!("{}EmittersExt", trait_name);
    let set_field = format_ident!("{}", snake(&trait_name.to_string()));

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
                pub fn connect<T>(&self, target: & ::eventful_rs::Erc<T>)
                where
                    T: #trait_name + ::eventful_rs::EventTarget,
                    #(#arg_types: Clone + Send + 'static,)*
                {
                    let weak = target.weak();
                    let event_loop = target.event_loop();
                    self.inner.add_connection(move |(#(#arg_names,)*)| {
                        let weak = weak.clone();
                        let event_loop = event_loop.clone();
                        let _ = event_loop.invoke(move || {
                            if let Some(target) = weak.upgrade() {
                                target.#method_name(#(#arg_names),*);
                            }
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
                &self.get().#set_field().#method_name
            }
        });
        extension_emitter_methods.push(quote! {
            fn #emit_name(&self, #(#arg_names: #arg_types),*)
            where
                #(#arg_types: Clone + Send + 'static,)*
            {
                self.#method_name().emit(#(#arg_names),*);
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

        pub trait #has_name {
            fn #set_field(&self) -> &#set_name;
        }

        pub trait #ext_signals_name: #has_name {
            #(#extension_signal_methods)*
        }

        trait #ext_emitter_name: #has_name {
            #(#extension_emitter_methods)*
        }

        impl<T: #has_name + ?Sized> #ext_signals_name for ::eventful_rs::Erc<T> {}

        impl<T: #has_name + ?Sized> #ext_emitter_name for T {}
    }
    .into()
}

// #[proc_macro_attribute]
// pub fn actions(_attr: TokenStream, item: TokenStream) -> TokenStream {
//     let mut trait_item = parse_macro_input!(item as ItemTrait);
//     let trait_name = &trait_item.ident;

//     // set our supertraits to: `Send + Sync + 'static`

//     trait_item.supertraits.clear();
//     trait_item.supertraits.push(syn::parse_quote!(Send));
//     trait_item.supertraits.push(syn::parse_quote!(Sync));
//     trait_item.supertraits.push(syn::parse_quote!('static));

//     let methods = trait_item
//         .items
//         .iter()
//         .filter_map(|item| {
//             let TraitItem::Fn(method) = item else {
//                 return None;
//             };
//             let method_name = &method.sig.ident;
//             let signal_name =
//                 format_ident!("{}{}Signal", trait_name, pascal(&method_name.to_string()));
//             let emit_name = format_ident!("emit_{}", method_name);

//             let mut names = Vec::new();
//             let mut types = Vec::<Type>::new();
//             for input in &method.sig.inputs {
//                 match input {
//                     FnArg::Receiver(_) => {}
//                     FnArg::Typed(arg) => {
//                         let Pat::Ident(pat) = arg.pat.as_ref() else {
//                             return Some(Err(syn::Error::new_spanned(
//                                 &arg.pat,
//                                 "event arguments must be simple identifiers",
//                             )));
//                         };
//                         names.push(pat.ident.clone());
//                         types.push((*arg.ty).clone());
//                     }
//                 }
//             }
//             Some(Ok((
//                 method_name.clone(),
//                 signal_name,
//                 emit_name,
//                 names,
//                 types,
//             )))
//         })
//         .collect::<Result<Vec<_>, syn::Error>>();

//     let methods = match methods {
//         Ok(methods) => methods,
//         Err(error) => return error.to_compile_error().into(),
//     };

//     let mut signal_defs = Vec::new();
//     let mut set_fields = Vec::new();
//     let mut extension_methods = Vec::new();
//     let mut extension_methods_arc = Vec::new();

//     for (method_name, signal_name, emit_name, arg_names, arg_types) in &methods {
//         signal_defs.push(quote! {
//             pub struct #signal_name {
//                 inner: ::eventful_rs::Event<(#(#arg_types,)*)>,
//             }

//             impl ::core::default::Default for #signal_name {
//                 fn default() -> Self {
//                     Self { inner: ::eventful_rs::Event::default() }
//                 }
//             }

//             impl #signal_name {
//                 pub fn connect<T>(&self, target: & ::eventful_rs::Erc<T>)
//                 where
//                     T: #trait_name + ::eventful_rs::EventTarget,
//                     #(#arg_types: Clone + Send + 'static,)*
//                 {
//                     let weak = target.weak();
//                     let event_loop = target.event_loop();
//                     self.inner.add_connection(move |(#(#arg_names,)*)| {
//                         let weak = weak.clone();
//                         let event_loop = event_loop.clone();
//                         let _ = event_loop.invoke(move || {
//                             if let Some(target) = weak.upgrade() {
//                                 target.#method_name(#(#arg_names),*);
//                             }
//                         });
//                     });
//                 }

//                 pub fn emit(&self, #(#arg_names: #arg_types),*)
//                 where
//                     #(#arg_types: Clone + Send + 'static,)*
//                 {
//                     self.inner.emit((#(#arg_names,)*));
//                 }
//             }
//         });
//         set_fields.push(quote!(pub #method_name: #signal_name));
//         extension_methods.push(quote! {
//             fn #method_name(&self) -> &#signal_name {
//                 &self.#set_field().#method_name
//             }

//             fn #emit_name(&self, #(#arg_names: #arg_types),*)
//             where
//                 #(#arg_types: Clone + Send + 'static,)*
//             {
//                 self.#method_name().emit(#(#arg_names),*);
//             }
//         });
//         extension_methods_arc.push(quote! {
//             fn #method_name(&self) -> &#signal_name {
//                 &self.get().#set_field().#method_name
//             }

//             fn #emit_name(&self, #(#arg_names: #arg_types),*)
//             where
//                 #(#arg_types: Clone + Send + 'static,)*
//             {
//                 self.get().#method_name().emit(#(#arg_names),*);
//             }
//         });
//     }

//     quote! {
//         #trait_item

//         #(#signal_defs)*

//         #[derive(Default)]
//         pub struct #set_name {
//             #(#set_fields,)*
//         }

//         pub trait #has_name {
//             fn #set_field(&self) -> &#set_name;
//         }

//         pub trait #ext_name: #has_name {
//             #(#extension_methods)*
//         }

//         impl<T: #has_name + ?Sized> #ext_name for T {}

//         impl<T: #has_name + ?Sized> #ext_name for ::eventful_rs::Erc<T> {
//             #(#extension_methods_arc)*
//         }
//     }
//     .into()
// }

// #[proc_macro_attribute]
// pub fn with_events(attr: TokenStream, item: TokenStream) -> TokenStream {
//     let trait_name = parse_macro_input!(attr as syn::Path);
//     let mut item = parse_macro_input!(item as ItemStruct);

//     let Some(trait_ident) = trait_name
//         .segments
//         .last()
//         .map(|segment| segment.ident.clone())
//     else {
//         return syn::Error::new_spanned(trait_name, "expected an event trait")
//             .to_compile_error()
//             .into();
//     };

//     let set_name = format_ident!("{}EventSet", trait_ident);
//     let has_name = format_ident!("Has{}Events", trait_ident);
//     let set_field = format_ident!("{}", snake(&trait_ident.to_string()));

//     match &mut item.fields {
//         syn::Fields::Named(fields) => {
//             fields.named.push(syn::parse_quote! {
//                 #set_field: #set_name
//             });
//         }

//         syn::Fields::Unit => {
//             let fields: syn::FieldsNamed = syn::parse_quote!({
//                 #set_field: #set_name
//             });

//             item.fields = syn::Fields::Named(fields);
//             item.semi_token = None;
//         }

//         syn::Fields::Unnamed(_) => {
//             return syn::Error::new_spanned(item, "tuple structs are not supported")
//                 .to_compile_error()
//                 .into();
//         }
//     }

//     let struct_name = &item.ident;
//     let (impl_generics, type_generics, where_clause) = item.generics.split_for_impl();

//     quote! {
//         #item

//         impl #impl_generics #has_name
//             for #struct_name #type_generics
//             #where_clause
//         {
//             fn #set_field(&self) -> &#set_name {
//                 &self.#set_field
//             }
//         }
//     }
//     .into()
// }

#[proc_macro_attribute]
pub fn with_actions(_attr: TokenStream, item: TokenStream) -> TokenStream {
    let item = parse_macro_input!(item as ItemImpl);
    let struct_type = &item.self_ty;
    let struct_name = if let Type::Path(type_path) = &**struct_type {
        type_path.path.segments.last().unwrap().ident.clone()
    } else {
        return syn::Error::new_spanned(&item.self_ty, "expected a struct type for the impl block")
            .to_compile_error()
            .into();
    };
    let trait_name = format_ident!("{}Actions", struct_name);
    let (impl_generics, type_generics, where_clause) = item.generics.split_for_impl();
    let mut method_signature = Vec::new();
    let mut method_implementation = Vec::new();

    // look for methods with the `#[action]` attribute
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
                    method_signature.push(trait_sig);

                    // generate an implementation for the method in the impl block
                    let method_impl = if is_async {
                        quote! {
                            quote! {
                                #sig {
                                    let weak = self.weak().clone();
                                    self.event_loop().invoke_async(async move || {
                                        if let Some(target) = weak.upgrade() {
                                            target.#method_name(#(#method_arg_names,)*).await;
                                        }
                                    });
                                }
                            }
                        }
                    } else {
                        quote! {
                            #sig {
                                let weak = self.weak().clone();
                                self.event_loop().invoke(move || {
                                    if let Some(target) = weak.upgrade() {
                                        target.#method_name(#(#method_arg_names,)*);
                                    }
                                });
                            }
                        }
                    };
                    method_implementation.push(method_impl);
                }
            }
        }
    }

    quote! {
        #item

        pub trait #trait_name {
            #(#method_signature;)*
        }

        impl #impl_generics #trait_name for ::eventful_rs::Erc<#struct_name #type_generics> #where_clause {
            #(#method_implementation)*
        }
    }.into()
}

#[proc_macro_attribute]
pub fn eventful(attr: TokenStream, item: TokenStream) -> TokenStream {
    if attr.is_empty() {
        let item = parse_macro_input!(item as ItemStruct);
        let struct_name = &item.ident;
        let (impl_generics, type_generics, where_clause) = item.generics.split_for_impl();

        return quote! {
            #item

            impl #impl_generics ::eventful_rs::EventTarget for #struct_name #type_generics #where_clause {
                fn event_loop(&self) -> impl ::eventful_rs::EventLoopHandle {
                    default_shard().handle()
                }
            }
        }.into();
    }

    let trait_name = parse_macro_input!(attr as syn::Path);
    let mut item = parse_macro_input!(item as ItemStruct);
    let struct_name = &item.ident;
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
    let has_name = format_ident!("Has{}Events", trait_ident);
    let set_field = format_ident!("{}", snake(&trait_ident.to_string()));

    match &mut item.fields {
        syn::Fields::Named(fields) => {
            fields.named.push(syn::parse_quote! {
                #set_field: #set_name
            });
        }

        syn::Fields::Unit => {
            let fields: syn::FieldsNamed = syn::parse_quote!({
                #set_field: #set_name
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

    quote! {
        #item

        impl #impl_generics ::eventful_rs::EventTarget for #struct_name #type_generics #where_clause {
            fn event_loop(&self) -> impl ::eventful_rs::EventLoopHandle {
                default_shard().handle()
            }
        }

        impl #impl_generics #has_name
            for #struct_name #type_generics
            #where_clause
        {
            fn #set_field(&self) -> &#set_name {
                &self.#set_field
            }
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
