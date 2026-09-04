use proc_macro::TokenStream;
use quote::{format_ident, quote};
use syn::{FnArg, ItemStruct, ItemTrait, Pat, TraitItem, Type, parse_macro_input};

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
    let ext_name = format_ident!("{}SignalsExt", trait_name);
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
    let mut extension_methods = Vec::new();

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
                pub fn connect<T>(&self, target: &::eventful_rs::Evr<T>)
                where
                    T: #trait_name + ::eventful_rs::EventTarget,
                    #(#arg_types: Clone + Send + 'static,)*
                {
                    let weak = target.weak();
                    let event_loop = target.event_loop();
                    self.inner.add_connection(move |(#(#arg_names,)*)| {
                        let weak = weak.clone();
                        let event_loop = event_loop.clone();
                        let _ = event_loop.post(move || {
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
        extension_methods.push(quote! {
            fn #method_name(&self) -> &#signal_name {
                &self.#set_field().#method_name
            }

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

        pub trait #ext_name: #has_name {
            #(#extension_methods)*
        }

        impl<T: #has_name + ?Sized> #ext_name for T {}
    }
    .into()
}

#[proc_macro_attribute]
pub fn with_events(attr: TokenStream, item: TokenStream) -> TokenStream {
    let trait_name = parse_macro_input!(attr as syn::Path);
    let mut item = parse_macro_input!(item as ItemStruct);

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

    let struct_name = &item.ident;
    let (impl_generics, type_generics, where_clause) = item.generics.split_for_impl();

    quote! {
        #item

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

#[proc_macro_attribute]
pub fn accept_events(attr: TokenStream, item: TokenStream) -> TokenStream {
    let event_loop = parse_macro_input!(attr as syn::Expr);
    let item = parse_macro_input!(item as ItemStruct);
    let name = &item.ident;
    let (impl_generics, type_generics, where_clause) = item.generics.split_for_impl();
    quote! {
        #item
        impl #impl_generics ::eventful_rs::EventTarget for #name #type_generics #where_clause {
            fn event_loop(&self) -> impl ::eventful_rs::EventLoopHandle {
                (#event_loop).handle()
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
