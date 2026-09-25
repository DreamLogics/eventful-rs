//! Expansion of the affinity macro family.
use proc_macro::TokenStream;
use quote::{format_ident, quote};
use syn::{ItemStruct, parse_macro_input, parse_quote};

/// Parsed optional interface and shard selection for an eventful struct.
#[derive(Default)]
struct EventfulArgs {
    /// Explicit event interface; absent means generate an empty interface.
    events: Option<syn::Path>,
    /// Explicit affinity; absent uses the module selection.
    shard: Option<syn::Path>,
}
impl syn::parse::Parse for EventfulArgs {
    fn parse(input: syn::parse::ParseStream) -> syn::Result<Self> {
        let mut args = Self::default();
        while !input.is_empty() {
            let path: syn::Path = input.parse()?;
            if input.peek(syn::Token![=]) {
                if !path.is_ident("shard") || args.shard.is_some() {
                    return Err(syn::Error::new_spanned(
                        path,
                        "expected one shard = Marker selection",
                    ));
                }
                input.parse::<syn::Token![=]>()?;
                args.shard = Some(input.parse()?);
            } else if args.events.is_none() {
                args.events = Some(path);
            } else {
                return Err(syn::Error::new_spanned(
                    path,
                    "expected one event interface",
                ));
            }
            if !input.is_empty() {
                input.parse::<syn::Token![,]>()?;
            }
        }
        Ok(args)
    }
}

/// Select a shard for eventful types in an inline module, including nested inline
/// modules. Paths are relative to the annotated module (e.g. super::Worker).
/// Explicit eventful selections and nested scopes override the inherited choice.
/// Out-of-line modules must select their own shard in their source file.
pub(crate) fn scope(attr: TokenStream, item: TokenStream) -> TokenStream {
    let args = parse_macro_input!(attr as EventfulArgs);
    let Some(shard) = args.shard.filter(|_| args.events.is_none()) else {
        return syn::Error::new(
            proc_macro2::Span::call_site(),
            "expected #[scope(shard = Marker)]",
        )
        .to_compile_error()
        .into();
    };
    let mut module = parse_macro_input!(item as syn::ItemMod);
    let Some((_, items)) = &mut module.content else {
        return syn::Error::new_spanned(
            module,
            "scope requires an inline module; select shards inside external module files",
        )
        .to_compile_error()
        .into();
    };
    if let Err(error) = apply_scope(items, &shard) {
        return error.to_compile_error().into();
    }
    quote!(#module).into()
}

/// Recursively attach affinity to inline structs, respecting explicit overrides.
fn apply_scope(items: &mut [syn::Item], shard: &syn::Path) -> syn::Result<()> {
    for item in items {
        match item {
            syn::Item::Struct(item) => {
                for attr in &mut item.attrs {
                    if attr
                        .path()
                        .segments
                        .last()
                        .is_some_and(|s| s.ident == "eventful")
                    {
                        let mut args = match &attr.meta {
                            syn::Meta::Path(_) => EventfulArgs::default(),
                            _ => attr.parse_args::<EventfulArgs>()?,
                        };
                        if args.shard.is_none() {
                            args.shard = Some(shard.clone());
                            let path = attr.path();
                            let events = args.events.map(|p| quote!(#p,));
                            *attr = parse_quote!(#[#path(#events shard = #shard)]);
                        }
                    }
                }
            }
            syn::Item::Mod(module) => {
                if module
                    .attrs
                    .iter()
                    .any(|a| a.path().segments.last().is_some_and(|s| s.ident == "scope"))
                {
                    continue;
                }
                if let Some((_, items)) = &mut module.content {
                    let mut nested = shard.clone();
                    if nested.leading_colon.is_none()
                        && nested.segments.first().is_some_and(|s| s.ident != "crate")
                    {
                        if nested.segments.first().is_some_and(|s| s.ident == "self") {
                            nested.segments = nested.segments.into_iter().skip(1).collect();
                        }
                        nested = parse_quote!(super::#nested);
                    }
                    apply_scope(items, &nested)?;
                }
            }
            _ => {}
        }
    }
    Ok(())
}

/// Add an `events` field and trait implementations needed for shard binding.
///
/// Pass an event interface, as in `#[eventful(ProducerEvents)]`, to let instances
/// of the annotated struct emit those events. Select its shard with `shard = Marker`
/// or an enclosing `#[scope(shard = Marker)]` attribute. Otherwise, the current
/// module must declare `file_scope!(shard = Marker);`.
/// Initialize the generated field with `events: Default::default()`.
pub(crate) fn eventful(attr: TokenStream, item: TokenStream) -> TokenStream {
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
    let args = parse_macro_input!(attr as EventfulArgs);
    let needs_to_gen_trait = args.events.is_none();
    let trait_name = args.events.unwrap_or_else(|| {
        let name = format_ident!("{}Events", struct_name);
        parse_quote!(#name)
    });
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

    let shard = args
        .shard
        .unwrap_or_else(|| parse_quote!(self::__EventfulFileShard));

    let gen_trait = if needs_to_gen_trait {
        let trait_ident_set = format_ident!("{}EventsEventSet", struct_name);
        quote! {
            /// Empty event interface for a value with no declared signals.
            pub trait #trait_ident {}

            /// Empty signal storage for this eventful value.
            pub struct #trait_ident_set {}

            impl<T> ::eventful_rs::ConnectEvents<T> for #trait_ident_set
            where
                T: ::eventful_rs::Eventful + ::eventful_rs::HasEvents<T::EventSetType> + 'static,
            {
                fn connect_events<S: ::eventful_rs::Sharded<T>>(&self, _target: &S)
                    -> ::eventful_rs::ConnectionGroup
                {
                    ::eventful_rs::ConnectionGroup::default()
                }
            }

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
            type Shard = #shard;
        }

    }
    .into()
}
