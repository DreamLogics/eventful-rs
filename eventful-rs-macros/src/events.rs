//! Expansion of the events macro family.
use proc_macro::TokenStream;
use quote::{format_ident, quote};
use syn::{FnArg, ItemTrait, Pat, TraitItem, Type, parse_macro_input};

use crate::{fresh_target, pascal};
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
pub(crate) fn events(attr: TokenStream, item: TokenStream) -> TokenStream {
    let runtime = crate::runtime_path();
    let event_trait: Option<syn::Path> = if attr.is_empty() {
        None
    } else {
        Some(parse_macro_input!(attr as syn::Path))
    };
    let mut trait_item = parse_macro_input!(item as ItemTrait);
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
        let error = if !sig.generics.params.is_empty() {
            Some(syn::Error::new_spanned(
                &sig.generics,
                "event methods cannot declare generic parameters; concrete argument types such as Vec<String> are supported",
            ))
        } else if sig.asyncness.is_some() {
            Some(syn::Error::new_spanned(
                sig,
                "event methods must be synchronous",
            ))
        } else if !matches!(sig.receiver(), Some(r) if matches!(r.kind, syn::ReceiverKind::Reference(_, _, None)))
        {
            Some(syn::Error::new_spanned(
                sig,
                "event methods require an &self receiver",
            ))
        } else if !matches!(&sig.output, syn::ReturnType::Default) {
            Some(syn::Error::new_spanned(
                &sig.output,
                "event methods must omit the return type",
            ))
        } else {
            None
        };
        if let Some(error) = error {
            return error.to_compile_error().into();
        }
    }
    let mut labels = Vec::new();
    for item in &mut trait_item.items {
        let TraitItem::Fn(method) = item else {
            unreachable!()
        };
        let mut label = None;
        for attr in &method.attrs {
            if attr.path().is_ident("with_label") {
                if label.is_some() {
                    return syn::Error::new_spanned(attr, "duplicate with_label attribute")
                        .to_compile_error()
                        .into();
                }
                match attr.parse_args::<Type>() {
                    Ok(ty) => label = Some(ty),
                    Err(error) => return error.to_compile_error().into(),
                }
            }
        }
        method
            .attrs
            .retain(|attr| !attr.path().is_ident("with_label"));
        labels.push(label);
    }
    let item_cfg = match crate::attributes::conditions(&trait_item.attrs) {
        Ok(attrs) => attrs,
        Err(e) => return e.to_compile_error().into(),
    };
    let trait_name = &trait_item.ident;
    let visibility = &trait_item.vis;
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
            let cfg = match crate::attributes::conditions(&method.attrs) {
                Ok(attrs) => attrs,
                Err(e) => return Some(Err(e)),
            };
            Some(Ok((method_name.clone(), signal_name, names, types, cfg)))
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

    for ((method_name, signal_name, arg_names, arg_types, cfg), label) in
        methods.iter().zip(&labels)
    {
        let target_ident = fresh_target(arg_names);
        let plain_name = method_name.to_string();
        let plain_name = plain_name.strip_prefix("r#").unwrap_or(&plain_name);
        let emit_tracked_name = format_ident!("emit_{}_tracked", plain_name);
        let emit_name = format_ident!("emit_{}", plain_name);

        let trait_bound = if let Some(event_trait) = &event_trait {
            quote! {
                #runtime::Eventful
                        + #runtime::HasEvents<T::EventSetType>
                        + #trait_name
                        + #event_trait
                        + Sized
                        + 'static
            }
        } else {
            quote! {
                #runtime::Eventful
                        + #runtime::HasEvents<T::EventSetType>
                        + #trait_name
                        + Sized
                        + 'static
            }
        };
        let label_type = label.as_ref().map(|ty| quote!(#ty)).unwrap_or(quote!(()));
        let mut reserved = arg_names.clone();
        reserved.push(target_ident.clone());
        let label_ident = fresh_target(&reserved);
        let label_param = label.as_ref().map(|ty| quote!(#label_ident: #ty,));
        let label_arg = label.as_ref().map(|_| quote!(#label_ident,));
        let dispatch_label = if label.is_some() {
            quote!(#label_ident)
        } else {
            quote!(())
        };
        let labelled_connect = label.as_ref().map(|ty| quote! {
            /// Subscribe using `subscription.matches(&emitted)` before queuing delivery.
            /// The receiver is held weakly. Dropping the connection keeps it active;
            /// use `disconnect` or `scoped` to remove the subscription.
            pub fn connect_labelled<T, S>(&self, target: &S, label: #ty)
                -> #runtime::Connection<(#(#arg_types,)*), #ty>
            where
                T: #trait_bound,
                S: #runtime::Sharded<T>,
                #(#arg_types: Clone + Send + 'static,)*
            {
                let handle = #runtime::ShardHandle::downgrade(&#runtime::Sharded::to_handle(target));
                let tracked_handle = handle.clone();
                self.inner.add_labelled_tracked_connection(Some(label), move |(#(#arg_names,)*)| {
                    #runtime::ShardHandle::upgrade_in_shard(&handle, move |#target_ident| {
                        #target_ident.#method_name(#(#arg_names),*);
                    });
                }, move |(#(#arg_names,)*)| {
                    tracked_handle.try_deferred_upgrade_in_shard(async move |#target_ident| {
                        #target_ident.#method_name(#(#arg_names),*);
                    })
                })
            }
        });
        signal_defs.push(quote! {
            /// Typed signal with weak listener connections and optional routing labels.
            #(#item_cfg)*
            #(#cfg)*
            #[derive(Debug)]
            #visibility struct #signal_name {
                inner: #runtime::Event<(#(#arg_types,)*), #label_type>,
            }

            #(#item_cfg)*
            #(#cfg)*
            impl ::core::default::Default for #signal_name {
                fn default() -> Self {
                    Self { inner: #runtime::Event::default() }
                }
            }

            #(#item_cfg)*
            #(#cfg)*
            impl #signal_name {
                /// Subscribe to every emission, regardless of its label.
                pub fn connect<T, S>(&self, target: &S) -> #runtime::Connection<(#(#arg_types,)*), #label_type>
                where
                    T: #trait_bound,
                    S: #runtime::Sharded<T>,
                    #(#arg_types: Clone + Send + 'static,)*
                {
                    let handle = #runtime::ShardHandle::downgrade(&#runtime::Sharded::to_handle(target));
                    let tracked_handle = handle.clone();
                    self.inner.add_tracked_connection(move |(#(#arg_names,)*)| {
                        #runtime::ShardHandle::upgrade_in_shard(&handle, move |#target_ident| {
                            #target_ident.#method_name(#(#arg_names),*);
                        });
                    }, move |(#(#arg_names,)*)| {
                        tracked_handle.try_deferred_upgrade_in_shard(async move |#target_ident| {
                            #target_ident.#method_name(#(#arg_names),*);
                        })
                    })
                }

                #labelled_connect

                /// Queue delivery to wildcard and matching subscriptions.
                /// A labelled signal takes its emitted label before the event arguments.
                pub fn emit(&self, #label_param #(#arg_names: #arg_types),*)
                where
                    #(#arg_types: Clone + Send + 'static,)*
                {
                    self.inner.emit_labelled(#dispatch_label, (#(#arg_names,)*));
                }

                /// Dispatch immediately and observe all selected deliveries.
                /// Rejected subscriptions are skipped; matching panics become delivery errors.
                pub fn emit_tracked(&self, #label_param #(#arg_names: #arg_types),*)
                    -> impl ::core::future::Future<Output = Result<(), #runtime::DeliveryError>> + Send + 'static + use<>
                where
                    #(#arg_types: Clone + Send + 'static,)*
                {
                    self.inner.emit_labelled_tracked(#dispatch_label, (#(#arg_names,)*))
                }
            }
        });

        set_fields.push(
            quote!(#(#cfg)* #[doc = "Signal storage for this event method."] #method_name: #signal_name),
        );
        extension_signal_methods.push(quote! {
            /// Access this event signal to connect listeners.
            #(#cfg)*
            fn #method_name(&self) -> &#signal_name {
                &self.events().#method_name
            }
        });
        extension_emitter_methods.push(quote! {
            /// Queue delivery; labelled signals take the label before event arguments.
            #(#cfg)*
            fn #emit_name(&self, #label_param #(#arg_names: #arg_types),*)
            where
                #(#arg_types: Clone + Send + 'static,)*
            {
                self.events().#method_name.emit(#label_arg #(#arg_names),*);
            }

            /// Dispatch immediately and await selected deliveries, reporting matching or handler errors.
            #(#cfg)*
            fn #emit_tracked_name(&self, #label_param #(#arg_names: #arg_types),*)
                -> impl ::core::future::Future<Output = Result<(), #runtime::DeliveryError>> + Send + 'static
            where
                #(#arg_types: Clone + Send + 'static,)*
            {
                self.events().#method_name.emit_tracked(#label_arg #(#arg_names),*)
            }
        });
    }

    let connect_calls = methods.iter().map(|(name, _, _, _, cfg)| {
        quote! {
            #(#cfg)* group.push(self.#name.connect(target));
        }
    });
    let accessors = methods.iter().map(|(name, signal, _, _, cfg)| {
        quote! {
            /// Borrow a signal without replacing its subscription storage.
            #(#cfg)*
            #visibility fn #name(&self) -> &#signal { &self.#name }
        }
    });
    let extra_bound = event_trait.as_ref().map(|path| quote!(+ #path));

    quote! {
        #trait_item

        #(#signal_defs)*

        /// Generated storage for all signals in the event interface.
        /// See the eventful-rs crate guide for a complete typed-event example.
        #(#item_cfg)*
        #[derive(Debug, Default)]
        #visibility struct #set_name {
            #(#set_fields,)*
        }

        #(#item_cfg)*
        impl #set_name { #(#accessors)* }

        #(#item_cfg)*
        impl<T> #runtime::ConnectEvents<T> for #set_name
        where
            T: #runtime::Eventful + #runtime::HasEvents<T::EventSetType>
                + #trait_name #extra_bound + 'static,
        {
            fn connect_events<S: #runtime::Sharded<T>>(&self, target: &S)
                -> #runtime::ConnectionGroup
            {
                let mut group = #runtime::ConnectionGroup::default();
                #(#connect_calls)*
                group
            }
        }

        /// Access individual signals through local values or remote handles.
        #(#item_cfg)*
        #visibility trait #ext_signals_name: #runtime::HasEvents<#set_name> {
            #(#extension_signal_methods)*
        }

        /// Emit ordinary or tracked events through shared signal storage.
        #(#item_cfg)*
        #visibility trait #ext_emitter_name: #runtime::HasEvents<#set_name>{
            #(#extension_emitter_methods)*
        }

        #(#item_cfg)*
        impl<T> #ext_signals_name for #runtime::ShardRc<T>
        where
            T: #runtime::Eventful<EventSetType = #set_name> + #runtime::HasEvents<#set_name> + Sized + 'static,
            #runtime::ShardRc<T>: #runtime::HasEvents<#set_name>,
        {}

        #(#item_cfg)*
        impl<T> #ext_signals_name for #runtime::ShardRcHandle<T>
        where
            T: #runtime::Eventful<EventSetType = #set_name> + #runtime::HasEvents<#set_name> + Sized + 'static,
            #runtime::ShardRcHandle<T>: #runtime::HasEvents<#set_name>,
        {}

        #(#item_cfg)*
        impl<T> #ext_signals_name for #runtime::ShardWeakHandle<T>
        where
            T: #runtime::Eventful<EventSetType = #set_name> + #runtime::HasEvents<#set_name> + Sized + 'static,
            #runtime::ShardWeakHandle<T>: #runtime::HasEvents<#set_name>,
        {}

        #(#item_cfg)*
        impl<T: #runtime::HasEvents<#set_name> + ?Sized> #ext_emitter_name for T {}
    }
    .into()
}
