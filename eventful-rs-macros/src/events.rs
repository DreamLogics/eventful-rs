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
/// supported. User-declared generic parameters are not supported. The macro adds
/// a defaulted receiver-role parameter: `Interface<Role = ()>`. Select a role with
/// `signal.connect_as::<Role, _>(&listener)`, or use `connect_fn` with a method or
/// `Fn(&Listener, Args...) + Send + Sync + 'static` callback. Both dispatch on the
/// listener's shard and hold it weakly. Optional `#[events(ExtraTrait)]` receiver
/// bounds also apply to callbacks. Ordinary `connect` selects the default `()` role.
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
    // Include macro arguments and label attributes before removing them below.
    let declaration = quote!(#trait_item #event_trait).to_string();
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

    // Reserve a parameter name absent from the declaration, including payload paths.
    let mut role = format_ident!("__EventfulRole");
    while declaration
        .split(|c: char| !c.is_alphanumeric() && c != '_')
        .any(|word| role == word)
    {
        role = format_ident!("{}_", role);
    }
    trait_item
        .generics
        .params
        .push(syn::parse_quote!(#role = ()));
    let extra_bound = event_trait.as_ref().map(|path| quote!(+ #path));
    let callback_bound = quote! {
        #runtime::Eventful + #runtime::HasEvents<T::EventSetType>
            #extra_bound + Sized + 'static
    };

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

    // Bulk connections require at least one signal enabled in this build.
    let enabled = methods
        .iter()
        .map(|(_, _, _, _, attrs)| {
            let predicates = attrs
                .iter()
                .map(|attr| crate::attributes::predicate(&attr.meta))
                .collect::<syn::Result<Vec<_>>>()?;
            Ok(quote!(all(#(#predicates),*)))
        })
        .collect::<syn::Result<Vec<_>>>();
    let enabled = match enabled {
        Ok(enabled) => enabled,
        Err(error) => return error.to_compile_error().into(),
    };
    let bulk_cfg = quote!(#[cfg(any(#(#enabled),*))]);

    let mut signal_defs = Vec::new();
    let mut set_fields = Vec::new();
    let mut extension_signal_methods = Vec::new();
    let mut extension_emitter_methods = Vec::new();

    for ((method_name, signal_name, arg_names, arg_types, cfg), label) in
        methods.iter().zip(&labels)
    {
        let target_ident = fresh_target(arg_names);
        let dispatch_args = (0..arg_names.len())
            .map(|index| format_ident!("__eventful_arg_{}", index))
            .collect::<Vec<_>>();
        let plain_name = method_name.to_string();
        let plain_name = plain_name.strip_prefix("r#").unwrap_or(&plain_name);
        let emit_tracked_name = format_ident!("emit_{}_tracked", plain_name);
        let emit_name = format_ident!("emit_{}", plain_name);

        let trait_bound = quote!(#callback_bound + #trait_name);
        let role_bound = quote!(#callback_bound + #trait_name<#role>);
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
        let labelled_connect = label.as_ref().map(|ty| {
            quote! {
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
                    self.connect_labelled_as::<(), T>(target, label)
                }

                /// Subscribe to matching labels using the selected receiver role.
                pub fn connect_labelled_as<#role: 'static, T>(
                    &self, target: &impl #runtime::Sharded<T>, label: #ty,
                ) -> #runtime::Connection<(#(#arg_types,)*), #ty>
                where
                    T: #role_bound,
                    #(#arg_types: Clone + Send + 'static,)*
                {
                    self.connect_labelled_fn(target, label, <T as #trait_name<#role>>::#method_name)
                }

                /// Subscribe a callback to matching labels on the receiver's shard.
                pub fn connect_labelled_fn<T>(
                    &self, target: &impl #runtime::Sharded<T>, label: #ty,
                    callback: impl Fn(&T, #(#arg_types),*) + Send + Sync + 'static,
                ) -> #runtime::Connection<(#(#arg_types,)*), #ty>
                where
                    T: #callback_bound,
                    #(#arg_types: Clone + Send + 'static,)*
                {
                    self.connect_callback(target, Some(label), callback)
                }
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
                    self.connect_as::<(), T>(target)
                }

                /// Subscribe using the selected receiver role.
                pub fn connect_as<#role: 'static, T>(
                    &self, target: &impl #runtime::Sharded<T>,
                ) -> #runtime::Connection<(#(#arg_types,)*), #label_type>
                where
                    T: #role_bound,
                    #(#arg_types: Clone + Send + 'static,)*
                {
                    self.connect_fn(target, <T as #trait_name<#role>>::#method_name)
                }

                /// Run a method or capturing callback on the receiver's shard.
                /// Captures must be Send + Sync; the receiver itself need not be.
                pub fn connect_fn<T>(
                    &self, target: &impl #runtime::Sharded<T>,
                    callback: impl Fn(&T, #(#arg_types),*) + Send + Sync + 'static,
                ) -> #runtime::Connection<(#(#arg_types,)*), #label_type>
                where
                    T: #callback_bound,
                    #(#arg_types: Clone + Send + 'static,)*
                {
                    self.connect_callback(target, None, callback)
                }

                /// Share callback storage across ordinary and tracked dispatch.
                fn connect_callback<T>(
                    &self, target: &impl #runtime::Sharded<T>,
                    subscription: Option<#label_type>,
                    callback: impl Fn(&T, #(#arg_types),*) + Send + Sync + 'static,
                ) -> #runtime::Connection<(#(#arg_types,)*), #label_type>
                where
                    T: #callback_bound,
                    #(#arg_types: Clone + Send + 'static,)*
                {
                    let handle = #runtime::ShardHandle::downgrade(&#runtime::Sharded::to_handle(target));
                    let tracked_handle = handle.clone();
                    let callback = ::std::sync::Arc::new(callback);
                    let tracked_callback = callback.clone();
                    self.inner.add_labelled_tracked_connection(subscription, move |(#(#dispatch_args,)*)| {
                        let callback = callback.clone();
                        #runtime::ShardHandle::upgrade_in_shard(&handle, move |#target_ident| {
                            callback(#target_ident, #(#dispatch_args),*);
                        });
                    }, move |(#(#dispatch_args,)*)| {
                        let callback = tracked_callback.clone();
                        tracked_handle.try_deferred_upgrade_in_shard(async move |#target_ident| {
                            callback(#target_ident, #(#dispatch_args),*);
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
            #(#cfg)* group.push(self.#name.connect_as::<#role, T>(target));
        }
    });
    let accessors = methods.iter().map(|(name, signal, _, _, cfg)| {
        quote! {
            /// Borrow a signal without replacing its subscription storage.
            #(#cfg)*
            #visibility fn #name(&self) -> &#signal { &self.#name }
        }
    });
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
        impl #set_name {
            #(#accessors)*

            #bulk_cfg
            /// Subscribe the receiver to every signal using the selected role.
            pub fn connect_events_as<#role: 'static, T>(
                &self, target: &impl #runtime::Sharded<T>,
            ) -> #runtime::ConnectionGroup
            where
                T: #callback_bound + #trait_name<#role>,
            {
                <Self as #runtime::ConnectEventsAs<T, #role>>::connect_events_as(self, target)
            }
        }

        #(#item_cfg)*
        #bulk_cfg
        impl<T> #runtime::ConnectEvents<T> for #set_name
        where
            T: #runtime::Eventful + #runtime::HasEvents<T::EventSetType>
                + #trait_name #extra_bound + 'static,
        {
            fn connect_events<S: #runtime::Sharded<T>>(&self, target: &S)
                -> #runtime::ConnectionGroup
            {
                self.connect_events_as::<(), T>(target)
            }
        }

        #(#item_cfg)*
        #bulk_cfg
        impl<T, #role: 'static> #runtime::ConnectEventsAs<T, #role> for #set_name
        where
            T: #callback_bound + #trait_name<#role>,
        {
            fn connect_events_as<S: #runtime::Sharded<T>>(&self, target: &S)
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
