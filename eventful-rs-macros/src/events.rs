//! Expansion of the events macro family.
use proc_macro::TokenStream;
use quote::{format_ident, quote};
use syn::{FnArg, ItemTrait, Pat, TraitItem, Type, parse_macro_input};

use crate::pascal;
/// Define a typed event interface with synchronous `&self` methods.
/// Generated signal-access extension traits and emission sets inherit the interface
/// visibility, so private interfaces can use private payload types.
///
/// Concrete argument types such as `Vec<String>` and `Option<Vec<String>>` are
/// supported. User-declared generic parameters are not supported. The macro adds
/// a defaulted receiver-role parameter: `Interface<Role = ()>`. Select a role with
/// `signal.role::<Role>().connect(&listener)`, or use `with_receiver(...).connect(...)` with a method or
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
/// The type must implement `eventful_rs::EventLabel`. Emission builders select
/// labels with `.labelled(label)`; handler signatures stay unchanged.
/// `signal.labelled(subscription).connect(&listener)` delivers only
/// when `subscription.matches(&emitted)` returns true. Ordinary `connect` and
/// bulk connections subscribe to every label. Matching occurs on the emitting
/// thread before cloning arguments or submitting work to the receiver's shard.
///
/// The interface must be declared at module level: generated items live in a
/// private submodule, which cannot name items declared inside a function body.
pub(crate) fn events(attr: TokenStream, item: TokenStream) -> TokenStream {
    let event_trait: Option<syn::Path> = if attr.is_empty() {
        None
    } else {
        Some(parse_macro_input!(attr as syn::Path))
    };
    let trait_item = parse_macro_input!(item as ItemTrait);
    match expand(event_trait, trait_item) {
        Ok(tokens) => tokens.into(),
        Err(error) => error.to_compile_error().into(),
    }
}

/// Event method names that would collide with generated inherent or trait methods
/// (`EventSet::role`, `Emissions::signals`, and `HasEvents::events`).
const RESERVED_METHODS: &[&str] = &["role", "signals", "events"];

/// Expand a parsed event interface; shared with the Slint bridge generator.
pub(crate) fn expand(
    event_trait: Option<syn::Path>,
    mut trait_item: ItemTrait,
) -> syn::Result<proc_macro2::TokenStream> {
    let runtime = crate::runtime_path();
    // Include macro arguments and label attributes before removing them below.
    let declaration = quote!(#trait_item #event_trait).to_string();
    if !trait_item.generics.params.is_empty() {
        return Err(syn::Error::new_spanned(
            &trait_item.generics,
            "generic event traits are not supported",
        ));
    }
    let mut signal_names = std::collections::HashMap::<String, syn::Ident>::new();
    for item in &trait_item.items {
        let TraitItem::Fn(method) = item else {
            return Err(syn::Error::new_spanned(
                item,
                "event traits may only contain methods",
            ));
        };
        let sig = &method.sig;
        let plain_name = sig.ident.to_string();
        let plain_name = plain_name.strip_prefix("r#").unwrap_or(&plain_name);
        if RESERVED_METHODS.contains(&plain_name) {
            return Err(syn::Error::new_spanned(
                &sig.ident,
                format!(
                    "event method name `{plain_name}` is reserved by the generated event API; rename the event"
                ),
            ));
        }
        if let Some(previous) = signal_names.insert(pascal(plain_name), sig.ident.clone()) {
            return Err(syn::Error::new_spanned(
                &sig.ident,
                format!(
                    "event methods `{previous}` and `{}` generate the same signal type name",
                    sig.ident
                ),
            ));
        }
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
            return Err(error);
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
                    return Err(syn::Error::new_spanned(
                        attr,
                        "duplicate with_label attribute",
                    ));
                }
                label = Some(attr.parse_args::<Type>()?);
            }
        }
        method
            .attrs
            .retain(|attr| !attr.path().is_ident("with_label"));
        labels.push(label);
    }
    let item_cfg = crate::attributes::conditions(&trait_item.attrs)?;
    let trait_name = &trait_item.ident;
    let visibility = &trait_item.vis;
    let set_name = format_ident!("{}EventSet", trait_name);
    let ext_signals_name = format_ident!("{}SignalsExt", trait_name);

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
        .collect::<Result<Vec<_>, syn::Error>>()?;

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
        .collect::<syn::Result<Vec<_>>>()?;
    let bulk_cfg = quote!(#[cfg(any(#(#enabled),*))]);

    let module = format_ident!("__eventful_{}", trait_name);
    let connections = format_ident!("{}Connections", trait_name);
    let emissions = format_ident!("{}Emissions", trait_name);
    let receiver_bound = format_ident!("__{}Receiver", trait_name);
    let mut aliases = Vec::new();
    let mut definitions = Vec::new();
    let exports = vec![
        quote!(#set_name),
        quote!(#emissions),
        quote!(#ext_signals_name),
        quote!(#connections),
    ];
    let mut fields = Vec::new();
    let mut defaults = Vec::new();
    let mut signal_accessors = Vec::new();
    let mut emission_accessors = Vec::new();
    let mut extension_accessors = Vec::new();
    let mut bulk_calls = Vec::new();

    for ((method, signal, names, types, cfg), label) in methods.iter().zip(&labels) {
        let emitter = format_ident!("{}Emission", signal);
        let label_alias = format_ident!("__{}Label", signal);
        let types = types
            .iter()
            .enumerate()
            .map(|(i, ty)| {
                let alias = format_ident!("__{}Arg{}", signal, i);
                aliases.push(quote! {
                    #(#item_cfg)* #(#cfg)* #[doc(hidden)] #[allow(non_camel_case_types)]
                    #visibility type #alias = #ty;
                });
                alias
            })
            .collect::<Vec<_>>();
        let label_ty = label.as_ref().map(|l| quote!(#l)).unwrap_or(quote!(()));
        aliases.push(quote! {
            #(#item_cfg)* #(#cfg)* #[doc(hidden)] #[allow(non_camel_case_types)]
            #visibility type #label_alias = #label_ty;
        });
        let dispatch_args = (0..names.len())
            .map(|i| format_ident!("__eventful_arg_{}", i))
            .collect::<Vec<_>>();
        let args = quote!((#(#types,)*));
        let bounds = quote!(#(#types: ::core::clone::Clone + ::core::marker::Send + 'static,)*);
        let state = quote!(#runtime::__builders);
        let initial_label = if label.is_some() {
            quote!(#state::All)
        } else {
            quote!(#state::Selected<()>)
        };
        let initial_value = if label.is_some() {
            quote!(#state::All)
        } else {
            quote!(#state::Selected(()))
        };
        let selected_label = label.as_ref().map(|_| quote! {
            #(#cfg)*
            impl<D, R> #signal<#state::All, D, R> {
                /// Match emitted labels before scheduling this subscription.
                pub fn labelled(self, label: #label_alias) -> #signal<#state::Selected<#label_alias>, D, R> {
                    #signal { inner: self.inner, label: #state::Selected(label), destination: self.destination, role: self.role }
                }
            }
            #(#cfg)*
            impl<'a, M> #emitter<'a, #state::All, M> {
                /// Select the emitted routing label.
                pub fn labelled(self, label: #label_alias) -> #emitter<'a, #state::Selected<#label_alias>, M> {
                    #emitter { inner: self.inner, label: #state::Selected(label), mode: self.mode }
                }
            }
        });
        aliases.push(quote! { #(#item_cfg)* #(#cfg)* #[allow(unused_imports)] #visibility use #module::{#signal, #emitter}; });
        fields.push(quote!(#(#cfg)* #method: #runtime::Event<#args, #label_alias>));
        defaults.push(quote!(#(#cfg)* #method: ::core::default::Default::default()));
        signal_accessors.push(quote! {
            #(#cfg)*
            /// Select this signal for subscription.
            pub fn #method(&self) -> #signal {
                #signal { inner: self.#method.clone(), label: #state::All, destination: #state::Unselected, role: #state::NoRole }
            }
        });
        emission_accessors.push(quote! {
            #(#cfg)*
            /// Build an emission of this event.
            pub fn #method(&self) -> #emitter<'_, #initial_label> {
                #emitter { inner: &self.signals.#method, label: #initial_value, mode: #state::Untracked }
            }
        });
        extension_accessors.push(quote! {
            #(#cfg)*
            /// Select this signal for subscription.
            fn #method(&self) -> #signal { self.events().#method() }
        });
        bulk_calls
            .push(quote! { #(#cfg)* group.push(self.#method().role::<#role>().connect(target)); });
        definitions.push(quote! {
            #(#cfg)*
            /// Subscription builder. Configuration is consumed when connected.
            #[must_use = "call connect to register the subscription"]
            pub struct #signal<L = #state::All, D = #state::Unselected, R = #state::NoRole> {
                inner: #runtime::Event<#args, #label_alias>, label: L, destination: D, role: R,
            }
            #(#cfg)*
            impl<L, D, R> ::core::fmt::Debug for #signal<L, D, R> {
                fn fmt(&self, f: &mut ::core::fmt::Formatter<'_>) -> ::core::fmt::Result { f.debug_struct(::core::stringify!(#signal)).finish_non_exhaustive() }
            }
            #(#cfg)*
            impl<L, D, R> #signal<L, D, R> {
                /// Number of registered subscriptions, including expired weak receivers.
                pub fn connection_count(&self) -> usize { self.inner.connection_count() }
            }
            #(#cfg)*
            impl<L> #signal<L> {
                /// Run a standalone callback on this shard.
                pub fn on_shard(self, handle: &#runtime::ShardEventHandle) -> #signal<L, #runtime::ShardEventHandle> {
                    #signal { inner: self.inner, label: self.label, destination: handle.clone(), role: self.role }
                }
                /// Run a callback with a weak receiver on its shard.
                pub fn with_receiver<T>(self, target: &impl #runtime::Sharded<T>) -> #signal<L, #runtime::ShardWeakHandle<T>>
                where T: #receiver_bound {
                    #signal { inner: self.inner, label: self.label, destination: #runtime::ShardHandle::downgrade(&#runtime::Sharded::to_handle(target)), role: self.role }
                }
                /// Select the receiver's event-interface role.
                pub fn role<Role: 'static>(self) -> #signal<L, #state::Unselected, #state::Role<Role>> {
                    #signal { inner: self.inner, label: self.label, destination: self.destination, role: ::core::default::Default::default() }
                }
            }
            #(#cfg)*
            impl<L, R> #signal<L, #state::Unselected, R>
            where L: #state::SubscriptionLabel<#label_alias>, R: #state::ReceiverRole {
                /// Connect the receiver's interface handler, holding it weakly.
                pub fn connect<T>(self, target: &impl #runtime::Sharded<T>) -> #runtime::Connection<#args, #label_alias>
                where T: #receiver_bound + #trait_name<R::Type>, #bounds {
                    #state::connect_receiver(&self.inner, self.label.subscription(), target, move |target, (#(#dispatch_args,)*)| {
                        <T as #trait_name<R::Type>>::#method(target, #(#dispatch_args),*);
                    })
                }
            }
            #(#cfg)*
            impl<L> #signal<L, #runtime::ShardEventHandle>
            where L: #state::SubscriptionLabel<#label_alias> {
                /// Register a queued callback. Captures must be Send + Sync.
                pub fn connect(self, callback: impl ::core::ops::Fn(#(#types),*) + ::core::marker::Send + ::core::marker::Sync + 'static) -> #runtime::Connection<#args, #label_alias>
                where #bounds {
                    #state::connect_shard(&self.inner, self.label.subscription(), self.destination, move |(#(#dispatch_args,)*)| callback(#(#dispatch_args),*))
                }
            }
            #(#cfg)*
            impl<L, T> #signal<L, #runtime::ShardWeakHandle<T>>
            where L: #state::SubscriptionLabel<#label_alias>, T: #receiver_bound {
                /// Register a callback receiving the weak receiver and event arguments.
                pub fn connect(self, callback: impl ::core::ops::Fn(&T, #(#types),*) + ::core::marker::Send + ::core::marker::Sync + 'static) -> #runtime::Connection<#args, #label_alias>
                where #bounds {
                    #state::connect_receiver(&self.inner, self.label.subscription(), &self.destination, move |target, (#(#dispatch_args,)*)| callback(target, #(#dispatch_args),*))
                }
            }
            #selected_label
            #(#cfg)*
            /// Borrowed emission builder; emission access cannot be recovered from a signal.
            #[must_use = "call emit to dispatch this event"]
            pub struct #emitter<'a, L = #initial_label, M = #state::Untracked> {
                inner: &'a #runtime::Event<#args, #label_alias>, label: L, mode: M,
            }
            #(#cfg)*
            impl<L, M> ::core::fmt::Debug for #emitter<'_, L, M> {
                fn fmt(&self, f: &mut ::core::fmt::Formatter<'_>) -> ::core::fmt::Result { f.debug_struct(::core::stringify!(#emitter)).finish_non_exhaustive() }
            }
            #(#cfg)*
            impl<'a, L> #emitter<'a, L> {
                /// Observe completion and delivery errors after dispatch.
                pub fn tracked(self) -> #emitter<'a, L, #state::Tracked> {
                    #emitter { inner: self.inner, label: self.label, mode: #state::Tracked }
                }
                /// Convert to subscription access, preserving any selected label.
                pub fn signal(self) -> #signal<L> {
                    #signal { inner: self.inner.clone(), label: self.label, destination: #state::Unselected, role: #state::NoRole }
                }
            }
            #(#cfg)*
            impl #emitter<'_, #state::Selected<#label_alias>> {
                /// Queue this event for all matching subscriptions.
                pub fn emit(self, #(#names: #types),*) where #bounds {
                    self.inner.emit_labelled(self.label.0, (#(#names,)*));
                }
            }
            #(#cfg)*
            impl #emitter<'_, #state::Selected<#label_alias>, #state::Tracked> {
                /// Submit immediately; dropping the completion future does not cancel delivery.
                pub fn emit(self, #(#names: #types),*) -> impl ::core::future::Future<Output = ::core::result::Result<(), #runtime::DeliveryError>> + ::core::marker::Send + 'static + use<>
                where #bounds {
                    self.inner.emit_labelled_tracked(self.label.0, (#(#names,)*))
                }
            }
        });
    }
    Ok(quote! {
        #trait_item
        #(#item_cfg)*
        #[doc(hidden)]
        #visibility trait #receiver_bound: #runtime::Eventful + #runtime::HasEvents<Self::EventSetType> #extra_bound + ::core::marker::Sized + 'static {}
        #(#item_cfg)*
        impl<T> #receiver_bound for T where T: #runtime::Eventful + #runtime::HasEvents<T::EventSetType> #extra_bound + 'static {}
        #(#item_cfg)*
        #[allow(non_snake_case)]
        mod #module {
            use super::*;
            #(#definitions)*
            /// Subscription-only storage for this event interface.
            #[derive(::core::fmt::Debug)]
            pub struct #set_name { #(#fields,)* }
            impl ::core::default::Default for #set_name { fn default() -> Self { Self { #(#defaults,)* } } }
            impl #set_name { #(#signal_accessors)* }
            /// Source-owned emission access. Handles retain only its subscription view.
            #[derive(::core::fmt::Debug, ::core::default::Default)]
            pub struct #emissions { signals: ::std::sync::Arc<#set_name> }
            impl #emissions {
                /// Borrow the subscription-only event set.
                pub fn signals(&self) -> &::std::sync::Arc<#set_name> { &self.signals }
                #(#emission_accessors)*
            }
            /// Access individual signals through local or remote source handles.
            pub trait #ext_signals_name: #runtime::HasEvents<#set_name> { #(#extension_accessors)* }
            impl<T> #ext_signals_name for #runtime::ShardRc<T>
            where T: #runtime::Eventful<EventSetType = #set_name> + #runtime::HasEvents<#set_name> + 'static {}
            impl<T> #ext_signals_name for #runtime::ShardRcHandle<T>
            where T: #runtime::Eventful<EventSetType = #set_name> + #runtime::HasEvents<#set_name> + 'static {}
            #bulk_cfg
            impl<T> #runtime::ConnectEvents<T> for #set_name
            where T: #receiver_bound + #trait_name {
                fn connect_events<S: #runtime::Sharded<T>>(&self, target: &S) -> #runtime::ConnectionGroup {
                    <Self as #runtime::ConnectEventsAs<T, ()>>::connect_events_as(self, target)
                }
            }
            #bulk_cfg
            impl<T, #role: 'static> #runtime::ConnectEventsAs<T, #role> for #set_name
            where T: #receiver_bound + #trait_name<#role> {
                fn connect_events_as<S: #runtime::Sharded<T>>(&self, target: &S) -> #runtime::ConnectionGroup {
                    let mut group = #runtime::ConnectionGroup::default(); #(#bulk_calls)* group
                }
            }
            /// Role-selected whole-interface subscription builder.
            #[must_use = "call connect to register subscriptions"]
            pub struct #connections<'a, R> { source: &'a #set_name, role: ::core::marker::PhantomData<fn() -> R> }
            impl<R> ::core::fmt::Debug for #connections<'_, R> {
                fn fmt(&self, f: &mut ::core::fmt::Formatter<'_>) -> ::core::fmt::Result { f.debug_struct(::core::stringify!(#connections)).finish_non_exhaustive() }
            }
            #bulk_cfg
            impl<#role: 'static> #connections<'_, #role> {
                /// Connect all signals with the selected receiver role.
                pub fn connect<T>(self, target: &impl #runtime::Sharded<T>) -> #runtime::ConnectionGroup
                where T: #receiver_bound + #trait_name<#role> {
                    <#set_name as #runtime::ConnectEventsAs<T, #role>>::connect_events_as(self.source, target)
                }
            }
            #bulk_cfg
            impl #set_name {
                /// Select a role for connections to every event in this interface.
                pub fn role<R>(&self) -> #connections<'_, R> {
                    #connections { source: self, role: ::core::marker::PhantomData }
                }
            }

        }
        #(#item_cfg)*
        #[allow(unused_imports)]
        #visibility use #module::{#(#exports),*};
        #(#aliases)*
    })
}
