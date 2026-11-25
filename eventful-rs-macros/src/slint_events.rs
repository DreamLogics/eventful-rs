//! Generate typed event sources from explicitly declared Slint callbacks.
use proc_macro::TokenStream;
use quote::{format_ident, quote};
use syn::{
    FnArg, ItemTrait, Pat, Path, Token, TraitItem, parse::Parse, parse::ParseStream,
    parse_macro_input,
};

/// The Slint type whose generated `on_*` methods install the callbacks.
enum Target {
    /// Rust path of a Slint-generated component, passed as `&Component`.
    Component(Path),
    /// Rust path of a Slint-generated global, passed as its handle `&Global<'_>`.
    Global(Path),
}

/// Parsed `component = Path` or `global = Path` attribute arguments.
struct Arguments {
    /// Exactly one component or global.
    target: Target,
}
impl Parse for Arguments {
    fn parse(input: ParseStream<'_>) -> syn::Result<Self> {
        /// Message for missing, unknown, or conflicting keys.
        const EXPECTED: &str =
            "expected exactly one of `component = ComponentType` or `global = GlobalType`";
        let mut target: Option<Target> = None;
        while !input.is_empty() {
            let key: syn::Ident = input.parse()?;
            let is_global = if key == "component" {
                false
            } else if key == "global" {
                true
            } else {
                return Err(syn::Error::new_spanned(key, EXPECTED));
            };
            input.parse::<Token![=]>()?;
            let path: Path = input.parse()?;
            match (&target, is_global) {
                (None, _) => {}
                (Some(Target::Component(_)), false) | (Some(Target::Global(_)), true) => {
                    return Err(syn::Error::new_spanned(
                        &key,
                        format!("duplicate `{key}` argument"),
                    ));
                }
                (Some(_), _) => {
                    return Err(syn::Error::new_spanned(
                        key,
                        "give either `component = ComponentType` or `global = GlobalType`, not both",
                    ));
                }
            }
            target = Some(if is_global {
                Target::Global(path)
            } else {
                Target::Component(path)
            });
            if input.is_empty() {
                break;
            }
            input.parse::<Token![,]>()?;
        }
        match target {
            Some(target) => Ok(Self { target }),
            None => Err(syn::Error::new(proc_macro2::Span::call_site(), EXPECTED)),
        }
    }
}

/// Expand the ordinary event interface plus an owner for weak callback forwarding.
pub(crate) fn slint_events(attr: TokenStream, item: TokenStream) -> TokenStream {
    let Arguments { target } = parse_macro_input!(attr as Arguments);
    let interface = parse_macro_input!(item as ItemTrait);
    match expand(target, interface) {
        Ok(tokens) => tokens.into(),
        Err(error) => error.to_compile_error().into(),
    }
}

/// Generate a bridge without referring to the downstream Slint dependency name.
fn expand(target: Target, interface: ItemTrait) -> syn::Result<proc_macro2::TokenStream> {
    let runtime = crate::runtime_path();
    let name = &interface.ident;
    let visibility = &interface.vis;
    let bridge = format_ident!("{}Bridge", name);
    let set = format_ident!("{}EventSet", name);
    let emissions = format_ident!("{}Emissions", name);
    let connections = format_ident!("{}Connections", name);
    let signals_ext = format_ident!("{}SignalsExt", name);
    let item_cfg = crate::attributes::conditions(&interface.attrs)?;
    let (handle, handle_type, kind) = match &target {
        Target::Component(path) => (format_ident!("component"), quote!(#path), "component"),
        Target::Global(path) => (format_ident!("global"), quote!(#path<'_>), "global"),
    };
    let struct_doc =
        format!(" Owns forwarding from the {kind}'s declared callbacks into typed events.");
    let new_doc = format!(" No {kind} or receiver is retained by the installed callbacks.");
    let mut registrations = Vec::new();
    let mut enabled = Vec::new();
    for item in &interface.items {
        let TraitItem::Fn(method) = item else {
            continue;
        };
        for attr in &method.attrs {
            if attr.path().is_ident("with_label") {
                return Err(syn::Error::new_spanned(
                    attr,
                    "Slint callback bridges do not support event labels",
                ));
            }
        }
        if !matches!(method.sig.output, syn::ReturnType::Default) {
            return Err(syn::Error::new_spanned(
                &method.sig.output,
                "Slint event callbacks must omit the return type; callbacks returning a value require a synchronous handler",
            ));
        }
        let cfg = crate::attributes::conditions(&method.attrs)?;
        let predicates = cfg
            .iter()
            .map(|attr| crate::attributes::predicate(&attr.meta))
            .collect::<syn::Result<Vec<_>>>()?;
        enabled.push(quote!(all(#(#predicates),*)));
        let method_name = &method.sig.ident;
        let plain_name = method_name.to_string();
        let setter = format_ident!(
            "on_{}",
            plain_name.strip_prefix("r#").unwrap_or(&plain_name)
        );
        let mut args = Vec::new();
        for input in &method.sig.inputs {
            if let FnArg::Typed(arg) = input {
                let Pat::Ident(pat) = arg.pat.as_ref() else {
                    return Err(syn::Error::new_spanned(
                        &arg.pat,
                        "event arguments must be simple identifiers",
                    ));
                };
                args.push(pat.ident.clone());
            }
        }
        if method_name == "connect_to" {
            return Err(syn::Error::new_spanned(
                method_name,
                "event method name `connect_to` is reserved by the generated Slint bridge; rename the event",
            ));
        }
        let source = crate::fresh_target(&args);
        registrations.push(quote! {
            #(#cfg)*
            {
                let #source = ::std::rc::Rc::downgrade(&events);
                #handle.#setter(move |#(#args),*| {
                    if let ::core::option::Option::Some(#source) = #source.upgrade() {
                        #source.#method_name().emit(#(#args),*);
                    }
                });
            }
        });
    }
    // Reuse all event validation, signal generation, role dispatch, and cfg handling.
    // Validation errors stop here so the bridge does not add cascading errors.
    let events = crate::events::expand(None, interface.clone())?;
    Ok(quote! {
        #events

        #[doc = #struct_doc]
        /// Keep this bridge alive, typically in the wrapper struct. It retains no UI
        /// component or global handle. Dropping it disables forwarding even if signal storage survives.
        /// Installing a bridge replaces existing handlers for the declared callbacks.
        /// Dropping an old bridge does not remove handlers installed afterwards.
        #(#item_cfg)*
        #[derive(::core::fmt::Debug)]
        #visibility struct #bridge {
            /// A separate local lifetime token prevents retained event storage from
            /// extending callback forwarding after this bridge is dropped.
            events: ::std::rc::Rc<#emissions>,
        }

        #(#item_cfg)*
        impl #bridge {
            /// Install forwarding on the UI thread for each declared Slint callback.
            /// Handlers are replaced, not chained. Delivery to receivers is queued.
            #[doc = #new_doc]
            #visibility fn new(#handle: &#handle_type) -> Self {
                let events = ::std::rc::Rc::new(<#emissions as ::core::default::Default>::default());
                #(#registrations)*
                Self { events }
            }

            /// Connect all bridged events to a receiver held weakly.
            /// Dropping the returned token does not disconnect it; use scoped() or
            /// disconnect() for explicit cleanup. Keep this bridge alive to forward.
            #[cfg(any(#(#enabled),*))]
            #visibility fn connect_to<T, S>(&self, receiver: &S) -> #runtime::ConnectionGroup
            where
                T: #runtime::Eventful + #runtime::HasEvents<T::EventSetType> + 'static,
                S: #runtime::Sharded<T>,
                #set: #runtime::ConnectEvents<T>,
            {
                #runtime::ConnectEvents::connect_events(&**self.events.signals(), receiver)
            }

            /// Select a role for every bridged event.
            #[cfg(any(#(#enabled),*))]
            #visibility fn role<Role>(&self) -> #connections<'_, Role> {
                self.events.signals().role()
            }

        }

        #(#item_cfg)*
        impl #signals_ext for #bridge {}

        #(#item_cfg)*
        impl #runtime::HasEvents<#set> for #bridge {
            fn events(&self) -> &::std::sync::Arc<#set> { self.events.signals() }
        }
    })
}
