//! Expansion of the dispatch macro family.
use proc_macro::TokenStream;
use quote::{format_ident, quote};
use syn::{
    Attribute, FnArg, ImplItemFn, ItemImpl, Pat, Safety, Type, parse_macro_input, parse_quote,
    visit::Visit, visit_mut::VisitMut,
};

use crate::fresh_target;
/// Generate handle wrappers for annotated methods in an inherent `impl`.
///
/// Mark methods with `#[asynced]` to await their results, or `#[action]` to queue
/// work from synchronous or async code without waiting. Original methods
/// keep their signatures. Unannotated methods remain accessible through references to shard-local
/// values, including inside `upgrade_in_shard` callbacks.
/// Dispatched methods require `&self` and `Send + 'static` arguments and results.
pub(crate) fn asynchronize(attr: TokenStream, item: TokenStream) -> TokenStream {
    let visibility = parse_macro_input!(attr as syn::Visibility);
    let item = parse_macro_input!(item as ItemImpl);
    match expand(visibility, item) {
        Ok(tokens) => tokens.into(),
        Err(error) => error.to_compile_error().into(),
    }
}

/// Dispatch style selected by a method annotation.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Kind {
    /// `#[action]`: queue without waiting.
    Action,
    /// `#[asynced]`: await the result.
    Asynced,
}

/// Classify a dispatch annotation by its final path segment.
fn kind(attr: &Attribute) -> Option<Kind> {
    let segment = attr.path().segments.last()?;
    if segment.ident == "action" {
        Some(Kind::Action)
    } else if segment.ident == "asynced" {
        Some(Kind::Asynced)
    } else {
        None
    }
}

/// Replace `Self` with the concrete struct type, since generated signatures live
/// in impls for handle types where `Self` names the handle.
struct ReplaceSelf<'a>(&'a Type);
impl ReplaceSelf<'_> {
    /// Rewrite `Self::Rest` into `<Struct>::Rest`, returning the rewritten segments.
    fn qualified(
        &self,
        qself: &Option<syn::QSelf>,
        path: &syn::Path,
    ) -> Option<proc_macro2::TokenStream> {
        let first = path.segments.first()?;
        if qself.is_some() || path.leading_colon.is_some() || first.ident != "Self" {
            return None;
        }
        let ty = self.0;
        let rest = path.segments.iter().skip(1);
        Some(quote!(<#ty> #(:: #rest)*))
    }
}
impl VisitMut for ReplaceSelf<'_> {
    fn visit_type_mut(&mut self, ty: &mut Type) {
        if matches!(ty, Type::Path(path) if path.qself.is_none() && path.path.is_ident("Self")) {
            *ty = self.0.clone();
            return;
        }
        syn::visit_mut::visit_type_mut(self, ty);
    }
    fn visit_type_path_mut(&mut self, path: &mut syn::TypePath) {
        if path.path.segments.len() > 1
            && let Some(tokens) = self.qualified(&path.qself, &path.path)
        {
            *path = parse_quote!(#tokens);
        }
        syn::visit_mut::visit_type_path_mut(self, path);
    }
    fn visit_expr_path_mut(&mut self, path: &mut syn::ExprPath) {
        if path.path.segments.len() > 1
            && let Some(tokens) = self.qualified(&path.qself, &path.path)
        {
            *path = parse_quote!(#tokens);
        }
        syn::visit_mut::visit_expr_path_mut(self, path);
    }
}

/// Find the first argument-position `impl Trait`.
struct FindImplTrait<'a>(Option<&'a syn::TypeImplTrait>);
impl<'a> Visit<'a> for FindImplTrait<'a> {
    fn visit_type_impl_trait(&mut self, ty: &'a syn::TypeImplTrait) {
        self.0.get_or_insert(ty);
    }
}

/// Attributes copied onto generated trait declarations. `expect` becomes `allow`
/// because the expected lint may not fire on the bodiless declaration.
fn forwarded_attributes(method: &ImplItemFn) -> Vec<Attribute> {
    method
        .attrs
        .iter()
        .filter_map(|attr| {
            let path = attr.path();
            if path.is_ident("expect") {
                let syn::Meta::List(list) = &attr.meta else {
                    return None;
                };
                let tokens = &list.tokens;
                return Some(parse_quote!(#[allow(#tokens)]));
            }
            [
                "doc",
                "deprecated",
                "must_use",
                "allow",
                "warn",
                "deny",
                "forbid",
            ]
            .iter()
            .any(|name| path.is_ident(name))
            .then(|| attr.clone())
        })
        .collect()
}

/// Validate one annotated method and build its trait signature and implementation.
fn dispatched(
    runtime: &syn::Path,
    struct_type: &Type,
    method: &ImplItemFn,
    kind: Kind,
) -> syn::Result<(proc_macro2::TokenStream, proc_macro2::TokenStream)> {
    let sig = &method.sig;
    if !matches!(sig.receiver(), Some(r) if matches!(r.kind, syn::ReceiverKind::Reference(_, _, None)))
        || matches!(sig.safety, Safety::Unsafe(_))
        || sig.constness.is_some()
        || sig.abi.is_some()
    {
        return Err(syn::Error::new_spanned(
            sig,
            "dispatched methods require a safe, non-const, non-extern &self receiver",
        ));
    }
    if !sig.generics.params.is_empty() {
        return Err(syn::Error::new_spanned(
            &sig.generics,
            "dispatched methods cannot declare generic or lifetime parameters",
        ));
    }
    let cfg = crate::attributes::conditions(&method.attrs)?;

    let mut trait_sig = sig.clone();
    let mut method_arg_names = Vec::new();
    for input in &mut trait_sig.inputs {
        let FnArg::Typed(arg) = input else {
            continue;
        };
        let mut finder = FindImplTrait(None);
        finder.visit_type(&arg.ty);
        if let Some(ty) = finder.0 {
            return Err(syn::Error::new_spanned(
                ty,
                "dispatched methods cannot take `impl Trait` arguments; use a concrete type",
            ));
        }
        let Pat::Ident(pat) = arg.pat.as_mut() else {
            return Err(syn::Error::new_spanned(
                &arg.pat,
                "dispatched method arguments must be simple identifiers",
            ));
        };
        if let Some((at, _)) = &pat.subpat {
            return Err(syn::Error::new_spanned(
                at,
                "dispatched method arguments cannot use `@` patterns",
            ));
        }
        // Binding modes belong to the original body; declarations take plain names.
        pat.by_ref = None;
        pat.mutability = None;
        method_arg_names.push(pat.ident.clone());
    }
    ReplaceSelf(struct_type).visit_signature_mut(&mut trait_sig);

    let method_name = &sig.ident;
    let target_ident = fresh_target(&method_arg_names);
    let is_async = sig.asyncness.is_some();
    let forwarded = forwarded_attributes(method);
    let (doc, body) = match kind {
        Kind::Action => {
            if matches!(&sig.output, syn::ReturnType::Type(_, ty)
                if !matches!(&**ty, Type::Tuple(tuple) if tuple.elems.is_empty()))
            {
                return Err(syn::Error::new_spanned(
                    &sig.output,
                    "action methods must return `()`",
                ));
            }
            trait_sig.asyncness = None;
            let body = if is_async {
                quote! {
                    #runtime::ShardHandle::upgrade_in_shard_async(self, async move |#target_ident| {
                        #target_ident.#method_name(#(#method_arg_names,)*).await;
                    });
                }
            } else {
                quote! {
                    #runtime::ShardHandle::upgrade_in_shard(self, move |#target_ident| {
                        #target_ident.#method_name(#(#method_arg_names,)*);
                    });
                }
            };
            (
                quote!(#[doc = " Queue this method on the value's shard without waiting."]),
                body,
            )
        }
        Kind::Asynced => {
            trait_sig.asyncness = Some(syn::token::Async::default());
            let call = if is_async {
                quote!(#target_ident.#method_name(#(#method_arg_names,)*).await)
            } else {
                quote!(#target_ident.#method_name(#(#method_arg_names,)*))
            };
            let body = quote! {
                #runtime::ShardHandle::deferred_upgrade_in_shard(self, async move |#target_ident| {
                    #call
                }).await
            };
            (
                quote!(#[doc = " Await this method on the value's shard; panics on dispatch failure."]),
                body,
            )
        }
    };
    let signature = quote! {
        #doc
        #(#cfg)*
        #(#forwarded)*
        #trait_sig;
    };
    // Forwarding to a deprecated original is not a new use of it.
    let allow_deprecated = method
        .attrs
        .iter()
        .any(|attr| attr.path().is_ident("deprecated"))
        .then(|| quote!(#[allow(deprecated)]));
    let implementation = quote! {
        #(#cfg)*
        #allow_deprecated
        #trait_sig { #body }
    };
    Ok((signature, implementation))
}

/// Expand a parsed inherent impl into the original impl plus the handle trait.
fn expand(
    visibility: syn::Visibility,
    mut item: ItemImpl,
) -> syn::Result<proc_macro2::TokenStream> {
    let runtime = crate::runtime_path();
    let item_cfg = crate::attributes::conditions(&item.attrs)?;
    if item.trait_.is_some() || !item.generics.params.is_empty() {
        return Err(syn::Error::new_spanned(
            &item,
            "asynchronize requires a non-generic inherent impl",
        ));
    }
    let struct_type = &item.self_ty;
    let Type::Path(type_path) = &**struct_type else {
        return Err(syn::Error::new_spanned(
            struct_type,
            "expected a struct type for the impl block",
        ));
    };
    let struct_name = &type_path.path.segments.last().unwrap().ident;
    let trait_name = format_ident!("{}Async", struct_name);

    let mut signatures = Vec::new();
    let mut implementations = Vec::new();
    for member in &item.items {
        let syn::ImplItem::Fn(method) = member else {
            continue;
        };
        let mut annotations = method
            .attrs
            .iter()
            .filter_map(|attr| kind(attr).map(|kind| (attr, kind)));
        let Some((_, kind)) = annotations.next() else {
            continue;
        };
        if let Some((extra, _)) = annotations.next() {
            return Err(syn::Error::new_spanned(
                extra,
                "a method takes at most one #[action] or #[asynced] annotation",
            ));
        }
        let (signature, implementation) = dispatched(&runtime, struct_type, method, kind)?;
        signatures.push(signature);
        implementations.push(implementation);
    }

    for member in &mut item.items {
        if let syn::ImplItem::Fn(method) = member {
            method.attrs.retain(|a| kind(a).is_none());
        }
    }
    if signatures.is_empty() {
        return Ok(quote!(#item));
    }
    let struct_type = &item.self_ty;

    Ok(quote! {
        #item

        /// Generated methods for dispatching calls through strong or weak handles.
        #[allow(async_fn_in_trait)]
        #(#item_cfg)*
        #visibility trait #trait_name {
            #(#signatures)*
        }

        #(#item_cfg)*
        impl #trait_name for #runtime::ShardWeakHandle<#struct_type> {
            #(#implementations)*
        }

        #(#item_cfg)*
        impl #trait_name for #runtime::ShardRcHandle<#struct_type> {
            #(#implementations)*
        }
    })
}
