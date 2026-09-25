//! Propagate conditional compilation without copying item-specific attributes.
use syn::{Attribute, Meta, Token, parse_quote, punctuated::Punctuated};

/// Retain cfg predicates, including those nested inside cfg_attr.
pub(crate) fn conditions(attributes: &[Attribute]) -> syn::Result<Vec<Attribute>> {
    attributes
        .iter()
        .filter_map(|attr| match condition(&attr.meta) {
            Ok(Some(meta)) => Some(Ok(parse_quote!(#[#meta]))),
            Ok(None) => None,
            Err(error) => Some(Err(error)),
        })
        .collect()
}

/// Extract only the conditional-compilation portion of a possibly nested attribute.
fn condition(meta: &Meta) -> syn::Result<Option<Meta>> {
    if meta.path().is_ident("cfg") {
        return Ok(Some(meta.clone()));
    }
    if !meta.path().is_ident("cfg_attr") {
        return Ok(None);
    }
    let Meta::List(list) = meta else {
        return Ok(None);
    };
    let parts = list.parse_args_with(Punctuated::<Meta, Token![,]>::parse_terminated)?;
    let mut parts = parts.into_iter();
    let Some(predicate) = parts.next() else {
        return Err(syn::Error::new_spanned(
            meta,
            "cfg_attr requires a predicate",
        ));
    };
    let nested = parts
        .map(|part| condition(&part))
        .collect::<syn::Result<Vec<_>>>()?
        .into_iter()
        .flatten()
        .collect::<Vec<_>>();
    if nested.is_empty() {
        Ok(None)
    } else {
        Ok(Some(parse_quote!(cfg_attr(#predicate, #(#nested),*))))
    }
}
