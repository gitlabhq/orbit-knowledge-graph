//! Proc-macro derives for the compiler pipeline.
//!
//! - `#[derive(PipelineEnv)]`   — implements capability traits + `new()` constructor
//! - `#[derive(PipelineState)]` — wraps fields in `Option`, implements capability traits + `from_*()` constructors

use proc_macro::TokenStream;
use quote::{format_ident, quote};
use syn::{parse_macro_input, Data, DeriveInput, Fields};

fn require_named_fields(
    input: &DeriveInput,
) -> &syn::punctuated::Punctuated<syn::Field, syn::Token![,]> {
    match &input.data {
        Data::Struct(data) => match &data.fields {
            Fields::Named(fields) => &fields.named,
            _ => panic!("derive macro requires named fields"),
        },
        _ => panic!("derive macro requires a struct"),
    }
}

/// Extract `T` from `Option<T>`. Returns `None` if the type isn't `Option<_>`.
fn peel_option(ty: &syn::Type) -> Option<&syn::Type> {
    if let syn::Type::Path(syn::TypePath { path, .. }) = ty {
        let seg = path.segments.last()?;
        if seg.ident != "Option" {
            return None;
        }
        if let syn::PathArguments::AngleBracketed(args) = &seg.arguments {
            if let Some(syn::GenericArgument::Type(inner)) = args.args.first() {
                return Some(inner);
            }
        }
    }
    None
}

fn to_pascal(field_name: &str) -> String {
    field_name
        .split('_')
        .map(|seg| {
            let mut chars = seg.chars();
            match chars.next() {
                Some(c) => c.to_uppercase().to_string() + chars.as_str(),
                None => String::new(),
            }
        })
        .collect()
}

/// Derive `PipelineEnv` + capability trait impls + `new()` constructor.
///
/// For each field `foo: T`, generates `impl HasFoo for MyEnv`.
/// The capability traits themselves are defined via `define_env_capabilities!`
/// in the compiler crate.
#[proc_macro_derive(PipelineEnv)]
pub fn derive_pipeline_env(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let name = &input.ident;
    let fields = require_named_fields(&input);

    let mut trait_impls = Vec::new();
    let mut ctor_params = Vec::new();
    let mut ctor_fields = Vec::new();

    for field in fields {
        let field_ident = field.ident.as_ref().unwrap();
        let field_ty = &field.ty;
        let trait_name = format_ident!("Has{}", to_pascal(&field_ident.to_string()));

        trait_impls.push(quote! {
            impl #trait_name for #name {
                fn #field_ident(&self) -> &#field_ty {
                    &self.#field_ident
                }
            }
        });

        ctor_params.push(quote! { #field_ident: #field_ty });
        ctor_fields.push(quote! { #field_ident });
    }

    let expanded = quote! {
        impl PipelineEnv for #name {}

        #(#trait_impls)*

        impl #name {
            pub fn new(#(#ctor_params),*) -> Self {
                Self { #(#ctor_fields),* }
            }
        }
    };

    TokenStream::from(expanded)
}

/// Derive `PipelineState` + capability trait impls + `from_*()` constructors.
///
/// Fields must be `Option<T>`. The macro peels the `Option` to determine
/// the inner type `T` for trait signatures.
///
/// For each field `foo: Option<T>`, generates `impl HasFoo for MyState`
/// with get/set/take methods. The capability traits and `Seal*` types
/// are defined via `define_state_capabilities!` in the compiler crate.
#[proc_macro_derive(PipelineState)]
pub fn derive_pipeline_state(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let name = &input.ident;
    let fields = require_named_fields(&input);

    let field_list: Vec<_> = fields.iter().collect();
    let mut trait_impls = Vec::new();
    let mut from_constructors = Vec::new();

    for field in &field_list {
        let field_ident = field.ident.as_ref().unwrap();
        let inner_ty = peel_option(&field.ty)
            .unwrap_or_else(|| panic!("PipelineState fields must be Option<T>"));
        let field_name_str = field_ident.to_string();
        let trait_name = format_ident!("Has{}", to_pascal(&field_name_str));
        let getter_mut = format_ident!("{}_mut", field_ident);
        let setter = format_ident!("set_{}", field_ident);
        let taker = format_ident!("take_{}", field_ident);

        trait_impls.push(quote! {
            impl #trait_name for #name {
                fn #field_ident(&self) -> Result<&#inner_ty> {
                    self.#field_ident.as_ref()
                        .ok_or_else(|| QueryError::PipelineInvariant(
                            format!("{} not yet populated", #field_name_str)
                        ))
                }

                fn #getter_mut(&mut self) -> Result<&mut #inner_ty> {
                    self.#field_ident.as_mut()
                        .ok_or_else(|| QueryError::PipelineInvariant(
                            format!("{} not yet populated", #field_name_str)
                        ))
                }

                fn #setter(&mut self, value: #inner_ty) {
                    self.#field_ident = Some(value);
                }

                fn #taker(&mut self) -> Result<#inner_ty> {
                    self.#field_ident.take()
                        .ok_or_else(|| QueryError::PipelineInvariant(
                            format!("{} not yet populated", #field_name_str)
                        ))
                }
            }
        });

        let from_fn = format_ident!("from_{}", field_ident);
        let other_fields: Vec<_> = field_list
            .iter()
            .filter(|f| f.ident.as_ref().unwrap() != field_ident)
            .map(|f| {
                let id = f.ident.as_ref().unwrap();
                quote! { #id: None }
            })
            .collect();

        from_constructors.push(quote! {
            pub fn #from_fn(value: impl Into<#inner_ty>) -> Self {
                Self {
                    #field_ident: Some(value.into()),
                    #(#other_fields),*
                }
            }
        });
    }

    let expanded = quote! {
        impl PipelineState for #name {}

        #(#trait_impls)*

        impl #name {
            #(#from_constructors)*
        }
    };

    TokenStream::from(expanded)
}
