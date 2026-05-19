use proc_macro::TokenStream;
use quote::{format_ident, quote};
use syn::{Data, DeriveInput, Fields, Lit, Meta, parse_macro_input};

fn require_named_fields(
    input: &DeriveInput,
) -> &syn::punctuated::Punctuated<syn::Field, syn::Token![,]> {
    match &input.data {
        Data::Struct(data) => match &data.fields {
            Fields::Named(fields) => &fields.named,
            _ => panic!("FromRecordBatch requires named fields"),
        },
        _ => panic!("FromRecordBatch requires a struct"),
    }
}

fn peel_option(ty: &syn::Type) -> Option<&syn::Type> {
    if let syn::Type::Path(syn::TypePath { path, .. }) = ty {
        let seg = path.segments.last()?;
        if seg.ident != "Option" {
            return None;
        }
        if let syn::PathArguments::AngleBracketed(args) = &seg.arguments
            && let Some(syn::GenericArgument::Type(inner)) = args.args.first()
        {
            return Some(inner);
        }
    }
    None
}

fn get_column_name(field: &syn::Field) -> String {
    for attr in &field.attrs {
        if !attr.path().is_ident("arrow") {
            continue;
        }
        if let Ok(Meta::NameValue(nv)) = attr.parse_args::<Meta>()
            && nv.path.is_ident("column")
            && let syn::Expr::Lit(syn::ExprLit {
                lit: Lit::Str(lit_str),
                ..
            }) = &nv.value
        {
            return lit_str.value();
        }
    }
    field.ident.as_ref().unwrap().to_string()
}

#[proc_macro_derive(FromRecordBatch, attributes(arrow))]
pub fn derive_from_record_batch(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let struct_name = &input.ident;
    let fields = require_named_fields(&input);

    let mut column_lookups = Vec::new();
    let mut required_checks = Vec::new();
    let mut field_extractions = Vec::new();

    for field in fields {
        let field_ident = field.ident.as_ref().unwrap();
        let column_name = get_column_name(field);
        let col_var = format_ident!("__col_{}", field_ident);
        let is_optional = peel_option(&field.ty).is_some();
        let inner_ty = peel_option(&field.ty).unwrap_or(&field.ty);

        column_lookups.push(quote! {
            let #col_var = batch.column_by_name(#column_name);
        });

        if !is_optional {
            required_checks.push(quote! {
                let #col_var = #col_var.ok_or_else(|| {
                    ::gkg_utils::arrow::extract::ArrowExtractError::MissingColumn {
                        column: #column_name.into(),
                    }
                })?;
            });

            field_extractions.push(quote! {
                let #field_ident = <#inner_ty as ::gkg_utils::arrow::extract::FromArrowArray>::from_arrow_array(
                    #col_var, __row
                )
                .map_err(|source| ::gkg_utils::arrow::extract::ArrowExtractError::Cell {
                    column: #column_name.into(),
                    row: __row,
                    source,
                })?
                .ok_or_else(|| ::gkg_utils::arrow::extract::ArrowExtractError::UnexpectedNull {
                    column: #column_name.into(),
                    row: __row,
                })?;
            });
        } else {
            required_checks.push(quote! {});

            field_extractions.push(quote! {
                let #field_ident = match #col_var {
                    Some(__col) => <#inner_ty as ::gkg_utils::arrow::extract::FromArrowArray>::from_arrow_array(
                        __col, __row
                    )
                    .map_err(|source| ::gkg_utils::arrow::extract::ArrowExtractError::Cell {
                        column: #column_name.into(),
                        row: __row,
                        source,
                    })?,
                    None => None,
                };
            });
        }
    }

    let field_names: Vec<_> = fields.iter().map(|f| f.ident.as_ref().unwrap()).collect();

    let expanded = quote! {
        impl ::gkg_utils::arrow::extract::FromRecordBatch for #struct_name {
            fn from_batches(
                batches: &[::arrow::record_batch::RecordBatch],
            ) -> ::std::result::Result<Vec<Self>, ::gkg_utils::arrow::extract::ArrowExtractError> {
                let mut __results = Vec::new();

                for batch in batches {
                    #(#column_lookups)*
                    #(#required_checks)*

                    for __row in 0..batch.num_rows() {
                        #(#field_extractions)*

                        __results.push(Self {
                            #(#field_names),*
                        });
                    }
                }

                Ok(__results)
            }
        }
    };

    TokenStream::from(expanded)
}
