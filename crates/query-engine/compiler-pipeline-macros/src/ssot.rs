//! `define_compiler_ctx!` proc macro implementation.
//!
//! Parses the DSL and generates:
//! - Per-phase view structs with compile-time field access enforcement
//! - Per-pipeline context structs with `new()` and `into_output()`
//! - Per-pipeline `run_<phase>` methods and `run_<pipeline>` runner functions

use proc_macro::TokenStream;
use quote::{format_ident, quote};
use syn::parse::{Parse, ParseStream};
use syn::{Ident, Result, Token, Type, braced, bracketed};

use crate::to_pascal;

// ─────────────────────────────────────────────────────────────────────────────
// Parsed representation
// ─────────────────────────────────────────────────────────────────────────────

struct Field {
    name: Ident,
    ty: Type,
}

struct PhaseDecl {
    name: Ident,
    reads_env: Vec<Ident>,
    reads_state: Vec<Ident>,
    mutates: Vec<Ident>,
}

struct PipelineDecl {
    name: Ident,
    env: Vec<Ident>,
    state: Vec<Ident>,
    run: Vec<Ident>,
}

struct CompilerCtxInput {
    env_fields: Vec<Field>,
    state_fields: Vec<Field>,
    phases: Vec<PhaseDecl>,
    pipelines: Vec<PipelineDecl>,
}

// ─────────────────────────────────────────────────────────────────────────────
// Parsing
// ─────────────────────────────────────────────────────────────────────────────

fn parse_ident_list(input: ParseStream) -> Result<Vec<Ident>> {
    let content;
    bracketed!(content in input);
    let mut idents = Vec::new();
    while !content.is_empty() {
        idents.push(content.parse::<Ident>()?);
        if !content.is_empty() {
            content.parse::<Token![,]>()?;
        }
    }
    Ok(idents)
}

fn parse_fields(input: ParseStream) -> Result<Vec<Field>> {
    let content;
    braced!(content in input);
    let mut fields = Vec::new();
    while !content.is_empty() {
        // Optional `pub`
        if content.peek(Token![pub]) {
            content.parse::<Token![pub]>()?;
        }
        let name: Ident = content.parse()?;
        content.parse::<Token![:]>()?;
        let ty: Type = content.parse()?;
        fields.push(Field { name, ty });
        if !content.is_empty() {
            content.parse::<Token![,]>()?;
        }
    }
    Ok(fields)
}

impl Parse for PhaseDecl {
    fn parse(input: ParseStream) -> Result<Self> {
        let name: Ident = input.parse()?;
        let content;
        braced!(content in input);

        let mut reads_env = Vec::new();
        let mut reads_state = Vec::new();
        let mut mutates = Vec::new();

        while !content.is_empty() {
            let key: Ident = content.parse()?;
            content.parse::<Token![:]>()?;
            match key.to_string().as_str() {
                "reads_env" => reads_env = parse_ident_list(&content)?,
                "reads_state" => reads_state = parse_ident_list(&content)?,
                "mutates" => mutates = parse_ident_list(&content)?,
                other => {
                    return Err(syn::Error::new(
                        key.span(),
                        format!(
                            "unknown phase grant: `{other}`, expected reads_env, reads_state, or mutates"
                        ),
                    ));
                }
            }
        }

        Ok(PhaseDecl {
            name,
            reads_env,
            reads_state,
            mutates,
        })
    }
}

impl Parse for PipelineDecl {
    fn parse(input: ParseStream) -> Result<Self> {
        let name: Ident = input.parse()?;
        let content;
        braced!(content in input);

        let mut env = Vec::new();
        let mut state = Vec::new();
        let mut run = Vec::new();

        while !content.is_empty() {
            let key: Ident = content.parse()?;
            content.parse::<Token![:]>()?;
            match key.to_string().as_str() {
                "env" => env = parse_ident_list(&content)?,
                "state" => state = parse_ident_list(&content)?,
                "run" => run = parse_ident_list(&content)?,
                other => {
                    return Err(syn::Error::new(
                        key.span(),
                        format!("unknown pipeline key: `{other}`, expected env, state, or run"),
                    ));
                }
            }
        }

        Ok(PipelineDecl {
            name,
            env,
            state,
            run,
        })
    }
}

impl Parse for CompilerCtxInput {
    fn parse(input: ParseStream) -> Result<Self> {
        let mut env_fields = Vec::new();
        let mut state_fields = Vec::new();
        let mut phases = Vec::new();
        let mut pipelines = Vec::new();

        while !input.is_empty() {
            let key: Ident = input.parse()?;
            match key.to_string().as_str() {
                "env" => env_fields = parse_fields(input)?,
                "state" => state_fields = parse_fields(input)?,
                "phases" => {
                    let content;
                    braced!(content in input);
                    while !content.is_empty() {
                        phases.push(content.parse::<PhaseDecl>()?);
                    }
                }
                "pipelines" => {
                    let content;
                    braced!(content in input);
                    while !content.is_empty() {
                        pipelines.push(content.parse::<PipelineDecl>()?);
                    }
                }
                other => {
                    return Err(syn::Error::new(
                        key.span(),
                        format!(
                            "unknown block: `{other}`, expected env, state, phases, or pipelines"
                        ),
                    ));
                }
            }
        }

        Ok(CompilerCtxInput {
            env_fields,
            state_fields,
            phases,
            pipelines,
        })
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Codegen helpers
// ─────────────────────────────────────────────────────────────────────────────

fn find_env_ty(fields: &[Field], name: &Ident) -> Type {
    fields
        .iter()
        .find(|f| f.name == *name)
        .unwrap_or_else(|| panic!("unknown env field: `{name}`"))
        .ty
        .clone()
}

fn find_state_ty(fields: &[Field], name: &Ident) -> Type {
    fields
        .iter()
        .find(|f| f.name == *name)
        .unwrap_or_else(|| panic!("unknown state field: `{name}`"))
        .ty
        .clone()
}

fn find_phase<'a>(phases: &'a [PhaseDecl], name: &Ident) -> &'a PhaseDecl {
    phases
        .iter()
        .find(|p| p.name == *name)
        .unwrap_or_else(|| panic!("unknown phase: `{name}`"))
}

// ─────────────────────────────────────────────────────────────────────────────
// Codegen
// ─────────────────────────────────────────────────────────────────────────────

pub fn generate(input: TokenStream) -> TokenStream {
    let ctx_input = syn::parse_macro_input!(input as CompilerCtxInput);
    let mut output = proc_macro2::TokenStream::new();

    // ── Phase view structs ──────────────────────────────────────────────
    for phase in &ctx_input.phases {
        let view_name = format_ident!("{}View", to_pascal(&phase.name.to_string()));

        let env_read_fields: Vec<_> = phase
            .reads_env
            .iter()
            .map(|name| {
                let ty = find_env_ty(&ctx_input.env_fields, name);
                quote! { pub #name: &'a #ty }
            })
            .collect();

        let state_read_fields: Vec<_> = phase
            .reads_state
            .iter()
            .map(|name| {
                let ty = find_state_ty(&ctx_input.state_fields, name);
                quote! { pub #name: &'a Option<#ty> }
            })
            .collect();

        let mutate_fields: Vec<_> = phase
            .mutates
            .iter()
            .map(|name| {
                let ty = find_state_ty(&ctx_input.state_fields, name);
                quote! { pub #name: &'a mut Option<#ty> }
            })
            .collect();

        output.extend(quote! {
            pub struct #view_name<'a> {
                #(#env_read_fields,)*
                #(#state_read_fields,)*
                #(#mutate_fields,)*
            }
        });
    }

    // ── Per-pipeline context structs and runners ────────────────────────
    for pipeline in &ctx_input.pipelines {
        let ctx_name = format_ident!("{}Ctx", to_pascal(&pipeline.name.to_string()));
        let run_fn = format_ident!("run_{}", pipeline.name);

        // Struct fields
        let env_struct_fields: Vec<_> = pipeline
            .env
            .iter()
            .map(|name| {
                let ty = find_env_ty(&ctx_input.env_fields, name);
                quote! { pub #name: #ty }
            })
            .collect();

        let state_struct_fields: Vec<_> = pipeline
            .state
            .iter()
            .map(|name| {
                let ty = find_state_ty(&ctx_input.state_fields, name);
                quote! { pub #name: Option<#ty> }
            })
            .collect();

        // Constructor params (env only)
        let ctor_params: Vec<_> = pipeline
            .env
            .iter()
            .map(|name| {
                let ty = find_env_ty(&ctx_input.env_fields, name);
                quote! { #name: #ty }
            })
            .collect();

        let env_field_names: Vec<_> = pipeline.env.iter().collect();
        let state_field_names: Vec<_> = pipeline.state.iter().collect();

        // Per-phase run methods
        let mut phase_methods = Vec::new();
        let mut phase_calls = Vec::new();

        for phase_name in &pipeline.run {
            let phase = find_phase(&ctx_input.phases, phase_name);
            let method_name = format_ident!("run_{}", phase_name);
            let view_name = format_ident!("{}View", to_pascal(&phase_name.to_string()));

            let view_env_reads: Vec<_> = phase
                .reads_env
                .iter()
                .map(|name| quote! { #name: &self.#name })
                .collect();

            let view_state_reads: Vec<_> = phase
                .reads_state
                .iter()
                .map(|name| quote! { #name: &self.#name })
                .collect();

            let view_mutates: Vec<_> = phase
                .mutates
                .iter()
                .map(|name| quote! { #name: &mut self.#name })
                .collect();

            phase_methods.push(quote! {
                fn #method_name(&mut self) -> crate::error::Result<()> {
                    #phase_name(&mut #view_name {
                        #(#view_env_reads,)*
                        #(#view_state_reads,)*
                        #(#view_mutates,)*
                    })
                }
            });

            phase_calls.push(quote! { ctx.#method_name()?; });
        }

        output.extend(quote! {
            pub struct #ctx_name {
                #(#env_struct_fields,)*
                #(#state_struct_fields,)*
            }

            impl #ctx_name {
                pub fn new(#(#ctor_params),*) -> Self {
                    Self {
                        #(#env_field_names,)*
                        #(#state_field_names: None,)*
                    }
                }

                #(#phase_methods)*
            }

            pub fn #run_fn(ctx: &mut #ctx_name) -> crate::error::Result<()> {
                #(#phase_calls)*
                Ok(())
            }
        });
    }

    TokenStream::from(output)
}
