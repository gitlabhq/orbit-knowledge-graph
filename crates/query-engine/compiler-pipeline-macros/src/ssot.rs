//! `define_compiler_ctx!` proc macro implementation.
//!
//! Generates:
//! - A `CompilerCtx` trait with guarded accessor methods for state fields
//! - Per-pipeline context structs that implement the trait
//! - Per-pipeline runner functions that set `current_phase` and call phases
//!
//! State fields are private. Access goes through trait methods that assert
//! the current phase has the required grant. Phase functions take
//! `&mut impl CompilerCtx`, so they work with any pipeline's ctx.

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
                        format!("unknown phase grant: `{other}`"),
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
                "phases" | "run" => run = parse_ident_list(&content)?,
                other => {
                    return Err(syn::Error::new(
                        key.span(),
                        format!("unknown pipeline key: `{other}`"),
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
                        format!("unknown block: `{other}`"),
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
// Helpers
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

fn phases_with_access(phases: &[PhaseDecl], field: &Ident) -> Vec<String> {
    phases
        .iter()
        .filter(|p| {
            p.reads_env.contains(field)
                || p.reads_state.contains(field)
                || p.mutates.contains(field)
        })
        .map(|p| p.name.to_string())
        .collect()
}

fn phases_with_mutate(phases: &[PhaseDecl], field: &Ident) -> Vec<String> {
    phases
        .iter()
        .filter(|p| p.mutates.contains(field))
        .map(|p| p.name.to_string())
        .collect()
}

// ─────────────────────────────────────────────────────────────────────────────
// Codegen
// ─────────────────────────────────────────────────────────────────────────────

pub fn generate(input: TokenStream) -> TokenStream {
    let ctx = syn::parse_macro_input!(input as CompilerCtxInput);
    let mut out = proc_macro2::TokenStream::new();

    // ── Trait with just signatures ─────────────────────────────────────
    let mut trait_sigs = Vec::new();

    for ef in &ctx.env_fields {
        let name = &ef.name;
        let ty = &ef.ty;
        trait_sigs.push(quote! { fn #name(&self) -> &#ty; });
    }

    trait_sigs.push(quote! {
        fn current_phase(&self) -> &'static str;
        fn set_current_phase(&mut self, phase: &'static str);
    });

    for sf in &ctx.state_fields {
        let name = &sf.name;
        let ty = &sf.ty;
        let getter_mut = format_ident!("{}_mut", name);
        let setter = format_ident!("set_{}", name);
        let taker = format_ident!("take_{}", name);

        trait_sigs.push(quote! { fn #name(&self) -> &Option<#ty>; });
        trait_sigs.push(quote! { fn #getter_mut(&mut self) -> &mut Option<#ty>; });
        trait_sigs.push(quote! { fn #setter(&mut self, value: #ty); });
        trait_sigs.push(quote! { fn #taker(&mut self) -> Option<#ty>; });
    }

    out.extend(quote! {
        pub trait CompilerCtx {
            #(#trait_sigs)*
        }
    });

    // ── Per-pipeline ctx structs + trait impls ──────────────────────────
    for pipeline in &ctx.pipelines {
        let ctx_name = format_ident!("{}Ctx", to_pascal(&pipeline.name.to_string()));
        let run_fn = format_ident!("run_{}", pipeline.name);

        // Struct fields
        let env_fields: Vec<_> = pipeline
            .env
            .iter()
            .map(|n| {
                let ty = find_env_ty(&ctx.env_fields, n);
                quote! { pub #n: #ty }
            })
            .collect();

        let state_fields: Vec<_> = pipeline
            .state
            .iter()
            .map(|n| {
                let ty = find_state_ty(&ctx.state_fields, n);
                quote! { #n: Option<#ty> }
            })
            .collect();

        // Constructor
        let ctor_params: Vec<_> = pipeline
            .env
            .iter()
            .map(|n| {
                let ty = find_env_ty(&ctx.env_fields, n);
                quote! { #n: #ty }
            })
            .collect();
        let env_names: Vec<_> = pipeline.env.iter().collect();
        let state_names: Vec<_> = pipeline.state.iter().collect();

        // Trait impl: env getters
        let mut trait_impls = Vec::new();

        for env_name in &pipeline.env {
            let ty = find_env_ty(&ctx.env_fields, env_name);
            trait_impls.push(quote! {
                fn #env_name(&self) -> &#ty { &self.#env_name }
            });
        }

        // For env fields the pipeline doesn't have, panic
        for ef in &ctx.env_fields {
            if !pipeline.env.contains(&ef.name) {
                let name = &ef.name;
                let ty = &ef.ty;
                let msg = format!(
                    "pipeline `{}` does not have env field `{}`",
                    pipeline.name, name
                );
                trait_impls.push(quote! {
                    fn #name(&self) -> &#ty { panic!(#msg) }
                });
            }
        }

        trait_impls.push(quote! {
            fn current_phase(&self) -> &'static str { self.current_phase }
            fn set_current_phase(&mut self, phase: &'static str) { self.current_phase = phase; }
        });

        // State accessors with guards
        for sf in &ctx.state_fields {
            let name = &sf.name;
            let ty = &sf.ty;
            let name_str = name.to_string();
            let getter_mut = format_ident!("{}_mut", name);
            let setter = format_ident!("set_{}", name);
            let taker = format_ident!("take_{}", name);

            let read_phases = phases_with_access(&ctx.phases, name);
            let read_arms: Vec<_> = read_phases.iter().map(|p| quote! { #p }).collect();
            let read_allowed = read_phases.join(", ");

            let mut_phases = phases_with_mutate(&ctx.phases, name);
            let mut_arms: Vec<_> = mut_phases.iter().map(|p| quote! { #p }).collect();
            let mut_allowed = mut_phases.join(", ");

            // Generate guard expressions. When no phases grant access,
            // always deny during pipeline execution (avoids empty matches!()).
            let read_guard = if read_arms.is_empty() {
                quote! {
                    panic!(
                        "phase `{}` cannot read `{}` (no phase has access)",
                        self.current_phase, #name_str
                    );
                }
            } else {
                quote! {
                    assert!(
                        matches!(self.current_phase, #(#read_arms)|*),
                        "phase `{}` cannot read `{}` (allowed: {})",
                        self.current_phase, #name_str, #read_allowed
                    );
                }
            };

            let mut_guard = if mut_arms.is_empty() {
                quote! {
                    panic!(
                        "phase `{}` cannot mutate `{}` (no phase has access)",
                        self.current_phase, #name_str
                    );
                }
            } else {
                quote! {
                    assert!(
                        matches!(self.current_phase, #(#mut_arms)|*),
                        "phase `{}` cannot mutate `{}` (allowed: {})",
                        self.current_phase, #name_str, #mut_allowed
                    );
                }
            };

            if pipeline.state.contains(name) {
                // Pipeline has this state field — generate guarded accessors
                trait_impls.push(quote! {
                    fn #name(&self) -> &Option<#ty> {
                        if !self.current_phase.is_empty() { #read_guard }
                        &self.#name
                    }
                    fn #getter_mut(&mut self) -> &mut Option<#ty> {
                        if !self.current_phase.is_empty() { #mut_guard }
                        &mut self.#name
                    }
                    fn #setter(&mut self, value: #ty) {
                        if !self.current_phase.is_empty() { #mut_guard }
                        self.#name = Some(value);
                    }
                    fn #taker(&mut self) -> Option<#ty> {
                        if !self.current_phase.is_empty() { #mut_guard }
                        self.#name.take()
                    }
                });
            } else {
                // Pipeline doesn't have this state field — panic
                let msg = format!(
                    "pipeline `{}` does not have state field `{}`",
                    pipeline.name, name
                );
                trait_impls.push(quote! {
                    fn #name(&self) -> &Option<#ty> { panic!(#msg) }
                    fn #getter_mut(&mut self) -> &mut Option<#ty> { panic!(#msg) }
                    fn #setter(&mut self, _: #ty) { panic!(#msg) }
                    fn #taker(&mut self) -> Option<#ty> { panic!(#msg) }
                });
            }
        }

        // Runner
        let phase_calls: Vec<_> = pipeline
            .run
            .iter()
            .map(|phase_name| {
                let phase_str = phase_name.to_string();
                quote! {
                    ctx.set_current_phase(#phase_str);
                    #phase_name(ctx)?;
                }
            })
            .collect();

        out.extend(quote! {
            pub struct #ctx_name {
                #(#env_fields,)*
                #(#state_fields,)*
                current_phase: &'static str,
            }

            impl #ctx_name {
                pub fn new(#(#ctor_params),*) -> Self {
                    Self {
                        #(#env_names,)*
                        #(#state_names: None,)*
                        current_phase: "",
                    }
                }
            }

            impl CompilerCtx for #ctx_name {
                #(#trait_impls)*
            }

            pub fn #run_fn(ctx: &mut #ctx_name) -> crate::error::Result<()> {
                #(#phase_calls)*
                ctx.set_current_phase("");
                Ok(())
            }
        });
    }

    TokenStream::from(out)
}
