//! Edge builder: converts SSA walk results into resolved call edges.
//!
//! Takes the walker's `WalkResult` (SSA graph + recorded reads) and the
//! `ResolutionContext` (indexes), resolves each reference read to concrete
//! definitions, and produces `ResolvedEdge`s for the graph.

use code_graph_types::{EdgeKind, ExpressionStep, NodeKind, Relationship};
use rustc_hash::FxHashSet;

use super::context::{DefRef, ResolutionContext};
use super::edges::{EdgeSource, ResolvedEdge};
use super::imports;
use super::rules::ResolutionRules;
use super::ssa::{SsaResolver, Value};
use super::walker::{AsAst, RecordedRead, WalkResult, walk_files};

/// Trait to get rules from the type parameter.
pub trait HasRules {
    fn rules() -> ResolutionRules;
}

/// Generic resolver parameterized by a `HasRules` type.
pub struct RulesResolver<R: HasRules>(std::marker::PhantomData<R>);

impl<A, R> super::resolver::ReferenceResolver<A> for RulesResolver<R>
where
    A: AsAst + Send + Sync,
    R: HasRules + Send + Sync,
{
    fn resolve(ctx: &ResolutionContext<A>) -> Vec<ResolvedEdge> {
        let rules = R::rules();
        let walk_result = walk_files(&rules, &ctx.results, &ctx.asts);
        build_edges(&rules, ctx, walk_result)
    }
}

/// Build edges from walk results. This is the pipeline's resolve stage.
fn build_edges<A>(
    rules: &ResolutionRules,
    ctx: &ResolutionContext<A>,
    mut walk_result: WalkResult,
) -> Vec<ResolvedEdge> {
    let mut edges = Vec::new();
    let reads = std::mem::take(&mut walk_result.reads);
    let mut resolver = Resolver::new(rules, ctx, &mut walk_result.ssa);

    for read in &reads {
        let result = &ctx.results[read.file_idx];
        let reference = &result.references[read.ref_idx];

        let resolved_defs = if let Some(ref chain) = reference.expression {
            resolver.resolve_chain(read, chain)
        } else {
            resolver.resolve_bare(read)
        };

        let source_enclosing = ctx.scopes.enclosing_scope(
            &result.file_path,
            reference.range.byte_offset.0,
            reference.range.byte_offset.1,
        );

        let (source, source_node, source_def_kind) = match source_enclosing {
            Some(s) => {
                let def_ref = DefRef {
                    file_idx: s.file_idx,
                    def_idx: s.def_idx,
                };
                let (def, _) = ctx.resolve_def(def_ref);
                (
                    EdgeSource::Definition(def_ref),
                    NodeKind::Definition,
                    Some(def.kind),
                )
            }
            None => (EdgeSource::File(read.file_idx), NodeKind::File, None),
        };

        for target in resolved_defs {
            let (target_def, _) = ctx.resolve_def(target);
            edges.push(ResolvedEdge {
                relationship: Relationship {
                    edge_kind: EdgeKind::Calls,
                    source_node,
                    target_node: NodeKind::Definition,
                    source_def_kind,
                    target_def_kind: Some(target_def.kind),
                },
                source,
                target,
                reference_range: reference.range,
            });
        }
    }

    edges
}

// ── Resolver ────────────────────────────────────────────────────

/// Stateful resolver that holds shared context for resolving references.
/// Eliminates parameter threading across all resolution functions.
struct Resolver<'a, A> {
    rules: &'a ResolutionRules,
    ctx: &'a ResolutionContext<A>,
    ssa: &'a mut SsaResolver,
    sep: &'a str,
}

impl<'a, A> Resolver<'a, A> {
    fn new(
        rules: &'a ResolutionRules,
        ctx: &'a ResolutionContext<A>,
        ssa: &'a mut SsaResolver,
    ) -> Self {
        Self {
            sep: rules.fqn_separator,
            rules,
            ctx,
            ssa,
        }
    }

    // ── Shared primitive ────────────────────────────────────────

    /// Convert an SSA `Value` to type name(s) for member lookup.
    /// This is the key shared operation used by chain resolution,
    /// compound key fallback, and base resolution.
    fn value_to_types(&mut self, value: &Value) -> Vec<String> {
        match value {
            Value::Type(t) => vec![t.clone()],
            Value::Def(f, d) => {
                let def = &self.ctx.results[*f].definitions[*d];
                if def.kind.is_type_container() {
                    vec![def.fqn.to_string()]
                } else if let Some(meta) = &def.metadata
                    && let Some(rt) = &meta.return_type
                {
                    vec![rt.clone()]
                } else {
                    vec![]
                }
            }
            _ => vec![],
        }
    }

    /// Find the enclosing type scope FQN for a reference.
    fn enclosing_type_fqn(
        &mut self,
        file_idx: usize,
        byte_start: usize,
        byte_end: usize,
    ) -> Option<String> {
        let result = &self.ctx.results[file_idx];
        let containing = self
            .ctx
            .scopes
            .containing_scopes(&result.file_path, byte_start, byte_end);
        containing
            .iter()
            .rev()
            .find(|s| result.definitions[s.def_idx].kind.is_type_container())
            .map(|s| result.definitions[s.def_idx].fqn.to_string())
    }

    // ── Bare name resolution ────────────────────────────────────

    /// Resolve a bare name (no expression chain) via SSA + fallbacks.
    fn resolve_bare(&mut self, read: &RecordedRead) -> Vec<DefRef> {
        let reaching = self.ssa.read_variable_stateless(&read.name, read.block);
        let reference = &self.ctx.results[read.file_idx].references[read.ref_idx];
        let enclosing = self.enclosing_type_fqn(
            read.file_idx,
            reference.range.byte_offset.0,
            reference.range.byte_offset.1,
        );

        let mut result = Vec::new();

        for value in &reaching.values {
            match value {
                Value::Def(f, d) => {
                    result.push(DefRef {
                        file_idx: *f,
                        def_idx: *d,
                    });
                }
                Value::Import(f, i) => {
                    let import = &self.ctx.results[*f].imports[*i];
                    result.extend(imports::resolve_import(self.ctx, import, self.sep, true));
                }
                Value::Type(type_name) => {
                    let members = self.ctx.members.lookup_member_with_supers(
                        type_name,
                        &read.name,
                        &self.ctx.definitions,
                    );
                    if !members.is_empty() {
                        result.extend(members);
                    } else {
                        let fqn = format!("{}{}{}", type_name, self.sep, read.name);
                        result.extend(self.ctx.definitions.lookup_fqn(&fqn));
                    }
                }
                _ => {}
            }
        }

        // Fallback 1: import strategies
        if result.is_empty() {
            result = imports::apply(
                &self.rules.import_strategies,
                self.ctx,
                read.file_idx,
                &read.name,
                self.sep,
            );
        }

        // Fallback 2: implicit member lookup on enclosing type
        if result.is_empty()
            && self.rules.implicit_member_lookup
            && let Some(type_fqn) = &enclosing
        {
            result.extend(self.ctx.members.lookup_member_with_supers(
                type_fqn,
                &read.name,
                &self.ctx.definitions,
            ));
        }

        dedup(&mut result);
        result
    }

    // ── Chain resolution ────────────────────────────────────────

    /// Resolve an expression chain like `[Ident("obj"), Call("method")]`.
    fn resolve_chain(&mut self, read: &RecordedRead, chain: &[ExpressionStep]) -> Vec<DefRef> {
        if chain.is_empty() {
            return vec![];
        }

        let reference = &self.ctx.results[read.file_idx].references[read.ref_idx];
        let enclosing = self.enclosing_type_fqn(
            read.file_idx,
            reference.range.byte_offset.0,
            reference.range.byte_offset.1,
        );

        // Step 1: resolve the base to type name(s)
        let mut current_types = self.resolve_base(&chain[0], read.block, enclosing.as_deref());

        if current_types.is_empty() {
            // Can't resolve base — fall back to bare name on last step
            return self.chain_fallback(read, chain);
        }

        // Step 2: walk remaining steps
        let mut compound_key = self.compound_key_base(&chain[0]);

        for (i, step) in chain[1..].iter().enumerate() {
            let is_last = i == chain.len() - 2;
            let member_name = match step {
                ExpressionStep::Call(n) | ExpressionStep::Field(n) => n,
                _ => continue,
            };

            // Look up member on current type(s)
            let (next_types, found_members) = self.walk_step(&current_types, step, member_name);

            if is_last && !found_members.is_empty() {
                let mut result = found_members;
                dedup(&mut result);
                return result;
            }

            // Compound key fallback (Python's self.db)
            if next_types.is_empty() && found_members.is_empty() {
                let recovered = self.compound_key_step(&mut compound_key, member_name, read.block);
                if !recovered.is_empty() {
                    current_types = recovered;
                    continue;
                }
            } else {
                compound_key.clear();
            }

            current_types = next_types;
            if current_types.is_empty() {
                break;
            }
        }

        vec![]
    }

    /// Resolve the first element of a chain to type name(s).
    fn resolve_base(
        &mut self,
        step: &ExpressionStep,
        block: super::ssa::BlockId,
        enclosing: Option<&str>,
    ) -> Vec<String> {
        match step {
            ExpressionStep::Ident(name) => {
                let reaching = self.ssa.read_variable_stateless(name, block);
                reaching
                    .values
                    .iter()
                    .flat_map(|v| self.value_to_types(v))
                    .collect()
            }
            ExpressionStep::This => enclosing
                .map(|fqn| vec![fqn.to_string()])
                .unwrap_or_default(),
            ExpressionStep::Super => self
                .rules
                .super_name
                .map(|name| {
                    let reaching = self.ssa.read_variable_stateless(name, block);
                    reaching
                        .values
                        .iter()
                        .filter_map(|v| match v {
                            Value::Type(t) => Some(t.clone()),
                            _ => None,
                        })
                        .collect()
                })
                .unwrap_or_default(),
            ExpressionStep::New(type_name) => vec![type_name.clone()],
            _ => vec![],
        }
    }

    /// Walk one step of a chain: look up member_name on each current type.
    /// Returns (next type names for further steps, found member DefRefs).
    fn walk_step(
        &mut self,
        current_types: &[String],
        step: &ExpressionStep,
        member_name: &str,
    ) -> (Vec<String>, Vec<DefRef>) {
        let mut next_types = Vec::new();
        let mut found_members = Vec::new();

        for type_name in current_types {
            let members = self.ctx.members.lookup_member_with_supers(
                type_name,
                member_name,
                &self.ctx.definitions,
            );
            for def_ref in &members {
                let def = &self.ctx.results[def_ref.file_idx].definitions[def_ref.def_idx];
                if matches!(step, ExpressionStep::Call(_)) {
                    if let Some(meta) = &def.metadata
                        && let Some(rt) = &meta.return_type
                    {
                        next_types.push(rt.clone());
                    }
                    if matches!(
                        def.kind,
                        code_graph_types::DefKind::Class | code_graph_types::DefKind::Constructor
                    ) {
                        next_types.push(def.fqn.to_string());
                    }
                }
                if matches!(step, ExpressionStep::Field(_))
                    && let Some(meta) = &def.metadata
                    && let Some(ta) = &meta.type_annotation
                {
                    next_types.push(ta.clone());
                }
            }
            found_members.extend(members);
        }

        (next_types, found_members)
    }

    /// Build the compound SSA key base from the first chain element.
    fn compound_key_base(&mut self, step: &ExpressionStep) -> String {
        match step {
            ExpressionStep::Ident(n) => n.clone(),
            ExpressionStep::This => self
                .rules
                .self_names
                .first()
                .map(|s| s.to_string())
                .unwrap_or_default(),
            ExpressionStep::Super => self
                .rules
                .super_name
                .map(|s| s.to_string())
                .unwrap_or_default(),
            _ => String::new(),
        }
    }

    /// Try compound key fallback: read "self.db" as a single SSA variable.
    fn compound_key_step(
        &mut self,
        compound_key: &mut String,
        member_name: &str,
        block: super::ssa::BlockId,
    ) -> Vec<String> {
        if compound_key.is_empty() {
            return vec![];
        }
        *compound_key = format!("{}{}{}", compound_key, self.sep, member_name);
        let reaching = self.ssa.read_variable_stateless(compound_key, block);
        reaching
            .values
            .iter()
            .flat_map(|v| self.value_to_types(v))
            .collect()
    }

    /// Fallback when chain base can't resolve: try bare name on last step.
    fn chain_fallback(&mut self, read: &RecordedRead, chain: &[ExpressionStep]) -> Vec<DefRef> {
        let last = match chain.last() {
            Some(ExpressionStep::Call(n) | ExpressionStep::Field(n)) => n,
            _ => return vec![],
        };
        let bare_read = RecordedRead {
            file_idx: read.file_idx,
            ref_idx: read.ref_idx,
            block: read.block,
            name: last.clone(),
        };
        self.resolve_bare(&bare_read)
    }
}

fn dedup(result: &mut Vec<DefRef>) {
    let mut seen = FxHashSet::default();
    result.retain(|r| seen.insert((r.file_idx, r.def_idx)));
}
