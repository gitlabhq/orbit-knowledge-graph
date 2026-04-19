//! Structured tracing for the v2 code-graph engine and resolver.
//!
//! Captures detailed internal state as the DSL engine parses files (SSA
//! writes/reads, scope changes, binding writes, ref dispatch) and as the
//! resolver links references across files (strategy attempts, chain steps,
//! lookup results).
//!
//! Enabled per-pipeline via `PipelineConfig::trace`. In integration tests,
//! set `trace: true` on a test suite to dump the full event log.

use std::fmt;
use std::sync::Mutex;

/// All observable events in the engine + resolver pipeline.
#[derive(Debug, Clone)]
pub enum TraceEvent {
    // ── SSA engine ──────────────────────────────────────────
    /// A new SSA block was created.
    SsaBlockCreated { block_id: usize },
    /// A predecessor edge was added to a block.
    SsaAddPredecessor { block_id: usize, pred_id: usize },
    /// A block was sealed (all predecessors known).
    SsaBlockSealed { block_id: usize },
    /// A variable was written in a block.
    SsaWrite {
        variable: String,
        block_id: usize,
        value: String,
    },
    /// A variable was read in a block, producing reaching values.
    SsaRead {
        variable: String,
        block_id: usize,
        values: Vec<String>,
    },
    /// A phi node was created.
    SsaPhiCreated {
        phi_id: usize,
        block_id: usize,
        variable: String,
    },
    /// A trivial phi was collapsed to a single value.
    SsaPhiTrivial { phi_id: usize, replacement: String },

    // ── DSL engine walk ─────────────────────────────────────
    /// A scope was pushed (class, function, module, etc.)
    ScopePush {
        name: String,
        kind: String,
        label: String,
        fqn: String,
        block_id: usize,
    },
    /// A scope was popped.
    ScopePop { name: String },
    /// A package node was matched.
    PackageMatched { name: String },
    /// A binding was written to SSA (variable declaration).
    BindingWrite {
        name: String,
        value: String,
        block_id: usize,
    },
    /// An import was recorded.
    ImportRecorded {
        path: String,
        name: String,
        alias: Option<String>,
        wildcard: bool,
        ssa_name: Option<String>,
        block_id: usize,
    },
    /// A reference was detected and queued for resolution.
    RefQueued {
        name: String,
        chain: Option<Vec<String>>,
        ssa_key: String,
        block_id: usize,
        enclosing_def: Option<u32>,
        is_return: bool,
    },
    /// A chain was built from a receiver expression.
    ChainBuilt { steps: Vec<String> },
    /// Return type was inferred for a definition.
    ReturnTypeInferred {
        def_index: u32,
        def_fqn: String,
        return_type: String,
    },
    /// A sibling reference was adopted.
    SiblingRefAdopted { name: String, def_index: u32 },
    /// Branch (if/match/switch) started.
    BranchEnter { node_kind: String, pre_block: usize },
    /// Branch arm entered.
    BranchArm { block_id: usize },
    /// Branch join block created.
    BranchJoin {
        block_id: usize,
        arm_blocks: Vec<usize>,
    },
    /// Loop started.
    LoopEnter {
        node_kind: String,
        header_block: usize,
        body_block: usize,
    },
    /// Loop exited.
    LoopExit { exit_block: usize },

    // ── DSL engine — Phase 1 ───────────────────────────────
    /// A definition was discovered during Phase 1 parsing.
    DefDiscovered {
        name: String,
        fqn: String,
        kind: String,
        label: String,
        is_top_level: bool,
    },

    // ── DSL engine — ref/chain matching ─────────────────────
    /// A reference rule was evaluated for a CST node.
    RefEvaluated {
        node_kind: String,
        matched: bool,
        name: Option<String>,
        has_chain: bool,
    },
    /// A chain step was matched against ChainConfig during build_expression_chain.
    ChainStepMatched {
        node_kind: String,
        category: String,
        text: String,
    },
    /// Instance attr rewrite attempted (Pass 2).
    InstanceAttrRewrite {
        original_key: String,
        compound_key: String,
        found_values: Vec<String>,
        chain_trimmed: bool,
    },

    // ── Graph construction ──────────────────────────────────
    /// An extends edge was linked during finalize.
    ExtendsLinked {
        child_fqn: String,
        super_type: String,
        resolved_to: Vec<String>,
    },
    /// Ancestor chain was built during finalize.
    AncestorChainBuilt { fqn: String, ancestors: Vec<String> },

    // ── Resolver ────────────────────────────────────────────
    /// Resolver started processing a reference.
    ResolveStart {
        name: String,
        chain: Option<Vec<String>>,
        reaching: Vec<String>,
        enclosing_def: Option<String>,
    },
    /// A bare resolution strategy was tried.
    ResolveBareStage {
        stage: String,
        name: String,
        result_count: usize,
        result_fqns: Vec<String>,
    },
    /// Chain resolution: base type FQNs were resolved.
    ResolveChainBase { step: String, types: Vec<String> },
    /// Chain resolution: a step was resolved.
    ResolveChainStep {
        depth: usize,
        step: String,
        member_name: String,
        scope_types: Vec<String>,
        found_count: usize,
        found_fqns: Vec<String>,
        next_types: Vec<String>,
    },
    /// Chain resolution: fallback to bare.
    ResolveChainFallback { name: String },
    /// Nested lookup was attempted.
    NestedLookup {
        scope_fqn: String,
        member_name: String,
        found: bool,
        result_fqns: Vec<String>,
    },
    /// Import resolution was attempted.
    ImportResolve {
        import_fqn: String,
        found: bool,
        result_fqns: Vec<String>,
    },
    /// Receiver type lookup was attempted.
    ReceiverTypeLookup {
        type_name: String,
        member_name: String,
        found_count: usize,
    },
    /// Implicit sub-scope lookup was tried.
    ImplicitSubScope {
        scope_fqn: String,
        sub_scope: String,
        member_name: String,
        found: bool,
    },
    /// SSA reaching def was resolved to graph nodes.
    ReachingDefResolved { value: String, result: String },
    /// Final resolution result.
    ResolveResult { name: String, targets: Vec<String> },
}

impl fmt::Display for TraceEvent {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            // SSA
            TraceEvent::SsaBlockCreated { block_id } => {
                write!(f, "ssa.block_new        B{block_id}")
            }
            TraceEvent::SsaAddPredecessor { block_id, pred_id } => {
                write!(f, "ssa.predecessor      B{block_id} <- B{pred_id}")
            }
            TraceEvent::SsaBlockSealed { block_id } => {
                write!(f, "ssa.seal             B{block_id}")
            }
            TraceEvent::SsaWrite {
                variable,
                block_id,
                value,
            } => {
                write!(
                    f,
                    "ssa.write            {variable} = {value}  [B{block_id}]"
                )
            }
            TraceEvent::SsaRead {
                variable,
                block_id,
                values,
            } => {
                let vals = if values.is_empty() {
                    "∅".to_string()
                } else {
                    values.join(", ")
                };
                write!(
                    f,
                    "ssa.read             {variable} -> {vals}  [B{block_id}]"
                )
            }
            TraceEvent::SsaPhiCreated {
                phi_id,
                block_id,
                variable,
            } => {
                write!(
                    f,
                    "ssa.phi_new          φ{phi_id} for {variable}  [B{block_id}]"
                )
            }
            TraceEvent::SsaPhiTrivial {
                phi_id,
                replacement,
            } => {
                write!(f, "ssa.phi_trivial      φ{phi_id} -> {replacement}")
            }

            // DSL engine
            TraceEvent::ScopePush {
                name,
                kind,
                label,
                fqn,
                block_id,
            } => {
                write!(
                    f,
                    "scope.push           {label} {name} ({fqn}) kind={kind}  [B{block_id}]"
                )
            }
            TraceEvent::ScopePop { name } => {
                write!(f, "scope.pop            {name}")
            }
            TraceEvent::PackageMatched { name } => {
                write!(f, "package              {name}")
            }
            TraceEvent::BindingWrite {
                name,
                value,
                block_id,
            } => {
                write!(f, "binding.write        {name} = {value}  [B{block_id}]")
            }
            TraceEvent::ImportRecorded {
                path,
                name,
                alias,
                wildcard,
                ssa_name,
                block_id,
            } => {
                let suffix = if *wildcard { " (wildcard)" } else { "" };
                let alias_str = alias
                    .as_ref()
                    .map(|a| format!(" as {a}"))
                    .unwrap_or_default();
                let ssa_str = ssa_name
                    .as_ref()
                    .map(|s| format!("  ssa={s}"))
                    .unwrap_or_default();
                write!(
                    f,
                    "import               {path}.{name}{alias_str}{suffix}{ssa_str}  [B{block_id}]"
                )
            }
            TraceEvent::RefQueued {
                name,
                chain,
                ssa_key,
                block_id,
                enclosing_def,
                is_return,
            } => {
                let chain_str = chain
                    .as_ref()
                    .map(|c| format!(" chain=[{}]", c.join(" -> ")))
                    .unwrap_or_default();
                let ret_str = if *is_return { " (return)" } else { "" };
                let enc_str = enclosing_def
                    .map(|d| format!(" enc=D{d}"))
                    .unwrap_or_default();
                write!(
                    f,
                    "ref.queue            {name}{chain_str}  ssa_key={ssa_key}{enc_str}{ret_str}  [B{block_id}]"
                )
            }
            TraceEvent::ChainBuilt { steps } => {
                write!(f, "chain.built          [{}]", steps.join(" -> "))
            }
            TraceEvent::ReturnTypeInferred {
                def_index,
                def_fqn,
                return_type,
            } => {
                write!(
                    f,
                    "return.inferred      D{def_index} ({def_fqn}) -> {return_type}"
                )
            }
            TraceEvent::SiblingRefAdopted { name, def_index } => {
                write!(f, "sibling.adopt        {name}  owner=D{def_index}")
            }
            TraceEvent::BranchEnter {
                node_kind,
                pre_block,
            } => {
                write!(f, "branch.enter         {node_kind}  [B{pre_block}]")
            }
            TraceEvent::BranchArm { block_id } => {
                write!(f, "branch.arm           [B{block_id}]")
            }
            TraceEvent::BranchJoin {
                block_id,
                arm_blocks,
            } => {
                let arms: Vec<String> = arm_blocks.iter().map(|b| format!("B{b}")).collect();
                write!(
                    f,
                    "branch.join          [B{block_id}] <- [{}]",
                    arms.join(", ")
                )
            }
            TraceEvent::LoopEnter {
                node_kind,
                header_block,
                body_block,
            } => {
                write!(
                    f,
                    "loop.enter           {node_kind}  header=B{header_block} body=B{body_block}"
                )
            }
            TraceEvent::LoopExit { exit_block } => {
                write!(f, "loop.exit            [B{exit_block}]")
            }

            // Phase 1
            TraceEvent::DefDiscovered {
                name,
                fqn,
                kind,
                label,
                is_top_level,
            } => {
                let top = if *is_top_level { " (top)" } else { "" };
                write!(
                    f,
                    "def.discovered       {label} {name} ({fqn}) kind={kind}{top}"
                )
            }

            // Ref/chain matching
            TraceEvent::RefEvaluated {
                node_kind,
                matched,
                name,
                has_chain,
            } => {
                if *matched {
                    let chain_str = if *has_chain { " +chain" } else { "" };
                    write!(
                        f,
                        "ref.eval             {node_kind} -> {}{}",
                        name.as_deref().unwrap_or("?"),
                        chain_str
                    )
                } else {
                    write!(f, "ref.eval             {node_kind} -> (no match)")
                }
            }
            TraceEvent::ChainStepMatched {
                node_kind,
                category,
                text,
            } => {
                write!(f, "chain.match          {node_kind} -> {category}({text})")
            }
            TraceEvent::InstanceAttrRewrite {
                original_key,
                compound_key,
                found_values,
                chain_trimmed,
            } => {
                let vals = if found_values.is_empty() {
                    "∅".to_string()
                } else {
                    found_values.join(", ")
                };
                let trim = if *chain_trimmed { " (trimmed)" } else { "" };
                write!(
                    f,
                    "attr.rewrite         {original_key} -> {compound_key} = [{vals}]{trim}"
                )
            }

            // Graph construction
            TraceEvent::ExtendsLinked {
                child_fqn,
                super_type,
                resolved_to,
            } => {
                if resolved_to.is_empty() {
                    write!(f, "extends.link         {child_fqn} -> {super_type} = ∅")
                } else {
                    write!(
                        f,
                        "extends.link         {child_fqn} -> {super_type} = [{}]",
                        resolved_to.join(", ")
                    )
                }
            }
            TraceEvent::AncestorChainBuilt { fqn, ancestors } => {
                write!(
                    f,
                    "ancestors.built      {fqn} -> [{}]",
                    ancestors.join(" -> ")
                )
            }

            // Resolver
            TraceEvent::ResolveStart {
                name,
                chain,
                reaching,
                enclosing_def,
            } => {
                let chain_str = chain
                    .as_ref()
                    .map(|c| format!(" chain=[{}]", c.join(" -> ")))
                    .unwrap_or_default();
                let reach_str = if reaching.is_empty() {
                    "∅".to_string()
                } else {
                    reaching.join(", ")
                };
                let enc_str = enclosing_def
                    .as_ref()
                    .map(|d| format!(" enc={d}"))
                    .unwrap_or_default();
                write!(
                    f,
                    "resolve.start        {name}{chain_str}  reaching=[{reach_str}]{enc_str}"
                )
            }
            TraceEvent::ResolveBareStage {
                stage,
                name,
                result_count,
                result_fqns,
            } => {
                if *result_count == 0 {
                    write!(f, "resolve.bare         {stage}({name}) -> ∅")
                } else {
                    write!(
                        f,
                        "resolve.bare         {stage}({name}) -> {result_count} [{}]",
                        result_fqns.join(", ")
                    )
                }
            }
            TraceEvent::ResolveChainBase { step, types } => {
                if types.is_empty() {
                    write!(f, "chain.base           {step} -> ∅")
                } else {
                    write!(f, "chain.base           {step} -> [{}]", types.join(", "))
                }
            }
            TraceEvent::ResolveChainStep {
                depth,
                step,
                member_name,
                scope_types,
                found_count,
                found_fqns,
                next_types,
            } => {
                let scopes = scope_types.join(", ");
                if *found_count == 0 {
                    write!(
                        f,
                        "chain.step[{depth}]       {step}({member_name}) in [{scopes}] -> ∅"
                    )
                } else {
                    write!(
                        f,
                        "chain.step[{depth}]       {step}({member_name}) in [{scopes}] -> {found_count} [{}]  next=[{}]",
                        found_fqns.join(", "),
                        next_types.join(", ")
                    )
                }
            }
            TraceEvent::ResolveChainFallback { name } => {
                write!(f, "chain.fallback       bare({name})")
            }
            TraceEvent::NestedLookup {
                scope_fqn,
                member_name,
                found,
                result_fqns,
            } => {
                if *found {
                    write!(
                        f,
                        "lookup.nested        {scope_fqn}.{member_name} -> [{}]",
                        result_fqns.join(", ")
                    )
                } else {
                    write!(f, "lookup.nested        {scope_fqn}.{member_name} -> ∅")
                }
            }
            TraceEvent::ImportResolve {
                import_fqn,
                found,
                result_fqns,
            } => {
                if *found {
                    write!(
                        f,
                        "lookup.import        {import_fqn} -> [{}]",
                        result_fqns.join(", ")
                    )
                } else {
                    write!(f, "lookup.import        {import_fqn} -> ∅")
                }
            }
            TraceEvent::ReceiverTypeLookup {
                type_name,
                member_name,
                found_count,
            } => {
                write!(
                    f,
                    "lookup.receiver      {type_name}.{member_name} -> {found_count}"
                )
            }
            TraceEvent::ImplicitSubScope {
                scope_fqn,
                sub_scope,
                member_name,
                found,
            } => {
                let result = if *found { "✓" } else { "∅" };
                write!(
                    f,
                    "lookup.sub_scope     {scope_fqn}.{sub_scope}.{member_name} -> {result}"
                )
            }
            TraceEvent::ReachingDefResolved { value, result } => {
                write!(f, "reaching.resolved    {value} -> {result}")
            }
            TraceEvent::ResolveResult { name, targets } => {
                if targets.is_empty() {
                    write!(f, "resolve.result       {name} -> ∅")
                } else {
                    write!(f, "resolve.result       {name} -> [{}]", targets.join(", "))
                }
            }
        }
    }
}

/// Collects trace events. Thread-safe via `Mutex` (lock only taken when enabled).
#[derive(Debug)]
pub struct Tracer {
    events: Mutex<Vec<TraceEvent>>,
    enabled: bool,
}

impl Default for Tracer {
    fn default() -> Self {
        Self::new(false)
    }
}

impl Tracer {
    pub fn new(enabled: bool) -> Self {
        Self {
            events: Mutex::new(if enabled {
                Vec::with_capacity(1024)
            } else {
                Vec::new()
            }),
            enabled,
        }
    }

    #[inline]
    pub fn enabled(&self) -> bool {
        self.enabled
    }

    #[inline]
    pub fn event(&self, event: TraceEvent) {
        if self.enabled {
            self.events.lock().unwrap().push(event);
        }
    }

    /// Drain all events, returning them for display.
    pub fn drain(&self) -> Vec<TraceEvent> {
        self.events.lock().unwrap().drain(..).collect()
    }

    /// Print all events to stderr with a header, then clear.
    pub fn dump(&self, header: &str) {
        if !self.enabled {
            return;
        }
        let events = self.drain();
        if events.is_empty() {
            return;
        }
        eprintln!("\n  ┌── TRACE: {header} ({} events) ──", events.len());
        for (i, event) in events.iter().enumerate() {
            eprintln!("  │ {i:4}  {event}");
        }
        eprintln!("  └──────────────────────────────────────\n");
    }

    /// Print all events to stderr grouped by file, then clear.
    pub fn dump_grouped(&self, header: &str) {
        if !self.enabled {
            return;
        }
        let events = self.drain();
        if events.is_empty() {
            return;
        }
        eprintln!("\n  ┌── TRACE: {header} ({} events) ──", events.len());

        // Group into sections: engine events, then resolver events
        let mut in_resolver = false;
        for (i, event) in events.iter().enumerate() {
            let is_resolve = matches!(
                event,
                TraceEvent::ResolveStart { .. }
                    | TraceEvent::ResolveBareStage { .. }
                    | TraceEvent::ResolveChainBase { .. }
                    | TraceEvent::ResolveChainStep { .. }
                    | TraceEvent::ResolveChainFallback { .. }
                    | TraceEvent::NestedLookup { .. }
                    | TraceEvent::ImportResolve { .. }
                    | TraceEvent::ReceiverTypeLookup { .. }
                    | TraceEvent::ImplicitSubScope { .. }
                    | TraceEvent::ReachingDefResolved { .. }
                    | TraceEvent::ResolveResult { .. }
                    | TraceEvent::InstanceAttrRewrite { .. }
            );
            if is_resolve && !in_resolver {
                eprintln!("  │");
                eprintln!("  │ ── resolver ──");
                in_resolver = true;
            }
            eprintln!("  │ {i:4}  {event}");
        }
        eprintln!("  └──────────────────────────────────────\n");
    }
}

/// Create a no-op tracer. Use this for production paths where tracing is disabled.
pub fn noop_tracer() -> Tracer {
    Tracer::new(false)
}

/// Return a `&'static Tracer` that is always disabled. One-time allocation
/// via `OnceLock` — `get_or_init` runs the closure exactly once, all
/// subsequent calls return the same pointer. Single 24-byte leak for the
/// process lifetime so SSA engine can store `&'a Tracer` without lifetime
/// gymnastics when no tracer is explicitly provided.
pub fn leaked_noop_tracer() -> &'static Tracer {
    use std::sync::OnceLock;
    static NOOP: OnceLock<&'static Tracer> = OnceLock::new();
    NOOP.get_or_init(|| Box::leak(Box::new(Tracer::new(false))))
}

// ── Global trace flag ───────────────────────────────────────────

use std::sync::atomic::{AtomicBool, Ordering};

static TRACE_ENABLED: AtomicBool = AtomicBool::new(false);

/// Set the global trace flag. When true, newly created tracers in the
/// pipeline will be enabled. Call this before running the pipeline.
/// Safe to call from any thread — uses atomic store.
pub fn set_thread_trace(enabled: bool) {
    TRACE_ENABLED.store(enabled, Ordering::SeqCst);
}

/// Check whether tracing is globally enabled.
pub fn is_thread_trace() -> bool {
    TRACE_ENABLED.load(Ordering::Relaxed)
}

/// Create a tracer respecting the global trace flag.
pub fn thread_tracer() -> Tracer {
    Tracer::new(is_thread_trace())
}
