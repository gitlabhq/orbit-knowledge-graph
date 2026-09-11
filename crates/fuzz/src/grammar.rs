use std::collections::HashMap;
use std::sync::OnceLock;

use compiler::passes::frontend::gql;
use pest_meta::ast::{Expr, Rule, RuleType};
use pest_meta::parser::{self, Rule as MetaRule};

const REPETITION_CAP: usize = 2;
const DEPTH_CAP: u32 = 48;
const UNREACHABLE_DEPTH: u32 = u32::MAX / 2;

const BUILTINS: &[(&str, &[&str])] = &[
    ("ANY", &["a"]),
    ("ASCII_DIGIT", &["0", "1"]),
    ("ASCII_NONZERO_DIGIT", &["1", "9"]),
    ("ASCII_ALPHA", &["a", "Z"]),
    ("ASCII_ALPHANUMERIC", &["a", "0"]),
    ("ASCII_HEX_DIGIT", &["0", "f"]),
    ("ASCII_OCT_DIGIT", &["0", "7"]),
    ("SOI", &[""]),
    ("EOI", &[""]),
];

pub struct Grammar {
    rules: Vec<Rule>,
    index: HashMap<String, usize>,
    distance: Vec<Vec<Option<u32>>>,
    depth: Vec<u32>,
    terminal_only: Vec<bool>,
    leaf_overrides: HashMap<usize, fn(usize) -> String>,
}

pub trait Chooser {
    fn choose(&mut self, arity: usize) -> Option<usize>;
}

#[derive(Default)]
pub struct Odometer {
    plan: Vec<(usize, usize)>,
    cursor: usize,
}

impl Odometer {
    pub fn advance(&mut self) -> bool {
        self.plan.truncate(self.cursor);
        self.cursor = 0;
        while let Some((choice, arity)) = self.plan.pop() {
            if choice + 1 < arity {
                self.plan.push((choice + 1, arity));
                return true;
            }
        }
        false
    }
}

impl Chooser for Odometer {
    fn choose(&mut self, arity: usize) -> Option<usize> {
        if let Some(&(choice, recorded)) = self.plan.get(self.cursor) {
            assert_eq!(recorded, arity, "derivation must be deterministic");
            self.cursor += 1;
            return Some(choice);
        }
        self.plan.push((0, arity));
        self.cursor += 1;
        Some(0)
    }
}

pub struct Bytes<'b>(pub &'b [u8]);

impl Chooser for Bytes<'_> {
    fn choose(&mut self, arity: usize) -> Option<usize> {
        let (byte, rest) = self.0.split_first()?;
        self.0 = rest;
        Some(usize::from(*byte) % arity)
    }
}

struct Walk<'c> {
    chooser: &'c mut dyn Chooser,
    target: Option<usize>,
    target_terminal_only: bool,
    reached: bool,
    local: bool,
    depth: u32,
    suppressed: u32,
    fresh: usize,
    out: String,
    pairs: Vec<usize>,
}

impl Grammar {
    pub fn orbit() -> &'static Grammar {
        static GRAMMAR: OnceLock<Grammar> = OnceLock::new();
        GRAMMAR.get_or_init(|| {
            Grammar::parse(gql::GRAMMAR)
                .expect("valid grammar")
                .with_leaf_override("Variable", fresh_identifier)
                .with_leaf_override("SchemaName", fresh_identifier)
        })
    }

    pub fn parse(source: &str) -> Result<Self, String> {
        let pairs = parser::parse(MetaRule::grammar_rules, source).map_err(|e| e.to_string())?;
        let rules = parser::consume_rules(pairs).map_err(|errors| {
            errors
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>()
                .join("\n")
        })?;
        let index: HashMap<String, usize> = rules
            .iter()
            .enumerate()
            .map(|(i, rule)| (rule.name.clone(), i))
            .collect();
        let mut grammar = Grammar {
            distance: Vec::new(),
            depth: vec![UNREACHABLE_DEPTH; rules.len()],
            terminal_only: vec![false; rules.len()],
            leaf_overrides: HashMap::new(),
            rules,
            index,
        };
        grammar.compute_distance();
        grammar.compute_depth();
        grammar.terminal_only = (0..grammar.rules.len())
            .map(|i| {
                !(0..grammar.rules.len()).any(|r| {
                    grammar.distance[i][r].is_some() && grammar.rules[r].ty != RuleType::Silent
                })
            })
            .collect();
        Ok(grammar)
    }

    pub fn with_leaf_override(mut self, rule: &str, leaf: fn(usize) -> String) -> Self {
        self.leaf_overrides.insert(self.index[rule], leaf);
        self
    }

    pub fn rules_reachable_from(&self, start: &str) -> Vec<&str> {
        let start = self.index[start];
        self.rules
            .iter()
            .enumerate()
            .filter(|(i, _)| *i == start || self.distance[start][*i].is_some())
            .map(|(_, rule)| rule.name.as_str())
            .collect()
    }

    pub fn local_derivations(&self, start: &str, rule: &str, cap: usize) -> Vec<String> {
        let mut odometer = Odometer::default();
        let mut texts = Vec::new();
        let mut attempts = 0usize;
        loop {
            texts.extend(self.walk(start, Some(self.index[rule]), &mut odometer));
            attempts += 1;
            assert!(
                attempts <= cap,
                "{rule} has more than {cap} local derivations"
            );
            if !odometer.advance() {
                return texts;
            }
        }
    }

    pub fn derive(&self, start: &str, chooser: &mut dyn Chooser) -> Option<String> {
        self.walk(start, None, chooser)
    }

    fn walk(
        &self,
        start: &str,
        target: Option<usize>,
        chooser: &mut dyn Chooser,
    ) -> Option<String> {
        let mut walk = Walk {
            chooser,
            target,
            target_terminal_only: target.is_some_and(|t| self.terminal_only[t]),
            reached: false,
            local: target.is_none(),
            depth: 0,
            suppressed: 0,
            fresh: 0,
            out: String::new(),
            pairs: Vec::new(),
        };
        self.expand_rule(self.index[start], &mut walk);
        if let Some(target) = target {
            assert!(
                walk.reached,
                "{} is unreachable from {start} in the minimal context",
                self.rules[target].name
            );
        }
        (self.parsed_pairs(&walk.out)? == walk.pairs).then_some(walk.out)
    }

    fn parsed_pairs(&self, text: &str) -> Option<Vec<usize>> {
        let mut pairs = Vec::new();
        let mut skip_below = None;
        for (depth, name) in gql::pair_outline(text)? {
            if skip_below.is_some_and(|d| depth > d) {
                continue;
            }
            skip_below = None;
            let Some(&index) = self.index.get(&name) else {
                continue;
            };
            pairs.push(index);
            if self.leaf_overrides.contains_key(&index) {
                skip_below = Some(depth);
            }
        }
        Some(pairs)
    }

    fn expand_rule(&self, index: usize, walk: &mut Walk<'_>) {
        let rule = &self.rules[index];
        let entering_target = walk.target == Some(index) && !walk.reached;
        let was_local = walk.local;
        if entering_target {
            walk.reached = true;
            walk.local = true;
        } else if walk.target.is_some()
            && (rule.ty != RuleType::Silent || self.terminal_only[index])
        {
            walk.local = false;
        }
        if walk.suppressed == 0 && rule.ty != RuleType::Silent {
            walk.pairs.push(index);
        }
        let opaque =
            matches!(rule.ty, RuleType::Atomic) || self.leaf_overrides.contains_key(&index);
        walk.suppressed += u32::from(opaque);
        let leads_to_target = walk
            .target
            .is_some_and(|t| !walk.reached && (t == index || self.distance[index][t].is_some()));
        match self.leaf_overrides.get(&index) {
            Some(leaf) if !walk.local && !leads_to_target => {
                walk.out.push_str(&leaf(walk.fresh));
                walk.fresh += 1;
            }
            _ => {
                walk.depth += 1;
                self.expand(&rule.expr, walk);
                walk.depth -= 1;
            }
        }
        walk.suppressed -= u32::from(opaque);
        walk.local = was_local;
    }

    fn shape_neutral(&self, inner: &Expr, walk: &Walk<'_>) -> bool {
        let leads_to_target = !walk.reached && self.expr_distance(inner, walk.target).is_some();
        let inside_terminal_target = walk.local && walk.target_terminal_only;
        !(leads_to_target || inside_terminal_target)
            && matches!(inner, Expr::Ident(name)
            if self.index.get(name).is_some_and(|&i| {
                self.rules[i].ty == RuleType::Silent && self.terminal_only[i]
            }))
    }

    fn expand(&self, expr: &Expr, walk: &mut Walk<'_>) {
        let target = walk.target;
        match expr {
            Expr::Str(s) | Expr::Insens(s) => walk.out.push_str(s),
            Expr::Range(low, high) => {
                let options = [low.as_str(), high.as_str()];
                let choice = self.decide(walk, options.len(), |_| None, 0);
                walk.out.push_str(options[choice]);
            }
            Expr::Ident(name) => match self.index.get(name) {
                Some(&index) => self.expand_rule(index, walk),
                None => {
                    let options = BUILTINS
                        .iter()
                        .find(|(builtin, _)| builtin == name)
                        .unwrap_or_else(|| panic!("unsupported builtin {name}"))
                        .1;
                    let choice = self.decide(walk, options.len(), |_| None, 0);
                    walk.out.push_str(options[choice]);
                }
            },
            Expr::Seq(first, second) => {
                self.expand(first, walk);
                self.expand(second, walk);
            }
            Expr::Choice(..) => {
                let alternatives = alternatives(expr);
                let minimal = alternatives
                    .iter()
                    .enumerate()
                    .min_by_key(|(_, alt)| self.expr_depth(alt))
                    .map(|(i, _)| i)
                    .expect("choice has alternatives");
                let choice = self.decide(
                    walk,
                    alternatives.len(),
                    |i| self.expr_distance(alternatives[i], target),
                    minimal,
                );
                self.expand(alternatives[choice], walk);
            }
            Expr::Opt(inner) => {
                let take = if self.shape_neutral(inner, walk) {
                    1
                } else {
                    self.decide(
                        walk,
                        2,
                        |i| {
                            (i == 1)
                                .then(|| self.expr_distance(inner, target))
                                .flatten()
                        },
                        0,
                    )
                };
                if take == 1 {
                    self.expand(inner, walk);
                }
            }
            Expr::Rep(inner) => self.repeat(inner, 0, None, walk),
            Expr::RepOnce(inner) => self.repeat(inner, 1, None, walk),
            Expr::RepExact(inner, n) => self.repeat(inner, *n as usize, Some(*n as usize), walk),
            Expr::RepMin(inner, n) => self.repeat(inner, *n as usize, None, walk),
            Expr::RepMax(inner, n) => self.repeat(inner, 0, Some(*n as usize), walk),
            Expr::RepMinMax(inner, low, high) => {
                self.repeat(inner, *low as usize, Some(*high as usize), walk)
            }
            Expr::NegPred(_) | Expr::PosPred(_) => {}
            other => panic!("unsupported grammar expression {other:?}"),
        }
    }

    fn repeat(&self, inner: &Expr, low: usize, grammar_high: Option<usize>, walk: &mut Walk<'_>) {
        let target = walk.target;
        for _ in 0..low {
            self.expand(inner, walk);
        }
        let extra = if low == 0 && self.shape_neutral(inner, walk) {
            1
        } else {
            let high = grammar_high
                .map_or(low + REPETITION_CAP, |h| h.min(low + REPETITION_CAP))
                .max(low);
            self.decide(
                walk,
                high - low + 1,
                |i| (i > 0).then(|| self.expr_distance(inner, target)).flatten(),
                0,
            )
        };
        for _ in 0..extra {
            self.expand(inner, walk);
        }
    }

    fn decide(
        &self,
        walk: &mut Walk<'_>,
        arity: usize,
        distance: impl Fn(usize) -> Option<u32>,
        minimal: usize,
    ) -> usize {
        if walk.depth > DEPTH_CAP {
            return minimal;
        }
        if walk.local {
            return walk.chooser.choose(arity).unwrap_or(minimal);
        }
        if walk.target.is_some() && !walk.reached {
            let nearest = (0..arity)
                .filter_map(|i| distance(i).map(|d| (d, i)))
                .min()
                .map(|(_, i)| i);
            if let Some(choice) = nearest {
                return choice;
            }
        }
        minimal
    }

    fn expr_distance(&self, expr: &Expr, target: Option<usize>) -> Option<u32> {
        let target = target?;
        let mut nearest: Option<u32> = None;
        visit_idents(expr, &mut |name| {
            if let Some(&index) = self.index.get(name) {
                let d = if index == target {
                    Some(0)
                } else {
                    self.distance[index][target]
                };
                nearest = match (nearest, d) {
                    (Some(a), Some(b)) => Some(a.min(b)),
                    (a, b) => a.or(b),
                };
            }
        });
        nearest
    }

    fn compute_distance(&mut self) {
        let n = self.rules.len();
        let mut distance = vec![vec![None; n]; n];
        for (i, rule) in self.rules.iter().enumerate() {
            visit_idents(&rule.expr, &mut |name| {
                if let Some(&j) = self.index.get(name) {
                    distance[i][j] = Some(1);
                }
            });
        }
        for k in 0..n {
            for i in 0..n {
                for j in 0..n {
                    if let (Some(a), Some(b)) = (distance[i][k], distance[k][j])
                        && distance[i][j].is_none_or(|d| a + b < d)
                    {
                        distance[i][j] = Some(a + b);
                    }
                }
            }
        }
        self.distance = distance;
    }

    fn compute_depth(&mut self) {
        loop {
            let next: Vec<u32> = self
                .rules
                .iter()
                .map(|rule| self.expr_depth(&rule.expr).saturating_add(1))
                .collect();
            if next == self.depth {
                return;
            }
            self.depth = next;
        }
    }

    fn expr_depth(&self, expr: &Expr) -> u32 {
        match expr {
            Expr::Str(_) | Expr::Insens(_) | Expr::Range(..) => 0,
            Expr::Ident(name) => self.index.get(name).map_or(0, |&i| self.depth[i]),
            Expr::Seq(a, b) => self.expr_depth(a).max(self.expr_depth(b)),
            Expr::Choice(a, b) => self.expr_depth(a).min(self.expr_depth(b)),
            Expr::Opt(_)
            | Expr::Rep(_)
            | Expr::RepMax(..)
            | Expr::NegPred(_)
            | Expr::PosPred(_) => 0,
            Expr::RepOnce(inner) | Expr::RepMin(inner, _) => self.expr_depth(inner),
            Expr::RepExact(inner, n) | Expr::RepMinMax(inner, n, _) => {
                if *n == 0 {
                    0
                } else {
                    self.expr_depth(inner)
                }
            }
            other => panic!("unsupported grammar expression {other:?}"),
        }
    }
}

fn fresh_identifier(counter: usize) -> String {
    format!("v{counter}")
}

fn alternatives(expr: &Expr) -> Vec<&Expr> {
    match expr {
        Expr::Choice(a, b) => {
            let mut all = alternatives(a);
            all.extend(alternatives(b));
            all
        }
        other => vec![other],
    }
}

fn visit_idents(expr: &Expr, visit: &mut impl FnMut(&str)) {
    match expr {
        Expr::Ident(name) => visit(name),
        Expr::Seq(a, b) | Expr::Choice(a, b) => {
            visit_idents(a, visit);
            visit_idents(b, visit);
        }
        Expr::Opt(inner)
        | Expr::Rep(inner)
        | Expr::RepOnce(inner)
        | Expr::RepExact(inner, _)
        | Expr::RepMin(inner, _)
        | Expr::RepMax(inner, _)
        | Expr::RepMinMax(inner, _, _) => visit_idents(inner, visit),
        _ => {}
    }
}
