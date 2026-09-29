use crate::error::{QueryError, Result};
use std::collections::HashMap;
use std::hash::{DefaultHasher, Hash, Hasher};

use super::generic::{Function, Node, Op, Operation, Program, Schema, Values};

#[derive(Clone, PartialEq)]
pub struct Candidate<S, F, E> {
    pub program: Program<S, F, E>,
    pub values: Values,
}

pub type Rule<S, F, E> = fn(&Candidate<S, F, E>) -> Result<Vec<Candidate<S, F, E>>>;

pub fn candidates<
    S: Operation + Clone + PartialEq,
    F: Function + Clone + PartialEq,
    E: Operation + Clone + PartialEq,
>(
    root: Node<S, F, E>,
    values: Values,
    rules: &[Rule<S, F, E>],
) -> Result<Vec<Candidate<S, F, E>>> {
    let initial = Candidate {
        program: Program {
            subplans: vec![],
            root,
        },
        values,
    };
    stages(initial, &[rules])
}

pub fn stages<
    S: Operation + Clone + PartialEq,
    F: Function + Clone + PartialEq,
    E: Operation + Clone + PartialEq,
>(
    initial: Candidate<S, F, E>,
    stages: &[&[Rule<S, F, E>]],
) -> Result<Vec<Candidate<S, F, E>>> {
    enumerate(initial, stages, &mut |_| Ok(()))
}

pub fn normalized_candidates<
    S: Operation + Clone + PartialEq,
    F: Function + Clone + PartialEq,
    E: Operation + Clone + PartialEq,
>(
    root: Node<S, F, E>,
    values: Values,
    rules: &[Rule<S, F, E>],
    mut normalize: impl FnMut(&mut Candidate<S, F, E>) -> Result<()>,
) -> Result<Vec<Candidate<S, F, E>>> {
    enumerate(
        Candidate {
            program: Program {
                subplans: vec![],
                root,
            },
            values,
        },
        &[rules],
        &mut normalize,
    )
}

fn enumerate<
    S: Operation + Clone + PartialEq,
    F: Function + Clone + PartialEq,
    E: Operation + Clone + PartialEq,
>(
    mut initial: Candidate<S, F, E>,
    stages: &[&[Rule<S, F, E>]],
    normalize: &mut impl FnMut(&mut Candidate<S, F, E>) -> Result<()>,
) -> Result<Vec<Candidate<S, F, E>>> {
    let output = initial.program.output(&initial.values)?;
    normalize(&mut initial)?;
    if initial.program.output(&initial.values)? != output {
        return Err(QueryError::PipelineInvariant(
            "normalization changed output contract".into(),
        ));
    }
    let preserved_values = initial.values.len();
    let mut alternatives = vec![initial];
    let mut seen = HashMap::<u64, Vec<usize>>::new();
    seen.entry(fingerprint(&alternatives[0].program))
        .or_default()
        .push(0);

    for rules in stages {
        let mut next = 0;

        while next < alternatives.len() {
            let candidate = alternatives[next].clone();
            let mut schemas = Vec::with_capacity(candidate.program.subplans.len());
            for subplan in &candidate.program.subplans {
                schemas.push(subplan.output_with(&candidate.values, &schemas)?);
            }
            let mut locations = Vec::new();
            for (owner, root) in candidate
                .program
                .subplans
                .iter()
                .chain(std::iter::once(&candidate.program.root))
                .enumerate()
            {
                collect_locations(root, owner, &mut Vec::new(), &mut locations);
            }

            let mut local = Candidate {
                program: Program {
                    subplans: Vec::new(),
                    root: candidate.program.root.clone(),
                },
                values: candidate.values.clone(),
            };
            for (owner, path) in locations {
                while local.program.subplans.len() < owner {
                    local
                        .program
                        .subplans
                        .push(candidate.program.subplans[local.program.subplans.len()].clone());
                }
                let root = if owner == candidate.program.subplans.len() {
                    &candidate.program.root
                } else {
                    &candidate.program.subplans[owner]
                };
                let root = path
                    .iter()
                    .fold(root, |node, index| &node.inputs[*index])
                    .clone();
                local.program.root = root;
                let mut local_output = None;

                for rule in *rules {
                    let rewrites = rule(&local)?;
                    for rewritten in rewrites {
                        if local_output.is_none() {
                            local_output = Some(
                                local
                                    .program
                                    .root
                                    .output_with(&local.values, &schemas[..owner])?,
                            );
                        }
                        validate_rewrite(
                            &local,
                            &rewritten,
                            &schemas[..owner],
                            local_output.as_ref().unwrap(),
                        )?;
                        let mut expanded = replace(&candidate, owner, &path, rewritten);
                        normalize(&mut expanded)?;
                        if expanded.program.output(&expanded.values)? != output {
                            return Err(QueryError::PipelineInvariant(
                                "normalization changed output contract".into(),
                            ));
                        }
                        expanded.canonicalize_values(preserved_values)?;

                        let bucket = seen.entry(fingerprint(&expanded.program)).or_default();
                        if !bucket.iter().any(|index| alternatives[*index] == expanded) {
                            bucket.push(alternatives.len());
                            alternatives.push(expanded);
                        }
                    }
                }
            }

            next += 1;
        }
    }

    Ok(alternatives)
}

fn fingerprint<S, F, E>(program: &Program<S, F, E>) -> u64 {
    fn node<S, F, E>(root: &Node<S, F, E>, state: &mut DefaultHasher) {
        std::mem::discriminant(&root.op).hash(state);
        root.inputs.len().hash(state);
        match &root.op {
            Op::Reference { subplan, exports } => {
                subplan.hash(state);
                exports.hash(state);
            }
            Op::Project(assignments) => {
                assignments.len().hash(state);
                for assignment in assignments {
                    assignment.output.hash(state);
                }
            }
            Op::Union { outputs, arms } => {
                outputs.hash(state);
                arms.hash(state);
            }
            Op::Join { kind, .. } => std::mem::discriminant(kind).hash(state),
            _ => {}
        }
        for input in &root.inputs {
            node(input, state);
        }
    }
    let mut state = DefaultHasher::new();
    program.subplans.len().hash(&mut state);
    for root in program
        .subplans
        .iter()
        .chain(std::iter::once(&program.root))
    {
        node(root, &mut state);
    }
    state.finish()
}

impl<S: Operation + Clone, F: Function + Clone, E: Operation + Clone> Candidate<S, F, E> {
    pub fn canonicalize(&mut self, preserved_values: usize) -> Result<()> {
        self.program.output(&self.values)?;
        self.canonicalize_values(preserved_values)?;
        self.program.output(&self.values)?;
        Ok(())
    }

    fn canonicalize_values(&mut self, preserved_values: usize) -> Result<()> {
        if preserved_values > self.values.len() {
            return Err(QueryError::PipelineInvariant(
                "canonicalization exceeds the value catalog".into(),
            ));
        }

        let mut order = Vec::new();
        let mut visited = vec![false; self.program.subplans.len()];

        fn dependencies<S: Clone, F: Clone, E: Clone>(
            node: &Node<S, F, E>,
            subplans: &[Node<S, F, E>],
            visited: &mut [bool],
            order: &mut Vec<usize>,
        ) {
            if let Op::Reference { subplan, .. } = &node.op
                && !visited[subplan.0]
            {
                visited[subplan.0] = true;
                dependencies(&subplans[subplan.0], subplans, visited, order);
                order.push(subplan.0);
            }

            for input in &node.inputs {
                dependencies(input, subplans, visited, order);
            }
        }

        dependencies(
            &self.program.root,
            &self.program.subplans,
            &mut visited,
            &mut order,
        );
        let indices: HashMap<_, _> = order
            .iter()
            .enumerate()
            .map(|(new, old)| (*old, new))
            .collect();
        self.program.subplans = order
            .into_iter()
            .map(|index| self.program.subplans[index].clone())
            .collect();

        let mut values = self.values.prefix(preserved_values);
        let mut mapping: HashMap<_, _> = values.ids().map(|id| (id, id)).collect();
        for root in self
            .program
            .subplans
            .iter_mut()
            .chain(std::iter::once(&mut self.program.root))
        {
            root.visit_mut(&mut |node| {
                if let Op::Reference { subplan, .. } = &mut node.op {
                    subplan.0 = indices[&subplan.0];
                }
            });
            root.map_values(&mut |value| {
                *value = *mapping.entry(*value).or_insert_with(|| {
                    values.allocate(
                        self.values
                            .data_type(*value)
                            .expect("validated value")
                            .clone(),
                    )
                });
            });
        }

        self.values = values;
        Ok(())
    }
}

fn collect_locations<S, F, E>(
    node: &Node<S, F, E>,
    owner: usize,
    path: &mut Vec<usize>,
    locations: &mut Vec<(usize, Vec<usize>)>,
) {
    for (index, input) in node.inputs.iter().enumerate() {
        path.push(index);
        collect_locations(input, owner, path, locations);
        path.pop();
    }

    locations.push((owner, path.clone()));
}

fn validate_rewrite<S: Operation + PartialEq, F: Function + PartialEq, E: Operation + PartialEq>(
    original: &Candidate<S, F, E>,
    rewritten: &Candidate<S, F, E>,
    schemas: &[Schema],
    output: &Schema,
) -> Result<()> {
    if !rewritten.values.extends(&original.values)
        || !rewritten
            .program
            .subplans
            .starts_with(&original.program.subplans)
    {
        return Err(QueryError::PipelineInvariant(
            "optimization changed existing values or subplan definitions".into(),
        ));
    }

    let mut schemas = schemas.to_vec();
    for subplan in &rewritten.program.subplans[original.program.subplans.len()..] {
        schemas.push(subplan.output_with(&rewritten.values, &schemas)?);
    }
    if *output
        != rewritten
            .program
            .root
            .output_with(&rewritten.values, &schemas)?
    {
        return Err(QueryError::PipelineInvariant(
            "optimization changed the output contract".into(),
        ));
    }

    Ok(())
}

fn replace<S: Clone, F: Clone, E: Clone>(
    original: &Candidate<S, F, E>,
    owner: usize,
    path: &[usize],
    mut rewritten: Candidate<S, F, E>,
) -> Candidate<S, F, E> {
    let additions = rewritten.program.subplans.split_off(owner);
    let count = additions.len();
    let mut program = original.program.clone();

    for root in program
        .subplans
        .iter_mut()
        .chain(std::iter::once(&mut program.root))
    {
        root.visit_mut(&mut |node| {
            if let Op::Reference { subplan, .. } = &mut node.op
                && subplan.0 >= owner
            {
                subplan.0 += count;
            }
        });
    }

    program.subplans.splice(owner..owner, additions);
    let root = if owner == original.program.subplans.len() {
        &mut program.root
    } else {
        &mut program.subplans[owner + count]
    };
    let target = path
        .iter()
        .fold(root, |node, index| &mut node.inputs[*index]);
    *target = rewritten.program.root;

    Candidate {
        program,
        values: rewritten.values,
    }
}

pub fn select<S: Operation, F: Function, E: Operation, Cost: Ord>(
    candidates: Vec<Candidate<S, F, E>>,
    mut cost: impl FnMut(&Program<S, F, E>) -> Cost,
) -> Result<Option<Candidate<S, F, E>>> {
    let mut best = None;

    for candidate in candidates {
        candidate.program.output(&candidate.values)?;
        let score = cost(&candidate.program);

        if best.as_ref().is_none_or(|(_, previous)| score < *previous) {
            best = Some((candidate, score));
        }
    }

    Ok(best.map(|(candidate, _)| candidate))
}

pub fn estimated_work<S, F, E>(
    program: &Program<S, F, E>,
    source_work: impl Fn(&S) -> u64,
) -> (u64, usize) {
    fn work<S, F, E>(
        node: &Node<S, F, E>,
        subplans: &[u64],
        source_work: &impl Fn(&S) -> u64,
    ) -> u64 {
        let own = match &node.op {
            Op::Read(source) => source_work(source),
            Op::Reference { subplan, .. } => subplans[subplan.0],
            _ => 1,
        };

        node.inputs.iter().fold(own, |total, input| {
            total.saturating_add(work(input, subplans, source_work))
        })
    }

    let mut subplans = Vec::new();
    for subplan in &program.subplans {
        subplans.push(work(subplan, &subplans, &source_work));
    }

    (
        work(&program.root, &subplans, &source_work),
        program.subplans.len(),
    )
}
