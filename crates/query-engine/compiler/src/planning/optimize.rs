use crate::error::{QueryError, Result};

use super::generic::{Function, Node, Op, Operation, Program, Values};

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
    initial.program.output(&initial.values)?;
    let mut alternatives = vec![initial];

    for rules in stages {
        let mut next = 0;

        while next < alternatives.len() {
            let candidate = alternatives[next].clone();
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

            for (owner, path) in locations {
                let root = if owner == candidate.program.subplans.len() {
                    &candidate.program.root
                } else {
                    &candidate.program.subplans[owner]
                };
                let root = path
                    .iter()
                    .fold(root, |node, index| &node.inputs[*index])
                    .clone();
                let local = Candidate {
                    program: Program {
                        subplans: candidate.program.subplans[..owner].to_vec(),
                        root,
                    },
                    values: candidate.values.clone(),
                };

                for rule in *rules {
                    for rewritten in rule(&local)? {
                        validate_rewrite(&local, &rewritten)?;
                        let expanded = replace(&candidate, owner, &path, rewritten);
                        expanded.program.output(&expanded.values)?;

                        if !alternatives.contains(&expanded) {
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

    if original.program.output(&original.values)? != rewritten.program.output(&rewritten.values)? {
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
