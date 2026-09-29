use crate::error::{QueryError, Result};

use super::generic::{Function, Node, Op, Operation, Program, Values};

#[derive(Clone)]
pub struct Candidate<S, F, E> {
    pub program: Program<S, F, E>,
    pub values: Values,
}

pub type Rule<S, F, E> = fn(&Candidate<S, F, E>) -> Result<Vec<Candidate<S, F, E>>>;

pub fn candidates<S: Operation + Clone, F: Function + Clone, E: Operation + Clone>(
    root: Node<S, F, E>,
    values: Values,
    rules: &[Rule<S, F, E>],
) -> Result<Vec<Candidate<S, F, E>>> {
    root.output(&values)?;
    enumerate(root, values, Vec::new(), rules)
}

fn enumerate<S: Operation + Clone, F: Function + Clone, E: Operation + Clone>(
    root: Node<S, F, E>,
    values: Values,
    subplans: Vec<Node<S, F, E>>,
    rules: &[Rule<S, F, E>],
) -> Result<Vec<Candidate<S, F, E>>> {
    let mut combinations = vec![(Vec::new(), values, subplans)];

    for input in root.inputs {
        let mut next = Vec::new();

        for (inputs, values, subplans) in combinations {
            for candidate in enumerate(input.clone(), values, subplans, rules)? {
                let mut inputs = inputs.clone();
                inputs.push(candidate.program.root);
                next.push((inputs, candidate.values, candidate.program.subplans));
            }
        }

        combinations = next;
    }

    let mut result = Vec::new();
    for (inputs, values, subplans) in combinations {
        let root = Node {
            op: root.op.clone(),
            inputs,
        };
        let original = Candidate {
            program: Program { subplans, root },
            values,
        };
        let expected = original.program.output(&original.values)?;
        let types = expected
            .iter()
            .map(|value| original.values.data_type(*value).cloned())
            .collect::<Result<Vec<_>>>()?;
        let mut alternatives = vec![original];

        for rule in rules {
            let mut additions = Vec::new();

            for candidate in &alternatives {
                for rewritten in rule(candidate)? {
                    let output = rewritten.program.output(&rewritten.values)?;
                    let output_types = output
                        .iter()
                        .map(|value| rewritten.values.data_type(*value).cloned())
                        .collect::<Result<Vec<_>>>()?;

                    if output != expected || output_types != types {
                        return Err(QueryError::PipelineInvariant(
                            "optimization changed the output contract".into(),
                        ));
                    }

                    additions.push(rewritten);
                }
            }

            alternatives.extend(additions);
        }

        result.extend(alternatives);
    }

    Ok(result)
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
