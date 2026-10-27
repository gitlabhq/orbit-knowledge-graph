use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};

use arrow::array::{Array, ListArray, StructArray};
use arrow::record_batch::RecordBatch;
use compiler::{Fold, MAX_SPLIT_GROUPS, MAX_SPLIT_OWNERS, OwnerSplit, ResultContext};
use orbit_arrow::{ArrowUtils, ColumnValue};

use super::{QueryResultError, QueryResultRow, column};

type OwnerKey = Vec<Option<i64>>;

pub(super) fn expand(
    batches: &[RecordBatch],
    split: &OwnerSplit,
    ctx: &ResultContext,
) -> Result<(Vec<QueryResultRow>, Vec<usize>), QueryResultError> {
    let mut rows = Vec::new();
    let mut ends = Vec::new();
    let mut checked_ids = HashSet::new();
    for batch in batches {
        let partials: &ListArray = column(batch, &split.partials_column)?;
        for row in 0..batch.num_rows() {
            if ends.len() == MAX_SPLIT_GROUPS as usize {
                return Err(QueryResultError::TooManyGroups);
            }
            if partials.is_null(row) {
                return Err(QueryResultError::InvalidMetadata);
            }
            let values = partials.value(row);
            let fields = values
                .as_any()
                .downcast_ref::<StructArray>()
                .filter(|fields| fields.num_columns() == split.partials.len())
                .ok_or(QueryResultError::InvalidMetadata)?;
            let mut base = ArrowUtils::extract_row(batch, row);
            base.remove(&split.partials_column);
            for index in 0..fields.len() {
                let mut columns = base.clone();
                for (name, values) in split.partials.iter().zip(fields.columns()) {
                    columns.insert(name.clone(), ArrowUtils::extract_value(values, index));
                }
                let partial = QueryResultRow::new(columns, Vec::new(), Vec::new());
                for node in ctx.nodes() {
                    let id = match split
                        .owners
                        .iter()
                        .find(|owner| owner.id_column == node.id_column)
                    {
                        Some(owner) => owner_key(&partial, &owner.key),
                        None => vec![partial.get_id(node)],
                    };
                    checked_ids.insert((node.alias.as_str(), id));
                }
                if checked_ids.len() > MAX_SPLIT_OWNERS {
                    return Err(QueryResultError::TooManyOwners);
                }
                rows.push(partial);
            }
            ends.push(rows.len());
        }
    }
    share_representatives(&mut rows, split);
    Ok((rows, ends))
}

fn owner_key(row: &QueryResultRow, key: &[String]) -> OwnerKey {
    key.iter()
        .map(|column| row.get_column_i64(column))
        .collect()
}

fn share_representatives(rows: &mut [QueryResultRow], split: &OwnerSplit) {
    for owner in &split.owners {
        let mut representatives: HashMap<OwnerKey, i64> = HashMap::new();
        for row in rows.iter() {
            if let Some(id) = row.get_column_i64(&owner.id_column) {
                representatives
                    .entry(owner_key(row, &owner.key))
                    .and_modify(|current| *current = (*current).min(id))
                    .or_insert(id);
            }
        }
        for row in rows.iter_mut() {
            if let Some(id) = representatives.get(&owner_key(row, &owner.key)) {
                row.set_column(owner.id_column.clone(), ColumnValue::Int64(*id));
            }
        }
    }
}

pub(super) fn fold(
    rows: Vec<QueryResultRow>,
    ends: &[usize],
    split: &OwnerSplit,
) -> Vec<QueryResultRow> {
    let mut groups = Vec::new();
    let mut start = 0;
    for &end in ends {
        let mut readable = rows[start..end].iter().filter(|row| row.authorized);
        start = end;
        let Some(first) = readable.next() else {
            continue;
        };
        let mut group = first.clone();
        for partial in readable {
            for measure in &split.measures {
                let combine = match measure.fold {
                    Fold::Add | Fold::Mean { .. } => add,
                    Fold::Min => least,
                    Fold::Max => greatest,
                };
                merge(&mut group, partial, &measure.name, combine);
                if let Fold::Mean { count } = &measure.fold {
                    merge(&mut group, partial, count, add);
                }
            }
        }
        for measure in &split.measures {
            if let Fold::Mean { count } = &measure.fold {
                let mean = match (
                    group.get(&measure.name).and_then(number),
                    group.get(count).and_then(number),
                ) {
                    (Some(sum), Some(count)) if count > 0.0 => ColumnValue::Float64(sum / count),
                    _ => ColumnValue::Null,
                };
                group.set_column(measure.name.clone(), mean);
            }
        }
        groups.push(group);
    }
    groups.sort_by(|left, right| {
        split
            .order
            .iter()
            .map(|(column, descending)| compare(left.get(column), right.get(column), *descending))
            .find(|order| order.is_ne())
            .unwrap_or(Ordering::Equal)
    });
    groups
        .into_iter()
        .skip(split.skip)
        .take(split.limit)
        .collect()
}

fn merge(
    group: &mut QueryResultRow,
    partial: &QueryResultRow,
    column: &str,
    combine: fn(&ColumnValue, &ColumnValue) -> ColumnValue,
) {
    let (Some(current), Some(next)) = (group.get(column), partial.get(column)) else {
        return;
    };
    let value = combine(current, next);
    group.set_column(column.to_string(), value);
}

fn add(left: &ColumnValue, right: &ColumnValue) -> ColumnValue {
    match (left, right) {
        (ColumnValue::Int64(left), ColumnValue::Int64(right)) => {
            ColumnValue::Int64(left.saturating_add(*right))
        }
        (ColumnValue::Null, value) | (value, ColumnValue::Null) => value.clone(),
        _ => match (number(left), number(right)) {
            (Some(left), Some(right)) => ColumnValue::Float64(left + right),
            _ => ColumnValue::Null,
        },
    }
}

fn least(left: &ColumnValue, right: &ColumnValue) -> ColumnValue {
    match compare(Some(left), Some(right), false) {
        Ordering::Greater => right.clone(),
        _ => left.clone(),
    }
}

fn greatest(left: &ColumnValue, right: &ColumnValue) -> ColumnValue {
    match compare(Some(left), Some(right), true) {
        Ordering::Greater => right.clone(),
        _ => left.clone(),
    }
}

fn compare(left: Option<&ColumnValue>, right: Option<&ColumnValue>, descending: bool) -> Ordering {
    let left = left.filter(|value| **value != ColumnValue::Null);
    let right = right.filter(|value| **value != ColumnValue::Null);
    match (left, right) {
        (None, None) => Ordering::Equal,
        (None, Some(_)) => Ordering::Greater,
        (Some(_), None) => Ordering::Less,
        (Some(left), Some(right)) => {
            let order = match (left, right) {
                (ColumnValue::Int64(left), ColumnValue::Int64(right)) => left.cmp(right),
                (ColumnValue::String(left), ColumnValue::String(right)) => left.cmp(right),
                (ColumnValue::Bool(left), ColumnValue::Bool(right)) => left.cmp(right),
                _ => number(left)
                    .partial_cmp(&number(right))
                    .unwrap_or(Ordering::Equal),
            };
            if descending { order.reverse() } else { order }
        }
    }
}

fn number(value: &ColumnValue) -> Option<f64> {
    match value {
        ColumnValue::Int64(value) => Some(*value as f64),
        ColumnValue::Float64(value) => Some(*value),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{ArrayRef, Float64Array, Int64Array, StringArray};
    use arrow::buffer::OffsetBuffer;
    use arrow::datatypes::{DataType, Field, Fields, Schema};
    use compiler::{EntityAuthConfig, SplitMeasure, SplitOwner};

    use super::*;
    use crate::{QueryResult, ResourceAuthorization};

    fn split(measures: Vec<SplitMeasure>, partials: &[&str], limit: usize) -> ResultContext {
        let mut ctx = ResultContext::new().with_split(OwnerSplit {
            partials_column: "_orbit_partials".into(),
            partials: partials.iter().map(|name| name.to_string()).collect(),
            owners: vec![SplitOwner {
                id_column: "_orbit_j_id".into(),
                key: vec!["_orbit_j_owner0".into()],
            }],
            measures,
            order: vec![("n".into(), true), ("name".into(), false)],
            skip: 0,
            limit,
        });
        ctx.add_node("j", "Job");
        ctx.add_entity_auth(
            "Job",
            EntityAuthConfig {
                resource_type: "ci_build".into(),
                ability: "read_build".into(),
                auth_id_column: "id".into(),
                owner_entity: None,
                required_access_level: 20,
            },
        );
        ctx
    }

    fn count(name: &str) -> SplitMeasure {
        SplitMeasure {
            name: name.into(),
            fold: Fold::Add,
        }
    }

    fn batch(names: &[&str], lengths: &[usize], partials: Vec<(&str, ArrayRef)>) -> RecordBatch {
        let fields: Fields = partials
            .iter()
            .map(|(name, values)| Field::new(*name, values.data_type().clone(), true))
            .collect();
        let values = StructArray::new(
            fields.clone(),
            partials.into_iter().map(|(_, values)| values).collect(),
            None,
        );
        let list = ListArray::new(
            Arc::new(Field::new("item", DataType::Struct(fields), true)),
            OffsetBuffer::from_lengths(lengths.iter().copied()),
            Arc::new(values),
            None,
        );
        let types = vec!["Job"; names.len()];
        let columns: Vec<(&str, ArrayRef)> = vec![
            ("name", Arc::new(StringArray::from(names.to_vec()))),
            ("_orbit_j_type", Arc::new(StringArray::from(types))),
            ("_orbit_partials", Arc::new(list)),
        ];
        let schema = Schema::new(
            columns
                .iter()
                .map(|(name, values)| Field::new(*name, values.data_type().clone(), true))
                .collect::<Vec<_>>(),
        );
        RecordBatch::try_new(
            Arc::new(schema),
            columns.into_iter().map(|(_, values)| values).collect(),
        )
        .unwrap()
    }

    fn ints(values: Vec<i64>) -> ArrayRef {
        Arc::new(Int64Array::from(values))
    }

    fn rows(result: &QueryResult, columns: &[&str]) -> Vec<Vec<ColumnValue>> {
        result
            .authorized_rows()
            .map(|row| {
                columns
                    .iter()
                    .map(|column| row.get(column).cloned().unwrap_or(ColumnValue::Null))
                    .collect()
            })
            .collect()
    }

    #[test]
    fn denied_owner_partials_leave_every_group_before_ranking() {
        let ctx = split(
            vec![count("n")],
            &["_orbit_j_owner0", "n", "_orbit_j_id"],
            2,
        );
        let batch = batch(
            &["build", "rspec", "lint"],
            &[2, 2, 1],
            vec![
                ("1", ints(vec![1000, 2000, 1000, 2000, 1000])),
                ("2", ints(vec![1, 9, 2, 1, 1])),
                ("3", ints(vec![14, 21, 11, 22, 13])),
            ],
        );
        let mut result = QueryResult::from_batches(&[batch], &ctx).unwrap();

        let checks = result.resource_checks();
        assert_eq!(checks.len(), 1);
        let mut ids = checks[0].ids.clone();
        ids.sort_unstable();
        assert_eq!(ids, vec![11, 21], "one representative per project");

        result.apply_authorizations(&[ResourceAuthorization {
            resource_type: "ci_build".into(),
            authorized: [(11, true), (21, false)].into_iter().collect(),
        }]);
        let name = |value: &str| ColumnValue::String(value.into());
        assert_eq!(
            rows(&result, &["name", "n"]),
            vec![
                vec![name("rspec"), ColumnValue::Int64(2)],
                vec![name("build"), ColumnValue::Int64(1)],
            ],
        );
    }

    #[test]
    fn means_and_minimums_fold_over_readable_partials() {
        let measures = vec![
            SplitMeasure {
                name: "avg_timeout".into(),
                fold: Fold::Mean {
                    count: "_orbit_avg_timeout_count".into(),
                },
            },
            SplitMeasure {
                name: "first".into(),
                fold: Fold::Min,
            },
            count("n"),
        ];
        let partials = [
            "_orbit_j_owner0",
            "avg_timeout",
            "_orbit_avg_timeout_count",
            "first",
            "n",
            "_orbit_j_id",
        ];
        let ctx = split(measures, &partials, 10);
        let batch = batch(
            &["rspec"],
            &[3],
            vec![
                ("1", ints(vec![1000, 2000, 3000])),
                ("2", Arc::new(Float64Array::from(vec![30.0, 10.0, 0.0]))),
                ("3", ints(vec![2, 1, 0])),
                (
                    "4",
                    Arc::new(StringArray::from(vec![
                        None,
                        Some("2024-01-02"),
                        Some("2024-01-03"),
                    ])),
                ),
                ("5", ints(vec![2, 1, 4])),
                ("6", ints(vec![1, 2, 3])),
            ],
        );
        let mut result = QueryResult::from_batches(&[batch], &ctx).unwrap();
        result.apply_authorizations(&[ResourceAuthorization {
            resource_type: "ci_build".into(),
            authorized: [(1, true), (2, true), (3, true)].into_iter().collect(),
        }]);
        assert_eq!(
            rows(&result, &["avg_timeout", "first", "n"]),
            vec![vec![
                ColumnValue::Float64(40.0 / 3.0),
                ColumnValue::String("2024-01-02".into()),
                ColumnValue::Int64(7),
            ]],
        );
    }

    #[test]
    fn too_many_owners_or_groups_fail_before_authorization() {
        let partials = ["_orbit_j_owner0", "n", "_orbit_j_id"];
        let ctx = split(vec![count("n")], &partials, 10);
        let owners = MAX_SPLIT_OWNERS + 1;
        let ids: Vec<i64> = (0..owners as i64).collect();
        let wide = batch(
            &["rspec"],
            &[owners],
            vec![
                ("1", ints(ids.clone())),
                ("2", ints(vec![1; owners])),
                ("3", ints(ids)),
            ],
        );
        assert_eq!(
            QueryResult::from_batches(&[wide], &ctx).unwrap_err(),
            QueryResultError::TooManyOwners,
        );

        let groups = MAX_SPLIT_GROUPS as usize + 1;
        let names: Vec<String> = (0..groups).map(|index| format!("job-{index}")).collect();
        let names: Vec<&str> = names.iter().map(String::as_str).collect();
        let many = batch(
            &names,
            &vec![1; groups],
            vec![
                ("1", ints(vec![1000; groups])),
                ("2", ints(vec![1; groups])),
                ("3", ints(vec![1; groups])),
            ],
        );
        assert_eq!(
            QueryResult::from_batches(&[many], &ctx).unwrap_err(),
            QueryResultError::TooManyGroups,
        );
    }
}
