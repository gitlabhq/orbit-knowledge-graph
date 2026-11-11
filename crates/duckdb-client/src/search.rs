use std::collections::HashMap;

use anyhow::{Context, Result};
use arrow::array::{Array, Int64Array, StringArray};
use arrow::record_batch::RecordBatch;
pub use orbit_authorization::Key;
use query_data_model::{Catalog, DuckDb, PropertyId};
use serde_json::{Map, Value};

use crate::{DuckDbClient, sql_lit, string_column};
use orbit_search::corpus::{EXCLUDE_LIKE, EXCLUDE_REGEX};

pub const GLOB_CHARS: [char; 3] = ['*', '?', '['];

#[derive(Debug, Clone, PartialEq)]
pub struct NodeValue {
    pub entity_type: String,
    pub id: Key,
    pub properties: Map<String, Value>,
}

pub struct NodeHydrator {
    entity_type: String,
    table: String,
    identity: String,
    columns: HashMap<String, String>,
    properties: Vec<String>,
}

impl NodeHydrator {
    pub fn new(catalog: &Catalog<DuckDb>, entity_type: &str) -> Result<Self> {
        let graph = catalog.graph();
        let entity = graph
            .entity_named(entity_type)
            .with_context(|| format!("entity {entity_type:?} does not exist"))?;
        let (_, source) = catalog
            .source(entity.id)
            .with_context(|| format!("entity {entity_type:?} is not in the local store"))?;
        let name = |property: &PropertyId| graph.property(*property).name.clone();
        let columns: HashMap<String, String> = entity
            .properties
            .iter()
            .filter_map(|property| Some((name(property), catalog.column(*property)?.to_string())))
            .collect();
        let [identity] = entity.identity.as_slice() else {
            anyhow::bail!("entity {entity_type:?} has a composite identity, which no read keys by");
        };
        let identity = *identity;
        let identity = catalog
            .column(identity)
            .with_context(|| format!("entity {entity_type:?} does not store its identity"))?
            .to_string();
        let properties = entity
            .default_columns
            .iter()
            .filter(|property| catalog.column(**property).is_some_and(|c| c != identity))
            .map(name)
            .collect();
        Ok(Self {
            entity_type: entity.name.clone(),
            table: source
                .table
                .clone()
                .context("the local store holds the entity without a table")?,
            identity,
            columns,
            properties,
        })
    }

    pub fn embedded(entity_type: &str) -> Result<Self> {
        let catalog = Catalog::<DuckDb>::embedded().context("failed to load the embedded model")?;
        Self::new(&catalog, entity_type)
    }

    pub fn column(&self, property: &str) -> Result<&str> {
        self.columns
            .get(property)
            .map(String::as_str)
            .with_context(|| {
                format!(
                    "{property:?} is not a database property of {}",
                    self.entity_type
                )
            })
    }

    pub fn table(&self) -> &str {
        &self.table
    }

    fn projection(&self) -> String {
        let properties = self
            .properties
            .iter()
            .map(|property| format!("{property} := n.{}", self.columns[property]))
            .collect::<Vec<_>>()
            .join(", ");
        format!(
            "n.{} AS id, to_json(struct_pack({properties})) AS properties",
            self.identity
        )
    }

    pub fn query(
        &self,
        client: &DuckDbClient,
        filters: &[(&str, Value)],
        ids: Option<&[Key]>,
    ) -> Result<Vec<NodeValue>> {
        if ids.is_some_and(<[Key]>::is_empty) {
            return Ok(Vec::new());
        }
        let mut predicates: Vec<String> = filters
            .iter()
            .enumerate()
            .map(|(index, (property, _))| {
                self.column(property)
                    .map(|column| format!("n.{column} = ?{}", index + 1))
            })
            .collect::<Result<_>>()?;
        if let Some(ids) = ids {
            predicates.push(format!("n.{} IN ({})", self.identity, id_list(ids)?));
        }
        let where_clause = if predicates.is_empty() {
            String::new()
        } else {
            format!(" WHERE {}", predicates.join(" AND "))
        };
        let params: Vec<Value> = filters.iter().map(|(_, value)| value.clone()).collect();
        let batches = client.query_arrow_json(
            &format!(
                "SELECT {} FROM {} n{}",
                self.projection(),
                self.table,
                where_clause
            ),
            &params,
        )?;
        let nodes = self.nodes_from_batches(&batches)?;
        let Some(ids) = ids else {
            return Ok(nodes);
        };
        let mut by_id: HashMap<Key, NodeValue> = nodes
            .into_iter()
            .map(|node| (node.id.clone(), node))
            .collect();
        Ok(ids.iter().filter_map(|id| by_id.remove(id)).collect())
    }

    fn nodes_from_batches(&self, batches: &[RecordBatch]) -> Result<Vec<NodeValue>> {
        key_column(batches, "id")?
            .into_iter()
            .zip(string_column(batches, "properties"))
            .map(|(id, properties)| {
                Ok(NodeValue {
                    entity_type: self.entity_type.clone(),
                    id,
                    properties: serde_json::from_str(&properties)?,
                })
            })
            .collect()
    }
}

/// Identity keys as SQL literals: integers bare, strings quoted.
fn id_list(ids: &[Key]) -> Result<String> {
    let literals = ids
        .iter()
        .map(|id| match id {
            Key::Int(id) => Ok(id.to_string()),
            Key::Text(id) | Key::Uuid(id) => Ok(sql_lit(id)),
            Key::Composite(_) => anyhow::bail!("a composite key names no one row"),
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(literals.join(", "))
}

/// An identity column's values as keys, in the column's own type.
fn key_column(batches: &[RecordBatch], name: &str) -> Result<Vec<Key>> {
    let mut keys = Vec::new();
    for column in batches
        .iter()
        .filter_map(|batch| batch.column_by_name(name))
    {
        if let Some(ids) = column.as_any().downcast_ref::<Int64Array>() {
            keys.extend(
                ids.iter()
                    .map(|id| id.map(Key::Int))
                    .collect::<Option<Vec<_>>>()
                    .context("a null id")?,
            );
        } else if let Some(ids) = column.as_any().downcast_ref::<StringArray>() {
            keys.extend(
                ids.iter()
                    .map(|id| id.map(|id| Key::Text(id.to_string())))
                    .collect::<Option<Vec<_>>>()
                    .context("a null id")?,
            );
        } else {
            anyhow::bail!("column {name} holds no integer or string ids");
        }
    }
    Ok(keys)
}

pub fn kind_scope(col: &str, kinds: &[String]) -> String {
    if kinds.is_empty() {
        return String::new();
    }
    let list = kinds
        .iter()
        .map(|k| sql_lit(&k.to_lowercase()))
        .collect::<Vec<_>>()
        .join(", ");
    format!("  AND lower({col}) IN ({list})\n")
}

pub fn path_scope(col: &str, paths: &[String], include_excluded: bool) -> String {
    if paths.is_empty() {
        return String::new();
    }
    let alternatives = paths
        .iter()
        .map(|p| {
            let p = p.trim_end_matches('/');
            let literal = format!(
                "{col} = {} OR starts_with({col}, {})",
                sql_lit(p),
                sql_lit(&format!("{p}/"))
            );
            let scope = match p.contains(GLOB_CHARS) {
                true => format!(
                    "{literal} OR {col} GLOB {} OR {col} GLOB {}",
                    sql_lit(p),
                    sql_lit(&format!("{p}/*"))
                ),
                false => literal,
            };
            if include_excluded {
                return format!("({scope})");
            }
            let opted_in = format!(
                "{} OR {}",
                excluded_path_predicate(&sql_lit(p)),
                excluded_path_predicate(&sql_lit(&format!("{p}/")))
            );
            format!(
                "(({scope}) AND ({opted_in} OR NOT {}))",
                excluded_path_predicate(col)
            )
        })
        .collect::<Vec<_>>()
        .join(" OR ");
    format!("  AND ({alternatives})\n")
}

pub fn excluded_path_predicate(col: &str) -> String {
    let likes = EXCLUDE_LIKE
        .iter()
        .map(|pat| format!("{col} LIKE {}", sql_lit(pat)));
    let regexes = EXCLUDE_REGEX
        .iter()
        .map(|re| format!("regexp_matches({col}, {})", sql_lit(re)));
    format!(
        "({})",
        likes.chain(regexes).collect::<Vec<_>>().join(" OR ")
    )
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;

    use super::{Key, id_list, key_column};

    fn batch(ids: Arc<dyn arrow::array::Array>, data_type: DataType) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("id", data_type, true)]));
        RecordBatch::try_new(schema, vec![ids]).unwrap()
    }

    #[test]
    fn identity_keys_keep_their_type_through_the_local_read() {
        let text = batch(Arc::new(StringArray::from(vec!["a", "b"])), DataType::Utf8);
        assert_eq!(
            key_column(&[text], "id").unwrap(),
            [Key::Text("a".into()), Key::Text("b".into())]
        );
        let ints = batch(Arc::new(Int64Array::from(vec![1, 2])), DataType::Int64);
        assert_eq!(
            key_column(&[ints], "id").unwrap(),
            [Key::Int(1), Key::Int(2)]
        );
        let null = batch(
            Arc::new(StringArray::from(vec![Some("a"), None])),
            DataType::Utf8,
        );
        assert!(key_column(&[null], "id").is_err());
        assert_eq!(
            id_list(&[Key::Int(7), Key::Text("o'k".into())]).unwrap(),
            "7, 'o''k'"
        );
    }
}
