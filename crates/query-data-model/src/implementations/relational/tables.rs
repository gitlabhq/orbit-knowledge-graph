use ontology::DataType;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnSchema {
    pub name: String,
    pub data_type: Option<DataType>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableSchema {
    pub name: String,
    pub columns: Vec<ColumnSchema>,
}

impl TableSchema {
    pub fn new(
        name: impl Into<String>,
        columns: impl IntoIterator<Item = (String, Option<DataType>)>,
    ) -> Self {
        Self {
            name: name.into(),
            columns: columns
                .into_iter()
                .map(|(name, data_type)| ColumnSchema { name, data_type })
                .collect(),
        }
    }

    pub fn column(&self, name: &str) -> Option<&ColumnSchema> {
        self.columns.iter().find(|column| column.name == name)
    }
}
