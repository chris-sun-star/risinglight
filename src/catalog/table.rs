// Copyright 2023 RisingLight Project Authors. Licensed under Apache-2.0.

use std::collections::{BTreeMap, HashMap};

use super::*;

/// The catalog of a table.
pub struct TableCatalog {
    id: TableId,
    name: String,
    type_: TableType,
    /// Mapping from column names to column ids
    column_idxs: HashMap<String, ColumnId>,
    columns: BTreeMap<ColumnId, ColumnCatalog>,
    primary_keys: Vec<ColumnId>,
}

/// The type of a table.
#[derive(Default, Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum TableType {
    /// Base table.
    #[default]
    Base,
    /// System table.
    System,
    /// Materialized view.
    MaterializedView,
}

impl TableCatalog {
    pub fn new(
        id: TableId,
        name: String,
        type_: TableType,
        columns: Vec<ColumnCatalog>,
        primary_keys: Vec<ColumnId>,
    ) -> TableCatalog {
        let mut table_catalog = TableCatalog {
            id,
            name,
            type_,
            column_idxs: HashMap::new(),
            columns: BTreeMap::new(),
            primary_keys,
        };
        table_catalog
            .add_column(ColumnCatalog::new(
                u32::MAX,
                DataTypeKind::Int64.not_null().to_column("_rowid_".into()),
            ))
            .unwrap();
        for col_catalog in columns {
            table_catalog.add_column(col_catalog).unwrap();
        }

        table_catalog
    }

    fn add_column(&mut self, col_catalog: ColumnCatalog) -> Result<ColumnId, CatalogError> {
        if self.column_idxs.contains_key(col_catalog.name()) {
            return Err(CatalogError::Duplicated(
                "column",
                col_catalog.name().into(),
            ));
        }
        let id = col_catalog.id();
        self.column_idxs
            .insert(col_catalog.name().to_string(), col_catalog.id());
        self.columns.insert(id, col_catalog);
        Ok(id)
    }

    pub fn contains_column(&self, name: &str) -> bool {
        self.column_idxs.contains_key(name)
    }

    pub fn all_columns(&self) -> BTreeMap<ColumnId, ColumnCatalog> {
        let mut columns = self.columns.clone();
        columns.remove(&u32::MAX); // remove rowid
        columns
    }

    pub fn all_columns_with_rowid(&self) -> BTreeMap<ColumnId, ColumnCatalog> {
        self.columns.clone()
    }

    pub fn get_column_id_by_name(&self, name: &str) -> Option<ColumnId> {
        self.column_idxs.get(name).cloned()
    }

    pub fn get_column_by_id(&self, id: ColumnId) -> Option<ColumnCatalog> {
        self.columns.get(&id).cloned()
    }

    pub fn get_column_by_name(&self, name: &str) -> Option<ColumnCatalog> {
        self.column_idxs
            .get(name)
            .and_then(|id| self.columns.get(id))
            .cloned()
    }

    pub fn name(&self) -> String {
        self.name.clone()
    }

    pub fn id(&self) -> TableId {
        self.id
    }

    pub fn primary_keys(&self) -> Vec<ColumnId> {
        self.primary_keys.clone()
    }

    pub fn is_base(&self) -> bool {
        self.type_ == TableType::Base
    }

    pub fn is_system(&self) -> bool {
        self.type_ == TableType::System
    }

    pub fn is_materialized_view(&self) -> bool {
        self.type_ == TableType::MaterializedView
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_table_catalog() {
        let col0 = ColumnCatalog::new(0, DataTypeKind::Int32.not_null().to_column("a".into()));
        let col1 = ColumnCatalog::new(1, DataTypeKind::Bool.not_null().to_column("b".into()));

        let col_catalogs = vec![col0, col1];
        let table_catalog = TableCatalog::new(0, "t".into(), TableType::Base, col_catalogs, vec![]);

        assert!(!table_catalog.contains_column("c"));
        assert!(table_catalog.contains_column("a"));
        assert!(table_catalog.contains_column("b"));

        assert_eq!(table_catalog.get_column_id_by_name("a"), Some(0));
        assert_eq!(table_catalog.get_column_id_by_name("b"), Some(1));

        let col0_catalog = table_catalog.get_column_by_id(0).unwrap();
        assert_eq!(col0_catalog.name(), "a");
        assert_eq!(col0_catalog.datatype().kind(), DataTypeKind::Int32);

        let col1_catalog = table_catalog.get_column_by_id(1).unwrap();
        assert_eq!(col1_catalog.name(), "b");
        assert_eq!(col1_catalog.datatype().kind(), DataTypeKind::Bool);
    }
}
