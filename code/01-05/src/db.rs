//! Top-level structure of the database.

use std::sync::Arc;

use crate::{
    array::DataChunk,
    binder::{BindError, Binder},
    catalog::{CatalogRef, DatabaseCatalog},
    executor::{ExecuteError, ExecutorBuilder},
    parser::{parse, ParserError},
    storage::InMemoryStorage,
};

/// The database instance.
pub struct Database {
    catalog: CatalogRef,
    executor_builder: ExecutorBuilder,
}

impl Default for Database {
    fn default() -> Self {
        Self::new()
    }
}

impl Database {
    /// Create a new database instance.
    pub fn new() -> Self {
        let catalog = Arc::new(DatabaseCatalog::new());
        let storage = Arc::new(InMemoryStorage::new());
        Database {
            catalog: catalog.clone(),
            executor_builder: ExecutorBuilder::new(catalog, storage),
        }
    }

    /// Run SQL queries and return the outputs.
    pub fn run(&self, sql: &str) -> Result<Vec<DataChunk>, Error> {
        // parse
        let stmts = parse(sql)?;
        let mut binder = Binder::new(self.catalog.clone());

        let mut outputs = vec![];
        for stmt in stmts {
            let bound_stmt = binder.bind(&stmt)?;
            debug!("{:#?}", bound_stmt);
            let mut executor = self.executor_builder.build(bound_stmt);
            let output = executor.execute()?;
            outputs.push(output);
        }
        Ok(outputs)
    }
}

/// The error type of database operations.
#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("parse error: {0}")]
    Parse(#[from] ParserError),
    #[error("bind error: {0}")]
    Bind(#[from] BindError),
    #[error("execute error: {0}")]
    Execute(#[from] ExecuteError),
}
