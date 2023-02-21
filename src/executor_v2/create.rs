// Copyright 2023 RisingLight Project Authors. Licensed under Apache-2.0.

use std::sync::Arc;

use super::*;
use crate::binder_v2::{CreateMView, CreateTable};
use crate::storage::Storage;
use crate::streaming::StreamManager;

/// The executor of `create table` statement.
pub struct CreateTableExecutor<S: Storage> {
    pub plan: CreateTable,
    pub storage: Arc<S>,
    pub stream: Arc<StreamManager>,
}

impl<S: Storage> CreateTableExecutor<S> {
    #[try_stream(boxed, ok = DataChunk, error = ExecutorError)]
    pub async fn execute(self) {
        let id = self
            .storage
            .create_table(
                self.plan.schema_id,
                &self.plan.table_name,
                &self.plan.columns,
                &self.plan.ordered_pk_ids,
            )
            .await?;

        if self.plan.with.contains_key("connector") {
            self.stream.create_source(id, &self.plan.with).await?;
        }

        let chunk = DataChunk::single(1);
        yield chunk
    }
}

/// The executor of `create materialized view` statement.
pub struct CreateMViewExecutor {
    pub args: CreateMView,
    pub query: RecExpr,
    pub stream: Arc<StreamManager>,
}

impl CreateMViewExecutor {
    #[try_stream(boxed, ok = DataChunk, error = ExecutorError)]
    pub async fn execute(self) {
        self.stream.create_mview(self.args, self.query)?;
        yield DataChunk::single(1);
    }
}
