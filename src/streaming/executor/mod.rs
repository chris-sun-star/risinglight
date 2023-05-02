// Copyright 2023 RisingLight Project Authors. Licensed under Apache-2.0.

use egg::{Id, Language};
use futures::stream::BoxStream;
use futures::Stream;
use futures_async_stream::try_stream;

use super::*;
use crate::array::StreamChunk;
use crate::executor_v2::spawn_stream;
use crate::planner::{Expr, RecExpr, TypeSchemaAnalysis};
use crate::storage::Storage;
use crate::types::{ColumnIndex, DataType};

mod projection;
mod table_scan;

use self::projection::*;
use self::table_scan::*;

pub type BoxDiffStream = BoxStream<'static, Result<StreamChunk>>;

/// Build the stream pipeline of a query.
pub struct Builder<'a> {
    streaming: &'a StreamManager,
    egraph: egg::EGraph<Expr, TypeSchemaAnalysis>,
    root: Id,
}

impl<'a> Builder<'a> {
    /// Create a new executor builder.
    pub fn new(streaming: &'a StreamManager, plan: &RecExpr) -> Self {
        let mut egraph = egg::EGraph::new(TypeSchemaAnalysis {
            catalog: streaming.catalog.clone(),
        });
        let root = egraph.add_expr(plan);
        Builder {
            streaming,
            egraph,
            root,
        }
    }

    fn node(&self, id: Id) -> &Expr {
        &self.egraph[id].nodes[0]
    }

    /// Extract a `RecExpr` from id.
    fn recexpr(&self, id: Id) -> RecExpr {
        self.node(id).build_recexpr(|id| self.node(id).clone())
    }

    /// Returns the output types of a plan node.
    fn plan_types(&self, id: Id) -> &[DataType] {
        let ty = self.egraph[id].data.type_.as_ref().unwrap();
        ty.kind.as_struct()
    }

    /// Resolve the column index of `expr` in `plan`.
    fn resolve_column_index(&self, expr: Id, plan: Id) -> RecExpr {
        let schema = &self.egraph[plan].data.schema;
        self.node(expr).build_recexpr(|id| {
            if let Some(idx) = schema.iter().position(|x| *x == id) {
                return Expr::ColumnIndex(ColumnIndex(idx as _));
            }
            match self.node(id) {
                Expr::Column(c) => panic!("column {c} not found from input"),
                e => e.clone(),
            }
        })
    }

    pub fn build(self) -> BoxDiffStream {
        self.build_id(self.root)
    }

    fn build_id(&self, id: Id) -> BoxDiffStream {
        use Expr::*;
        let stream = match self.node(id).clone() {
            Scan([table, list]) => TableScanExecutor {
                stream: self.streaming.get_stream(self.node(table).as_table()),
                columns: (self.node(list).as_list().iter())
                    .map(|id| self.node(*id).as_column())
                    .collect(),
            }
            .execute(),

            Proj([projs, child]) => ProjectionExecutor {
                projs: self.resolve_column_index(projs, child),
            }
            .execute(self.build_id(child)),

            node => panic!("not a plan: {node:?}"),
        };
        spawn_stream(&self.node(id).to_string(), stream)
    }
}
