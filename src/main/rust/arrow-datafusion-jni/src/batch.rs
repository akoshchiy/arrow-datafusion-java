use std::{collections::VecDeque, sync::Arc};

use arrow::{
    datatypes::{Schema, SchemaRef},
    error::ArrowError,
    record_batch::{RecordBatch, RecordBatchReader},
};

pub struct RecordBatchVecReader {
    schema: SchemaRef,
    batches: VecDeque<RecordBatch>,
}

impl RecordBatchVecReader {
    pub fn new(schema: SchemaRef, batches: Vec<RecordBatch>) -> Self {
        Self {
            schema,
            batches: VecDeque::from(batches),
        }
    }
}

impl Iterator for RecordBatchVecReader {
    type Item = Result<RecordBatch, ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        let item = self.batches.pop_front();
        item.map(Ok)
    }
}

impl RecordBatchReader for RecordBatchVecReader {
    fn schema(&self) -> arrow::datatypes::SchemaRef {
        self.schema.clone()
    }
}

pub fn concat(
    df_schema: Arc<Schema>,
    batches: Vec<RecordBatch>,
) -> Result<RecordBatch, ArrowError> {
    if batches.is_empty() {
        return Ok(RecordBatch::new_empty(df_schema));
    }
    let schema = batches[0].schema();
    arrow::compute::concat_batches(&schema, &batches)
}
