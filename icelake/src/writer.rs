//! Writer interface to easily append records to an Iceberg table.
//!
//! Inspired by the [delta-rs](https://docs.rs/deltalake/0.12.0/deltalake/) crate.
use std::sync::Arc;
use std::collections::HashMap;

use chrono;
use rand::Rng;
use uuid::Uuid;
use bytes::Bytes;
use arrow_schema::{SchemaRef as ArrowSchemaRef};
use arrow_array::RecordBatch;
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;

use crate::{IcebergResult, IcebergError, IcebergTable, IcebergFile};
use crate::value::Value;
use crate::transaction::AppendFilesOperation;
use crate::partition::{PartitionSpec, PartitionValues};
use crate::manifest::{DataFile, DataFileContent, DataFileFormat};
use crate::arrow::iceberg_to_arrow_schema;

/// Writes Apache Arrow `RecordBatch`es to an Iceberg table in Parquet format.
pub struct RecordBatchWriter {
    arrow_schema: ArrowSchemaRef,
    partition_spec: PartitionSpec,
    // Per-partition parquet writer.
    writers: HashMap<PartitionValues, ArrowWriter<Vec<u8>>>,
    // Common properties for all ArrowWriters
    writer_props: WriterProperties,
    // All files flushed to storage and ready to be commited.
    flushed_files: Vec<(IcebergFile, DataFile)>,
    // Unique operation id to be used in file names.
    // Helps identify files created by the same write operation.
    operation_id: String,
}

impl RecordBatchWriter {
    /// Creates a new `RecordBatchWriter` for the given table, deriving the schema and
    /// partition fields from it.
    pub fn for_table(table: &IcebergTable) -> IcebergResult<Self> {
        Ok(Self {
            arrow_schema: Arc::new(iceberg_to_arrow_schema(table.current_schema()?)?),
            partition_spec: table.current_partition_spec()?,
            writers: HashMap::new(),
            writer_props: WriterProperties::builder()
                .set_compression(parquet::basic::Compression::UNCOMPRESSED)
                .set_dictionary_enabled(false)
                .set_encoding(parquet::basic::Encoding::PLAIN)
                .build(),
            flushed_files: Vec::new(),
            operation_id: Self::new_operation_id()
        })
    }

    fn new_operation_id() -> String {
        rand::thread_rng()
            .sample_iter(&rand::distributions::Alphanumeric)
            .take(8)
            .map(char::from)
            .collect()
    }

    pub fn arrow_schema(&self) -> ArrowSchemaRef {
        self.arrow_schema.clone()
    }

    fn get_or_insert_writer<'a>(
        &mut self,
        source_values: HashMap<i32, Option<Value>>
    ) -> IcebergResult<&mut ArrowWriter<Vec<u8>>> {
        let partition_values = self.partition_spec.partition_values(source_values)?;

        Ok(match self.writers.contains_key(&partition_values) {
            true => self.writers.get_mut(&partition_values).unwrap(),
            false => {
                let writer = ArrowWriter::try_new(
                    Vec::new(),
                    self.arrow_schema.clone(),
                    Some(self.writer_props.clone())
                )?;

                self.writers.entry(partition_values)
                    .or_insert(writer)
            }
        })
    }

    pub fn write_partition(
        &mut self,
        source_values: HashMap<i32, Option<Value>>,
        batch: &RecordBatch
    ) -> IcebergResult<()> {
        if batch.schema() != self.arrow_schema {
            return Err(IcebergError::SchemaError {
                message: "schema mismatch".to_string()
            })
        }

        let writer = self.get_or_insert_writer(source_values)?;
        writer.write(batch)?;

        Ok(())
    }

    async fn flush_writer(
        &mut self,
        table: &mut IcebergTable,
        partition_values: PartitionValues,
        mut writer: ArrowWriter<Vec<u8>>,
        filename: &str
    ) -> IcebergResult<(IcebergFile, DataFile)> {
        writer.flush()?;

        // Find the number of records by summing over all parquet row groups.
        let record_count = writer.flushed_row_groups()
            .iter()
            .map(|row_group| row_group.num_rows())
            .sum();

        // Consume the writer and obtain the written data.
        let data = writer.into_inner()?;

        let file = table.new_data_file(
            &partition_values,
            filename,
            Bytes::from(data)
        )?;

        let data_file = DataFile::builder(
            DataFileContent::Data,
            &file.url(),
            DataFileFormat::Parquet,
            record_count,
            i64::try_from(file.len()).map_err(|_| {
                IcebergError::CustomError {
                    message: "Failed to create data file: too large".to_string()
                }
            })?)
            .with_partition_values(partition_values)
            .build();

        Ok((file, data_file))
    }

    /// Flushes all enqueued `RecordBatch`es to the table as Parquet files, but does
    /// not commit them.
    ///
    /// Call [`RecordBatchWriter::commit()`] to commit all flushed files.
    ///
    /// In case of an error, orphan files may be left in the object store.
    pub async fn flush(&mut self, table: &mut IcebergTable) -> IcebergResult<()> {
        let writers = std::mem::take(&mut self.writers);
        let file_count = writers.len();

        let now = chrono::Utc::now().format("%Y%m%d_%H%M%S");

        for (partition_values, writer) in writers.into_iter() {
            // Unique file UUID.
            let file_uuid = Uuid::new_v4().to_string();
            let filename = format!(
                "{}_{:05x}_{}-{}.parquet",
                now, file_count, self.operation_id, file_uuid,
            );

            let (file, data_file) = self.flush_writer(
                table,
                partition_values,
                writer,
                &filename
            ).await?;

            file.save().await?;

            self.flushed_files.push((file, data_file));
        }

        Ok(())
    }

    /// Flushes and commits all enqueued `RecordBatch`es to the table.
    ///
    /// In case of an error, orphan files may be left in the object store.
    pub async fn commit(&mut self, table: &mut IcebergTable) -> IcebergResult<()> {
        self.flush(table).await?;

        self.operation_id = Self::new_operation_id();

        let data_files = std::mem::take(&mut self.flushed_files)
            .into_iter().map(|tup| tup.1);

        let mut transaction = table.new_transaction();
        let mut operation = AppendFilesOperation::new();
        operation.append_files(data_files);
        transaction.set_operation(Box::new(operation));

        transaction.commit().await
    }
}
