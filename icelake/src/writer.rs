//! Writer interface to easily append records to an Iceberg table.
//!
//! Inspired by the [delta-rs](https://docs.rs/deltalake/0.12.0/deltalake/) crate.
use std::sync::Arc;
use std::collections::HashMap;

use chrono;
use rand::Rng;
use uuid::Uuid;
use bytes::Bytes;
use arrow_schema::{
    Schema as ArrowSchema,
    SchemaRef as ArrowSchemaRef,
    Field as ArrowField,
    FieldRef as ArrowFieldRef,
    Fields as ArrowFields,
    DataType as ArrowDataType,
};
use arrow_array::RecordBatch;
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;

use crate::{IcebergResult, IcebergError, IcebergTable, IcebergFile};
use crate::value::Value;
use crate::transaction::AppendFilesOperation;
use crate::partition::{PartitionSpec, PartitionValues};
use crate::manifest::{DataFile, DataFileContent, DataFileFormat};
use crate::schema::arrow::iceberg_to_arrow_schema;

fn arrow_field_add_parquet_id(arrow_field: &ArrowFieldRef) -> ArrowFieldRef {
    let mut metadata = arrow_field.metadata().clone();
    if let Some(field_id) = metadata.get("ICEBERG:field_id") {
        metadata.insert("PARQUET:field_id".to_string(), field_id.clone());
    }

    Arc::new(
        ArrowField::new(
            arrow_field.name(),
            match arrow_field.data_type() {
                ArrowDataType::List(nested_field) => {
                    ArrowDataType::List(
                        arrow_field_add_parquet_id(nested_field)
                    )
                },
                ArrowDataType::FixedSizeList(nested_field, s) => {
                    ArrowDataType::FixedSizeList(
                        arrow_field_add_parquet_id(nested_field),
                        *s
                    )
                },
                ArrowDataType::LargeList(nested_field) => {
                    ArrowDataType::LargeList(
                        arrow_field_add_parquet_id(nested_field),
                    )
                },
                ArrowDataType::Struct(nested_fields) => {
                    ArrowDataType::Struct(
                        arrow_fields_add_parquet_id(nested_fields),
                    )
                },
                ArrowDataType::Union(..) => { todo!() },
                ArrowDataType::Dictionary(..) => { todo!() },
                ArrowDataType::Map(nested_field, s) => {
                    ArrowDataType::Map(
                        arrow_field_add_parquet_id(nested_field),
                        *s
                    )
                },
                ArrowDataType::RunEndEncoded(..) => { todo!() },
                data_type => data_type.clone(),
            },
            arrow_field.is_nullable()
        ).with_metadata(metadata)
    )
}

fn arrow_fields_add_parquet_id(arrow_fields: &ArrowFields) -> ArrowFields {
    arrow_fields.into_iter().map(arrow_field_add_parquet_id).collect()
}

/// Adds parquet field ids to arrow field metadata, based on the iceberg field id if
/// present.
fn arrow_schema_add_parquet_ids(arrow_schema: ArrowSchema) -> ArrowSchema {
    ArrowSchema::new_with_metadata(
        arrow_fields_add_parquet_id(arrow_schema.fields()),
        arrow_schema.metadata().clone()
    )
}

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
        let arrow_schema = iceberg_to_arrow_schema(table.current_schema()?)?;
        let arrow_schema = arrow_schema_add_parquet_ids(arrow_schema);

        Ok(Self {
            arrow_schema: Arc::new(arrow_schema),
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
        partition_values: PartitionValues
    ) -> IcebergResult<&mut ArrowWriter<Vec<u8>>> {
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

        let partition_values = self.partition_spec.partition_values(source_values)?;
        let writer = self.get_or_insert_writer(partition_values)?;
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
        transaction.add_operation(Box::new(operation));

        transaction.commit().await
    }
}
