use chrono;
use rand::Rng;
use uuid::Uuid;
use bytes::Bytes;
use arrow::datatypes::{SchemaRef as ArrowSchemaRef};
use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;

use crate::{IcebergResult, IcebergError, IcebergTable, IcebergFile};
use crate::transaction::AppendFilesOperation;
use crate::manifest::{DataFile, DataFileContent, DataFileFormat};

/// Writes Apache Arrow `RecordBatch`es to an Iceberg table in Parquet format.
pub struct RecordBatchWriter {
    arrow_schema: ArrowSchemaRef,
    writer: Option<ArrowWriter<Vec<u8>>>,
    records: usize
}

impl RecordBatchWriter {
    pub fn new(arrow_schema: ArrowSchemaRef) -> Self {
        RecordBatchWriter {
            arrow_schema: arrow_schema.clone(),
            writer: None,
            records: 0
        }
    }

    /// Enqueues the provided `RecordBatch` to be written to the table.
    pub fn write(&mut self, batch: &RecordBatch) -> IcebergResult<()> {
        if self.writer.is_none() {
            self.writer = Some(ArrowWriter::try_new(
                Vec::new(),
                self.arrow_schema.clone(),
                Some(parquet::file::properties::WriterProperties::builder()
                    .set_compression(parquet::basic::Compression::UNCOMPRESSED)
                    .set_dictionary_enabled(false)
                    .set_encoding(parquet::basic::Encoding::PLAIN)
                    .build())
            )?);
            self.records = 0;
        }

        self.writer.as_mut().unwrap().write(batch)?;
        self.records += batch.num_rows();

        Ok(())
    }

    async fn commit_data_file(
        &mut self,
        table: &mut IcebergTable,
        file: &IcebergFile,
        records: usize
    ) -> IcebergResult<()> {
        let mut transaction = table.new_transaction();
        let mut operation = AppendFilesOperation::new();
        operation.append_file(DataFile::builder(
            DataFileContent::Data,
            &file.url(),
            DataFileFormat::Parquet,
            i64::try_from(records).map_err(|_| {
                IcebergError::CustomError {
                    message: "Failed to create data file: too many records".to_string()
                }
            })?,
            i64::try_from(file.len()).map_err(|_| {
                IcebergError::CustomError {
                    message: "Failed to create data file: too large".to_string()
                }
            })?,
        ).build());
        transaction.set_operation(Box::new(operation));
        transaction.commit().await?;

        Ok(())
    }

    /// Commits all enqueued `RecordBatch`es to the provided [`IcebergTable`]
    pub async fn commit(&mut self, table: &mut IcebergTable) -> IcebergResult<()> {
        if let Some(writer) = self.writer.take() {
            // Consume the writer and obtain the written data.
            let data = writer.into_inner()?;
            let records = std::mem::replace(&mut self.records, 0);

            // Unique operation id to be used in file names.
            // Helps identify files created by the same write operation.
            let operation_id: String = rand::thread_rng()
                .sample_iter(&rand::distributions::Alphanumeric)
                .take(8)
                .map(char::from)
                .collect();

            // Currently the file count is always 1.
            let file_count = 1;
            let now = chrono::Utc::now().format("%Y%m%d_%H%M%S");
            let file_uuid = Uuid::new_v4().to_string();
            // Use a file format similar to the one used by AWS Athena.
            let file = table.new_data_file(
                &format!(
                    "{}_{:05}_{}-{}.parquet",
                    now, file_count, operation_id, file_uuid,
                ),
                Bytes::from(data)
            )?;

            file.save().await?;
            
            let result = self.commit_data_file(table, &file, records).await;
            if result.is_err() {
                _ = file.delete().await;
            }

            result
        } else {
            Ok(())
        }
    }
}
