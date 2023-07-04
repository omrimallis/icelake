//! Interface to Iceberg table transactions.
use rand::Rng;
use uuid::Uuid;
use bytes::Bytes;
use async_trait;

use crate::{IcebergTable, IcebergTableMetadata, IcebergFile, IcebergResult};
use crate::schema::Schema;
use crate::schema::update::SchemaUpdate;
use crate::utils;
use crate::manifest::{
    ManifestList, Manifest, ManifestContentType,
    ManifestEntry, ManifestEntryStatus,
    ManifestWriter,
    DataFile
};
use crate::snapshot::{
    Snapshot, SnapshotSummary,
    SnapshotOperation, SnapshotLog
};

/// An operation that can be applied to an iceberg table as part of a transaction.
#[async_trait::async_trait]
pub trait TableOperation {
    async fn apply(
        &self,
        table: &IcebergTable,
        metadata: &IcebergTableMetadata
    ) -> IcebergResult<TransactionState>;
}

/// An empty operation that does not affect tht table.
///
/// Applying this operation as part of a transaction will result in the table's
/// sequence number increasing, but no new snapshot will be generated.
pub struct DoNothingOperation;

impl DoNothingOperation {
    pub fn new() -> Self { Self {} }
}

#[async_trait::async_trait]
impl TableOperation for DoNothingOperation {
    async fn apply(
        &self,
        _table: &IcebergTable,
        _metadata: &IcebergTableMetadata
    ) -> IcebergResult<TransactionState> {
        Ok(TransactionState {
            snapshot: None,
            schema: None,
            files: Vec::new()
        })
    }
}

/// An operation for updating the schema of the table.
pub struct UpdateSchemaOperation {
    schema: Option<Schema>
}

impl UpdateSchemaOperation {
    pub fn new() -> Self {
        Self {
            schema: None
        }
    }

    /// Sets the schema to be set as the current schema.
    ///
    /// Note: The schema id associated with the input schema is ignored. A new id
    /// will be assigned automatically based on the next available schema id for the
    /// table.
    pub fn set_schema(&mut self, schema: Schema) {
        self.schema = Some(schema);
    }
}

#[async_trait::async_trait]
impl TableOperation for UpdateSchemaOperation {
    async fn apply(
        &self,
        _table: &IcebergTable,
        metadata: &IcebergTableMetadata
    ) -> IcebergResult<TransactionState> {
        let new_schema = match &self.schema {
            Some(schema) => {
                // We use this only to check that the schema update can be applied.
                let _schema_update = SchemaUpdate::between(
                    metadata.current_schema(),
                    schema
                )?;

                let mut new_schema = schema.clone();
                // Set a new schema id.
                new_schema.set_id(
                    metadata.schemas
                        .iter()
                        .map(|schema| schema.id())
                        .max()
                        .unwrap() + 1
                );

                Some(new_schema)
            },
            None => None
        };

        Ok(TransactionState {
            snapshot: None,
            schema: new_schema,
            files: Vec::new()
        })
    }
}

/// An operation to append data files to the table.
pub struct AppendFilesOperation {
    data_files: Vec<DataFile>,
}

impl AppendFilesOperation {
    pub fn new() -> Self { Self { data_files: Vec::new() } }

    /// Adds a file to the list of data files to be appended.
    pub fn append_file(&mut self, file: DataFile) {
        self.data_files.push(file);
    }

    /// Adds all given files to the list of data files to be appended.
    pub fn append_files(&mut self, files: impl IntoIterator<Item=DataFile>) {
        self.data_files.extend(files);
    }

    fn create_manifest(
        &self,
        metadata: &IcebergTableMetadata,
        snapshot_id: i64,
    ) -> IcebergResult<Manifest> {
        // Create a new manifest with the list of new files.
        let mut manifest = Manifest::new(
            metadata.current_schema().clone(),
            metadata.current_partition_spec().clone(),
            ManifestContentType::Data
        );
        for data_file in &self.data_files {
            manifest.add_manifest_entry(ManifestEntry::new(
                ManifestEntryStatus::Added,
                snapshot_id,
                data_file.clone()
            ));
        }
        Ok(manifest)
    }
}

#[async_trait::async_trait]
impl TableOperation for AppendFilesOperation {
    async fn apply(
        &self,
        table: &IcebergTable,
        metadata: &IcebergTableMetadata
    ) -> IcebergResult<TransactionState> {
        let current_snapshot = metadata.current_snapshot();
        let mut current_manifest_list = match current_snapshot {
            Some(current_snapshot) => {
                table.read_manifest_list(current_snapshot).await?
            },
            None => ManifestList::new()
        };

        let new_snapshot_id = rand::thread_rng().gen_range(0..i64::MAX);

        let manifest = self.create_manifest(metadata, new_snapshot_id)?;

        let mut manifest_file = table.new_metadata_file(
            &format!("{}-m0.avro", Uuid::new_v4().to_string()),
            Bytes::new()
        )?;

        // Encode the on-disk manifest file.
        let writer = ManifestWriter::new(
            metadata.last_sequence_number,
            new_snapshot_id
        );
        let (manifest_content, manifest_file_entry) = writer.write(
            &manifest_file.url(), &manifest
        )?;

        manifest_file.set_bytes(manifest_content);

        // Update the manifest list with a ManifestFile pointing to the new
        // manifest file.
        current_manifest_list.push(manifest_file_entry);

        // Write a new manifest list to storage
        let manifest_list_file = table.new_metadata_file(
            &format!(
                "snap-{}-1-{}.avro",
                new_snapshot_id,
                Uuid::new_v4().to_string()
            ),
            Bytes::from(current_manifest_list.encode()?)
        )?;

        // Build a snapshot summary
        let mut summary_builder = SnapshotSummary::builder();
        if let Some(snapshot) = current_snapshot {
            summary_builder.copy_totals(&snapshot.summary);
        }
        summary_builder
            .operation(SnapshotOperation::Append)
            .add_data_files(manifest.entries().len().try_into().unwrap());

        Ok(TransactionState {
            snapshot: Some(Snapshot {
                snapshot_id: new_snapshot_id,
                // Use current snapshot as parent, or None for first snapshot.
                parent_snapshot_id: current_snapshot.map(|s| s.snapshot_id),
                // Sequence number increased by 1 for every new snapshot.
                sequence_number: metadata.last_sequence_number,
                timestamp_ms: metadata.last_updated_ms,
                // Path to the manifest list file of this snapshot.
                manifest_list: manifest_list_file.url(),
                // Snapshot statistics summary
                summary: summary_builder.build(),
                schema_id: Some(metadata.current_schema_id),
            }),
            schema: None,
            files: vec![manifest_list_file, manifest_file]
        })
    }
}

/// Stores results of operations to be performed as part of this transaction.
/// Each operation returns a state to reflect the changes it applies.
pub struct TransactionState {
    /// New snapshot created by an operation.
    snapshot: Option<Snapshot>,
    /// Updated schema
    schema: Option<Schema>,
    /// List of files pending to be written to the table's storage.
    files: Vec<IcebergFile>
}

/// A transaction for performing multiple operations on a table.
pub struct Transaction<'a> {
    table: &'a mut IcebergTable,
    operations: Vec<Box<dyn TableOperation>>,
}

impl<'a> Transaction<'a> {
    pub fn new(table: &'a mut IcebergTable) -> Self {
        Self {
            table: table,
            operations: Vec::new(),
        }
    }

    /// Adds an operation to be performed as part of this transaction.
    pub fn add_operation(&mut self, operation: Box<dyn TableOperation>) {
        self.operations.push(operation);
    }

    /// Attempts to commit this transaction to the table, applying all operations
    /// one after the other and generating new table metadata.
    pub async fn commit(&mut self) -> IcebergResult<()> {
        for operation in &self.operations {
            let current_metadata = self.table.current_metadata()?;
            let mut new_metadata = (*current_metadata).clone();
            new_metadata.last_updated_ms = utils::current_time_ms()?;
            new_metadata.last_sequence_number += 1;

            // Apply the operation, potentially producing a new snapshot and a new
            // schema.
            let state = operation.apply(self.table, &new_metadata).await?;

            if let Some(snapshot) = state.snapshot {
                // Set the new snapshot set as current.
                new_metadata.current_snapshot_id = Some(snapshot.snapshot_id);
                new_metadata.snapshot_log
                    .get_or_insert_with(Vec::new)
                    .push(SnapshotLog::new(snapshot.snapshot_id, snapshot.timestamp_ms));
                new_metadata.snapshots
                    .get_or_insert_with(Vec::new)
                    .push(snapshot);
            }

            if let Some(schema) = state.schema {
                new_metadata.current_schema_id = schema.id();
                new_metadata.schemas.push(schema);
            }

            // Write all files created by the operation to storage.
            for file in state.files {
                // TODO: Remove previously written files on failure.
                file.save().await?;
            }

            // TODO: If a commit fails we need to revert changes.
            self.table.commit(new_metadata).await?
        }

        Ok(())
    }
}
