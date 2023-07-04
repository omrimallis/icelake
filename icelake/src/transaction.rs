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

#[async_trait::async_trait]
pub trait TableOperation {
    async fn apply(
        &self,
        table: &IcebergTable,
        metadata: &IcebergTableMetadata
    ) -> IcebergResult<TransactionState>;
}

/// Used to append files to the table.
pub struct AppendFilesOperation {
    data_files: Vec<DataFile>,
}

impl AppendFilesOperation {
    pub fn new() -> Self { Self { data_files: Vec::new() } }

    pub fn append_file(&mut self, file: DataFile) {
        self.data_files.push(file);
    }

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

    /// Sets the operation to be commited by this transaction.
    pub fn add_operation(&mut self, operation: Box<dyn TableOperation>) {
        self.operations.push(operation);
    }

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
