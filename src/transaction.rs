//! Interface to Iceberg table transactions.
use rand::Rng;
use uuid::Uuid;
use bytes::Bytes;

use crate::{IcebergTable, IcebergTableMetadata, IcebergResult};
use crate::utils;
use crate::manifest::{
    ManifestList, Manifest, ManifestContentType,
    ManifestEntry, ManifestEntryStatus, 
    ManifestWriter,
    DataFile
};
use crate::snapshot::{
    Snapshot, SnapshotSummary, SnapshotSummaryBuilder,
    SnapshotOperation, SnapshotLog
};
use crate::partition::PartitionSpec;
use crate::storage::IcebergPath;

pub struct TableFile {
    pub path: IcebergPath,
    pub content: Bytes,
}

pub trait TableOperation {
    fn apply(
        &self,
        table: &IcebergTable,
        state: &mut TransactionState,
    ) -> IcebergResult<()>;
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

    fn create_manifest(
        &self,
        table: &IcebergTable,
        state: &mut TransactionState,
    ) -> IcebergResult<Manifest> {
        // Create a new manifest with the list of new files.
        let mut manifest = Manifest::try_new(
            table.current_schema()?,
            PartitionSpec::new(),
            ManifestContentType::Data
        )?;
        for data_file in &self.data_files {
            manifest.add_manifest_entry(ManifestEntry::new(
                ManifestEntryStatus::Added,
                state.snapshot_id,
                data_file.clone()
            ));
        }
        Ok(manifest)
    }
}

impl TableOperation for AppendFilesOperation {
    fn apply(
        &self,
        table: &IcebergTable,
        state: &mut TransactionState,
    ) -> IcebergResult<()> {
        let manifest = self.create_manifest(table, state)?;

        // Update the snapshot summary
        state.summary_builder
            .operation(SnapshotOperation::Append) 
            .add_data_files(manifest.entries().len().try_into().unwrap());

        // Encode the on-disk manifest file.
        let manifest_path = table.storage().create_metadata_path(
            &format!("{}-m0.avro", Uuid::new_v4().to_string())
        );

        let writer = ManifestWriter::new(state.sequence_number, state.snapshot_id);
        let (manifest_content, manifest_file) = writer.write(
            &table.storage().to_uri(&manifest_path),
            manifest
        )?;

        // Update the manifest list with a ManifestFile pointing to the new
        // manifest file.
        state.manifest_list.push(manifest_file);

        // Add the manifest to the list of files to be written to storage.
        state.files.push((manifest_path, manifest_content));

        Ok(())
    }
}

/// Stores states of operations to be performed as part of this transaction.
/// Each operation updates the state to reflect the changes it applies.
pub struct TransactionState {
    /// Sequence number of the commit.
    sequence_number: i64,
    /// Id of the new snapshot to be committed by this transaction.
    snapshot_id: i64,
    /// Timestamp of the commit in Iceberg convention (millis since epoch).
    timestamp: i64,
    /// Builds the summary of the new snapshot.
    summary_builder: SnapshotSummaryBuilder,
    /// Contains the full new list of manifests.
    manifest_list: ManifestList,
    /// Contains list of files to be written to the table's storage. Each entry stores
    /// the path of the file and its content.
    files: Vec<(IcebergPath, Bytes)>
}

pub struct Transaction<'a> {
    table: &'a mut IcebergTable,
    operation: Option<Box<dyn TableOperation>>,
}

impl<'a> Transaction<'a> {
    pub fn new(table: &'a mut IcebergTable) -> Self {
        Self {
            table: table,
            operation: None,
        }
    }

    pub fn set_operation(&mut self, operation: Box<dyn TableOperation>) {
        self.operation = Some(operation);
    }

    /// Prepares a new table metadata based on the transactions's state created by the
    /// inner operations. Writes all manifest lists and manifest files to storage.
    async fn prepare_metadata(
        &mut self, state: TransactionState
    ) -> IcebergResult<IcebergTableMetadata> {
        let current_schema = self.table.current_schema()?;
        let current_snapshot = self.table.current_snapshot()?;
        let current_metadata = self.table.current_metadata()?;
        
        let mut new_metadata = (*current_metadata).clone();
        new_metadata.last_updated_ms = state.timestamp;
        new_metadata.last_sequence_number = state.sequence_number;

        // Write all files created by the operation to storage.
        for (path, content) in state.files {
            // TODO: Remove previously written files on failure.
            self.table.storage().put(&path, content).await?
        }

        if !state.manifest_list.is_empty() {
            // Write a new manifest list to storage
            let uuid = Uuid::new_v4().to_string();
            let manifest_list_path = self.table.storage().create_metadata_path(
                &format!("snap-{}-1-{}.avro", state.snapshot_id, uuid)
            );
            self.table.storage().put(
                &manifest_list_path,
                bytes::Bytes::from(state.manifest_list.encode()?)
            ).await?;

            // Create a new snapshot pointing to the newly created manifest list.
            let snapshot = Snapshot {
                snapshot_id: state.snapshot_id,
                // Use current snapshot as parent, or None for first snapshot.
                parent_snapshot_id: current_snapshot.map(|s| s.snapshot_id),
                // Sequence number increased by 1 for every new snapshot.
                sequence_number: state.sequence_number,
                timestamp_ms: state.timestamp,
                // Path to the manifest list file of this snapshot.
                manifest_list: self.table.storage().to_uri(&manifest_list_path),
                // Snapshot statistics summary
                summary: state.summary_builder.build(),
                schema_id: Some(current_schema.id()),
            };

            // Set the new snapshot set as current.
            new_metadata.current_snapshot_id = Some(snapshot.snapshot_id);
            new_metadata.snapshot_log
                .get_or_insert(Vec::new())
                .push(SnapshotLog::new(snapshot.snapshot_id, snapshot.timestamp_ms));
            new_metadata.snapshots
                .get_or_insert(Vec::new())
                .push(snapshot);
        }

        Ok(new_metadata)
    }

    pub async fn commit(&mut self) -> IcebergResult<()> {
        let current_snapshot = self.table.current_snapshot()?;
        let current_metadata = self.table.current_metadata()?;

        let manifest_list = match current_snapshot {
            Some(current_snapshot) => {
                self.table.read_manifest_list(current_snapshot).await?
            },
            None => ManifestList::new()
        };

        let mut summary_builder = SnapshotSummary::builder();
        if let Some(current_snapshot) = current_snapshot {
            summary_builder.copy_totals(&current_snapshot.summary);
        }

        let mut state = TransactionState {
            sequence_number: current_metadata.last_sequence_number + 1,
            snapshot_id: rand::thread_rng().gen_range(0..i64::MAX),
            timestamp: utils::current_time_ms()?,
            summary_builder: summary_builder,
            manifest_list: manifest_list,
            files: Vec::new(),
        };

        // If an operation was provided, apply it. Otherwise do nothing, a new snapshot
        // and metadata will be generated with updated sequence numbers and timestamps.
        if let Some(operation) = &self.operation {
            operation.apply(self.table, &mut state)?;
        }

        let new_metadata = self.prepare_metadata(state).await?;
        // TODO: Remove written files.
        self.table.commit(new_metadata).await
    }
}
