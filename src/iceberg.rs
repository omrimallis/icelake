use std::fmt;
use std::collections::HashMap;

use uuid::Uuid;
use serde::{Serialize, Deserialize};
use serde_repr::{Serialize_repr, Deserialize_repr};
use serde_json;

use crate::{IcebergError, IcebergResult};
use crate::utils;
use crate::schema::Schema;
use crate::partition::PartitionSpec;
use crate::sort::SortOrder;
use crate::transaction::Transaction;
use crate::storage::{IcebergObjectStore, IcebergPath};
use crate::snapshot::{Snapshot, SnapshotLog, SnapshotReference};
use crate::manifest::{ManifestList};

#[derive(Debug, Clone, Serialize_repr, Deserialize_repr)]
#[repr(i32)]
pub enum IcebergTableVersion {
    V1 = 1,
    V2 = 2,
}

impl fmt::Display for IcebergTableVersion {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", match *self {
            IcebergTableVersion::V1 => 1,
            IcebergTableVersion::V2 => 2
        })
    }
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct IcebergTableMetadata {
    /// An integer version number for the format.
    /// Should always be set to 2.
    pub format_version: IcebergTableVersion,
    /// A UUID that identifies the table
    pub table_uuid: String,
    /// Location tables base location
    pub location: String,
    /// The table’s highest assigned sequence number, a monotonically increasing long
    /// that tracks the order of snapshots in a table.
    pub last_sequence_number: i64,
    /// Timestamp in milliseconds from the unix epoch when the table was last updated.
    pub last_updated_ms: i64,
    /// An integer; the highest assigned column ID for the table. This is used to ensure
    /// columns are always assigned an unused ID when evolving schemas.
    pub last_column_id: i32,
    /// A list of schemas, stored as objects with schema-id.
    pub schemas: Vec<Schema>,
    /// ID of the table’s current schema.
    pub current_schema_id: i32,
    /// A list of partition specs, stored as full partition spec objects.
    pub partition_specs: Vec<PartitionSpec>,
    /// ID of the “current” spec that writers should use by default.
    pub default_spec_id: i32,
    /// An integer; the highest assigned partition field ID across all partition specs
    /// for the table. This is used to ensure partition fields are always assigned an
    /// unused ID when evolving specs.
    pub last_partition_id: i32,
    /// A string to string map of table properties. This is used to control settings
    /// that affect reading and writing and is not intended to be used for arbitrary
    /// metadata. For example, commit.retry.num-retries is used to control the number
    /// of commit retries.
    pub properties: Option<HashMap<String, String>>,
    /// long ID of the current table snapshot; must be the same as the current ID of
    /// the main branch in refs.
    pub current_snapshot_id: Option<i64>,
    /// A list of valid snapshots. Valid snapshots are snapshots for which all data
    /// files exist in the file system. A data file must not be deleted from the file
    /// system until the last snapshot in which it was listed is garbage collected.
    pub snapshots: Option<Vec<Snapshot>>,
    /// A list (optional) of timestamp and snapshot ID pairs that encodes changes to the
    /// current snapshot for the table. Each time the current-snapshot-id is changed, a
    /// new entry should be added with the last-updated-ms and the new
    /// current-snapshot-id. When snapshots are expired from the list of valid
    /// snapshots, all entries before a snapshot that has expired should be removed.
    pub snapshot_log: Option<Vec<SnapshotLog>>,
    /// A list (optional) of timestamp and metadata file location pairs that encodes
    /// changes to the previous metadata files for the table. Each time a new metadata
    /// file is created, a new entry of the previous metadata file location should be
    /// added to the list. Tables can be configured to remove oldest metadata log
    /// entries and keep a fixed-size log of the most recent entries after a commit.
    pub metadata_log: Option<Vec<MetadataLog>>,
    /// A list of sort orders, stored as full sort order objects.
    pub sort_orders: Vec<SortOrder>,
    /// Default sort order id of the table. Note that this could be used by writers,
    /// but is not used when reading because reads use the specs stored in manifest
    /// files.
    pub default_sort_order_id: i32,
    /// A map of snapshot references. The map keys are the unique snapshot reference
    /// names in the table, and the map values are snapshot reference objects. There is
    /// always a main branch reference pointing to the current-snapshot-id even if the
    /// refs map is null.
    pub refs: Option<HashMap<String, SnapshotReference>>,
    // TODO: Table statistics
}

impl IcebergTableMetadata {
    pub fn try_new(
        location: String,
        schemas: Vec<Schema>,
        current_schema_id: i32
    ) -> IcebergResult<Self> {
        let partition_spec = PartitionSpec::new();
        let partition_spec_id = partition_spec.spec_id;
        let sort_order = SortOrder::new();
        let sort_order_id = sort_order.order_id;

        // Ensure current_schema_id is present in the list of schemas
        let current_schema = 
            schemas.iter().find(|&schema| schema.id() == current_schema_id)
            .ok_or(IcebergError::SchemaNotFound { schema_id: current_schema_id })?;

        // Infer the maximum field id of the current schema.
        let last_column_id = current_schema.max_field_id() + 1;

        Ok(Self {
            format_version: IcebergTableVersion::V2,
            table_uuid: Uuid::new_v4().to_string(),
            location: location,
            last_sequence_number: 0,
            last_updated_ms: utils::current_time_ms()?, 
            last_column_id: last_column_id,
            schemas: schemas,
            current_schema_id: current_schema_id,
            partition_specs: vec![partition_spec],
            default_spec_id: partition_spec_id,
            last_partition_id: 1000,
            properties: Some(HashMap::new()),
            current_snapshot_id: Some(-1),
            snapshots: Some(Vec::new()),
            snapshot_log: Some(Vec::new()),
            metadata_log: Some(Vec::new()),
            sort_orders: vec![sort_order],
            default_sort_order_id: sort_order_id,
            refs: Some(HashMap::new()),
        })
    }

    pub fn current_schema(&self) -> &Schema {
        // Panic if not found, as we are validating this in the constructor.
        self.schemas.iter().find(|&schema| schema.id() == self.current_schema_id)
            .expect("current_schema_id does not match any schema")
    }

    /// Return the latest snapshot of the table. May return None if the table has no
    /// snapshots, or if its current_snapshot_id is invalid.
    pub fn current_snapshot(&self) -> Option<&Snapshot> {
        self.snapshots.as_ref().and_then(|snapshots| {
            self.current_snapshot_id.and_then(|current_snapshot_id| {
                snapshots.iter().find(|&snapshot| {
                    snapshot.snapshot_id == current_snapshot_id
                })
            })
        })
    }
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone)]
#[serde(rename_all = "kebab-case")]
/// Encodes changes to the previous metadata files for the table
pub struct MetadataLog {
    /// The file for the log.
    pub metadata_file: String,
    /// Time new metadata was created
    pub timestamp_ms: i64,
}

impl MetadataLog {
    pub fn new(metadata_file: &str, timestamp_ms: i64) -> Self {
        Self {
            metadata_file: metadata_file.to_string(),
            timestamp_ms: timestamp_ms
        }
    }
}

pub struct IcebergTableState {
    // UUID identifying the latest snapshot of the table.
    pub version_uuid: String,
    // Path to the current metadata file on the object store.
    pub metadata_path: IcebergPath,
}

pub struct IcebergTable {
    /// Latest state of the table, changes after each commit.
    /// Maybe be None for tables that were not initialized.
    pub state: Option<IcebergTableState>,
    /// Table metadata that includes schemas & snapshots
    pub metadata: Option<IcebergTableMetadata>,
    /// Used to access data and metadata files
    pub storage: IcebergObjectStore,
}

impl IcebergTable {
    pub fn new(storage: IcebergObjectStore) -> Self {
        Self {
            state: None,
            metadata: None,
            storage: storage,
        }
    }

    pub fn location(&self) -> &str {
        self.storage.location()
    }

    /// Returns the currently set schema for the table.
    ///
    /// # Errors
    ///
    /// This function will return `TableNotInitialized` if the table has not been
    /// initialized through either `IcebergTable::create()` or `IcebergTable::load()`.
    pub fn current_schema(&self) -> IcebergResult<&Schema> {
        self.metadata.as_ref()
            .map(|metadata| metadata.current_schema())
            .ok_or(IcebergError::TableNotInitialized)
    }

    /// Returns the latest snapshot of the table, or `None` if the table has no
    /// snapshots.
    ///
    /// # Errors
    ///
    /// This function will return `TableNotInitialized` if the table has not been
    /// initialized through either `IcebergTable::create()` or `IcebergTable::load()`.
    pub fn current_snapshot(&self) -> IcebergResult<Option<&Snapshot>> {
        self.metadata.as_ref()
            .map(|metadata| metadata.current_snapshot())
            .ok_or(IcebergError::TableNotInitialized)
    }

    /// Returns a reference to the current table's metadata.
    ///
    /// # Errors
    ///
    /// This function will return `TableNotInitialized` if the table has not been
    /// initialized through either `IcebergTable::create()` or `IcebergTable::load()`.
    pub fn current_metadata(&self) -> IcebergResult<&IcebergTableMetadata> {
        self.metadata.as_ref().ok_or(IcebergError::TableNotInitialized)
    }

    /// Returns the full URI of the current table's metadata file.
    ///
    /// # Errors
    ///
    /// This function will return `TableNotInitialized` if the table has not been
    /// initialized through either `IcebergTable::create()` or `IcebergTable::load()`.
    pub fn current_metadata_uri(&self) -> IcebergResult<String> {
        self.state.as_ref().map(|state| self.storage.to_uri(&state.metadata_path))
            .ok_or(IcebergError::TableNotInitialized)
    }

    /// Reads the manifest list for the given snapshot.
    pub async fn read_manifest_list(
        &self,
        snapshot: &Snapshot
    ) -> IcebergResult<ManifestList> {
        let path = IcebergPath::from_url(&snapshot.manifest_list)?;
        let bytes = self.storage.get(&path).await?;

        ManifestList::decode(bytes.as_ref())
    }

    /// Commits the given metadata to the table, replacing the existing metadata.
    pub async fn commit(
        &mut self,
        mut metadata: IcebergTableMetadata,
    ) -> IcebergResult<()> {
        // Add a log entry to the new metadata about the previous metadata.
        if let Some(current_metadata) = &self.metadata {
           if let Some(state) = &self.state {
               metadata.metadata_log
                   .get_or_insert(Vec::new())
                   .push(MetadataLog::new(
                       &self.storage.to_uri(&state.metadata_path),
                       current_metadata.last_updated_ms
               ));
           }
       }

        // Generate a new UUID for this version, and set it only after the new metadata
        // file has been committed.
        let new_version_uuid = Uuid::new_v4().to_string();
        let metadata_path = self.storage.create_metadata_path(
            &format!(
                "{:05}-{}.metadata.json",
                metadata.last_sequence_number,
                new_version_uuid
            )
        );

        let json = serde_json::to_string_pretty(&metadata)
            .map_err(|e| IcebergError::SerializeMetadataJson { source: e })?;

        // TODO: This should be an atomic operation.
        // TODO: Testing for failures is needed.
        self.storage.put(&metadata_path, bytes::Bytes::from(json)).await?;

        self.metadata = Some(metadata);
        self.state = Some(IcebergTableState {
            version_uuid: new_version_uuid,
            metadata_path: metadata_path,
        });

        Ok(())
    }

    /// Initializes a new Iceberg table with the given schema.
    pub async fn create(&mut self, schema: Schema) -> IcebergResult<()> {
        if schema.fields().is_empty() {
            Err(IcebergError::EmptySchema)
        } else {
            let current_schema_id = schema.id();
            let metadata = IcebergTableMetadata::try_new(
                self.storage.location().to_string(),
                vec![schema],
                current_schema_id
            )?;

            self.commit(metadata).await
        }
    }
    
    pub fn new_transaction(&mut self) -> Transaction {
        Transaction::new(self)
    }

    // Load an existing Iceberg table state from storage.
    pub fn load(&mut self) {
        // TODO: Implement
    }
}
