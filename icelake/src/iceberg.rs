//! Iceberg table implementation.
use std::fmt;
use std::sync::Arc;
use std::collections::HashMap;

use uuid::Uuid;
use bytes::Bytes;
use regex::Regex;
use serde::{Serialize, Deserialize};
use serde_repr::{Serialize_repr, Deserialize_repr};
use serde_json;
use lazy_static::lazy_static;
use murmur3::murmur3_32;

use crate::{IcebergError, IcebergResult};
use crate::utils;
use crate::schema::Schema;
use crate::partition::{PartitionSpecModel, PartitionSpec, PartitionField};
use crate::sort::SortOrder;
use crate::transaction::Transaction;
use crate::storage::{IcebergStorage, IcebergPath};
use crate::snapshot::{Snapshot, SnapshotLog, SnapshotReference};
use crate::manifest::{ManifestList};

#[derive(Debug, Serialize_repr, Deserialize_repr, PartialEq, Clone)]
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

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
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
    partition_specs: Vec<PartitionSpecModel>,
    /// ID of the “current” spec that writers should use by default.
    pub default_spec_id: i32,
    /// the highest assigned partition field ID across all partition specs
    /// for the table.
    ///
    /// This is used to ensure partition fields are always assigned an
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
        current_schema_id: i32,
        partition_fields: Option<Vec<PartitionField>>
    ) -> IcebergResult<Self> {
        let sort_order = SortOrder::new();
        let sort_order_id = sort_order.order_id;

        // Ensure current_schema_id is present in the list of schemas
        let current_schema =
            schemas.iter().find(|&schema| schema.id() == current_schema_id)
            .ok_or(IcebergError::SchemaNotFound { schema_id: current_schema_id })?;

        let partition_spec = match partition_fields {
            Some(partition_fields) => {
                PartitionSpec::try_new(
                    0,
                    partition_fields,
                    current_schema.clone()
                )?
            },
            None => {
                PartitionSpec::unpartitioned()
            }
        };
        let partition_spec_id = partition_spec.spec_id();
        let last_partition_id = partition_spec.last_assigned_field_id();

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
            partition_specs: vec![partition_spec.model()],
            default_spec_id: partition_spec_id,
            last_partition_id: last_partition_id,
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

    // TODO: This function will panic if the metadata object is invalid, which
    // can happen if it was deserialized directly.
    pub fn current_schema(&self) -> &Schema {
        // Panic if not found, as we are validating this in the constructor.
        self.schemas.iter().find(|&schema| schema.id() == self.current_schema_id)
            .expect("current_schema_id does not match any schema")
    }

    /// Return the latest snapshot of the table. May return `None` if the table has no
    /// snapshots, or if its `current_snapshot_id` is invalid.
    pub fn current_snapshot(&self) -> Option<&Snapshot> {
        self.snapshots.as_ref().and_then(|snapshots| {
            self.current_snapshot_id.and_then(|current_snapshot_id| {
                snapshots.iter().find(|&snapshot| {
                    snapshot.snapshot_id == current_snapshot_id
                })
            })
        })
    }

    // TODO: This function will panic if the metadata object is invalid, which
    // can happen if it was deserialized directly.
    pub fn current_partition_spec(&self) -> PartitionSpec {
        let model = self.partition_specs.iter().find(
            |spec| spec.spec_id == self.default_spec_id
        ).expect("default_spec_id does not match any partition spec");

        PartitionSpec::try_new(
            model.spec_id,
            model.fields.clone(),
            self.current_schema().clone()
        ).unwrap()
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

/// Holds the current state of the Iceberg table, changing with each commit.
pub struct IcebergTableState {
    /// UUID identifying the latest snapshot of the table.
    pub version_uuid: Uuid,
    /// Path to the current metadata file on the object store.
    pub metadata_path: IcebergPath,
}

pub struct IcebergTable {
    /// Latest state of the table, changes after each commit.
    /// Maybe be None for tables that were not initialized.
    state: Option<IcebergTableState>,
    /// Table metadata that includes schemas & snapshots
    metadata: Option<IcebergTableMetadata>,
    /// Used to access data and metadata files
    storage: Arc<IcebergStorage>,
}

/// The main interface for working with Iceberg tables.
impl IcebergTable {
    /// Creates an uninitialized Iceberg table on the given storage.
    ///
    /// The table must be initialized with either [`IcebergTable::load()`] or
    /// [`IcebergTable::create()`].
    pub fn new(storage: Arc<IcebergStorage>) -> Self {
        Self {
            state: None,
            metadata: None,
            storage: storage,
        }
    }

    /// Returns the full URL location of this table.
    pub fn location(&self) -> &str {
        self.storage.location()
    }

    /// Returns the underlying storage of this table.
    pub fn storage(&self) -> Arc<IcebergStorage> {
        self.storage.clone()
    }

    /// Returns the currently set schema for the table.
    ///
    /// # Errors
    ///
    /// This function will return [`IcebergError::TableNotInitialized`] if the table has not been
    /// initialized with either [`IcebergTable::create()`] or [`IcebergTable::load()`].
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
    /// This function will return [`IcebergError::TableNotInitialized`] if the table has not been
    /// initialized with either [`IcebergTable::create()`] or [`IcebergTable::load()`].
    pub fn current_snapshot(&self) -> IcebergResult<Option<&Snapshot>> {
        self.metadata.as_ref()
            .map(|metadata| metadata.current_snapshot())
            .ok_or(IcebergError::TableNotInitialized)
    }

    /// Returns a reference to the current table's metadata.
    ///
    /// # Errors
    ///
    /// This function will return [`IcebergError::TableNotInitialized`] if the table has not been
    /// initialized with either [`IcebergTable::create()`] or [`IcebergTable::load()`].
    pub fn current_metadata(&self) -> IcebergResult<&IcebergTableMetadata> {
        self.metadata.as_ref().ok_or(IcebergError::TableNotInitialized)
    }

    /// Returns the full URI of the current table's metadata file.
    ///
    /// # Errors
    ///
    /// This function will return [`IcebergError::TableNotInitialized`] if the table has not been
    /// initialized with either [`IcebergTable::create()`] or [`IcebergTable::load()`].
    pub fn current_metadata_uri(&self) -> IcebergResult<String> {
        self.state.as_ref().map(|state| self.storage.to_uri(&state.metadata_path))
            .ok_or(IcebergError::TableNotInitialized)
    }

    pub fn current_partition_spec(&self) -> IcebergResult<PartitionSpec> {
        Ok(self
            .current_metadata()?
            .current_partition_spec())
    }

    /// Reads the manifest list for the given snapshot.
    pub async fn read_manifest_list(
        &self,
        snapshot: &Snapshot
    ) -> IcebergResult<ManifestList> {
        let path = self.storage.create_path_from_url(&snapshot.manifest_list)?;
        let bytes = self.storage.get(&path).await?;

        ManifestList::decode(bytes.as_ref())
    }

    /// Commits the given metadata to the table, replacing the existing metadata.
    ///
    /// This is a low level interface. Prefer using [`IcebergTable::new_transaction()`]
    /// to create a new [`Transaction`] and commit it instead.
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

        let json = serde_json::to_string_pretty(&metadata)
            .map_err(|e| IcebergError::SerializeMetadataJson { source: e })?;

        // Generate a new UUID for this version, and set it only after the new metadata
        // file has been committed.
        let new_version_uuid = Uuid::new_v4();
        let metadata_file = self.new_metadata_file(
            &format!(
                "{:05}-{}.metadata.json",
                metadata.last_sequence_number,
                new_version_uuid.to_string()
            ),
            Bytes::from(json)
        )?;


        // TODO: This should be an atomic operation.
        // TODO: Testing for failures is needed.
        metadata_file.save().await?;

        self.metadata = Some(metadata);
        self.state = Some(IcebergTableState {
            version_uuid: new_version_uuid,
            metadata_path: metadata_file.path().clone(),
        });

        Ok(())
    }

    /// Initializes a new Iceberg table with the given schema at the table's location.
    /// This will create the first metadata file for the table, with the given schema
    /// set as the current schema.
    ///
    /// # Errors
    ///
    /// This function may fail if the schema is invalid (empty), or if commiting the
    /// metadata file to object store fails, in which case [`IcebergError::ObjectStore`]
    /// is returned.
    pub async fn create(&mut self, schema: Schema) -> IcebergResult<()> {
        if schema.fields().is_empty() {
            Err(IcebergError::SchemaError {
                message: "schema is empty".to_string()
            })
        } else {
            let current_schema_id = schema.id();
            let metadata = IcebergTableMetadata::try_new(
                self.storage.location().to_string(),
                vec![schema],
                current_schema_id,
                None
            )?;

            self.commit(metadata).await
        }
    }

    async fn get_latest_state(&self) -> IcebergResult<IcebergTableState> {
        lazy_static! {
            static ref METADATA_FILE_REGEX: Regex =
                Regex::new(concat!(
                    r#"^metadata/[0-9]+-"#,
                    r#"([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})"#,
                    r#".metadata.json$"#))
                .unwrap();
        }

        let objects = self.storage.list(Some(&IcebergPath::from("metadata"))).await?;

        // Find the highest numbered metadata file.
        let metadata_path = objects.into_iter()
            .filter_map(|obj_meta| {
                METADATA_FILE_REGEX.is_match(obj_meta.location.as_ref())
                    .then(|| obj_meta.location)
            })
            .max_by(|p, q| p.as_ref().cmp(q.as_ref()))
            .ok_or_else(|| {
                IcebergError::MetadataNotFound(self.location().to_string())
            })?;

        let captures = METADATA_FILE_REGEX.captures(metadata_path.as_ref())
            .ok_or_else(|| {
                IcebergError::MetadataNotFound(self.location().to_string())
            })?;

        let uuid = Uuid::parse_str(captures.get(1).unwrap().as_str())
            .map_err(|_| {
                IcebergError::MetadataNotFound(self.location().to_string())
            })?;

        Ok(IcebergTableState {
            version_uuid: uuid,
            metadata_path: metadata_path
        })
    }

    /// Loads an existing Iceberg table state from storage.
    ///
    /// # Errors
    ///
    /// This function returns [`IcebergError::MetadataNotFound`] if the table's metadata
    /// could not be located. In this case the table's state is left untouched and
    /// [`IcebergTable::create()`] can be called instead.
    /// [`IcebergError::InvalidMetadata`] is returned if the table's metadata could be
    /// found but could not be correctly parsed.
    pub async fn load(&mut self) -> IcebergResult<()> {
        let state = self.get_latest_state().await?;

        let bytes = self.storage.get(&state.metadata_path).await?;

        let metadata = serde_json::from_slice::<IcebergTableMetadata>(&bytes)
            .map_err(|e| IcebergError::InvalidMetadata { source: e })?;

        self.state = Some(state);
        self.metadata = Some(metadata);

        Ok(())
    }

    /// Initiates a new transaction on this table. Only a single transaction can be
    /// created at any given time.
    pub fn new_transaction(&mut self) -> Transaction {
        Transaction::new(self)
    }

    /// Creates a new data file for this table.
    ///
    /// This function assigns a path for a new file but does not save its content
    /// to storage. Call [`save()`](IcebergFile::save) on the result to save it.
    pub fn new_data_file(
        &self,
        filename: &str,
        bytes: Bytes
    ) -> IcebergResult<IcebergFile> {
        // Prefix every data file with a hash component.
        let hash = murmur3_32(&mut std::io::Cursor::new(filename), 0)?;
        let hash: [u8; 4] = hash.to_be_bytes();

        let path = IcebergPath::from_iter(vec![
            "data",
            &format!("{:02x}{:02x}{:02x}{:02x}",
                hash[0], hash[1], hash[2], hash[3]
            ),
            filename
        ]);

        Ok(IcebergFile::new(self.storage.clone(), path, bytes))
    }

    pub fn new_metadata_file(
        &self,
        filename: &str,
        bytes: Bytes
    ) -> IcebergResult<IcebergFile> {
        let path = IcebergPath::from_iter(vec![
            "metadata", filename
        ]);

        Ok(IcebergFile::new(self.storage.clone(), path, bytes))
    }
}

/// The main interface for creating or loading Iceberg tables.
///
/// This struct lets you load existing tables or create new ones easily.
///
/// # Examples
///
/// Load an Iceberg table from the local filesystem:
///
/// ```rust
/// use icelake::{IcebergTableLoader, IcebergResult};
///
/// #[tokio::main]
/// async fn main() -> IcebergResult<()> {
///     let table = IcebergTableLoader::from_url("file:///tmp/iceberg/users")
///         .load().await;
///
///     match table {
///         Ok(table) => println!("Table loaded"),
///         Err(..) => println!("Table might not exist"),
///     }
///
///     Ok(())
/// }
/// ```
pub struct IcebergTableLoader {
    table_url: String,
    storage_options: HashMap<String, String>
}

impl IcebergTableLoader {
    /// Creates a new IcebergTableLoader to load or create the table from the given URL.
    ///
    /// The URL determines the type of the backing object store. For example,
    /// `s3://bucket/table/` will create an S3-backed Iceberg table and
    /// `file:///path/to/table` will create it on the local filesystem.
    pub fn from_url(table_url: &str) -> Self {
        Self {
            table_url: table_url.to_string(),
            storage_options: HashMap::new(),
        }
    }

    /// Sets options for the storage, e.g. access credentials. The valid options depend
    /// on the type of storage as determined by the table url. For a list of valid
    /// options see [`IcebergStorage::from_url`].
    pub fn with_storage_options(
        mut self,
        storage_options: HashMap<String, String>
    ) -> Self {
        self.storage_options.extend(storage_options);
        self
    }

    /// Attempts to read storage options from environment variables.
    /// Currently supported environment variables:
    /// * AWS - `AWS_ACCESS_KEY_ID`, `AWS_DEFAULT_REGION`, `AWS_SECRET_ACCESS_KEY`
    pub fn with_env_options(mut self) -> Self {
        if let Ok(value) = std::env::var("AWS_DEFAULT_REGION") {
            self.storage_options.insert("aws_region".to_string(), value);
        }
        if let Ok(value) = std::env::var("AWS_ACCESS_KEY_ID") {
            self.storage_options.insert("aws_access_key_id".to_string(), value);
        }
        if let Ok(value) = std::env::var("AWS_SECRET_ACCESS_KEY") {
            self.storage_options.insert("aws_secret_access_key".to_string(), value);
        }

        self
    }

    fn build(self) -> IcebergResult<IcebergTable> {
        let storage = IcebergStorage::from_url(
            &self.table_url,
            self.storage_options,
        )?;

        Ok(IcebergTable::new(Arc::new(storage)))
    }

    /// Loads the state of an existing Iceberg table from storage.
    ///
    /// # Errors
    ///
    /// This function returns [`IcebergError::MetadataNotFound`] if the table's metadata
    /// could not be located.  [`IcebergError::InvalidMetadata`] is returned if the
    /// table's metadata could be found but could not be correctly parsed.
    /// [`IcebergError::ObjectStore`] could be returned if there was an error reading
    /// from the object storage.
    pub async fn load(self) -> IcebergResult<IcebergTable> {
        let mut table = self.build()?;

        table.load().await?;

        Ok(table)
    }

    /// Loads the state of an existing Iceberg table from storage, or creates it if it
    /// does not exist.
    ///
    /// # Errors
    ///
    /// [`IcebergError::ObjectStore`] could be returned if there was an error reading
    /// or writing to the object storage.
    pub async fn load_or_create(self, schema: Schema) -> IcebergResult<IcebergTable> {
        let mut table = self.build()?;

        let result = table.load().await;
        match result {
            Ok(..) => Ok(table),
            Err(err) => {
                match err {
                    IcebergError::MetadataNotFound(..) => {
                        table.create(schema).await?;
                        Ok(table)
                    },
                    _ => Err(err)
                }
            }
        }
    }
}

pub struct IcebergFile {
    storage: Arc<IcebergStorage>,
    path: IcebergPath,
    bytes: Bytes
}

impl IcebergFile {
    pub fn new(
        storage: Arc<IcebergStorage>,
        path: IcebergPath,
        bytes: Bytes
    ) -> Self {
        Self {
            storage: storage,
            path: path,
            bytes: bytes
        }
    }

    pub fn len(&self) -> usize {
        self.bytes.len()
    }

    /// Returns the relative path of this file.
    pub fn path(&self) -> &IcebergPath {
        &self.path
    }

    /// Returns the full URL of this file.
    pub fn url(&self) -> String {
        self.storage.to_uri(&self.path)
    }

    pub fn set_bytes(&mut self, bytes: Bytes) {
        self.bytes = bytes;
    }

    /// Saves the file to the table's object store.
    pub async fn save(&self) -> IcebergResult<()> {
        self.storage.put(&self.path, self.bytes.clone()).await
    }

    /// Deletes the file from the table's object store.
    pub async fn delete(&self) -> IcebergResult<()> {
        self.storage.delete(&self.path).await
    }
}

#[cfg(test)]
mod tests {
    use crate::{IcebergError, IcebergTableMetadata};
    use crate::schema::{Schema, SchemaField, SchemaType, PrimitiveType};

    fn create_schema(schema_id: i32) -> Schema {
        Schema::new(schema_id, vec![
            SchemaField::new(
                0,
                "id",
                true,
                SchemaType::Primitive(PrimitiveType::Long)
            ),
            SchemaField::new(
                1,
                "ts",
                false,
                SchemaType::Primitive(PrimitiveType::Timestamp)
            ),
            SchemaField::new(
                1,
                "user_id",
                false,
                SchemaType::Primitive(PrimitiveType::Int)
            )
        ])
    }

    #[test]
    fn new_metadata() {
        let schema0 = create_schema(0);
        let schema1 = create_schema(1);
        let metadata = IcebergTableMetadata::try_new(
            "s3://bucket/path/to/table".to_string(),
            vec![schema0, schema1],
            1,
            None
        ).unwrap();

        assert_eq!(metadata.current_schema().id(), 1);
    }

    #[test]
    fn new_metadata_with_incorrect_schema_id() {
        let schema = create_schema(0);

        // Schema id 1 is not in the list of schemas
        assert!(matches!(
            IcebergTableMetadata::try_new(
                "s3://bucket/path/to/table".to_string(),
                vec![schema],
                1,
                None
            ),
            Err(IcebergError::SchemaNotFound {..})
        ));
    }

    #[test]
    fn deserialize_metadata() {
        let metadata_json = r#"
            {
              "format-version" : 2,
              "table-uuid" : "395c54df-2023-450b-bbde-11b18b1dcc90",
              "location" : "s3://bucket/table",
              "last-sequence-number" : 2,
              "last-updated-ms" : 1681727363902,
              "last-column-id" : 9,
              "current-schema-id" : 0,
              "schemas" : [ {
                "type" : "struct",
                "schema-id" : 0,
                "fields" : [ {
                  "id" : 1,
                  "name" : "id",
                  "required" : false,
                  "type" : "int"
                } ]
              } ],
              "default-spec-id" : 0,
              "partition-specs" : [ {
                "spec-id" : 0,
                "fields" : [ ]
              } ],
              "last-partition-id" : 999,
              "default-sort-order-id" : 0,
              "sort-orders" : [ {
                "order-id" : 0,
                "fields" : [ ]
              } ],
              "properties" : {
                "table_type" : "ICEBERG"
              },
              "current-snapshot-id" : 380809481963248367,
              "snapshots" : [ {
                "sequence-number" : 1,
                "snapshot-id" : 8134731796600310831,
                "timestamp-ms" : 1681727363693,
                "summary" : {
                  "operation" : "delete",
                  "changed-partition-count" : "0",
                  "total-records" : "0",
                  "total-files-size" : "0",
                  "total-data-files" : "0",
                  "total-delete-files" : "0",
                  "total-position-deletes" : "0",
                  "total-equality-deletes" : "0"
                },
                "manifest-list" : "s3://bucket/table/metadata/snap-8134731796600310831-1-bc48b12a-048b-4b2f-8438-f6bfb17f2921.avro",
                "schema-id" : 0
              }, {
                "sequence-number" : 2,
                "snapshot-id" : 380809481963248367,
                "parent-snapshot-id" : 8134731796600310831,
                "timestamp-ms" : 1681727363902,
                "summary" : {
                  "operation" : "append",
                  "added-data-files" : "1",
                  "added-records" : "1000",
                  "added-files-size" : "84834",
                  "changed-partition-count" : "1",
                  "total-records" : "1000",
                  "total-files-size" : "84834",
                  "total-data-files" : "1",
                  "total-delete-files" : "0",
                  "total-position-deletes" : "0",
                  "total-equality-deletes" : "0"
                },
                "manifest-list" : "s3://bucket/table/metadata/snap-380809481963248367-1-f0cef43d-6447-4eb5-acb0-dafb4887b64e.avro",
                "schema-id" : 0
              } ],
              "snapshot-log" : [ {
                "timestamp-ms" : 1681727363902,
                "snapshot-id" : 380809481963248367
              } ],
              "metadata-log" : [ {
                "timestamp-ms" : 1681727362693,
                "metadata-file" : "s3://bucket/table/metadata/00000-5af42d97-7af3-41af-83f0-1173a2ac8681.metadata.json"
              } ]
            }"#;

        let metadata = serde_json::from_str::<IcebergTableMetadata>(metadata_json)
            .unwrap();

        assert_eq!(metadata.table_uuid, "395c54df-2023-450b-bbde-11b18b1dcc90");
        assert_eq!(metadata.current_schema_id, 0);
        assert_eq!(metadata.current_snapshot_id.unwrap(), 380809481963248367);
        assert_eq!(metadata.snapshots.unwrap().len(), 2);
        assert_eq!(metadata.snapshot_log.unwrap().len(), 1);
        assert_eq!(metadata.metadata_log.unwrap().len(), 1);
    }
}
