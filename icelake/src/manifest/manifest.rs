//! Manifest files and manifest lists.
use std::fmt;

use serde::{Serialize, Deserialize};
use serde_repr::{Serialize_repr, Deserialize_repr};
use apache_avro;

use crate::{IcebergError, IcebergResult, IcebergTableVersion};
use crate::schema::Schema;
use crate::partition::PartitionSpec;
use super::datafile::DataFile;

/// Avro schema for the manifest_file struct
static MANIFEST_FILE_SCHEMA: &str = r#"
{
  "type": "record",
  "name": "manifest_file",
  "fields": [
    { "name": "manifest_path", "type": "string", "field-id": 500 },
    { "name": "manifest_length", "type": "long", "field-id": 501 },
    { "name": "partition_spec_id", "type": "int", "field-id": 502 },
    { "name": "content", "type": "int", "field-id": 517 },
    { "name": "sequence_number", "type": "long", "field-id": 515 },
    { "name": "min_sequence_number", "type": "long", "field-id": 516 },
    { "name": "added_snapshot_id", "type": "long", "field-id": 503 },
    { "name": "added_data_files_count", "type": "int", "field-id": 504 },
    { "name": "existing_data_files_count", "type": "int", "field-id": 505 },
    { "name": "deleted_data_files_count", "type": "int", "field-id": 506 },
    { "name": "added_rows_count", "type": "long", "field-id": 512 },
    { "name": "existing_rows_count", "type": "long", "field-id": 513 },
    { "name": "deleted_rows_count", "type": "long", "field-id": 514 },
    { "name": "partitions",
      "type": [
        "null",
        {
          "type": "array",
          "items": {
            "type": "record",
            "name": "field_summary",
            "fields": [
              { "name": "contains_null", "type": "boolean", "field-id": 509 },
              {
                "name": "contains_nan",
                "type": [ "null", "boolean" ],
                "default": null,
                "field-id": 518
              },
              {
                "name": "lower_bound",
                "type": [ "null", "bytes" ],
                "default": null,
                "field-id": 510
              },
              {
                "name": "upper_bound",
                "type": [ "null", "bytes" ],
                "default": null,
                "field-id": 511
              }
            ]
          },
          "element-id": 508
        }
      ],
      "default": null,
      "field-id": 507
    }
  ]
}
"#;


#[derive(Debug, PartialEq, Eq, Clone, Deserialize_repr)]
#[repr(u8)]
pub enum ManifestEntryStatus {
    Existing = 0,
    Added = 1,
    Deleted = 2,
}

/// Tracks a single data or delete file in a manifest.
///
/// The `ManifestEntry` stores status, metrics and tracking information about the
/// data or delete file.
#[derive(Debug, Clone)]
pub struct ManifestEntry {
    /// Used to track additions and deletions. Deletes are informational only and not
    /// used in scans. Field meaning: 0: EXISTING 1: ADDED 2: DELETED
    pub status: ManifestEntryStatus,
    /// Snapshot id where the file was added, or deleted if status is 2. Inherited
    /// when null.
    pub snapshot_id: Option<i64>,
    /// Data sequence number of the file. Inherited when null and status is 1 (added).
    pub sequence_number: Option<i64>,
    /// File sequence number indicating when the file was added. Inherited when null
    /// and status is 1 (added).
    pub file_sequence_number: Option<i64>,
    /// File path, partition tuple, metrics.
    pub data_file: DataFile,
}

impl ManifestEntry {
    pub fn new(
        status: ManifestEntryStatus,
        snapshot_id: i64,
        data_file: DataFile
    ) -> Self {
        Self {
            status: status,
            snapshot_id: Some(snapshot_id),
            sequence_number: None,
            file_sequence_number: None,
            data_file: data_file
        }
    }

    pub fn status(&self) -> ManifestEntryStatus {
        self.status.clone()
    }

    pub fn existing(&self) -> bool {
        self.status == ManifestEntryStatus::Existing
    }

    pub fn added(&self) -> bool {
        self.status == ManifestEntryStatus::Added
    }

    pub fn deleted(&self) -> bool {
        self.status == ManifestEntryStatus::Deleted
    }

    pub fn snapshot_id(&self) -> Option<i64> {
        self.snapshot_id
    }

    pub fn sequence_number(&self) -> Option<i64> {
        self.sequence_number
    }

    pub fn file_sequence_number(&self) -> Option<i64> {
        self.file_sequence_number
    }

    pub fn data_file(&self) -> &DataFile {
        &self.data_file
    }

    pub fn with_status(mut self, status: ManifestEntryStatus) -> Self {
        self.status = status;
        self
    }
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub enum ManifestContentType {
    Data,
    Deletes,
}

impl fmt::Display for ManifestContentType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", match *self {
            ManifestContentType::Data => "data",
            ManifestContentType::Deletes => "deletes"
        })
    }
}

impl std::str::FromStr for ManifestContentType {
    type Err = IcebergError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "data" => Ok(ManifestContentType::Data),
            "deletes" => Ok(ManifestContentType::Deletes),
            _ => Err(IcebergError::ManifestError(
                format!("invalid manifest content type '{s}'")
            ))
        }
    }
}

/// An Iceberg manifest stores a list of data or delete files, along with each
/// file’s partition data tuple, metrics, and tracking information.
/// A manifest stores files for a single partition spec. The partition spec of each
/// manifest is also used to transform predicates on the table’s data rows into
/// predicates on partition values that are used during job planning to select files
/// from a manifest.
pub struct Manifest {
    /// The table schema at the time the manifest was written.
    schema: Schema,
    /// ID of the schema used to write the manifest.
    schema_id: i32,
    /// Partition spec of the data files within this manifest.
    partition_spec: PartitionSpec,
    /// Iceberg table version (V1 or V2)
    format_version: IcebergTableVersion,
    /// Type of content files tracked by the manifest: “data” or “deletes”
    content_type: ManifestContentType,
    /// On-disk representation of the data files tracked by this manifest.
    entries: Vec<ManifestEntry>,
}

impl Manifest {
    pub fn new(
        schema: Schema,
        partition_spec: PartitionSpec,
        content_type: ManifestContentType
    ) -> Self {
        let schema_id = schema.id();
        Self {
            schema: schema,
            schema_id: schema_id,
            partition_spec: partition_spec,
            format_version: IcebergTableVersion::V2,
            content_type: content_type,
            entries: Vec::new(),
        }
    }

    pub fn add_manifest_entry(&mut self, manifest: ManifestEntry) {
        self.entries.push(manifest);
    }

    pub fn add_manifest_entries(
        &mut self,
        entries: impl IntoIterator<Item = ManifestEntry>
    ) {
        self.entries.extend(entries);
    }

    pub fn schema(&self) -> &Schema {
        &self.schema
    }

    pub fn schema_id(&self) -> i32 {
        self.schema_id
    }

    pub fn partition_spec(&self) -> &PartitionSpec {
        &self.partition_spec
    }

    pub fn format_version(&self) -> IcebergTableVersion {
        self.format_version.clone()
    }

    pub fn content_type(&self) -> ManifestContentType {
        self.content_type.clone()
    }

    pub fn entries(&self) -> &Vec<ManifestEntry> {
        &self.entries
    }

    pub fn entries_mut(&mut self) -> &mut Vec<ManifestEntry> {
        &mut self.entries
    }

    pub fn into_entries(self) -> impl Iterator<Item = ManifestEntry> {
        self.entries.into_iter()
    }

    pub fn min_sequence_number(&self) -> Option<i64> {
        self.entries.iter()
            .filter_map(|entry| entry.sequence_number)
            .min()
    }

    /// Returns the number of manifest entries within this manifest with status
    /// ManifestEntryStatus::Added
    pub fn added_data_files_count(&self) -> i32 {
        self.entries.iter()
            .filter(|entry| entry.status == ManifestEntryStatus::Added)
            .count()
            .try_into().unwrap()
    }

    /// Returns the number of manifest entries within this manifest with status
    /// ManifestEntryStatus::Existing
    pub fn existing_data_files_count(&self) -> i32 {
        self.entries.iter()
            .filter(|entry| entry.status == ManifestEntryStatus::Existing)
            .count()
            .try_into().unwrap()
    }

    /// Returns the number of manifest entries within this manifest with status
    /// ManifestEntryStatus::Deleted
    pub fn deleted_data_files_count(&self) -> i32 {
        self.entries.iter()
            .filter(|entry| entry.status == ManifestEntryStatus::Deleted)
            .count()
            .try_into().unwrap()
    }

    /// Returns the number of rows in all of files in the manifest that have status
    /// ManifestEntryStatus::Added
    pub fn added_rows_count(&self) -> i64 {
        self.entries.iter()
            .filter(|entry| entry.status == ManifestEntryStatus::Added)
            .map(|entry| entry.data_file.record_count)
            .sum()
    }

    /// Returns the number of rows in all of files in the manifest that have status
    /// ManifestEntryStatus::Existing
    pub fn existing_rows_count(&self) -> i64 {
        self.entries.iter()
            .filter(|entry| entry.status == ManifestEntryStatus::Existing)
            .map(|entry| entry.data_file.record_count)
            .sum()
    }

    /// Returns the number of rows in all of files in the manifest that have status
    /// ManifestEntryStatus::Existing
    pub fn deleted_rows_count(&self) -> i64 {
        self.entries.iter()
            .filter(|entry| entry.status == ManifestEntryStatus::Deleted)
            .map(|entry| entry.data_file.record_count)
            .sum()
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct PartitionFieldSummary {
    /// Whether the manifest contains at least one partition with a null value for the
    /// field.
    contains_null: bool,
    /// Whether the manifest contains at least one partition with a NaN value for the
    /// field.
    contains_nan: Option<bool>,
    /// Lower bound for the non-null, non-NaN values in the partition field, or null if
    /// all values are null or NaN.
    lower_bound: Option<Vec<u8>>,
    /// Upper bound for the non-null, non-NaN values in the partition field, or null if
    /// all values are null or NaN.
    upper_bound: Option<Vec<u8>>,
}

#[derive(Debug, PartialEq, Eq, Clone, Serialize_repr, Deserialize_repr)]
#[repr(i32)]
pub enum ManifestFileType {
    Data = 0,
    Delete = 1,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ManifestFile {
    /// Location of the manifest file.
    pub manifest_path: String,
    /// Length of the manifest file in bytes.
    pub manifest_length: i64,
    /// ID of a partition spec used to write the manifest; must be listed in table
    /// metadata partition-specs.
    pub partition_spec_id: i32,
    /// The type of files tracked by the manifest, either data or delete files;
    /// 0 for data files and 1 for delete files.
    pub content: ManifestFileType,
    /// The sequence number when the manifest was added to the table.
    pub sequence_number: i64,
    /// The minimum data sequence number of all live data or delete files in the
    /// manifest.
    pub min_sequence_number: i64,
    /// ID of the snapshot where the manifest file was added.
    pub added_snapshot_id: i64,
    /// Number of entries in the manifest that have status ADDED.
    pub added_data_files_count: i32,
    /// Number of entries in the manifest that have status EXISTING.
    pub existing_data_files_count: i32,
    /// Number of entries in the manifest that have status DELETED.
    pub deleted_data_files_count: i32,
    /// Number of rows in all of files in the manifest that have status ADDED.
    pub added_rows_count: i64,
    /// Number of rows in all of files in the manifest that have status EXISTING.
    pub existing_rows_count: i64,
    /// Number of rows in all of files in the manifest that have status DELETED.
    pub deleted_rows_count: i64,
    /// A list of field summaries for each partition field in the spec. Each field in
    /// the list corresponds to a field in the manifest file’s partition spec.
    pub partitions: Option<Vec<PartitionFieldSummary>>,
}

/// An Iceberg manifest list file contains entries of ManifestFile serialized
/// to Avro. Each ManifestFile points to the path of the actual manifest file on the
/// underlying storage.
pub struct ManifestList {
    manifests: Vec<ManifestFile>,
}

impl ManifestList {
    pub fn new() -> Self { Self { manifests: Vec::new() } }

    pub fn push(&mut self, manifest: ManifestFile) {
        self.manifests.push(manifest);
    }

    pub fn is_empty(&self) -> bool {
        self.manifests.is_empty()
    }

    pub fn manifest_files(&self) -> &Vec<ManifestFile> {
        &self.manifests
    }

    /// Encodes the manifest list to an Avro manifest list file.
    /// Consumes the ManifestList object.
    pub fn encode(self) -> IcebergResult<Vec<u8>> {
        let schema = apache_avro::Schema::parse_str(MANIFEST_FILE_SCHEMA)?;
        let mut writer = apache_avro::Writer::new(&schema, Vec::<u8>::new());
        writer.extend_ser(self.manifests)?;
        Ok(writer.into_inner()?)
    }

    /// Decodes a ManifestList from an Avro-encoded manifest list file.
    pub fn decode(data: &[u8]) -> IcebergResult<Self> {
        let schema = apache_avro::Schema::parse_str(MANIFEST_FILE_SCHEMA)?;
        let reader = apache_avro::Reader::with_schema(&schema, data)?;

        let manifests: Result<Vec<ManifestFile>, _> = reader.into_iter().map(|res| {
            res.and_then(|value| apache_avro::from_value::<ManifestFile>(&value))
        }).collect();

        Ok(Self { manifests: manifests? })
    }
}

impl IntoIterator for ManifestList {
    type Item = ManifestFile;
    type IntoIter = std::vec::IntoIter<Self::Item>;

    fn into_iter(self) -> Self::IntoIter {
        self.manifests.into_iter()
    }
}
