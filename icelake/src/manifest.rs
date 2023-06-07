//! Interface to Iceberg table manifest lists and manifest files.
use std::fmt;
use std::collections::HashMap;

use bytes::Bytes;
use serde::{
    Serialize, Serializer, ser::SerializeSeq,
    Deserialize, Deserializer
};
use serde_repr::{Serialize_repr, Deserialize_repr};
use serde_json::{
    json,
    value::Value as JsonValue
};
use apache_avro;

use crate::{IcebergResult, IcebergError, IcebergTableVersion};
use crate::schema::{Schema, SchemaType, StructField, PrimitiveType, StructType};
use crate::partition::PartitionSpec;

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

/// Produces the Avro schema for a partition field as a json value.
///
/// See the Avro spec for supported types and logical types.
/// [https://avro.apache.org/docs/1.11.1/specification/]
fn partition_field_avro_schema(field: &StructField) -> IcebergResult<JsonValue> {
    match &field.r#type {
        SchemaType::Primitive(p) => {
            Ok(match p {
                PrimitiveType::Boolean => json!({"type": "boolean"}),
                PrimitiveType::Int => json!({"type": "int"}),
                PrimitiveType::Long => json!({"type": "long"}),
                PrimitiveType::Float => json!({"type": "float"}),
                PrimitiveType::Double => json!({"type": "double"}),
                PrimitiveType::Decimal{precision, scale} => {
                    json!({
                        "type": "bytes",
                        "logicalType": "decimal",
                        "precision": precision,
                        "scale": scale
                    })
                },
                PrimitiveType::Date => {
                    json!({
                        "type": "int",
                        "logicalType": "date",
                    })
                },
                PrimitiveType::Time => {
                    json!({
                        "type": "long",
                        "logicalType": "time-micros",
                    })
                },
                PrimitiveType::Timestamp => {
                    json!({
                        "type": "long",
                        "logicalType": "timestamp-micros"
                    })
                },
                PrimitiveType::Timestamptz => {
                    json!({
                        "type": "long",
                        "logicalType": "local-timestamp-micros"
                    })
                },
                PrimitiveType::String => json!({"type": "string"}),
                PrimitiveType::Uuid => {
                    json!({
                        "type": "string",
                        "logicalType": "uuid"
                    })
                },
                PrimitiveType::Fixed(size) => {
                    json!({
                        "type": "fixed",
                        "size": size,
                        "name": field.name
                    })
                },
                PrimitiveType::Binary => json!({"type": "bytes"})
            })
        },
        _ => {
            Err(IcebergError::PartitionError {
                message: format!(
                    "partition field {} has non-primitive type",
                    field.name
                )
            })
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Partition {
    // TODO
}

#[derive(Debug, Clone, Serialize_repr, Deserialize_repr)]
#[repr(i32)]
pub enum DataFileContent {
    Data = 0,
    PositionDelete = 1,
    EqualityDelete = 2,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum DataFileFormat {
    Avro,
    ORC,
    Parquet
}

// Have to manually implement the Serialize trait for the DataFileFormat enum
// due to bug in the Apache Avro create.
impl Serialize for DataFileFormat {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(match *self {
            DataFileFormat::Avro => "AVRO",
            DataFileFormat::ORC => "ORC",
            DataFileFormat::Parquet => "PARQUET",
        })
    }
}

/// Map used by data files to store column statistics by column id.
#[derive(Debug, Clone)]
struct ColumnMap<V> {
    inner: HashMap<i32, V>
}

impl<V> ColumnMap<V> {
    pub fn new() -> Self { Self { inner: HashMap::new() } }

    pub fn insert(&mut self, k: i32, v: V) -> Option<V> { self.inner.insert(k, v) }
}

impl<V> Serialize for ColumnMap<V>
where
    V: Serialize
{
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        #[derive(Debug, Serialize)]
        struct KeyValueRecord<'a, V> {
            key: i32,
            value: &'a V
        }

        let mut seq = serializer.serialize_seq(Some(self.inner.len()))?;
        for (k, v) in &self.inner {
            seq.serialize_element(&KeyValueRecord {
                key: *k,
                value: v
            })?;
        }
        seq.end()
    }
}

/// Deserialize trait implementation converting a list of key value records
/// into a ColumnMap object.
impl<'de, V> Deserialize<'de> for ColumnMap<V>
where
    V: Deserialize<'de>
{
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Debug, Deserialize)]
        struct KeyValueRecord<V> {
            key: i32,
            value: V
        }

        let records = Vec::<KeyValueRecord<V>>::deserialize(deserializer)?;
        let mut map = ColumnMap::new();
        for record in records {
            map.insert(record.key, record.value);
        }
        Ok(map)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DataFile {
    /// Type of content stored by the data file: data, equality deletes, or position
    /// deletes.
    content: DataFileContent,
    /// Full URI for the file with FS scheme.
    file_path: String,
    /// String file format name: avro, orc or parquet.
    file_format: DataFileFormat,
    /// Partition data tuple, schema based on the partition spec output using
    /// partition field ids for the struct field ids.
    partition: Partition,
    /// Number of records in this file.
    record_count: i64,
    /// Total file size in bytes.
    file_size_in_bytes: i64,
    /// Map from column id to the total size on disk of all regions that store the
    /// column. Does not include bytes necessary to read other columns, like footers.
    /// Leave null for row-oriented formats (Avro).
    column_sizes: Option<ColumnMap<i64>>,
    /// Map from column id to number of values in the column (including null and NaN
    /// values).
    value_counts: Option<ColumnMap<i64>>,
    /// Map from column id to number of null values in the column.
    null_value_counts: Option<ColumnMap<i64>>,
    /// Map from column id to number of NaN values in the column.
    nan_value_counts: Option<ColumnMap<i64>>,
    /// Map from column id to number of distinct values in the column; distinct counts
    /// must be derived using values in the file by counting or using sketches, but not
    /// using methods like merging existing distinct counts.
    distinct_counts: Option<ColumnMap<i64>>,
    /// Map from column id to lower bound in the column serialized as binary. Each value
    /// must be less than or equal to all non-null, non-NaN values in the column for
    /// the file.
    lower_bounds: Option<ColumnMap<Vec<u8>>>,
    /// Map from column id to upper bound in the column serialized as binary. Each value
    /// must be greater than or equal to all non-null, non-Nan values in the column for
    /// the file.
    upper_bounds: Option<ColumnMap<Vec<u8>>>,
    /// Implementation-specific key metadata for encryption.
    key_metadata: Option<Vec<u8>>,
    /// Split offsets for the data file. For example, all row group offsets in a Parquet
    /// file. Must be sorted ascending.
    split_offsets: Option<Vec<i64>>,
    /// Field ids used to determine row equality in equality delete files. Required
    /// when content=2 and should be null otherwise. Fields with ids listed in this
    /// column must be present in the delete file.
    equality_ids: Option<Vec<i32>>,
    /// ID representing sort order for this file.
    sort_order_id: Option<i32>,
}

impl DataFile {
    pub fn new(
        content: DataFileContent,
        file_path: &str,
        file_format: DataFileFormat,
        record_count: i64,
        file_size_in_bytes: i64,
    ) -> Self {
        Self {
            content: content,
            file_path: String::from(file_path),
            file_format: file_format,
            partition: Partition {},
            record_count: record_count,
            file_size_in_bytes: file_size_in_bytes,
            column_sizes: None,
            value_counts: None,
            null_value_counts: None,
            nan_value_counts: None,
            distinct_counts: None,
            lower_bounds: None,
            upper_bounds: None,
            key_metadata: None,
            split_offsets: None,
            equality_ids: None,
            sort_order_id: None,
        }
    }

    /// Builds the Avro schema for the partition spec, and returns it as a JSON value.
    ///
    /// The Avro schema is dynamically determined according to the fields of the
    /// partition spec and the schema of the table. It is used when encoding the partition
    /// spec as part of a manifest file.
    ///
    /// An example of a resulting Avro schema:
    /// ```json
    /// {
    ///     "name": "partition",
    ///     "type": {
    ///         "type": "record",
    ///         "name": "r102",
    ///         "fields": [
    ///             {
    ///                 "name": "shipdate",
    ///                 "type": [
    ///                     "null",
    ///                     {
    ///                         "type": "int",
    ///                         "logicalType": "date"
    ///                     }
    ///                 ],
    ///                 "default": null,
    ///                 "field-id": 1000
    ///             }
    ///         ]
    ///     },
    ///     "doc": "Partition data tuple, schema based on the partition spec",
    ///     "field-id": 102
    /// }
    /// ```
    fn partition_avro_schema(
        partition_type: StructType
    ) -> IcebergResult<JsonValue> {

        let fields_schema: Vec<JsonValue> = partition_type.fields
            .iter()
            .map(|field| -> IcebergResult<JsonValue> {
                Ok(serde_json::json!({
                    "name": field.name,
                    "type": [
                        "null",
                        partition_field_avro_schema(field)?
                    ],
                    "default": "null",
                    "field-id": field.id,
                }))
            })
            .collect::<IcebergResult<Vec<JsonValue>>>()?;

        Ok(serde_json::json!({
            "name": "partition",
            "type": {
                "type": "record",
                "name": "r102",
                "fields": fields_schema
            },
            "doc": "Partition data tuple, schema based on the partition spec",
            "field-id": 102
        }))
    }

    /// Builds the Avro schema for the DataFile, and returns it as a JSON value.
    ///
    /// The Avro schema is dynamically determined according to the fields of the
    /// partition spec and the schema of the table. It is used when encoding the partition
    /// spec as part of a manifest file.
    pub fn avro_schema(
        schema: &Schema,
        partition_spec: &PartitionSpec
    ) -> IcebergResult<JsonValue> {
        let partition_type = partition_spec.as_struct_type(schema)?;

        let data_file_fields_schema = json!([
            { "name": "content", "type": "int", "field-id": 134},
            { "name": "file_path", "type": "string", "field-id": 100 },
            { "name": "file_format", "type": "string", "field-id": 101 },
            Self::partition_avro_schema(partition_type)?,
            { "name": "record_count", "type": "long", "field-id": 103 },
            { "name": "file_size_in_bytes", "type": "long", "field-id": 104 },
            {
                "name": "column_sizes",
                "type": [
                    "null",
                    {
                        "type": "array",
                        "items": {
                            "type": "record",
                            "name": "k117_v118",
                            "fields": [
                            { "name": "key", "type": "int", "field-id": 117 },
                            { "name": "value", "type": "long", "field-id": 118 }
                            ]
                        },
                        "logicalType": "map"
                    }
                ],
                "default": null,
                "field-id": 108
            },
            {
                "name": "value_counts",
                "type": [
                    "null",
                    {
                        "type": "array",
                        "items": {
                            "type": "record",
                            "name": "k119_v120",
                            "fields": [
                            { "name": "key", "type": "int", "field-id": 119 },
                            { "name": "value", "type": "long", "field-id": 120 }
                            ]
                        },
                        "logicalType": "map"
                    }
                ],
                "default": null,
                "field-id": 109
            },
            {
                "name": "null_value_counts",
                "type": [
                    "null",
                    {
                        "type": "array",
                        "items": {
                            "type": "record",
                            "name": "k121_v122",
                            "fields": [
                            { "name": "key", "type": "int", "field-id": 121 },
                            { "name": "value", "type": "long", "field-id": 122 }
                            ]
                        },
                        "logicalType": "map"
                    }
                ],
                "default": null,
                "field-id": 110
            },
            {
                "name": "nan_value_counts",
                "type": [
                    "null",
                    {
                        "type": "array",
                        "items": {
                            "type": "record",
                            "name": "k138_v139",
                            "fields": [
                            { "name": "key", "type": "int", "field-id": 138 },
                            { "name": "value", "type": "long", "field-id": 139 }
                            ]
                        },
                        "logicalType": "map"
                    }
                ],
                "default": null,
                "field-id": 137
            },
            {
                "name": "distinct_counts",
                "type": [
                    "null",
                    {
                        "type": "array",
                        "items": {
                            "type": "record",
                            "name": "k123_v124",
                            "fields": [
                            { "name": "key", "type": "int", "field-id": 123 },
                            { "name": "value", "type": "long", "field-id": 124 }
                            ]
                        },
                        "logicalType": "map"
                    }
                ],
                "default": null,
                "field-id": 111
            },
            {
                "name": "lower_bounds",
                "type": [
                    "null",
                    {
                        "type": "array",
                        "items": {
                            "type": "record",
                            "name": "k126_v127",
                            "fields": [
                            { "name": "key", "type": "int", "field-id": 126 },
                            { "name": "value", "type": "bytes", "field-id": 127 }
                            ]
                        },
                        "logicalType": "map"
                    }
                ],
                "default": null,
                "field-id": 125
            },
            {
                "name": "upper_bounds",
                "type": [
                    "null",
                    {
                        "type": "array",
                        "items": {
                            "type": "record",
                            "name": "k129_v130",
                            "fields": [
                            { "name": "key", "type": "int", "field-id": 129 },
                            { "name": "value", "type": "bytes", "field-id": 130 }
                            ]
                        },
                        "logicalType": "map"
                    }
                ],
                "default": null,
                "field-id": 128
            },
            {
                "name": "key_metadata",
                "type": [ "null", "bytes" ],
                "default": null,
                "field-id": 131
            },
            {
                "name": "split_offsets",
                "type": [ "null", { "type": "array", "items": "long", "element-id": 133 } ],
                "default": null,
                "field-id": 132
            },
            {
                "name": "equality_ids",
                "type": [ "null", { "type": "array", "items": "int", "element-id": 136 } ],
                "default": null,
                "field-id": 135
            },
            {
                "name": "sort_order_id",
                "type": [ "null", "int" ],
                "default": null,
                "field-id": 140
            }
        ]);

        Ok(json!({
            "name": "data_file",
            "type": {
                "type": "record",
                "name": "r2",
                "fields": data_file_fields_schema,
            },
            "field-id": 2
        }))
    }
}

#[derive(Debug, Serialize_repr, Deserialize_repr, PartialEq, Eq, Clone)]
#[repr(i32)]
pub enum ManifestEntryStatus {
    Existing = 0,
    Added = 1,
    Deleted = 2,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ManifestEntry {
    /// Used to track additions and deletions. Deletes are informational only and not
    /// used in scans. Field meaning: 0: EXISTING 1: ADDED 2: DELETED
    status: ManifestEntryStatus,
    /// Snapshot id where the file was added, or deleted if status is 2. Inherited
    /// when null.
    snapshot_id: Option<i64>,
    /// Data sequence number of the file. Inherited when null and status is 1 (added).
    sequence_number: Option<i64>,
    /// File sequence number indicating when the file was added. Inherited when null
    /// and status is 1 (added).
    file_sequence_number: Option<i64>,
    /// File path, partition tuple, metrics.
    data_file: DataFile,
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

    pub fn avro_schema(
        schema: &Schema,
        partition_spec: &PartitionSpec
    ) -> IcebergResult<apache_avro::schema::Schema> {
        let schema = json!({
            "type": "record",
            "name": "manifest_entry",
            "fields": [
                { "name": "status", "type": "int", "field-id": 0 },
                {
                    "name": "snapshot_id",
                    "type": [ "null", "long" ],
                    "default": null,
                    "field-id": 1
                },
                {
                    "name": "sequence_number",
                    "type": [ "null", "long" ],
                    "default": null,
                    "field-id": 3
                },
                {
                    "name": "file_sequence_number",
                    "type": [ "null", "long" ],
                    "default": null,
                    "field-id": 4
                },
                DataFile::avro_schema(schema, partition_spec)?
            ]
        });

        Ok(apache_avro::schema::Schema::parse(&schema)?)
    }
}

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
    content: ManifestContentType,
    /// On-disk representation of the data files tracked by this manifest.
    entries: Vec<ManifestEntry>,
}

impl Manifest {
    pub fn new(
        schema: Schema,
        partition_spec: PartitionSpec,
        content: ManifestContentType
    ) -> Self {
        let schema_id = schema.id();
        Self {
            schema: schema,
            schema_id: schema_id,
            partition_spec: partition_spec,
            format_version: IcebergTableVersion::V2,
            content: content,
            entries: Vec::new(),
        }
    }

    pub fn add_manifest_entry(&mut self, manifest: ManifestEntry) {
        self.entries.push(manifest);
    }

    pub fn entries(&self) -> &Vec<ManifestEntry> {
        &self.entries
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

#[derive(Debug, Serialize_repr, Deserialize_repr)]
#[repr(i32)]
pub enum ManifestFileType {
    Data = 0,
    Delete = 1,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ManifestFile {
    /// Location of the manifest file.
    manifest_path: String,
    /// Length of the manifest file in bytes.
    manifest_length: i64,
    /// ID of a partition spec used to write the manifest; must be listed in table
    /// metadata partition-specs.
    partition_spec_id: i32,
    /// The type of files tracked by the manifest, either data or delete files;
    /// 0 for data files and 1 for delete files.
    content: ManifestFileType,
    /// The sequence number when the manifest was added to the table.
    sequence_number: i64,
    /// The minimum data sequence number of all live data or delete files in the
    /// manifest.
    min_sequence_number: i64,
    /// ID of the snapshot where the manifest file was added.
    added_snapshot_id: i64,
    /// Number of entries in the manifest that have status ADDED.
    added_data_files_count: i32,
    /// Number of entries in the manifest that have status EXISTING.
    existing_data_files_count: i32,
    /// Number of entries in the manifest that have status DELETED.
    deleted_data_files_count: i32,
    /// Number of rows in all of files in the manifest that have status ADDED.
    added_rows_count: i64,
    /// Number of rows in all of files in the manifest that have status EXISTING.
    existing_rows_count: i64,
    /// Number of rows in all of files in the manifest that have status DELETED.
    deleted_rows_count: i64,
    /// A list of field summaries for each partition field in the spec. Each field in
    /// the list corresponds to a field in the manifest file’s partition spec.
    partitions: Option<Vec<PartitionFieldSummary>>,
}

pub struct ManifestWriter {
    sequence_number: i64,
    snapshot_id: i64,
}

impl ManifestWriter {
    pub fn new(sequence_number: i64, snapshot_id: i64) -> Self {
        Self { sequence_number, snapshot_id }
    }

    /// Constructs the Avro schema for the ManifestEntry struct.
    ///
    /// The schema is dynamically constructed since it changes according to the partition
    /// spec of the manifest.
    fn manifest_entry_schema(
        manifest: &Manifest
    ) -> IcebergResult<apache_avro::schema::Schema> {
        ManifestEntry::avro_schema(
            &manifest.schema,
            &manifest.partition_spec
        )
    }

    /// Serializes the manifest into an Avro manifest file.
    /// Consumes the Manifest object.
    fn encode(&self, manifest: Manifest) -> IcebergResult<Bytes> {
        let avro_schema = Self::manifest_entry_schema(&manifest)?;
        let mut writer = apache_avro::Writer::new(&avro_schema, Vec::<u8>::new());

        // The Avro file's key-value metadata contains the schema of the table
        // and the partition spec for the files in the manifest.
        let mut metadata = HashMap::<String, String>::new();
        metadata.insert("schema".to_string(), manifest.schema.encode()?);
        metadata.insert("schema-id".to_string(), manifest.schema_id.to_string());
        metadata.insert(
            "partition-spec".to_string(),
            serde_json::to_string(&manifest.partition_spec.fields)
                .map_err(|e| IcebergError::SerializeJson { source: e })?
        );
        metadata.insert(
            "partition-spec-id".to_string(),
            manifest.partition_spec.spec_id.to_string()
        );
        metadata.insert("format-version".to_owned(), manifest.format_version.to_string());
        metadata.insert("content".to_owned(), manifest.content.to_string());
        for (key, value) in metadata {
            writer.add_user_metadata(key, value)?;
        }
        writer.extend_ser(manifest.entries)?;
        Ok(Bytes::from(writer.into_inner()?))
    }

    /// Serializes the manifest into Avro binary format. Returns the serialized bytes
    /// alongside with a corresponding ManifestFile object pointing to the manifest.
    /// The ManifestFile can the be added to a ManifestList. The function encodes the
    /// manifest but does not actually write it to storage.
    /// The function consumes the Manifest object.
    pub fn write(
        &self,
        manifest_path: &str,
        manifest: Manifest
    ) -> IcebergResult<(Bytes, ManifestFile)> {
        let mut manifest_file = ManifestFile {
            manifest_path: manifest_path.to_string(),
            manifest_length: 0,
            partition_spec_id: 0,
            content: match manifest.content {
                ManifestContentType::Data => ManifestFileType::Data,
                ManifestContentType::Deletes => ManifestFileType::Delete
            },
            sequence_number: self.sequence_number,
            min_sequence_number: manifest.min_sequence_number()
                .unwrap_or(self.sequence_number),
            added_snapshot_id: self.snapshot_id,
            added_data_files_count: manifest.added_data_files_count(),
            existing_data_files_count: manifest.existing_data_files_count(),
            deleted_data_files_count: manifest.deleted_data_files_count(),
            added_rows_count: manifest.added_rows_count(),
            existing_rows_count: manifest.existing_rows_count(),
            deleted_rows_count: manifest.deleted_rows_count(),
            partitions: None,
        };

        let encoded = self.encode(manifest)?;

        // Update the manifest_length only after encoding
        manifest_file.manifest_length = encoded.len().try_into().unwrap();
        
        Ok((encoded, manifest_file))
    }
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
