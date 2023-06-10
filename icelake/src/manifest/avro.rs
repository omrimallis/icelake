//! Serialization and deserialization of manifests to Avro format.
use std::collections::HashMap;

use apache_avro::{
    Error as AvroError,
    Writer as AvroWriter,
    schema::Schema as AvroSchema,
    types::Record as AvroRecord,
    types::Value as AvroValue
};
use serde_json::{json, value::Value as JsonValue};

use crate::{IcebergResult, IcebergError};
use crate::schema::{StructField, SchemaType, PrimitiveType, StructType};
use crate::partition::PartitionSpec;
use super::manifest::{Manifest, ManifestEntry, ManifestEntryStatus};
use super::datafile::{DataFile, DataFileContent, DataFileFormat};

/// Serializes ManifestEntry to Avro.
struct ManifestEntrySerializer {
    data_file_schema: AvroSchema,
    manifest_entry_schema: AvroSchema
}

impl ManifestEntrySerializer {
    pub fn try_new(partition_spec: &PartitionSpec) -> IcebergResult<Self> {
        // The Avro schemas are dynamically built based on the Iceberg schema and
        // partition spec.
        let data_file_schema_json = Self::build_data_file_schema(partition_spec)?;
        let manifest_entry_schema_json = Self::build_manifest_entry_schema(
            &data_file_schema_json
        );

        Ok(Self {
            data_file_schema: AvroSchema::parse(&data_file_schema_json)?,
            manifest_entry_schema: AvroSchema::parse(&manifest_entry_schema_json)?
        })
    }

    pub fn schema(&self) -> &AvroSchema {
        &self.manifest_entry_schema
    }

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

    /// Builds the Avro schema for the partition spec, and returns it as a JSON value.
    ///
    /// The Avro schema is dynamically determined according to the fields of the
    /// partition spec and the schema of the table.
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
                        Self::partition_field_avro_schema(field)?
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
    /// The Avro schema is dynamically constructed according to the fields of the
    /// partition spec and the schema of the table. It is used when encoding the partition
    /// spec as part of a manifest file.
    fn build_data_file_schema(
        partition_spec: &PartitionSpec
    ) -> IcebergResult<JsonValue> {
        let partition_type = partition_spec.as_struct_type();

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

    fn build_manifest_entry_schema(
        data_file_schema: &JsonValue
    ) -> JsonValue {
        json!({
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
                data_file_schema
            ]
        })
    }

    /// Serialize a HashMap into Avro as a list of structs of the form
    /// `{"key": k, "value": v}`.
    fn serialize_opt_map<K, V>(map: &Option<HashMap<K, V>>) -> AvroValue
        where
            K: Into<AvroValue> + Clone,
            V: Into<AvroValue> + Clone
    {
        if let Some(map) = map.as_ref() {
            AvroValue::Union(1, Box::new(AvroValue::Array(
                map.iter().map(|(k, v)| {
                    AvroValue::Record(vec![
                        ("key".to_string(), k.clone().into()),
                        ("value".to_string(), v.clone().into())
                    ])
                }).collect()
            )))
        } else {
            AvroValue::Union(0, Box::new(AvroValue::Null))
        }
    }

    fn serialize_opt_list<E>(list: &Option<Vec<E>>) -> AvroValue
        where
            E: Into<AvroValue> + Clone
    {
        if let Some(list) = list.as_ref() {
            AvroValue::Union(1, Box::new(AvroValue::Array(
                list.iter().map(|e| e.clone().into()).collect()
            )))
        } else {
            AvroValue::Union(0, Box::new(AvroValue::Null))
        }
    }

    /// Serializes an optional value. An optional value is encoded as a union
    /// of null type and the real type. This assumes that the Avro schema has
    /// a type of the form: `[ "null", inner_type ]`, i.e. the null must come first.
    fn serialize_opt<V>(value: &Option<V>) -> AvroValue
    where
        V: Into<AvroValue> + Clone
    {
        if let Some(value) = value {
            AvroValue::Union(1, Box::new(value.clone().into()))
        } else {
            AvroValue::Union(0, Box::new(AvroValue::Null))
        }
    }

    /// Manually serializes a DataFile struct with Avro. We can't use serde with Avro
    /// for this struct since its schema is dynamic.
    fn serialize_data_file(&self, data_file: &DataFile) -> IcebergResult<AvroRecord> {
        // This will fail only if data_file_schema is not of "record" type.
        let mut record = AvroRecord::new(&self.data_file_schema).unwrap();

        record.put("content", match data_file.content {
            DataFileContent::Data => 0,
            DataFileContent::PositionDelete => 1,
            DataFileContent::EqualityDelete => 2,
        });
        record.put("file_path", data_file.file_path.as_str());
        record.put("file_format", match data_file.file_format {
            DataFileFormat::Avro => "AVRO",
            DataFileFormat::ORC => "ORC",
            DataFileFormat::Parquet => "PARQUET"
        });
        record.put("partition", AvroValue::Record(
            data_file.partition.iter().map(|(k, v)| -> Result<_, AvroError> {
                Ok((
                    k.clone(),
                    apache_avro::to_value(v)?
                ))
            }).collect::<Result<Vec<_>, AvroError>>()?
        ));
        record.put("record_count", data_file.record_count);
        record.put("file_size_in_bytes", data_file.file_size_in_bytes);
        record.put(
            "column_sizes",
            Self::serialize_opt_map(&data_file.column_sizes)
        );
        record.put(
            "value_counts",
            Self::serialize_opt_map(&data_file.value_counts)
        );
        record.put(
            "null_value_counts",
            Self::serialize_opt_map(&data_file.null_value_counts)
        );
        record.put(
            "nan_value_counts",
            Self::serialize_opt_map(&data_file.nan_value_counts)
        );
        record.put(
            "distinct_counts",
            Self::serialize_opt_map(&data_file.distinct_counts)
        );
        record.put(
            "lower_bounds",
            Self::serialize_opt_map(&data_file.lower_bounds)
        );
        record.put(
            "upper_bounds",
            Self::serialize_opt_map(&data_file.upper_bounds)
        );
        record.put(
            "key_metadata",
            Self::serialize_opt(&data_file.key_metadata)
        );
        record.put(
            "split_offsets",
            Self::serialize_opt_list(&data_file.split_offsets)
        );
        record.put(
            "equality_ids",
            Self::serialize_opt_list(&data_file.equality_ids)
        );
        record.put(
            "sort_order_id",
            Self::serialize_opt(&data_file.sort_order_id)
        );

        Ok(record)
    }

    fn serialize(&self, manifest_entry: &ManifestEntry) -> IcebergResult<AvroRecord> {
        // This will fail only if data_file_schema is not of "record" type.
        let mut record = AvroRecord::new(&self.manifest_entry_schema).unwrap();

        record.put("status", match manifest_entry.status {
            ManifestEntryStatus::Existing => 0,
            ManifestEntryStatus::Added => 1,
            ManifestEntryStatus::Deleted => 2,
        });
        record.put(
            "snapshot_id",
            Self::serialize_opt(&manifest_entry.snapshot_id)
        );
        record.put(
            "sequence_number",
            Self::serialize_opt(&manifest_entry.sequence_number)
        );
        record.put(
            "file_sequence_number",
            Self::serialize_opt(&manifest_entry.file_sequence_number)
        );

        record.put("data_file", self.serialize_data_file(&manifest_entry.data_file)?);

        Ok(record)
    }
}

/// Serializes the manifest into Avro format.
pub(super) fn serialize_manifest_to_avro(manifest: &Manifest) -> IcebergResult<Vec<u8>> {
    let serializer = ManifestEntrySerializer::try_new(manifest.partition_spec())?;

    let mut writer = AvroWriter::new(serializer.schema(), Vec::new());

    // The Avro file's key-value metadata contains the schema of the table
    // and the partition spec for the files in the manifest.
    let mut metadata = HashMap::<String, String>::new();
    metadata.insert("schema".to_string(), manifest.schema().encode()?);
    metadata.insert("schema-id".to_string(), manifest.schema_id().to_string());
    metadata.insert(
        "partition-spec".to_string(),
        serde_json::to_string(&manifest.partition_spec().fields())
            .map_err(|e| IcebergError::SerializeJson { source: e })?
    );
    metadata.insert(
        "partition-spec-id".to_string(),
        manifest.partition_spec().spec_id().to_string()
    );
    metadata.insert("format-version".to_owned(), manifest.format_version().to_string());
    metadata.insert("content".to_owned(), manifest.content_type().to_string());
    for (key, value) in metadata {
        writer.add_user_metadata(key, value)?;
    }

    for entry in manifest.entries() {
        let s = serializer.serialize(entry)?;
        writer.append(s)?;
    }

    Ok(writer.into_inner()?)
}
