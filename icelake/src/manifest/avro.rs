//! Serialization and deserialization of manifests to Avro format.
use std::collections::HashMap;

use apache_avro::{
    self,
    Reader as AvroReader,
    Writer as AvroWriter,
    schema::Schema as AvroSchema,
    types::Record as AvroRecord,
    types::Value as AvroValue
};
use serde::Deserialize;
use serde_json::{json, value::Value as JsonValue};
use chrono::{
    NaiveDate, NaiveDateTime, NaiveTime, DateTime, Utc,
    Days, Duration
};

use crate::{IcebergResult, IcebergError, IcebergTableVersion};
use crate::schema::{Schema, Field, SchemaType, PrimitiveType, StructType};
use crate::value::Value;
use crate::partition::{PartitionSpec, PartitionField, PartitionValues};
use super::manifest::{
    Manifest, ManifestEntry, ManifestEntryStatus, ManifestContentType
};
use super::datafile::{DataFile, DataFileContent, DataFileFormat};

impl TryFrom<Value> for AvroValue {
    type Error = IcebergError;

    fn try_from(value: Value) -> IcebergResult<Self> {
        match value {
            Value::Boolean(b) => Ok(AvroValue::Boolean(b)),
            Value::Int(i) => Ok(AvroValue::Int(i)),
            Value::Long(l) => Ok(AvroValue::Long(l)),
            Value::Float(f) => Ok(AvroValue::Float(f)),
            Value::Double(d) => Ok(AvroValue::Double(d)),
            Value::Date(date) => {
                Ok(AvroValue::Date(
                    date.signed_duration_since(NaiveDate::default()).num_days()
                        .try_into()
                        .map_err(|_| IcebergError::ValueError(format!(
                            "can't serialize date value to avro: \
                            date {date} is too far from 1970-01-01"
                        )))?
                ))
            },
            Value::Time(time) => {
                Ok(AvroValue::TimeMicros(
                    time.signed_duration_since(NaiveTime::default())
                        .num_microseconds()
                        .ok_or_else(|| IcebergError::ValueError(format!(
                            "can't serialize time value to avro: \
                            time {time} is too far from 1970-01-01"
                        )))?
                ))
            },
            Value::Timestamp(ts) => {
                Ok(AvroValue::TimestampMicros(
                    ts.signed_duration_since(NaiveDateTime::default())
                        .num_microseconds()
                        .ok_or_else(|| IcebergError::ValueError(format!(
                            "can't serialize timestamp value to avro: \
                            timestamp {ts} is too far from 1970-01-01"
                        )))?
                ))
            },
            Value::Timestamptz(ts) => {
                Ok(AvroValue::TimestampMicros(
                    ts.signed_duration_since(DateTime::<Utc>::default())
                        .num_microseconds()
                        .ok_or_else(|| IcebergError::ValueError(format!(
                            "can't serialize timestamptz value to avro: \
                            timestamp {ts} is too far from 1970-01-01"
                        )))?
                ))
            },
            Value::String(s) => Ok(AvroValue::String(s)),
            Value::Uuid(u) => Ok(AvroValue::Uuid(u)),
            Value::Fixed(bytes) => Ok(AvroValue::Fixed(bytes.len(), bytes)),
            Value::Binary(bytes) => Ok(AvroValue::Bytes(bytes)),
            _ => {
                Err(IcebergError::Unsupported(
                    "can't serialize complex values to avro yet".to_string()
                ))
            }
        }
    }
}

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
    fn partition_field_avro_schema(field: &Field) -> IcebergResult<JsonValue> {
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
                    "default": JsonValue::Null,
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

    /// Serializes an Iceberg Value (that might be null) to an Avro value.
    fn serialize_opt_value(value: Option<Value>) -> IcebergResult<AvroValue> {
        Ok(if let Some(value) = value {
            AvroValue::Union(1, Box::new(value.try_into()?))
        } else {
            AvroValue::Union(0, Box::new(AvroValue::Null))
        })
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
            data_file.partition.values()
                .iter()
                .map(|(k, v)| -> IcebergResult<_> {
                    Ok((
                        k.clone(),
                        Self::serialize_opt_value(v.clone())?
                    ))
                })
                .collect::<IcebergResult<Vec<_>>>()?
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

/// Serializes a manifest into binary Avro format.
pub(super) fn serialize_manifest(manifest: &Manifest) -> IcebergResult<Vec<u8>> {
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
    metadata.insert(
        "format-version".to_string(),
        manifest.format_version().to_string()
    );
    metadata.insert(
        "content".to_string(),
        manifest.content_type().to_string()
    );
    for (key, value) in metadata {
        writer.add_user_metadata(key, value)?;
    }

    for entry in manifest.entries() {
        let s = serializer.serialize(entry)?;
        writer.append(s)?;
    }

    Ok(writer.into_inner()?)
}

fn parse_metadata_item<T, R>(reader: &AvroReader<R>, key: &str) -> IcebergResult<T>
where
    T: std::str::FromStr,
    R: std::io::Read,
{
    reader.user_metadata().get(key)
        .ok_or_else(|| {
            IcebergError::ManifestError(
                format!("manifest's metadata has no key '{}'", key)
            )
        })
        .and_then(|bytes| {
            String::from_utf8(bytes.clone())
                .map_err(|_| {
                    IcebergError::ManifestError(format!(
                        "manifest's metadata does not contain a valid utf-8 string \
                        for key '{}'", key
                    ))
                })
                .and_then(|s| {
                    s.parse::<T>().map_err(|_| {
                        IcebergError::ManifestError(format!(
                            "maniefst's metadata does not contain a valid value \
                            for key '{}", key
                        ))
                    })
                })
        })
}

fn deserialize_manifest_metadata<R>(
    reader: &AvroReader<R>
) ->  IcebergResult<Manifest>
where
    R: std::io::Read
{
    let schema = reader.user_metadata().get("schema")
        .ok_or_else(||
            IcebergError::ManifestError(
                "manifest's metadata has no key 'schema'".to_string()
            )
        )
        .and_then(|bytes| Schema::decode(bytes).map_err(|e|
            IcebergError::ManifestError(
                format!("invalid schema in manifest's metadata: {e}")
            )
        ))?;

    let partition_fields = reader.user_metadata().get("partition-spec")
        .ok_or_else(||
            IcebergError::ManifestError(
                "manifest's metadata has no key 'schema'".to_string()
            )
        )
        .and_then(|bytes|
            serde_json::from_slice::<Vec<PartitionField>>(bytes)
                .map_err(|e| {
                    IcebergError::ManifestError(format!(
                        "manifest's metadata does not contain valid partition \
                        fields: {e}"
                    ))
                })
        )?;

    let partition_spec_id: i32 = parse_metadata_item(&reader, "partition-spec-id")?;
    let format_version: IcebergTableVersion = parse_metadata_item(
        &reader, "format-version"
    )?;

    let content_type: ManifestContentType = parse_metadata_item(
        &reader, "content"
    )?;

    if format_version != IcebergTableVersion::V2 {
        Err(IcebergError::Unsupported(
            format!("table version {format_version} not supported")
        ))
    } else {
        Ok(Manifest::new(
            schema.clone(),
            PartitionSpec::try_new(
                partition_spec_id,
                partition_fields,
                schema
            )?,
            content_type
        ))
    }

}

fn deserialize_key<'de, T>(
    record: &'de Vec<(String, AvroValue)>,
    key: &str
) -> IcebergResult<T>
where
    T: Deserialize<'de>
{
    let index = record.iter().position(|(k, _v)| k == key)
        .ok_or_else(|| IcebergError::ManifestError(format!("missing key '{key}'")))?;

    let (_k, v) = record.get(index).unwrap();

    Ok(apache_avro::from_value::<T>(&v)?)
}

fn deserialize_opt_key<'de, T>(
    record: &'de Vec<(String, AvroValue)>,
    key: &str
) -> IcebergResult<Option<T>>
where
    T: Deserialize<'de>
{
    match record.iter().position(|(k, _v)| k == key) {
        Some(index) => {
            let (_k, v) = record.get(index).unwrap();
            Ok(apache_avro::from_value::<Option<T>>(&v)?)
        },
        None => Ok(None)
    }
}

fn deserialize_opt_map<'de, K, V>(
    record: &'de Vec<(String, AvroValue)>,
    key: &str
) -> IcebergResult<Option<HashMap<K, V>>>
where
    K: Deserialize<'de> + std::hash::Hash + std::cmp::Eq,
    V: Deserialize<'de>
{
    #[derive(Deserialize)]
    struct Entry<K, V> {
        key: K,
        value: V,
    }

    Ok(match record.iter().position(|(k, _v)| k == key) {
        Some(index) => {
            let (_k, v) = record.get(index).unwrap();

            let entries = apache_avro::from_value::<Option<Vec<Entry<K, V>>>>(&v)?;

            // convert vector of Entry<K,V> into HashMap<K,V>
            entries.map(|vec| {
                vec.into_iter()
                    .map(|entry| (entry.key, entry.value))
                    .collect()
            })
        },
        None => None
    })
}

fn deserialize_binary_entry<'de, K>(
    record: &'de Vec<(String, AvroValue)>
) -> IcebergResult<(K, Vec<u8>)>
where
    K: Deserialize<'de> + std::hash::Hash + std::cmp::Eq,
{
    let key = record.iter().find(|(k, _v)| k == "key")
        .ok_or_else(|| IcebergError::ManifestError(
            "invalid map entry: missing key".to_string()
        ))
        .and_then(|(_k, v)| {
            apache_avro::from_value::<K>(v)
                .map_err(|e| IcebergError::AvroError { source: e })
        })?;

    let value = record.iter().find(|(k, _v)| k == "value")
        .ok_or_else(|| IcebergError::ManifestError(
            "invalid map entry: missing value".to_string()
        ))
        .and_then(|(_k, v)| {
            match v {
                AvroValue::Bytes(bytes) => Ok(bytes),
                _ => {
                    Err(IcebergError::ManifestError(format!(
                        "invalid map entry: expected byte array, found: {v:?}"
                    )))
                }
            }
        })?;

    Ok((key, value.clone()))
}

/// Deserialized an Avro map of keys to binary values.
/// Couldn't make it work using Vec<u8>, so deserialize everything manually.
fn deserialize_opt_binary_map<'de, K>(
    record: &'de Vec<(String, AvroValue)>,
    key: &str
) -> IcebergResult<Option<HashMap<K, Vec<u8>>>>
where
    K: Deserialize<'de> + std::hash::Hash + std::cmp::Eq,
{
    Ok(record.iter().find(|(k, _v)| k == key)
        .map(|(_k, v)| {
            match v {
                AvroValue::Null => Ok(None),
                AvroValue::Union(_i, u) => {
                    match &**u {
                        AvroValue::Null => Ok(None),
                        AvroValue::Array(array) => {
                            Ok(Some(array.iter().map(|entry| {
                                match entry {
                                    AvroValue::Record(record) => {
                                        deserialize_binary_entry::<'de>(record)
                                    },
                                    _ => {
                                        Err(IcebergError::ManifestError(
                                            format!("expected record, got {entry:?}")
                                        ))
                                    }
                                }
                            }).collect::<IcebergResult<Vec<_>>>()?))

                        },
                        _ => {
                            Err(IcebergError::ManifestError(
                                format!("expected array, got {u:?}")
                            ))
                        }
                    }
                },
                _ => {
                    Err(IcebergError::ManifestError(
                        format!("expected union, got {v:?}")
                    ))
                }
            }
        })
        .transpose()?
        .flatten()
        .map(|vec| HashMap::from_iter(vec)))
}

fn deserialize_value(value: AvroValue) -> IcebergResult<Option<Value>> {
    match value {
        AvroValue::Null => Ok(None),
        AvroValue::Boolean(b) => Ok(Some(Value::Boolean(b))),
        AvroValue::Int(i) => Ok(Some(Value::Int(i))),
        AvroValue::Long(l) => Ok(Some(Value::Long(l))),
        AvroValue::Float(f) => Ok(Some(Value::Float(f))),
        AvroValue::Double(d) => Ok(Some(Value::Double(d))),
        AvroValue::Bytes(bytes) => Ok(Some(Value::Binary(bytes))),
        AvroValue::String(s) => Ok(Some(Value::String(s))),
        AvroValue::Fixed(_size, bytes) => Ok(Some(Value::Fixed(bytes))),
        AvroValue::Array(values) => {
            Ok(Some(Value::List(
                values
                    .into_iter()
                    .map(|v| deserialize_value(v))
                    .collect::<IcebergResult<Vec<_>>>()?
            )))
        },
        AvroValue::Date(days) => {
            // 'days' is the number of days since 1970-01-01
            let date = NaiveDate::default().checked_add_days(
                Days::new(days.try_into().map_err(|_| {
                    IcebergError::ValueError(format!(
                        "can't create a date value from number of days {days}",
                    ))
                })?)
            ).ok_or_else(||
                IcebergError::ValueError(format!(
                    "can't create a date value from number of days {days}"
                ))
            )?;

            Ok(Some(Value::Date(date)))
        },
        AvroValue::TimeMillis(millis) => {
            let (time, _) = NaiveTime::default().overflowing_add_signed(
                Duration::milliseconds(millis.into())
            );

            Ok(Some(Value::Time(time)))
        },
        AvroValue::TimeMicros(micros) => {
            let (time, _) = NaiveTime::default().overflowing_add_signed(
                Duration::microseconds(micros)
            );

            Ok(Some(Value::Time(time)))
        },
        AvroValue::TimestampMillis(millis) => {
            let ts = NaiveDateTime::from_timestamp_millis(millis)
                .ok_or_else(||
                    IcebergError::ValueError(
                        "avro timestamp value is out of range".to_string()
                    )
                )?;

            Ok(Some(Value::Timestamp(ts)))
        },
        AvroValue::TimestampMicros(micros) => {
            let ts = NaiveDateTime::from_timestamp_micros(micros)
                .ok_or_else(||
                    IcebergError::ValueError(
                        "avro timestamp value is out of range".to_string()
                    )
                )?;

            Ok(Some(Value::Timestamp(ts)))
        },
        AvroValue::Uuid(uuid) => Ok(Some(Value::Uuid(uuid))),
        _ => {
            Err(IcebergError::Unsupported(format!(
                "can't deserialize avro value {value:?} yet"
            )))
        }
    }
}

fn deserialize_opt_value(value: AvroValue) -> IcebergResult<Option<Value>> {
    match value {
        AvroValue::Null => Ok(None),
        AvroValue::Union(_i, u) => {
            deserialize_value(*u)
        },
        _ => Err(IcebergError::ManifestError(
            format!("expected union or null, got {value:?}")
        ))
    }
}

fn deserialize_partition<'de>(
    record: &'de Vec<(String, AvroValue)>,
    key: &str
) -> IcebergResult<PartitionValues> {
    let index = record.iter().position(|(k, _v)| k == key)
        .ok_or_else(|| IcebergError::ManifestError(format!("missing key '{key}'")))?;

    let (_k, v) = record.get(index).unwrap();

    let partition_record = match v {
        AvroValue::Record(record) => Ok(record),
        _ => Err(IcebergError::ManifestError(format!("expected record, got {v:?}")))
    }?;

    Ok(PartitionValues::from_iter(
        partition_record.into_iter().map(|(k, v)| {
            Ok((
                k.clone(),
                deserialize_opt_value(v.clone())?
            ))
        }).collect::<IcebergResult<Vec<_>>>()?
    ))
}

fn deserialize_data_file(value: AvroValue) -> IcebergResult<DataFile> {
    let record = match value {
        AvroValue::Record(record) => Ok(record),
        _ => Err(IcebergError::ManifestError(
            format!("expected record, got {value:?}")
        ))
    }?;

    Ok(DataFile {
        content: deserialize_key(&record, "content")?,
        file_path: deserialize_key(&record, "file_path")?,
        file_format: deserialize_key::<String>(&record, "file_format")?.parse()?,
        partition: deserialize_partition(&record, "partition")?,
        record_count: deserialize_key(&record, "record_count")?,
        file_size_in_bytes: deserialize_key(&record, "file_size_in_bytes")?,
        column_sizes: deserialize_opt_map(&record, "column_sizes")?,
        value_counts: deserialize_opt_map(&record, "value_counts")?,
        null_value_counts: deserialize_opt_map(&record, "null_value_counts")?,
        nan_value_counts: deserialize_opt_map(&record, "nan_value_counts")?,
        distinct_counts: deserialize_opt_map(&record, "distinct_counts")?,
        lower_bounds: deserialize_opt_binary_map(&record, "lower_bounds")?,
        upper_bounds: deserialize_opt_binary_map(&record, "upper_bounds")?,
        key_metadata: deserialize_opt_key(&record, "key_metadata")?,
        split_offsets: deserialize_opt_key(&record, "split_offsets")?,
        equality_ids: deserialize_opt_key(&record, "equality_ids")?,
        sort_order_id: deserialize_opt_key(&record, "sort_order_id")?,
    })
}

fn deserialize_manifest_entry(value: AvroValue) -> IcebergResult<ManifestEntry> {
    let record = match value {
        AvroValue::Record(record) => Ok(record),
        _ => Err(IcebergError::ManifestError(
            format!("expected record, got {value:?}")
        ))
    }?;

    let status = deserialize_key(&record, "status")?;
    let snapshot_id = deserialize_opt_key(&record, "snapshot_id")?;
    let sequence_number = deserialize_opt_key(&record, "sequence_number")?;
    let file_sequence_number = deserialize_opt_key(&record, "file_sequence_number")?;

    Ok(ManifestEntry {
        status: status,
        snapshot_id: snapshot_id,
        sequence_number: sequence_number,
        file_sequence_number: file_sequence_number,
        data_file: deserialize_data_file(
            record.into_iter().find(|(k, _v)| k == "data_file")
                .ok_or_else(|| IcebergError::ManifestError(
                    format!("missing key 'data_File'")
                ))
                .map(|(_k, v)| v)?
        )?
    })
}

pub(super) fn deserialize_manifest(input: &[u8]) -> IcebergResult<Manifest> {
    let reader = AvroReader::<&[u8]>::new(input)?;

    let mut manifest = deserialize_manifest_metadata(&reader)?;
    manifest.add_manifest_entries(
        reader.into_iter()
            .map(|value| {
                value.map_err(|e| IcebergError::AvroError { source: e })
                    .and_then(|v| deserialize_manifest_entry(v))
            })
            .collect::<IcebergResult<Vec<ManifestEntry>>>()?
    );

    Ok(manifest)
}
