//! Tests for manifest files serialization and deserialization.
use chrono::NaiveDate;

use icelake::IcebergTableVersion;
use icelake::value::Value;
use icelake::schema::{Schema, Field, PrimitiveType};
use icelake::partition::{
    PartitionSpec, PartitionField, PartitionTransform, PartitionValues
};
use icelake::manifest::{
    Manifest, ManifestContentType,
    ManifestReader, ManifestWriter,
    ManifestEntry, ManifestEntryStatus,
    DataFile, DataFileContent, DataFileFormat
};

const LINEITEM_TABLE_PATH: &str = "./tests/data/lineitem";

fn lineitem_schema() -> Schema {
    Schema::new(0, vec![
        Field::new_primitive(1, "orderkey", false, PrimitiveType::Long),
        Field::new_primitive(2, "partkey", false, PrimitiveType::Long),
        Field::new_primitive(3, "suppkey", false, PrimitiveType::Long),
        Field::new_primitive(4, "linenumber", false, PrimitiveType::Int),
        Field::new_primitive(5, "quantity", false, PrimitiveType::Double),
        Field::new_primitive(6, "extendedprice", false, PrimitiveType::Double),
        Field::new_primitive(7, "discount", false, PrimitiveType::Double),
        Field::new_primitive(8, "tax", false, PrimitiveType::Double),
        Field::new_primitive(9, "returnflag", false, PrimitiveType::String),
        Field::new_primitive(10, "linestatus", false, PrimitiveType::String),
        Field::new_primitive(11, "shipdate", false, PrimitiveType::Date),
        Field::new_primitive(12, "commitdate", false, PrimitiveType::Date),
        Field::new_primitive(13, "receiptdate", false, PrimitiveType::Date),
        Field::new_primitive(14, "shipinstruct", false, PrimitiveType::String),
        Field::new_primitive(15, "shipmode", false, PrimitiveType::String),
        Field::new_primitive(16, "comment", false, PrimitiveType::String),
    ])
}

#[test]
fn serialize_manifest() {
    let schema = lineitem_schema();

    let partition_spec = PartitionSpec::try_new(
        0,
        vec![
            PartitionField::new(
                3, 1000, "suppkey", PartitionTransform::Identity,
            ),
            PartitionField::new(
                11, 1001, "shipdate", PartitionTransform::Identity,
            )
        ],
        schema.clone()
    ).unwrap();

    let mut manifest = Manifest::new(
        schema,
        partition_spec,
        ManifestContentType::Data
    );

    let data_file = DataFile::builder(
        DataFileContent::Data,
        "/tmp/data_file",
        DataFileFormat::Parquet,
        1000,
        10000
    ).with_partition_values(PartitionValues::from_iter([
        (
            "suppkey".to_string(),
            Some(Value::Int(555))
        ),
        (
            "shipdate".to_string(),
            Some(Value::Date(NaiveDate::from_ymd_opt(2023, 1, 1).unwrap()))
        )
    ])).build();

    let snapshot_id = 3988626671889928484;

    manifest.add_manifest_entry(ManifestEntry::new(
        ManifestEntryStatus::Added,
        snapshot_id,
        data_file
    ));

    let (bytes, _manifest_file) = ManifestWriter::new(1, snapshot_id)
        .write("/tmp/manifest", &manifest)
        .unwrap();

    // Now see that we can read it back.
    ManifestReader::new().read(&bytes).unwrap();
}

/// Deserialize a manifest file created by a different engine (Athena)
#[test]
fn deserialize_manifest() {
    let bytes = std::fs::read(
        std::path::Path::new(LINEITEM_TABLE_PATH).join(
            "metadata/7733a113-2437-4a4a-b284-8639c7919024-m0.avro"
        )
    ).unwrap();

    let reader = ManifestReader::new();

    let manifest = reader.read(&bytes).unwrap();

    assert_eq!(manifest.schema(), &lineitem_schema());
    assert_eq!(manifest.schema_id(), 0);
    assert_eq!(manifest.partition_spec().spec_id(), 0);
    assert_eq!(
        manifest.partition_spec().fields(),
        &vec![
            PartitionField::new(11, 1000, "shipdate", PartitionTransform::Identity)
        ]
    );
    assert_eq!(manifest.format_version(), IcebergTableVersion::V2);
    assert_eq!(manifest.content_type(), ManifestContentType::Data);

    for entry in manifest.entries() {
        assert_eq!(entry.status(), ManifestEntryStatus::Added);
        assert_eq!(entry.snapshot_id(), Some(3988626671889928484));
        assert_eq!(entry.sequence_number(), None);
        assert_eq!(entry.file_sequence_number(), None);
    }

    let data_file = manifest.entries()[0].data_file();

    assert_eq!(data_file.content, DataFileContent::Data);
    assert_eq!(
        &data_file.file_path,
        "s3://breezelabs-temporary-data/iceberg/lineitem/\
        data/62284ba0/shipdate=1992-03-03/\
        20230605_120826_00064_h4uvw-6c277677-95b1-43fc-bb31-5eb3c2eed4bb.parquet"
    );
    assert_eq!(data_file.file_format, DataFileFormat::Parquet);
    assert_eq!(data_file.record_count, 4300);
    assert_eq!(data_file.file_size_in_bytes, 117662);

    for i in 1..=16 {
        assert!(data_file.column_sizes.as_ref().unwrap().get(&i).is_some());
        assert!(data_file.value_counts.as_ref().unwrap().get(&i).is_some());
        assert!(data_file.null_value_counts.as_ref().unwrap().get(&i).is_some());
        assert!(data_file.lower_bounds.as_ref().unwrap().get(&i).is_some());
        assert!(data_file.upper_bounds.as_ref().unwrap().get(&i).is_some());
    }

    assert_eq!(*data_file.column_sizes.as_ref().unwrap().get(&1).unwrap(), 9163);
    assert_eq!(*data_file.value_counts.as_ref().unwrap().get(&1).unwrap(), 4300);
    assert_eq!(*data_file.null_value_counts.as_ref().unwrap().get(&1).unwrap(), 0);
    assert_eq!(
        *data_file.lower_bounds.as_ref().unwrap().get(&1).unwrap(),
        vec![0x47, 0x8c, 0x2c, 0, 0, 0, 0, 0]
    );
    assert_eq!(
        *data_file.upper_bounds.as_ref().unwrap().get(&1).unwrap(),
        vec![0x64, 0x5d, 0x18, 0x23, 0, 0, 0, 0]
    );
    assert!(data_file.key_metadata.is_none());
    assert!(data_file.split_offsets.is_none());
    assert!(data_file.equality_ids.is_none());
    assert_eq!(data_file.sort_order_id, Some(0));

    assert_eq!(data_file.partition.values()[0].0, "shipdate");
    let Some(Value::Date(date)) = data_file.partition.values()[0].1 else {
        panic!()
    };
    assert_eq!(date, NaiveDate::from_ymd_opt(1992, 3, 3).unwrap());
}
