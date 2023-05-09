// Parts of this module were taken from
// https://github.com/oliverdaff/iceberg-rs/
use regex::Regex;
use serde::{
    de::{self, IntoDeserializer},
    Deserialize, Deserializer, Serialize,
};

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone)]
#[serde(rename_all = "lowercase", remote = "Self")]
/// A Transformation applied to each source column to produce a value.
pub enum Transform {
    /// Always produces `null`
    Void,
    /// Source value, unmodified
    Identity,
    /// Extract a date or timestamp year as years from 1970
    Year,
    /// Extract a date or timestamp month as months from 1970-01-01
    Month,
    /// Extract a date or timestamp day as days from 1970-01-01
    Day,
    /// Extract a date or timestamp hour as hours from 1970-01-01 00:00:00
    Hour,
    /// Hash of value, mod N
    Bucket(u32),
    /// Value truncated to width
    Truncate(u32),
}

impl<'de> Deserialize<'de> for Transform {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        if s.starts_with("bucket") {
            deserialize_bucket(s.into_deserializer())
        } else if s.starts_with("truncate") {
            deserialize_truncate(s.into_deserializer())
        } else {
            Transform::deserialize(s.into_deserializer())
        }
    }
}

impl Serialize for Transform {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use Transform::*;
        match self {
            Bucket(mod_n) => serializer.serialize_str(&format!("bucket[{mod_n}]")),
            Truncate(width) => serializer.serialize_str(&format!("truncate[{width}]")),
            _ => Transform::serialize(self, serializer),
        }
    }
}

fn deserialize_bucket<'de, D>(deserializer: D) -> Result<Transform, D::Error>
where
    D: Deserializer<'de>,
{
    let this = String::deserialize(deserializer)?;
    let re: Regex = Regex::new(r#"^bucket\[(?P<n>\d+)\]$"#).unwrap();
    let err_msg = format!("Invalid bucket format {}", this);

    let caps = re
        .captures(&this)
        .ok_or_else(|| de::Error::custom(&err_msg))?;
    let bucket: u32 = caps
        .name("n")
        .ok_or_else(|| de::Error::custom(&err_msg))
        .and_then(|p| {
            p.as_str()
                .parse()
                .map_err(|_| de::Error::custom("bucket not u32"))
        })?;
    Ok(Transform::Bucket(bucket))
}

fn deserialize_truncate<'de, D>(deserializer: D) -> Result<Transform, D::Error>
where
    D: Deserializer<'de>,
{
    let this = String::deserialize(deserializer)?;
    let re: Regex = Regex::new(r#"^truncate\[(?P<w>\d+)\]$"#).unwrap();
    let err_msg = format!("Invalid truncate format {}", this);

    let caps = re
        .captures(&this)
        .ok_or_else(|| de::Error::custom(&err_msg))?;
    let width: u32 = caps
        .name("w")
        .ok_or_else(|| de::Error::custom(&err_msg))
        .and_then(|p| {
            p.as_str()
                .parse()
                .map_err(|_| de::Error::custom("bucket not u32"))
        })?;
    Ok(Transform::Truncate(width))
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone)]
#[serde(rename_all = "kebab-case")]
/// Tables are configured with a partition spec that defines how to produce a tuple of partition values from a record.
pub struct PartitionField {
    /// A source column id from the table’s schema
    pub source_id: i32,
    /// A partition field id that is used to identify a partition field and is unique within a partition spec.
    /// In v2 table metadata, it is unique across all partition specs.
    pub field_id: i32,
    /// A partition name.
    pub name: String,
    /// A transform that is applied to the source column to produce a partition value.
    pub transform: Transform,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone)]
#[serde(rename_all = "kebab-case")]
/// A definition of how partition values are derived from data fields.
pub struct PartitionSpec {
    /// Identifier for the specification
    pub spec_id: i32,
    /// Fields for the specification
    pub fields: Vec<PartitionField>,
}

impl PartitionSpec {
    /// Create a new, empty partition spec.
    pub fn new() -> Self { Self { spec_id: 0, fields: Vec::new() } }
}
