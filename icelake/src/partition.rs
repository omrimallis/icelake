//! Interface to Iceberg table partitions.
//!
//! An Iceberg table is partitioned according to a [`PartitionSpec`].
//! The spec is composed of multiple [`PartitionField`]s which specify how to transform
//! source values in the table to the partition's value.
//! `PartitionSpec` is tightly coupled to a [`Schema`] since the partition fields reference
//! specific schema fields by their id.
//!
//! For each row in the table, the spec can be applied to the source values to produce
//! [`PartitionValues`]. Each [`PartitionValues`] can be encoded to a path which is
//! used as the object store path to store this partition's data files.
//!
//! # Examples
//!
//! Create a [`PartitionSpec`] by a `product_id` field and year derived from a `timestamp`
//! field.
//!
//! ```rust
//!
//! use icelake::schema::{Schema, Field, SchemaType, PrimitiveType};
//! use icelake::partition::{PartitionSpec, PartitionField, PartitionTransform};
//!
//! let schema_id = 0;
//! let schema = Schema::new(schema_id, vec![
//!     Field::new(
//!         0,
//!         "id",
//!         false,
//!         SchemaType::Primitive(PrimitiveType::Int)
//!     ),
//!     Field::new(
//!         1,
//!         "product_id",
//!         false,
//!         SchemaType::Primitive(PrimitiveType::Int)
//!     ),
//!     Field::new(
//!         2,
//!         "timestamp",
//!         false,
//!         SchemaType::Primitive(PrimitiveType::Timestamp)
//!     )
//! ]);
//!
//! let spec = PartitionSpec::builder(0, schema.clone())
//!     .add_identity_field("product_id").unwrap()
//!     .add_year_field("timestamp").unwrap()
//!     .build();
//! ```
use std::collections::{HashMap, HashSet};

use regex::Regex;
use serde::{
    de::{self, IntoDeserializer},
    Deserialize, Deserializer, Serialize,
};
use chrono::{Datelike, NaiveDate, NaiveDateTime, DateTime, Utc};

use crate::{IcebergResult, IcebergError};
use crate::schema::{
    Schema, SchemaType, Field,
    StructType, PrimitiveType
};
use crate::value::Value;

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone)]
#[serde(rename_all = "lowercase", remote = "Self")]
/// A Transformation applied to a source column to produce a partition value.
pub enum PartitionTransform {
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

trait Transform {
    fn get_result_type(field_type: SchemaType) -> IcebergResult<SchemaType>;
    fn apply(value: &Value) -> IcebergResult<Value>;
}

struct IdentityTransform;
impl Transform for IdentityTransform {
    fn get_result_type(field_type: SchemaType) -> IcebergResult<SchemaType> {
        Ok(field_type)
    }

    fn apply(value: &Value) -> IcebergResult<Value> {
        Ok(value.clone())
    }
}

struct YearTransform;
impl Transform for YearTransform {
    fn get_result_type(field_type: SchemaType) -> IcebergResult<SchemaType> {
        match field_type {
            SchemaType::Primitive(PrimitiveType::Date)
            | SchemaType::Primitive(PrimitiveType::Timestamp)
            | SchemaType::Primitive(PrimitiveType::Timestamptz) => {
                Ok(SchemaType::Primitive(PrimitiveType::Int))
            },
            _ => {
                Err(IcebergError::PartitionError {
                    message: format!(
                        "can't apply year transform to field of type {}",
                        field_type
                    )
                })
            }
        }
    }

    fn apply(value: &Value) -> IcebergResult<Value> {
        match value {
            Value::Date(date) => {
                Ok(Value::Int(date.year() - 1970))
            },
            Value::Timestamp(timestamp) => {
                Ok(Value::Int(timestamp.year() - 1970))
            },
            Value::Timestamptz(timestamptz) => {
                Ok(Value::Int(timestamptz.year() - 1970))
            },
            _ => {
                Err(IcebergError::PartitionError {
                    message: format!(
                        "can't apply year transform to value {}",
                        value
                    )
                })
            }
        }
    }
}

struct DayTransform;
impl Transform for DayTransform {
    fn get_result_type(field_type: SchemaType) -> IcebergResult<SchemaType> {
        match field_type {
            SchemaType::Primitive(PrimitiveType::Date)
            | SchemaType::Primitive(PrimitiveType::Timestamp)
            | SchemaType::Primitive(PrimitiveType::Timestamptz) => {
                Ok(SchemaType::Primitive(PrimitiveType::Int))
            },
            _ => {
                Err(IcebergError::PartitionError {
                    message: format!(
                        "can't apply day transform to field of type {}",
                        field_type
                    )
                })
            }
        }
    }

    fn apply(value: &Value) -> IcebergResult<Value> {
        match value {
            Value::Date(date) => {
                Ok(Value::Int(
                    date.signed_duration_since(NaiveDate::default())
                        .num_days()
                        .try_into()
                        .map_err(|_| {
                            IcebergError::PartitionError {
                                message: format!(
                                    "date {date} is too far from 1970-01-01"
                                )
                            }
                        })?
                ))
            },
            Value::Timestamp(timestamp) => {
                Ok(Value::Int(
                    timestamp.signed_duration_since(NaiveDateTime::default())
                        .num_days()
                        .try_into()
                        .map_err(|_| {
                            IcebergError::PartitionError {
                                message: format!(
                                    "timestamp {timestamp} is too far from 1970-01-01"
                                )
                            }
                        })?
                ))
            },
            Value::Timestamptz(timestamptz) => {
                Ok(Value::Int(
                    timestamptz.signed_duration_since(DateTime::<Utc>::default())
                        .num_days()
                        .try_into()
                        .map_err(|_| {
                            IcebergError::PartitionError {
                                message: format!(
                                    "timestamp {timestamptz} is too far from 1970-01-01"
                                )
                            }
                        })?
                ))
            },
            _ => {
                Err(IcebergError::PartitionError {
                    message: format!(
                        "can't apply day transform to value {}",
                        value
                    )
                })
            }
        }
    }
}

impl PartitionTransform {
    /// Returns the field type resulting from applying this transform to the input
    /// type.
    ///
    /// # Errors
    ///
    /// This function will return [`IcebergError::PartitionError`] if this transform
    /// cannot be applied to the input type. For example, a `Year` transform can only
    /// be applied to date or timestamp types.
    pub fn get_result_type(&self, field_type: SchemaType) -> IcebergResult<SchemaType> {
        match self {
            PartitionTransform::Void => {
                Ok(field_type)
            },
            PartitionTransform::Identity => {
                IdentityTransform::get_result_type(field_type)
            },
            PartitionTransform::Year => {
                YearTransform::get_result_type(field_type)
            }
            PartitionTransform::Day => {
                DayTransform::get_result_type(field_type)
            },
            _ => {
                Err(IcebergError::PartitionError {
                    message: format!("transform {self:?} not yet supported")
                })
            }
        }
    }

    /// Applies this trasnform to the input value.
    ///
    /// An input of `None` represents a null value. All transforms will return `None`
    /// in this case, representing a null output.
    ///
    /// # Errors
    ///
    /// This function will return [`IcebergError::PartitionError`] if this transform
    /// cannot be applied to the input type. For example, a `Year` transform can only
    /// be applied to date or timestamp types.
    pub fn apply(&self, value: Option<Value>) -> IcebergResult<Option<Value>> {
        if let Some(value) = value {
            match self {
                PartitionTransform::Void => { Ok(None) },
                PartitionTransform::Identity => {
                    Ok(Some(IdentityTransform::apply(&value)?))
                },
                PartitionTransform::Year => {
                    Ok(Some(YearTransform::apply(&value)?))
                },
                PartitionTransform::Day => {
                    Ok(Some(DayTransform::apply(&value)?))
                },
                // TODO
                _ => {
                    Err(IcebergError::PartitionError {
                        message: format!("transform {self:?} not yet supported")
                    })
                }
            }
        } else {
            // All transforms must return null for a null input value.
            Ok(None)
        }
    }
}

impl<'de> Deserialize<'de> for PartitionTransform {
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
            PartitionTransform::deserialize(s.into_deserializer())
        }
    }
}

impl Serialize for PartitionTransform {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use PartitionTransform::*;
        match self {
            Bucket(mod_n) => serializer.serialize_str(&format!("bucket[{mod_n}]")),
            Truncate(width) => serializer.serialize_str(&format!("truncate[{width}]")),
            _ => PartitionTransform::serialize(self, serializer),
        }
    }
}

fn deserialize_bucket<'de, D>(deserializer: D) -> Result<PartitionTransform, D::Error>
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
    Ok(PartitionTransform::Bucket(bucket))
}

fn deserialize_truncate<'de, D>(deserializer: D) -> Result<PartitionTransform, D::Error>
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
    Ok(PartitionTransform::Truncate(width))
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone)]
#[serde(rename_all = "kebab-case")]
/// Specification of a single partition field within a `PartitionSpec`.
pub struct PartitionField {
    /// A source column id from the table’s schema.
    pub source_id: i32,
    /// A partition field id that is used to identify a partition field and is unique
    /// within a partition spec.
    pub field_id: i32,
    /// A partition name.
    pub name: String,
    /// A transform that is applied to the source column to produce a partition value.
    pub transform: PartitionTransform,
}

impl PartitionField {
    pub fn new(
        source_id: i32,
        field_id: i32,
        name: &str,
        transform: PartitionTransform
    ) -> Self {
        Self {
            source_id: source_id,
            field_id: field_id,
            name: name.to_string(),
            transform: transform
        }
    }
}

/// Partition spec struct that can be directly serialized or deserialized
/// but may contain integrity errors.
#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone)]
#[serde(rename_all = "kebab-case")]
pub(crate) struct PartitionSpecModel {
    /// Unique identifier for this partition spec within an Iceberg table.
    pub spec_id: i32,
    /// The partitioning fields.
    pub fields: Vec<PartitionField>,
}

/// Specification of table-level partitioning. Defines how partition values are derived
/// from the data fields of the table.
#[derive(Debug, Clone)]
pub struct PartitionSpec {
    model: PartitionSpecModel,
    /// Lookup for schema fields by their source id.
    field_by_id: HashMap<i32, Field>,
}

pub const UNPARTITIONED_LAST_ASSIGNED_FIELD_ID: i32 = 999;

impl PartitionSpec {
    /// Creates a new partition spec for the given schema.
    ///
    /// # Errors
    ///
    /// [`IcebergError::PartitionError`] is returned if the list of fields is invalid,
    /// or if its not applicable for the given schema, for example:
    /// * If there are duplicate partition field ids.
    /// * If one of the partition fields references a field that does not exist within
    ///   the schema as a primitive type.
    pub fn try_new(
        spec_id: i32,
        fields: Vec<PartitionField>,
        schema: Schema
    ) -> IcebergResult<Self> {
        let mut uniq_id: HashSet<i32> = HashSet::new();
        let mut uniq_name: HashSet<String> = HashSet::new();

        // Check correctness of all partition fields.
        fields.iter().map(|field| -> IcebergResult<()> {
            if !uniq_id.insert(field.field_id) {
                return Err(IcebergError::PartitionError {
                    message: "partition spec contains duplicate field ids".to_string()
                });
            }

            if field.name.is_empty() {
                return Err(IcebergError::PartitionError {
                    message: "partition field has empty name".to_string()
                });
            }

            if !uniq_name.insert(field.name.clone()) {
                return Err(IcebergError::PartitionError {
                    message: "partition spec contains duplicate field names".to_string()
                });
            }

            Ok(())
        }).collect::<IcebergResult<_>>()?;

        let field_by_id = Self::field_by_id(schema);

        // TODO: Ensure validity of partition names. See
        // checkAndAddPartitionName() in the Iceberg Java implementation.
        // TODO: Ensure no redundant partitions on the same source field.

        // Ensure validity of the fields for the given schema.
        for field in &fields {
            match field_by_id.get(&field.source_id) {
                Some(source_field) => {
                    // Will fail if the transform can't be applied to the source field.
                    field.transform.get_result_type(source_field.r#type.clone())?;
                },
                None => {
                    return Err(IcebergError::PartitionError {
                        message: format!(
                            "source field id {} not found in schema",
                            field.source_id
                        )
                    })
                }
            }
        }

        Ok(Self {
            model: PartitionSpecModel {
                spec_id: spec_id,
                fields: fields
            },
            field_by_id: field_by_id
        })
    }

    /// Creates a new builder to easily build a `PartitionSpec`.
    pub fn builder(spec_id: i32, schema: Schema) -> PartitionSpecBuilder {
        PartitionSpecBuilder::new(spec_id, schema)
    }

    pub fn spec_id(&self) -> i32 {
        self.model.spec_id
    }

    pub fn fields(&self) -> &Vec<PartitionField> {
        &self.model.fields
    }

    pub(crate) fn model(self) -> PartitionSpecModel {
        self.model
    }

    pub fn is_empty(&self) -> bool {
        self.fields().is_empty()
    }

    /// Builds a lookup map from the schema's primitive fields.
    fn field_by_id(schema: Schema) -> HashMap<i32, Field> {
        // Temporary queue of fields to be processed.
        let mut queue: Vec<Field> = schema.fields().into();
        // List of valid field ids.
        let mut lookup: HashMap<i32, Field> = HashMap::new();

        // Process recursively.
        while let Some(source_field) = queue.pop() {
            match source_field.r#type {
                SchemaType::Primitive(_) => {
                    lookup.insert(source_field.id, source_field);
                },
                SchemaType::Struct(struct_type) => {
                    // Queue the nested fields.
                    queue.extend(struct_type.fields.into_iter());
                },
                // Fields nested in lists and maps are not allowed.
                SchemaType::List(_) | SchemaType::Map(_) => {}
            };
        }

        lookup
    }

    /// Creates an empty PartitionSpec for unpartitioned tables.
    pub fn unpartitioned() -> Self {
        Self {
            model: PartitionSpecModel {
                spec_id: 0,
                fields: Vec::new()
            },
            field_by_id: HashMap::new()
        }
    }

    /// Obtains the highest assigned `field_id` of the fields in the partition.
    ///
    /// If this `PartitionSpec` has no fields (unpartitioned), then the constant
    /// [`UNPARTITIONED_LAST_ASSIGNED_FIELD_ID`] is returned.
    /// `None` if this partition spec contains no fields.
    pub fn last_assigned_field_id(&self) -> i32 {
        self.fields().iter().map(|field| field.field_id).max()
            .unwrap_or(UNPARTITIONED_LAST_ASSIGNED_FIELD_ID)
    }

    /// Returns the partition fields of this spec as a `StructType` with transformations
    /// applied to the source fields in `schema`.
    ///
    /// Each PartitionField is converted to a [`Field`] with its name preserved,
    /// its `field_id` becoming the `Field`'s id and its type converted according
    /// to its transform.
    pub fn as_struct_type(&self) -> StructType {
        let struct_fields = self.fields()
            .iter()
            .map(|field| {
                // These can't fail because we validate the fields in the constructor.
                let source_field = self.field_by_id.get(&field.source_id).unwrap();
                let result_type = field.transform.get_result_type(
                    source_field.r#type.clone()
                ).unwrap();

                Field::new(
                    field.field_id,
                    &field.name,
                    false,
                    result_type
                )
            }).collect();

        StructType::new(struct_fields)
    }

    /// Applies the partition spec to the given table values and returns the partition
    /// values.
    ///
    /// `source_values` should contain a mapping of source columns ids in the
    /// table's schema to their values. The source column ids must match the source
    /// fields specified when the `PartitionSpec` was created. A `None` value represents
    /// null.
    ///
    /// Returns a list of partition field names and their values after transformations
    /// have been applied. The list is ordered according to the partition spec's field
    /// order.
    ///
    /// # Errors
    ///
    /// [`IcebergError::PartitionError`] is returned if `source_values` is missing
    /// a value for one of the partition fields.
    pub fn partition_values(
        &self,
        source_values: HashMap<i32, Option<Value>>
    ) -> IcebergResult<PartitionValues> {
        self.fields()
            .iter()
            .map(|field| {
                let value = source_values.get(&field.source_id).ok_or_else(|| {
                    IcebergError::PartitionError {
                        message: format!(
                            "missing partition value for field id {}",
                            field.source_id
                        )
                    }
                })?;

                let value = field.transform.apply(value.clone())?;

                Ok((field.name.clone(), value))
            }).collect()
    }
}

/// Builder struct to create new `PartitionSpec`s easily.
pub struct PartitionSpecBuilder {
    spec_id: i32,
    schema: Schema,
    fields: Vec<PartitionField>,
    last_field_id: i32,
}

impl PartitionSpecBuilder {
    pub fn new(spec_id: i32, schema: Schema) -> Self {
        Self {
            spec_id: spec_id,
            schema: schema,
            fields: Vec::new(),
            last_field_id: UNPARTITIONED_LAST_ASSIGNED_FIELD_ID + 1,
        }
    }

    fn add_field(
        mut self,
        source_field: &str,
        name: &str,
        transform: PartitionTransform
    ) -> IcebergResult<Self> {
        let source_field = self.schema.get_field_by_name(source_field)
            .ok_or_else(|| {
                IcebergError::PartitionError {
                    message: format!(
                        "source field name {source_field} not found in schema",
                    )
                }
            })?;

        // Fail if this transform can't be applied to the source field.
        transform.get_result_type(source_field.r#type.clone())?;

        if self.fields.iter().any(|field| field.source_id == source_field.id) {
            Err(IcebergError::PartitionError {
                message: format!(
                    "redundant partitioning on source field '{}' with id {}",
                    source_field.name, source_field.id
                )
            })
        } else {
            self.fields.push(PartitionField::new(
                source_field.id,
                self.last_field_id,
                name,
                transform
            ));
            self.last_field_id += 1;
            Ok(self)
        }
    }

    /// Adds an untransformed partitioning field.
    ///
    /// # Errors
    ///
    /// [`IcebergError::PartitionError`] is returned if `source_field` does not refer
    /// to a valid field in `schema`, or was already used as a partitioning field.
    pub fn add_identity_field(self, source_field: &str) -> IcebergResult<Self> {
        self.add_field(source_field, source_field, PartitionTransform::Identity)
    }

    /// Adds a year-transformed partitioning field.
    ///
    /// # Errors
    ///
    /// [`IcebergError::PartitionError`] is returned if `source_field` does not refer
    /// to a valid field in `schema`, or was already used as a partitioning field.
    pub fn add_year_field(self, source_field: &str) -> IcebergResult<Self> {
        self.add_field(
            source_field,
            &format!("{}_year", source_field),
            PartitionTransform::Year
        )
    }

    pub fn build(self) -> PartitionSpec {
        // This should never fail because we validate the fields as they are added.
        PartitionSpec::try_new(self.spec_id, self.fields, self.schema).unwrap()
    }
}

/// Represents the (transformed) values of a single partition.
///
/// # Examples
///
/// ```rust
/// use chrono::NaiveDate;
///
/// use icelake::value::Value;
/// use icelake::partition::PartitionValues;
///
/// let partition_values = PartitionValues::from_iter([
///     (
///         "user_id".to_string(),
///         Some(Value::Int(42))
///     ),
///     (
///         "date".to_string(),
///         Some(Value::Date(NaiveDate::from_ymd(2022, 1, 1)))
///     )
/// ]);
///
/// assert_eq!(
///     partition_values.to_string(),
///     "user_id=42/date=2022-01-01"
/// );
/// ```
#[derive(Debug, Clone)]
pub struct PartitionValues {
    values: Vec<(String, Option<Value>)>
}

impl PartitionValues {
    /// Returns the partition values as tuples of partition field name and its value.
    pub fn values(&self) -> &Vec<(String, Option<Value>)> {
        &self.values
    }

    /// Returns the path components of the partition as a
    /// list of strings in `{partition_name}={partition_value}` format.
    pub fn path_parts(&self) -> Vec<String> {
        self.values.iter()
            .map(|(name, value)| {
                format!("{}={}",
                    name,
                    match value {
                        None => "null".to_string(),
                        Some(value) => value.to_string()
                    }
                )
            })
            .collect()
    }
}

impl Default for PartitionValues {
    fn default() -> Self {
        Self { values: Default::default() }
    }
}

impl FromIterator<(String, Option<Value>)> for PartitionValues {
    /// Builds a `PartitionValues` from a list of path components.
    fn from_iter<I: IntoIterator<Item=(String, Option<Value>)>>(iter: I) -> Self {
        Self {
            values: Vec::from_iter(iter)
        }
    }
}

impl std::fmt::Display for PartitionValues {
    /// Formats the values as full partition path, e.g. `date=2020-01-01/user=111`.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.path_parts().join("/"))
    }
}

impl std::hash::Hash for PartitionValues {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.to_string().hash(state);
    }
}

impl PartialEq for PartitionValues {
    fn eq(&self, other: &Self) -> bool {
        self.to_string() == other.to_string()
    }
}

impl Eq for PartitionValues {}


#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use chrono::NaiveDate;

    use crate::{IcebergError};
    use crate::schema::{
        Schema, Field,
        SchemaType, StructType, PrimitiveType
    };
    use crate::value::Value;
    use crate::partition::{PartitionSpec, PartitionField, PartitionTransform};

    fn create_partition_fields() -> Vec<PartitionField> {
        vec![
            PartitionField::new(
                1, 1000, "user_id", PartitionTransform::Identity
            ),
            PartitionField::new(
                2, 1001, "ts_day", PartitionTransform::Day
            )
        ]
    }

    fn create_schema() -> Schema {
        Schema::new(0, vec![
            Field::new(
                0,
                "id",
                true,
                SchemaType::Primitive(PrimitiveType::Long)
            ),
            Field::new(
                1,
                "user_id",
                true,
                SchemaType::Primitive(PrimitiveType::String)
            ),
            Field::new(
                2,
                "ts",
                false,
                SchemaType::Primitive(PrimitiveType::Timestamp)
            )
        ])
    }

    fn create_partition_spec() -> PartitionSpec {
        PartitionSpec::try_new(
            0,
            create_partition_fields(),
            create_schema()
        ).unwrap()
    }

    #[test]
    fn valid_partition_spec() {
        let spec = create_partition_spec();

        assert_eq!(spec.spec_id(), 0);
        assert_eq!(spec.fields().len(), 2);
    }

    #[test]
    fn invalid_partition_spec() {
        // Create a spec with duplicate field_id
        let result = PartitionSpec::try_new(0, vec![
            PartitionField::new(
                1, 1001, "id", PartitionTransform::Identity
            ),
            PartitionField::new(
                2, 1002, "user_id", PartitionTransform::Identity
            ),
            PartitionField::new(
                3, 1002, "ts", PartitionTransform::Day
            )],
            create_schema()
        );

        assert!(matches!(result, Err(IcebergError::PartitionError{..})));
    }

    #[test]
    fn invalid_partition_spec_for_schema() {
        // Schema is missing the source field with id '1'
        let schema = Schema::new(0, vec![
            Field::new(
                0,
                "id",
                true,
                SchemaType::Primitive(PrimitiveType::Long)
            )
        ]);

        let result = PartitionSpec::try_new(
            0,
            create_partition_fields(),
            schema
        );
        assert!(matches!(result, Err(IcebergError::PartitionError{..})));
    }

    #[test]
    fn as_struct() {
        let spec = create_partition_spec();

        assert_eq!(
            spec.as_struct_type(),
            StructType::new(vec![
                Field::new(
                    1000,
                    "user_id",
                    false,
                    SchemaType::Primitive(PrimitiveType::String)
                ),
                Field::new(
                    1001,
                    "ts_day",
                    false,
                    SchemaType::Primitive(PrimitiveType::Int)
                )
            ])
        );
    }

    #[test]
    fn partition_values_with_valid_values() {
        let spec = create_partition_spec();
        let values = HashMap::from([
            // user_id
            (1, Some(Value::String("a".to_string()))),
            // ts
            (2, Some(Value::Timestamp(
                NaiveDate::from_ymd_opt(2023, 1, 1)
                    .unwrap()
                    .and_hms_opt(0, 0, 0)
                    .unwrap())))
        ]);

        assert_eq!(
            spec.partition_values(values).unwrap().to_string(),
            "user_id=a/ts_day=19358"
        )
    }

    #[test]
    fn partition_values_with_invalid_values() {
        let spec = create_partition_spec();
        // Partition value for ts is missing
        let values = HashMap::from([
            // user_id
            (1, Some(Value::String("a".to_string()))),
        ]);

        assert!(matches!(
            spec.partition_values(values),
            Err(IcebergError::PartitionError{..})
        ));

        // Partition value for ts is of wrong type
        let values = HashMap::from([
            // user_id
            (1, Some(Value::String("a".to_string()))),
            // ts is missing
            (2, Some(Value::String("b".to_string()))),
        ]);

        assert!(matches!(
            spec.partition_values(values),
            Err(IcebergError::PartitionError{..})
        ));
    }
}
