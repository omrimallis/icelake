//! Interface to Iceberg table partitions.
use std::collections::{HashMap, HashSet};

use regex::Regex;
use serde::{
    de::{self, IntoDeserializer},
    Deserialize, Deserializer, Serialize,
};

use crate::{IcebergResult, IcebergError};
use crate::schema::{
    Schema, SchemaType, StructField,
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

impl PartitionTransform {
    fn get_result_type_date(field_type: SchemaType) -> IcebergResult<SchemaType> {
        match field_type {
            SchemaType::Primitive(PrimitiveType::Date)
            | SchemaType::Primitive(PrimitiveType::Timestamp)
            | SchemaType::Primitive(PrimitiveType::Timestamptz) => {
                Ok(SchemaType::Primitive(PrimitiveType::Int))
            },
            _ => {
                Err(IcebergError::PartitionError {
                    message: format!(
                        "can't apply date transform to field of type {}",
                        field_type
                    )
                })
            }
        }
    }

    fn get_result_type_hour(field_type: SchemaType) -> IcebergResult<SchemaType> {
        match field_type {
            SchemaType::Primitive(PrimitiveType::Timestamp)
            | SchemaType::Primitive(PrimitiveType::Timestamptz) => {
                Ok(SchemaType::Primitive(PrimitiveType::Int))
            },
            _ => {
                Err(IcebergError::PartitionError {
                    message: format!(
                        "can't apply date transform to field of type {}",
                        field_type
                    )
                })
            }
        }
    }

    fn get_result_type_bucket(field_type: SchemaType) -> IcebergResult<SchemaType> {
        match field_type {
            SchemaType::Primitive(PrimitiveType::Int)
            | SchemaType::Primitive(PrimitiveType::Long)
            | SchemaType::Primitive(PrimitiveType::Decimal{..})
            | SchemaType::Primitive(PrimitiveType::Date)
            | SchemaType::Primitive(PrimitiveType::Time)
            | SchemaType::Primitive(PrimitiveType::Timestamp)
            | SchemaType::Primitive(PrimitiveType::Timestamptz)
            | SchemaType::Primitive(PrimitiveType::String)
            | SchemaType::Primitive(PrimitiveType::Uuid)
            | SchemaType::Primitive(PrimitiveType::Fixed(_))
            | SchemaType::Primitive(PrimitiveType::Binary) => {
                Ok(SchemaType::Primitive(PrimitiveType::Int))
            },
            _ => {
                Err(IcebergError::PartitionError {
                    message: format!(
                        "can't apply bucket transform to field of type {}",
                        field_type
                    )
                })
            }
        }
    }

    fn get_result_type_truncate(field_type: SchemaType) -> IcebergResult<SchemaType> {
        match field_type {
            SchemaType::Primitive(PrimitiveType::Int)
            | SchemaType::Primitive(PrimitiveType::Long)
            | SchemaType::Primitive(PrimitiveType::Decimal{..})
            | SchemaType::Primitive(PrimitiveType::String) => {
                Ok(field_type)
            },
            _ => {
                Err(IcebergError::PartitionError {
                    message: format!(
                        "can't apply truncate transform to field of type {}",
                        field_type
                    )
                })
            }
        }
    }

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
                Ok(field_type)
            },
            PartitionTransform::Year
            | PartitionTransform::Month
            | PartitionTransform::Day => {
                PartitionTransform::get_result_type_date(field_type)
            },
            PartitionTransform::Hour => {
                PartitionTransform::get_result_type_hour(field_type)
            },
            PartitionTransform::Bucket(_) => {
                PartitionTransform::get_result_type_bucket(field_type)
            },
            PartitionTransform::Truncate(_) => {
                PartitionTransform::get_result_type_truncate(field_type)
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
                PartitionTransform::Identity => { Ok(Some(value)) },
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

/// Specification of table-level partitioning.
///
/// This struct defines how partition values are derived from the data fields of the
/// table.
#[derive(Debug, Clone)]
pub struct PartitionSpec {
    model: PartitionSpecModel,
    /// Lookup for schema fields by their source id.
    field_by_id: HashMap<i32, StructField>,
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

    pub fn spec_id(&self) -> i32 {
        self.model.spec_id
    }

    pub fn fields(&self) -> &Vec<PartitionField> {
        &self.model.fields
    }

    pub(crate) fn model(self) -> PartitionSpecModel {
        self.model
    }

    /// Builds a lookup map from the schema's primitive fields.
    fn field_by_id(schema: Schema) -> HashMap<i32, StructField> {
        // Temporary queue of fields to be processed.
        let mut queue: Vec<StructField> = schema.fields().into();
        // List of valid field ids.
        let mut lookup: HashMap<i32, StructField> = HashMap::new();

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
    /// Each PartitionField is converted to a [`StructField`] with its name preserved,
    /// its `field_id` becoming the `StructField`'s id and its type converted according
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

                StructField::new(
                    field.field_id,
                    &field.name,
                    false,
                    result_type
                )
            }).collect();

        StructType::new(struct_fields)
    }
}

#[cfg(test)]
mod tests {
    use crate::{IcebergError};
    use crate::schema::{
        Schema, SchemaField, StructField,
        SchemaType, StructType, PrimitiveType
    };
    use crate::partition::{PartitionSpec, PartitionField, PartitionTransform};

    fn create_partition_fields() -> Vec<PartitionField> {
        vec![
            PartitionField::new(
                1, 1000, "user_id", PartitionTransform::Identity
            ),
            PartitionField::new(
                2, 1001, "year", PartitionTransform::Year
            ),
            PartitionField::new(
                2, 1002, "month", PartitionTransform::Month
            ),
            PartitionField::new(
                2, 1003, "day", PartitionTransform::Day
            )
        ]
    }

    fn create_schema() -> Schema {
        Schema::new(0, vec![
            SchemaField::new(
                0,
                "id",
                true,
                SchemaType::Primitive(PrimitiveType::Long)
            ),
            SchemaField::new(
                1,
                "user_id",
                true,
                SchemaType::Primitive(PrimitiveType::String)
            ),
            SchemaField::new(
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
        assert_eq!(spec.fields().len(), 4);
    }

    #[test]
    fn invalid_partition_spec() {
        // Create a spec with duplicate field_id
        let result = PartitionSpec::try_new(0, vec![
            PartitionField::new(
                1, 1001, "year", PartitionTransform::Year
            ),
            PartitionField::new(
                1, 1002, "month", PartitionTransform::Month
            ),
            PartitionField::new(
                1, 1002, "day", PartitionTransform::Day
            )],
            create_schema()
        );

        assert!(matches!(result, Err(IcebergError::PartitionError{..})));
    }

    #[test]
    fn invalid_partition_spec_for_schema() {
        // Schema is missing the source field with id '1'
        let schema = Schema::new(0, vec![
            SchemaField::new(
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
                StructField::new(
                    1000,
                    "user_id",
                    false,
                    SchemaType::Primitive(PrimitiveType::String)
                ),
                StructField::new(
                    1001,
                    "year",
                    false,
                    SchemaType::Primitive(PrimitiveType::Int)
                ),
                StructField::new(
                    1002,
                    "month",
                    false,
                    SchemaType::Primitive(PrimitiveType::Int)
                ),
                StructField::new(
                    1003,
                    "day",
                    false,
                    SchemaType::Primitive(PrimitiveType::Int)
                )
            ])
        );
    }
}
