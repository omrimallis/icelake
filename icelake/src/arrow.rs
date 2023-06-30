//! Conversion between Iceberg table schema and Arrow schema
use std::sync::Arc;
use std::collections::HashMap;

use arrow_schema::ArrowError;
use arrow_schema::{
    Schema as ArrowSchema, Field as ArrowField, Fields as ArrowFields,
    DataType as ArrowDataType, TimeUnit as ArrowTimeUnit,
};

use crate::{IcebergResult, IcebergError};
use crate::schema::{
    Schema, SchemaField, SchemaType,
    PrimitiveType, ListType, StructType, StructField
};

const FIELD_ID_KEY: &str = "ICEBERG:field_id";

impl TryFrom<&SchemaType> for ArrowDataType {
    type Error = ArrowError;

    fn try_from(t: &SchemaType) -> Result<Self, Self::Error> {
        Ok(match t {
            SchemaType::Primitive(primitive_type) => {
                match primitive_type {
                    PrimitiveType::Boolean => ArrowDataType::Boolean,
                    PrimitiveType::Int => ArrowDataType::Int32,
                    PrimitiveType::Long => ArrowDataType::Int64,
                    PrimitiveType::Float => ArrowDataType::Float32,
                    PrimitiveType::Double => ArrowDataType::Float64,
                    PrimitiveType::Decimal{precision, scale} => {
                        ArrowDataType::Decimal128(
                            *precision,
                            i8::try_from(*scale).map_err(|_| {
                                ArrowError::SchemaError(format!(
                                    "can't convert decimal with scale {scale}"
                                ))
                            })?
                        )
                    },
                    PrimitiveType::Date => ArrowDataType::Date32,
                    PrimitiveType::Time => {
                        ArrowDataType::Time64(ArrowTimeUnit::Microsecond)
                    },
                    PrimitiveType::Timestamp => {
                        ArrowDataType::Timestamp(ArrowTimeUnit::Microsecond, None)
                    },
                    PrimitiveType::Timestamptz => {
                        ArrowDataType::Timestamp(
                            ArrowTimeUnit::Microsecond,
                            Some(Arc::from("UTC"))
                        )
                    },
                    PrimitiveType::String => ArrowDataType::Utf8,
                    PrimitiveType::Uuid => ArrowDataType::FixedSizeBinary(16),
                    PrimitiveType::Fixed(size) => {
                        ArrowDataType::FixedSizeBinary(
                            i32::try_from(*size).map_err(|_| {
                                ArrowError::SchemaError(format!(
                                    "can't convert fixed size binary with size {size}"
                                ))
                            })?
                        )
                    },
                    PrimitiveType::Binary => ArrowDataType::Binary,
                }
            },
            SchemaType::Struct(struct_type) => {
                let converted_fields: Result<Vec<ArrowField>, _> = struct_type.fields
                    .iter()
                    .map(|field| field.try_into())
                    .collect();

                ArrowDataType::Struct(ArrowFields::from(converted_fields?))
            },
            SchemaType::List(list_type) => {
                ArrowDataType::List(Arc::new(
                    list_type.field().try_into()?
                ))
            },
            SchemaType::Map(map_type) => {
                let mut kv: Vec<ArrowField> = Vec::new();
                kv.push(map_type.key().try_into()?);
                kv.push(map_type.value().try_into()?);

                ArrowDataType::Map(
                    Arc::new(ArrowField::new(
                        "entries",
                        ArrowDataType::Struct(ArrowFields::from(kv)),
                        true
                    )),
                    false
                )
            }
        })
    }
}

impl TryFrom<&StructField> for ArrowField {
    type Error = ArrowError;

    fn try_from(field: &StructField) -> Result<Self, Self::Error> {
        let converted_type: ArrowDataType = (&field.r#type).try_into()?;

        let arrow_field = ArrowField::new(
            field.name.clone(),
            converted_type,
            !field.required
        ).with_metadata(HashMap::from_iter([
            (
                FIELD_ID_KEY.to_string(),
                field.id.to_string()
            )
        ]));

        Ok(arrow_field)
    }
}

impl TryFrom<StructField> for ArrowField {
    type Error = ArrowError;

    fn try_from(field: StructField) -> Result<Self, Self::Error> {
        ArrowField::try_from(&field)
    }
}

impl TryFrom<&Schema> for ArrowSchema {
    type Error = ArrowError;

    fn try_from(schema: &Schema) -> Result<Self, Self::Error> {
        let converted_fields: Result<Vec<ArrowField>, _> = schema.fields()
            .iter()
            .map(|field| field.try_into())
            .collect();

        Ok(ArrowSchema::new(converted_fields?))
    }
}

impl TryFrom<Schema> for ArrowSchema {
    type Error = ArrowError;

    fn try_from(schema: Schema) -> Result<Self, Self::Error> {
        ArrowSchema::try_from(&schema)
    }
}

impl TryFrom<&ArrowDataType> for SchemaType {
    type Error = ArrowError;

    fn try_from(arrow_type: &ArrowDataType) -> Result<Self, Self::Error> {
        match arrow_type {
            ArrowDataType::Boolean => Ok(SchemaType::Primitive(PrimitiveType::Boolean)),
            ArrowDataType::Int8 => Ok(SchemaType::Primitive(PrimitiveType::Int)),
            ArrowDataType::Int16 => Ok(SchemaType::Primitive(PrimitiveType::Int)),
            ArrowDataType::Int32 => Ok(SchemaType::Primitive(PrimitiveType::Int)),
            ArrowDataType::Int64 => Ok(SchemaType::Primitive(PrimitiveType::Long)),
            ArrowDataType::UInt8 => Ok(SchemaType::Primitive(PrimitiveType::Int)),
            ArrowDataType::UInt16 => Ok(SchemaType::Primitive(PrimitiveType::Int)),
            ArrowDataType::UInt32 => Ok(SchemaType::Primitive(PrimitiveType::Long)),
            ArrowDataType::Float16 => Ok(SchemaType::Primitive(PrimitiveType::Float)),
            ArrowDataType::Float32 => Ok(SchemaType::Primitive(PrimitiveType::Float)),
            ArrowDataType::Float64 => Ok(SchemaType::Primitive(PrimitiveType::Double)),
            // Timestamps without timezone.
            // Iceberg supports only up to microsecond precision.
            ArrowDataType::Timestamp(ArrowTimeUnit::Second, None)
            | ArrowDataType::Timestamp(ArrowTimeUnit::Millisecond, None)
            | ArrowDataType::Timestamp(ArrowTimeUnit::Microsecond, None) => {
                Ok(SchemaType::Primitive(PrimitiveType::Timestamp))
            },
            // Timestamps with timezone.
            // Iceberg supports only up to microsecond precision.
            ArrowDataType::Timestamp(ArrowTimeUnit::Second, Some(_tz))
            | ArrowDataType::Timestamp(ArrowTimeUnit::Millisecond, Some(_tz))
            | ArrowDataType::Timestamp(ArrowTimeUnit::Microsecond, Some(_tz)) => {
                Ok(SchemaType::Primitive(PrimitiveType::Timestamptz))
            },
            ArrowDataType::Date32 => Ok(SchemaType::Primitive(PrimitiveType::Date)),
            ArrowDataType::Date64 => Ok(SchemaType::Primitive(PrimitiveType::Date)),
            // Time of day. Iceberg supports only up to microsecond precision.
            ArrowDataType::Time32(ArrowTimeUnit::Second)
            | ArrowDataType::Time32(ArrowTimeUnit::Millisecond)
            | ArrowDataType::Time32(ArrowTimeUnit::Microsecond) => {
                Ok(SchemaType::Primitive(PrimitiveType::Time))
            },
            ArrowDataType::Time64(ArrowTimeUnit::Second)
            | ArrowDataType::Time64(ArrowTimeUnit::Millisecond)
            | ArrowDataType::Time64(ArrowTimeUnit::Microsecond) => {
                Ok(SchemaType::Primitive(PrimitiveType::Time))
            },
            ArrowDataType::Binary => Ok(SchemaType::Primitive(PrimitiveType::Binary)),
            ArrowDataType::FixedSizeBinary(size) => {
                // Convert i32 to u64
                let converted_size = <i32 as TryInto<u64>>::try_into(*size)
                    .map_err(|_| {
                        ArrowError::SchemaError(format!(
                            "can't convert Fixed-size binary with negative size {size}"
                        ))
                    }
                )?;

                Ok(SchemaType::Primitive(PrimitiveType::Fixed(converted_size)))
            },
            ArrowDataType::Utf8 => Ok(SchemaType::Primitive(PrimitiveType::String)),
            ArrowDataType::List(field)
            | ArrowDataType::FixedSizeList(field, _)
            | ArrowDataType::LargeList(field) => {
                Ok(SchemaType::List(ListType::new(
                    // TODO: Handle field IDs
                    0,
                    !field.is_nullable(),
                    field.data_type().try_into()?
                )))
            },
            ArrowDataType::Struct(fields) => {
                Ok(SchemaType::Struct(StructType::new(
                    fields.iter().map(|field| field.as_ref().try_into())
                        .collect::<Result<Vec<StructField>, _>>()?
                )))
            },
            ArrowDataType::Decimal128(p, s) => {
                let converted_scale = <i8 as TryInto<u8>>::try_into(*s)
                    .map_err(|_| {
                        ArrowError::SchemaError(format!(
                            "can't convert decimal with negative scale {s}"
                        ))
                    }
                )?;

                Ok(SchemaType::Primitive(PrimitiveType::Decimal {
                    precision: *p,
                    scale: converted_scale,
                }))
            },

            // TODO: Handle ArrowDataType::Map, ArrowDataType::Dictionary

            // ArrowDataType::Null
            // ArrowDataType::Unit64
            // ArrowDataType::Duration
            // ArrowDataType::Interval
            // ArrowDataType::LargeBinary
            // ArrowDataType::Decimal256
            dt => {
                Err(ArrowError::SchemaError(format!(
                    "unsupported Arrow data type for Iceberg: {dt}"
                )))
            }
        }
    }
}

impl TryFrom<&ArrowField> for SchemaField {
    type Error = ArrowError;

    fn try_from(arrow_field: &ArrowField) -> Result<Self, Self::Error> {
        Ok(SchemaField::new(
            // TODO: Handle field IDs
            0,
            arrow_field.name(),
            !arrow_field.is_nullable(),
            arrow_field.data_type().try_into()?,
        ))
    }
}

/// Converts an Iceberg table schema to an Arrow schema.
///
/// Iceberg field ids are encoded in the Arrow field metadata with the key
/// `"ICEBERG:field_id"`.
pub fn iceberg_to_arrow_schema(schema: &Schema) -> IcebergResult<ArrowSchema> {
    <ArrowSchema as TryFrom<&Schema>>::try_from(schema).map_err(|e| {
        IcebergError::SchemaError {
            message: format!("Failed to convert arrow schema: {e}")
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn new_arrow_field(
        name: impl Into<String>,
        data_type: ArrowDataType,
        nullable: bool,
        iceberg_id: i32
    ) -> ArrowField {
        ArrowField::new(name, data_type, nullable)
            .with_metadata(HashMap::from_iter([
                (
                    FIELD_ID_KEY.to_string(),
                    iceberg_id.to_string()
                )
            ]))
    }

    #[test]
    fn iceberg_to_arrow_struct() {
        // Ensure Iceberg struct fields are converted to Arrow structs correctly.
        let field = StructField::new(
            0,
            "user",
            false, 
            SchemaType::Struct(StructType::new(vec![
                StructField::new(
                    1,
                    "id",
                    true,
                    SchemaType::Primitive(PrimitiveType::Int)
                ),
                StructField::new(
                    2,
                    "name",
                    true,
                    SchemaType::Primitive(PrimitiveType::String)
                )
            ]))
        );

        let arrow_field: ArrowField = field.try_into().unwrap();
        
        assert_eq!(arrow_field, new_arrow_field(
            "user",
            ArrowDataType::Struct(ArrowFields::from(vec![
                new_arrow_field("id", ArrowDataType::Int32, false, 1),
                new_arrow_field("name", ArrowDataType::Utf8, false, 2)
            ])),
            true,
            0
        ));
    }

    #[test]
    fn iceberg_to_arrow_list() {
        let field = StructField::new(
            0,
            "users",
            false,
            SchemaType::List(ListType::new(
                1,
                false,
                SchemaType::Primitive(PrimitiveType::String)
            ))
        );

        let arrow_field: ArrowField = field.try_into().unwrap();

        assert_eq!(arrow_field, new_arrow_field(
            "users",
            ArrowDataType::List(Arc::new(new_arrow_field(
                "element",
                ArrowDataType::Utf8,
                true,
                1
            ))),
            true,
            0
        ));
    }

    #[test]
    fn iceberg_to_arrow_schema() {
        let schema = Schema::new(0, vec![
            SchemaField::new(
                1,
                "id",
                true,
                SchemaType::Primitive(PrimitiveType::Int)
            ),
            SchemaField::new(
                2,
                "name",
                true,
                SchemaType::Primitive(PrimitiveType::String)
            )
        ]);

        let arrow_schema: ArrowSchema = schema.try_into().unwrap();

        assert_eq!(arrow_schema, ArrowSchema::new(vec![
            new_arrow_field("id", ArrowDataType::Int32, false, 1),
            new_arrow_field("name", ArrowDataType::Utf8, false, 2)
        ]));
    }
}
