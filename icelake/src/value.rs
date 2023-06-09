//! Interface for workign with Iceberg field values.
//!
//! This module provides [`Value`] which represents a single value of a field in an
//! Iceberg table.
use std::collections::HashMap;

use uuid::Uuid;
use chrono::{NaiveDate, NaiveTime, NaiveDateTime, DateTime, Utc};
use serde::{Serialize, Deserialize};

use crate::{IcebergResult, IcebergError};
use crate::schema::{
    SchemaType, StructField, PrimitiveType, StructType, ListType
};

/// Represents any valid Iceberg field value.
///
/// The value can be serialized and deserialized from JSON using the `serde_json`
/// module. The JSON form adheres to the
/// [Iceberg single-value serialization spec](https://iceberg.apache.org/spec/#json-single-value-serialization).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum Value {
    Boolean(bool),
    Int(i32),
    Long(i64),
    Float(f32),
    Double(f64),
    // TODO: Decimal
    Date(NaiveDate),
    #[serde(with = "time_serde")]
    Time(NaiveTime),
    #[serde(with = "timestamp_serde")]
    Timestamp(NaiveDateTime),
    #[serde(with = "timestamptz_serde")]
    Timestamptz(DateTime<Utc>),
    String(String),
    Uuid(Uuid),
    #[serde(with = "binary_serde")]
    Fixed(Vec<u8>),
    #[serde(with = "binary_serde")]
    Binary(Vec<u8>),
    Struct(HashMap<String, Value>),
    List(Vec<Value>),
    // TODO
    // Map { keys: Vec<Value>, values: Vec<Value> }
}

impl Value {
    /// Obtains the schema type of a value.
    ///
    /// Note: For `Value::Struct` and `Value::List`, the nested fields of the resulting
    /// `StructType` and `ListType` are all assigned a zero id and assumed to be
    /// optional.
    ///
    /// # Errors
    ///
    /// [`IcebergError::SchemaError`] is returned if the `SchemaType` can't be inferred.
    /// This might happen, for example, if an empty list is given.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use icelake::value::Value;
    /// use icelake::schema::{SchemaType, PrimitiveType};
    ///
    /// assert_eq!(
    ///     Value::Int(0).get_type().unwrap(),
    ///     SchemaType::Primitive(PrimitiveType::Int)
    /// );
    /// ```
    pub fn get_type(&self) -> IcebergResult<SchemaType> {
        match self {
            Value::Boolean(_) => {
                Ok(SchemaType::Primitive(PrimitiveType::Boolean))
            },
            Value::Int(_) => {
                Ok(SchemaType::Primitive(PrimitiveType::Int))
            },
            Value::Long(_) => {
                Ok(SchemaType::Primitive(PrimitiveType::Long))
            },
            Value::Float(_) => {
                Ok(SchemaType::Primitive(PrimitiveType::Float))
            },
            Value::Double(_) => {
                Ok(SchemaType::Primitive(PrimitiveType::Double))
            },
            Value::Date(_) => {
                Ok(SchemaType::Primitive(PrimitiveType::Date))
            },
            Value::Time(_) => {
                Ok(SchemaType::Primitive(PrimitiveType::Time))
            },
            Value::Timestamp(_) => {
                Ok(SchemaType::Primitive(PrimitiveType::Timestamp))
            },
            Value::Timestamptz(_) => {
                Ok(SchemaType::Primitive(PrimitiveType::Timestamptz))
            },
            Value::String(_) => {
                Ok(SchemaType::Primitive(PrimitiveType::String))
            },
            Value::Uuid(_) => {
                Ok(SchemaType::Primitive(PrimitiveType::Uuid))
            },
            Value::Fixed(array) => {
                Ok(SchemaType::Primitive(PrimitiveType::Fixed(
                    u64::try_from(array.len()).map_err(|_| 
                        IcebergError::SchemaError {
                            message: "fixed byte-array is too long".to_string()
                        }
                    )?
                )))
            },
            Value::Binary(_) => Ok(SchemaType::Primitive(PrimitiveType::Binary)),
            Value::Struct(fields) => {
                let nested_fields: IcebergResult<Vec<StructField>> = fields.iter()
                    .map(|(name, value)| {
                        Ok(StructField::new(
                            0,
                            name,
                            false,
                            value.get_type()?
                        ))
                    })
                    .collect();
                Ok(SchemaType::Struct(StructType::new(nested_fields?)))
            },
            Value::List(fields) => {
                if fields.is_empty() {
                    Err(IcebergError::SchemaError {
                        message: "can't infer schema type: list is empty".to_string()
                    })
                } else {
                    Ok(SchemaType::List(ListType::new(
                        0,
                        false,
                        fields[0].get_type()?
                    )))
                }
            }
        }
    }
}

impl TryFrom<Value> for Vec<u8> {
    type Error = IcebergError;

    fn try_from(value: Value) -> Result<Self, Self::Error> {
        match value {
            Value::Boolean(b) => {
                Ok(vec![
                    if b {
                        0x00
                    } else {
                        0x01
                    }
                ])
            },
            Value::Int(i) => {
                Ok(Vec::from(i.to_le_bytes()))
            },
            Value::Long(l) => {
                Ok(Vec::from(l.to_le_bytes()))
            },
            Value::Float(f) => {
                Ok(Vec::from(f.to_le_bytes()))
            },
            Value::Double(d) => {
                Ok(Vec::from(d.to_le_bytes()))
            },
            Value::Date(date) => {
                // Days since 1970-01-01
                let duration = date - NaiveDate::default();
                let days = i32::try_from(duration.num_days()).map_err(|_| {
                    IcebergError::SchemaError {
                        message: format!(
                            "date {date} is too far from 1970-01-01"
                        )
                    }
                })?;

                Ok(Vec::from(days.to_le_bytes()))
            },
            Value::Time(time) => {
                // Microseconds since 1970-01-01
                let duration = time - NaiveTime::default();
                let micros: i64 = duration.num_microseconds().unwrap();
                Ok(Vec::from(micros.to_le_bytes()))
            },
            Value::Timestamp(timestamp) => {
                let duration = timestamp - NaiveDateTime::default();
                let micros: i64 = duration.num_microseconds().ok_or_else(|| {
                    IcebergError::SchemaError {
                        message: format!(
                            "timestamp {timestamp} is too far from 1970-01-01"
                        )
                    }
                })?;

                Ok(Vec::from(micros.to_le_bytes()))
            },
            // TODO: Implement the rest
            _ => {
                Err(IcebergError::SchemaError {
                    message: format!(
                        "value {value:?} can't be converted to binary form"
                    )
                })
            }
        }
    }
}

/// Custom serializer and deserializer for `NaiveTime` which uses
/// microsecond precision instead of the default nanosecond.
mod time_serde {
    use chrono::NaiveTime;
    use serde::{self, Deserialize, Serializer, Deserializer};

    // Truncate to milliseconds.
    const FORMAT: &'static str = "%H:%M:%S%.6f";

    pub fn serialize<S>(
        time: &NaiveTime,
        serializer: S,
    ) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let s = format!("{}", time.format(FORMAT).to_string());
        serializer.serialize_str(&s)
    }

    pub fn deserialize<'de, D>(
        deserializer: D,
    ) -> Result<NaiveTime, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        NaiveTime::parse_from_str(&s, FORMAT).map_err(serde::de::Error::custom)
    }
}

/// Custom serializer and deserializer for `NaiveDateTime` which uses
/// microsecond precision instead of the default nanosecond.
mod timestamp_serde {
    use chrono::NaiveDateTime;
    use serde::{self, Deserialize, Serializer, Deserializer};

    // Truncate to milliseconds.
    const FORMAT: &'static str = "%Y-%m-%dT%H:%M:%S%.6f";

    pub fn serialize<S>(
        datetime: &NaiveDateTime,
        serializer: S,
    ) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let s = format!("{}", datetime.format(FORMAT).to_string());
        serializer.serialize_str(&s)
    }

    pub fn deserialize<'de, D>(
        deserializer: D,
    ) -> Result<NaiveDateTime, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        NaiveDateTime::parse_from_str(&s, FORMAT).map_err(serde::de::Error::custom)
    }
}

/// Custom serializer and deserializer for `DateTime` which uses
/// microsecond precision instead of the default nanosecond.
mod timestamptz_serde {
    use chrono::{DateTime, Utc, TimeZone};
    use serde::{self, Deserialize, Serializer, Deserializer};

    // Truncate to milliseconds.
    const FORMAT: &'static str = "%Y-%m-%dT%H:%M:%S%.6f%:z";

    pub fn serialize<S>(
        datetime: &DateTime<Utc>,
        serializer: S,
    ) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let s = format!("{}", datetime.format(FORMAT));
        serializer.serialize_str(&s)
    }

    pub fn deserialize<'de, D>(
        deserializer: D,
    ) -> Result<DateTime<Utc>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        Utc.datetime_from_str(&s, FORMAT).map_err(serde::de::Error::custom)
    }
}

/// Custom serializer and deserializer for `Value::Fixed` and `Value::Binary`,
/// encoding them as hex strings.
mod binary_serde {
    use serde::{self, Deserialize, Serializer, Deserializer};

    pub fn serialize<S>(
        binary: &Vec<u8>,
        serializer: S,
    ) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let s = binary.iter().map(|x| format!("{:02x}", x)).collect::<String>();
        serializer.serialize_str(&s)
    }

    pub fn deserialize<'de, D>(
        deserializer: D,
    ) -> Result<Vec<u8>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        if s.len() % 2 != 0 {
            Err(serde::de::Error::custom("invalid hex string"))
        } else {
            // Decode from hex string
            (0..s.len())
                .step_by(2)
                .map(|i| {
                    u8::from_str_radix(&s[i..i + 2], 16).map_err(
                        serde::de::Error::custom
                    )
                })
                .collect()
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use serde_json;
    use uuid::Uuid;
    use chrono::{NaiveDate, NaiveTime, NaiveDateTime, DateTime, Utc};
    use crate::value::Value;

    /// Tests that serializing Value to json matches the single-value JSON
    /// serialization spec in Iceberg. See:
    /// https://iceberg.apache.org/spec/#json-single-value-serialization
    #[test]
    fn json_serialization() {
        let value = Value::Int(42);
        assert_eq!(serde_json::to_string(&value).unwrap(), "42");

        let value = Value::Date(NaiveDate::from_ymd_opt(2023, 1, 1).unwrap());
        assert_eq!(serde_json::to_string(&value).unwrap(), "\"2023-01-01\"");

        // Time values encode only millisecond precision.
        let value = Value::Time(
            "22:31:08.123456789".parse::<NaiveTime>().unwrap()
        );
        assert_eq!(serde_json::to_string(&value).unwrap(), "\"22:31:08.123456\"");

        let value = Value::Timestamp(
            "2017-11-16T22:31:08.123456789".parse::<NaiveDateTime>().unwrap()
        );
        assert_eq!(
            serde_json::to_string(&value).unwrap(),
            "\"2017-11-16T22:31:08.123456\""
        );

        let value = Value::Timestamptz(
            "2017-11-16T22:31:08.123456789+00:00".parse::<DateTime<Utc>>().unwrap()
        );
        assert_eq!(
            serde_json::to_string(&value).unwrap(),
            "\"2017-11-16T22:31:08.123456+00:00\""
        );

        let value = Uuid::parse_str("f79c3e09-677c-4bbd-a479-3f349cb785e7").unwrap();
        assert_eq!(
            serde_json::to_string(&value).unwrap(),
            "\"f79c3e09-677c-4bbd-a479-3f349cb785e7\""
        );

        let value = Value::Binary(vec![0x00, 0x01, 0x02, 0xff]);
        assert_eq!(
            serde_json::to_string(&value).unwrap(), "\"000102ff\""
        );

        let value = Value::Struct(HashMap::from([
            ("a".to_string(), Value::Int(1)),
            ("b".to_string(), Value::Int(2))
        ]));
        assert!(
            (serde_json::to_string(&value).unwrap() == r#"{"a":1,"b":2}"#) ||
            (serde_json::to_string(&value).unwrap() == r#"{"b":2,"a":1}"#)
        );

        let value = Value::List(vec![Value::Int(1), Value::Int(2), Value::Int(3)]);
        assert_eq!(serde_json::to_string(&value).unwrap(), "[1,2,3]");
    }
}
