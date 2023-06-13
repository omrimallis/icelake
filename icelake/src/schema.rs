//! Iceberg table schema implementation.
//!
//! This module provides [`Schema`] which represents the schema of an Iceberg table. Each
//! schema has a `schema_id` and a vector of fields represented by [`SchemaField`].
//!
//! In an Iceberg schema, each field has an associated type, represented in this module
//! by [`SchemaType`]. The schema can be highly complex with nested fields such as
//! [`SchemaType::Struct`].
//!
//! Each top-level and nested field has an associated id. Care must be taken to ensure
//! that field ids are unique in the schema and consistent when evolving schemas.
//!
//! ## Creating a simple schema
//!
//! Create a simple schema made of primitive types:
//!
//! ```rust
//! use icelake::schema::{Schema, SchemaField, SchemaType, PrimitiveType};
//!
//! let schema_id = 0;
//! let schema = Schema::new(schema_id, vec![
//!     SchemaField::new(
//!         0,
//!         "id",
//!         true,
//!         SchemaType::Primitive(PrimitiveType::Long)
//!     ),
//!     SchemaField::new(
//!         1,
//!         "ts",
//!         false,
//!         SchemaType::Primitive(PrimitiveType::Timestamp)
//!     )
//! ]);
//! ```
//!
//! ## Creating a schema with nested types
//!
//! Iceberg provides structs, lists and maps for creating complex nested fields.
//! An example of using a struct field for storing two nested string fields:
//!
//! ```rust
//! use icelake::schema::{Schema, SchemaField, SchemaType, PrimitiveType};
//! use icelake::schema::{StructType, StructField};
//!
//! let schema_id = 0;
//! let schema = Schema::new(schema_id, vec![
//!     SchemaField::new(
//!         0,
//!         "user",
//!         false,
//!         SchemaType::Struct(StructType::new(vec![
//!             StructField::new(
//!                 1,
//!                 "first_name",
//!                 true,
//!                 SchemaType::Primitive(PrimitiveType::String)
//!             ),
//!             StructField::new(
//!                 2,
//!                 "last_name",
//!                 true,
//!                 SchemaType::Primitive(PrimitiveType::String)
//!             )
//!         ]))
//!     )
//! ]);
//! ```
use std::cell::RefCell;
use std::borrow::Cow;

use regex::Regex;
use serde::{
    de::{self, IntoDeserializer},
    Serialize, Serializer, Deserialize, Deserializer
};

use crate::{IcebergResult, IcebergError};

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone)]
#[serde(rename_all = "lowercase")]
#[serde(remote = "Self")]
/// An enum of possible primitive field types.
pub enum PrimitiveType {
    /// True or False
    Boolean,
    /// 32-bit signed integer
    Int,
    /// 64-bit signed integer
    Long,
    /// 32-bit IEEE 753 floating bit.
    Float,
    /// 64-bit IEEE 753 floating bit.
    Double,
    /// Fixed point decimal
    Decimal {
        /// The number of digits in the number.
        precision: u8,
        /// The number of digits to the right of the decimal point.
        scale: u8,
    },
    /// Calendar date without timezone or time.
    Date,
    /// Time of day without date or timezone.
    Time,
    /// Timestamp without timezone
    Timestamp,
    /// Timestamp with timezone
    Timestamptz,
    /// Arbitrary-length character sequences
    String,
    /// Universally Unique Identifiers
    Uuid,
    /// Fixed length byte array
    Fixed(u64),
    /// Arbitrary-length byte array.
    Binary,
}

/// Serialize for PrimitiveType with special handling for
/// Decimal and Fixed types.
impl Serialize for PrimitiveType {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        use PrimitiveType::*;
        match self {
            Decimal {
                precision: p,
                scale: s,
            } => serializer.serialize_str(&format!("decimal({p},{s})")),
            Fixed(l) => serializer.serialize_str(&format!("fixed[{l}]")),
            _ => PrimitiveType::serialize(self, serializer),
        }
    }
}

/// Serialize for PrimitiveType wit special handling for
/// Decimal and Fixed types.
impl<'de> Deserialize<'de> for PrimitiveType {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        if s.starts_with("decimal") {
            deserialize_decimal(s.into_deserializer())
        } else if s.starts_with("fixed") {
            deserialize_fixed(s.into_deserializer())
        } else {
            PrimitiveType::deserialize(s.into_deserializer())
        }
    }
}

/// Parsing for the Decimal PrimitiveType
fn deserialize_decimal<'de, D>(deserializer: D) -> Result<PrimitiveType, D::Error>
where
    D: Deserializer<'de>,
{
    let this = String::deserialize(deserializer)?;
    let re = Regex::new(r#"^decimal\((?P<p>\d+),(?P<s>\d+)\)$"#).unwrap();

    let err_msg = format!("Invalid decimal format {}", this);

    let caps = re
        .captures(&this)
        .ok_or_else(|| de::Error::custom(&err_msg))?;
    let precision: u8 = caps
        .name("p")
        .ok_or_else(|| de::Error::custom(&err_msg))
        .and_then(|p| {
            p.as_str()
                .parse()
                .map_err(|_| de::Error::custom("precision not u8"))
        })?;
    let scale: u8 = caps
        .name("s")
        .ok_or_else(|| de::Error::custom(&err_msg))
        .and_then(|p| {
            p.as_str()
                .parse()
                .map_err(|_| de::Error::custom("scale not u8"))
        })?;
    Ok(PrimitiveType::Decimal { precision, scale })
}

/// Deserialize for the Fixed PrimitiveType
fn deserialize_fixed<'de, D>(deserializer: D) -> Result<PrimitiveType, D::Error>
where
    D: Deserializer<'de>,
{
    let this = String::deserialize(deserializer)?;
    let re = Regex::new(r#"^fixed\[(?P<l>\d+)\]$"#).unwrap();

    let err_msg = format!("Invalid fixed format {}", this);

    let caps = re
        .captures(&this)
        .ok_or_else(|| de::Error::custom(&err_msg))?;
    let length: u64 = caps
        .name("l")
        .ok_or_else(|| de::Error::custom(&err_msg))
        .and_then(|p| {
            p.as_str()
                .parse()
                .map_err(|_| de::Error::custom("length not u64"))
        })?;
    Ok(PrimitiveType::Fixed(length))
}

static STRUCT_TAG: &str = "struct";
static LIST_TAG: &str = "list";
static MAP_TAG: &str = "map";

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone)]
#[serde(rename_all = "kebab-case")]
/// A complex field type that contains a tuple of nested fields.
///
/// Each nested field in the struct is named and has an integer id that is unique in the
/// table schema.  Each field can be either optional or required, meaning that values
/// can (or cannot) be null. Nested fields may be any type, including a [StructType].
/// Nested fields may have an optional comment or doc string.
pub struct StructType {
    /// Always set to "struct".
    pub r#type: Cow<'static, str>,
    /// The fields of the struct.
    pub fields: Vec<StructField>,
}

impl StructType {
    pub fn new(fields: Vec<StructField>) -> Self {
        let tag = Cow::Borrowed(STRUCT_TAG);
        Self { r#type: tag, fields: fields }
    }
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone)]
/// Details of a struct in a field.
pub struct StructField {
    /// Unique Id
    pub id: i32,
    /// Field Name
    pub name: String,
    /// Optional or required, meaning that values can (or can not be null)
    pub required: bool,
    /// Field can have any type
    pub r#type: SchemaType,
    /// Fields can have any optional comment or doc string.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub doc: Option<String>,
    // TODO: initial-default and write-default
}

impl StructField {
    pub fn new(
        id: i32,
        name: &str,
        required: bool,
        r#type: SchemaType
    ) -> Self {
        StructField::new_with_doc(id, name, required, r#type, None)
    }

    pub fn new_with_doc(
        id: i32,
        name: &str,
        required: bool,
        r#type: SchemaType,
        doc: Option<String>
    ) -> Self {
        Self {
            id: id,
            name: name.to_string(),
            required: required,
            r#type: r#type,
            doc: doc,
        }
    }

    pub fn with_required(mut self, required: bool) -> Self {
        self.required = required;
        self
    }

    pub fn with_doc(mut self, doc: String) -> Self {
        self.doc = Some(doc);
        self
    }
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone)]
#[serde(rename_all = "kebab-case")]
/// A field type that represents a list of identical elements.
pub struct ListType {
    /// Always set to "list".
    pub r#type: Cow<'static, str>,
    /// Unique identifier for the element
    pub element_id: i32,
    /// If the element is mandatory.
    pub element_required: bool,
    /// The type of the element.
    pub element: Box<SchemaType>,
}

impl ListType {
    pub fn new(element_id: i32, element_required: bool, element: SchemaType) -> Self {
        let tag = Cow::Borrowed(LIST_TAG);
        Self {
            r#type: tag,
            element_id: element_id,
            element_required: element_required,
            element: Box::new(element)
        }
    }
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone)]
#[serde(rename_all = "kebab-case")]
/// A complex field type that contains key-value pairs.
///
/// A map is a collection of key-value pairs with a key type and a value type.
/// Both the key field and value field each have an integer id that is unique
/// in the table schema. Map keys are required and map values can be either
/// optional or required. Both map keys and map values may be any type,
/// including nested types.
pub struct MapType {
    /// Always set to "map".
    pub r#type: Cow<'static, str>,
    /// Unique key field id
    pub key_id: i32,
    /// Type of the map key
    pub key: Box<SchemaType>,
    /// Unique key for the value id
    pub value_id: i32,
    /// Indicates if the value is required.
    pub value_required: bool,
    /// Type of the value.
    pub value: Box<SchemaType>,
}

impl MapType {
    pub fn new(
        key_id: i32,
        key: SchemaType,
        value_id: i32,
        value_required: bool,
        value: SchemaType
    ) -> Self {
        let tag = Cow::Borrowed(MAP_TAG);
        Self {
            r#type: tag,
            key_id: key_id,
            key: Box::new(key),
            value_id: value_id,
            value_required: value_required,
            value: Box::new(value),
        }
    }
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone)]
#[serde(untagged)]
/// Represents the type of a field in an Iceberg table schema.
pub enum SchemaType {
    /// A primitive field type.
    Primitive(PrimitiveType),
    /// A struct field type.
    Struct(StructType),
    /// A list field type.
    List(ListType),
    /// A map field type.
    Map(MapType),
}

impl SchemaType {
    /// Obtains the maximal id of all recursively-nested fields inside this type,
    /// or `None` if this type is primitive.
    pub fn max_nested_field_id(&self) -> Option<i32> {
        match &self {
            // Primitive types don't have nested fields.
            SchemaType::Primitive(_) => None,
            SchemaType::Struct(s) => {
                s.fields.iter().map(|field| -> i32 {
                    // Call recursively on nested fields.
                    if let Some(max_nested_id) = field.r#type.max_nested_field_id() {
                        std::cmp::max(field.id, max_nested_id)
                    } else {
                        field.id
                    }
                }).max()
            },
            SchemaType::List(l) => {
                let mut max = l.element_id;
                if let Some(max_nested_id) = l.element.max_nested_field_id() {
                    max = std::cmp::max(max, max_nested_id)
                }
                Some(max)
            },
            SchemaType::Map(m) => {
                let mut max = std::cmp::max(m.key_id, m.value_id);
                // Call recursively on nested key and value types.
                if let Some(max_nested_id) = m.key.max_nested_field_id() {
                    max = std::cmp::max(max, max_nested_id);
                }
                if let Some(max_nested_id) = m.value.max_nested_field_id() {
                    max = std::cmp::max(max, max_nested_id);
                }
                Some(max)
            }
        }
    }
}

impl std::fmt::Display for SchemaType {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}

/// An Iceberg table schema.
#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone)]
#[serde(rename_all = "kebab-case")]
pub struct Schema {
    /// Unique schema identifier.
    schema_id: i32,
    /// Optionally track the set of primitive fields that identify rows in a table.
    #[serde(skip_serializing_if = "Option::is_none")]
    identifier_field_ids: Option<Vec<u32>>,
    /// Actual fields, embedded as a Struct object as per the Iceberg spec.
    /// Must be of variant SchemaType::Struct
    #[serde(flatten)]
    schema: SchemaType,
}

/// A top-level field in the Iceberg table schema.
pub type SchemaField = StructField;

impl Schema {
    pub fn new(
        schema_id: i32,
        fields: Vec<SchemaField>
    ) -> Self {
        Self {
            schema_id: schema_id,
            identifier_field_ids: None,
            schema: SchemaType::Struct(StructType::new(fields)),
        }
    }

    pub fn id(&self) -> i32 { self.schema_id }

    pub fn max_field_id(&self) -> i32 {
        self.schema.max_nested_field_id().unwrap()
    }

    pub fn fields(&self) -> &[SchemaField] {
        match &self.schema {
            SchemaType::Struct(s) => &s.fields,
            _ => panic!("unexpected schema type")
        }
    }

    pub fn get_field_by_name(&self, name: &str) -> Option<&SchemaField> {
        self.fields().iter()
            .find(|field| field.name == name)
    }

    pub fn encode(&self) -> IcebergResult<String> {
        serde_json::to_string(self).map_err(
            |e| IcebergError::SerializeSchemaJson { source: e }
        )
    }
}

/// Provides an interface for building Iceberg schemas with automatic field id
/// assignment.
///
/// Normally when creating a [`Schema`] one has to manually assign field id to every
/// schema id, including fields nested inside structs, lists and maps.
/// `SchemaBuilder` internally tracks field ids instead and automatically assigns the
/// next id to each new field.
///
/// # Examples
///
/// Build a schema with nested struct fields.
///
/// ```rust
/// use icelake::schema::SchemaBuilder;
///
/// let schema_id = 0;
/// let mut schema_builder = SchemaBuilder::new(schema_id);
/// schema_builder.add_fields(vec![
///     schema_builder.new_int_field("id").with_required(true),
///     schema_builder.new_timestamp_field("ts"),
///     schema_builder.new_struct_field("name", vec![
///         schema_builder.new_string_field("first"),
///         schema_builder.new_string_field("last")
///     ])
/// ]);
/// let schema = schema_builder.build();
/// ```
pub struct SchemaBuilder {
    schema_id: i32,
    next_field_id: RefCell<i32>,
    fields: Vec<SchemaField>,
}

impl SchemaBuilder {
    pub fn new(schema_id: i32) -> Self {
        Self {
            schema_id: schema_id,
            next_field_id: RefCell::new(1),
            fields: Vec::new(),
        }
    }

    pub fn add_field(&mut self, field: SchemaField) {
        self.fields.push(field);
    }

    pub fn add_fields(&mut self, fields: Vec<SchemaField>) {
        self.fields.extend(fields);
    }

    fn next_field_id(&self) -> i32 {
        self.next_field_id.replace_with(|&mut old| old + 1)
    }

    pub fn new_primitive_field(
        &self,
        name: &str,
        r#type: PrimitiveType
    ) -> SchemaField {
        SchemaField::new(
            self.next_field_id(),
            name,
            false,
            SchemaType::Primitive(r#type)
        )
    }

    pub fn new_timestamp_field(&self, name: &str) -> SchemaField {
        self.new_primitive_field(name, PrimitiveType::Timestamp)
    }

    pub fn new_string_field(&self, name: &str) -> SchemaField {
        self.new_primitive_field(name, PrimitiveType::String)
    }

    pub fn new_int_field(&self, name: &str) -> SchemaField {
        self.new_primitive_field(name, PrimitiveType::Int)
    }

    pub fn new_long_field(&self, name: &str) -> SchemaField {
        self.new_primitive_field(name, PrimitiveType::Long)
    }

    pub fn new_boolean_field(&self, name: &str) -> SchemaField {
        self.new_primitive_field(name, PrimitiveType::Boolean)
    }

    pub fn new_double_field(&self, name: &str) -> SchemaField {
        self.new_primitive_field(name, PrimitiveType::Double)
    }

    pub fn new_list_field(&self, name: &str, subtype: SchemaType) -> SchemaField {
        SchemaField::new(
            self.next_field_id(),
            name,
            false,
            SchemaType::List(ListType::new(
                self.next_field_id(),
                true,
                subtype
            ))
        )
    }

    pub fn new_map_field(
        &self,
        name: &str,
        key_type: SchemaType,
        value_type: SchemaType,
        value_required: bool
    ) -> SchemaField {
        SchemaField::new(
            self.next_field_id(),
            name,
            false,
            SchemaType::Map(MapType::new(
                self.next_field_id(),
                key_type,
                self.next_field_id(),
                value_required,
                value_type
            )),
        )
    }

    pub fn new_struct_field(&self, name: &str, subfields: Vec<SchemaField>) -> SchemaField {
        SchemaField::new(
            self.next_field_id(),
            name,
            false,
            SchemaType::Struct(StructType::new(subfields))
        )
    }

    pub fn build(&mut self) -> Schema {
        Schema::new(self.schema_id, std::mem::take(&mut self.fields))
    }
}
