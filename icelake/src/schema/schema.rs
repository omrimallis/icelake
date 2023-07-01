//! Implementation of Iceberg data types and schemas.

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

impl std::fmt::Display for PrimitiveType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PrimitiveType::Boolean => write!(f, "bool"),
            PrimitiveType::Int => write!(f, "int"),
            PrimitiveType::Long => write!(f, "long"),
            PrimitiveType::Float => write!(f, "float"),
            PrimitiveType::Double => write!(f, "double"),
            PrimitiveType::Decimal { precision: p, scale: s } =>
                write!(f, "decimal({},{})", p, s),
            PrimitiveType::Date => write!(f, "date"),
            PrimitiveType::Time => write!(f, "time"),
            PrimitiveType::Timestamp => write!(f, "timestamp"),
            PrimitiveType::Timestamptz => write!(f, "timestamptz"),
            PrimitiveType::String => write!(f, "string"),
            PrimitiveType::Uuid => write!(f, "uuid"),
            PrimitiveType::Fixed(size) => write!(f, "fixed[{}]", size),
            PrimitiveType::Binary => write!(f, "binary"),
        }
    }
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
    pub fields: Vec<Field>,
}

impl StructType {
    pub fn new(fields: Vec<Field>) -> Self {
        let tag = Cow::Borrowed(STRUCT_TAG);
        Self { r#type: tag, fields: fields }
    }

    pub fn fields(&self) -> &[Field] {
        &self.fields
    }
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone)]
/// An Iceberg schema field.
pub struct Field {
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

impl Field {
    pub fn new(
        id: i32,
        name: &str,
        required: bool,
        r#type: SchemaType
    ) -> Self {
        Self {
            id: id,
            name: name.to_string(),
            required: required,
            r#type: r#type,
            doc: None,
        }
    }

    /// Creates a new `Field` with type [`SchemaType::Primitive`]
    pub fn new_primitive(
        id: i32,
        name: &str,
        required: bool,
        primitive: PrimitiveType
    ) -> Self {
        Self::new(
            id, name, required, SchemaType::Primitive(primitive)
        )
    }

    /// Creates a new `Field` with type [`SchemaType::Struct`]
    pub fn new_struct(
        id: i32,
        name: &str,
        required: bool,
        fields: impl IntoIterator<Item = Field>
    ) -> Self {
        Self::new(
            id, name, required,
            SchemaType::Struct(StructType::new(Vec::from_iter(fields)))
        )
    }

    /// Creates a new `Field` with type [`SchemaType::List`]
    pub fn new_list(
        id: i32,
        name: &str,
        required: bool,
        field: Field,
    ) -> Self {
        Self::new(
            id, name, required,
            SchemaType::List(ListType::new(
                field.id,
                field.required,
                field.r#type
            ))
        )
    }

    /// Creates a new `Field` with type [`SchemaType::Map`]
    pub fn new_map(
        id: i32,
        name: &str,
        required: bool,
        key: Field,
        value: Field,
    ) -> Self {
        Self::new(
            id, name, required,
            SchemaType::Map(MapType::new(
                key.id,
                key.r#type,
                value.id,
                value.required,
                value.r#type
            ))
        )
    }

    pub fn id(&self) -> i32 {
        self.id
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn required(&self) -> bool {
        self.required
    }

    pub fn doc(&self) -> Option<&str> {
        self.doc.as_ref().map(|x| x.as_str())
    }

    pub fn schema_type(&self) -> &SchemaType {
        &self.r#type
    }

    pub fn with_required(mut self, required: bool) -> Self {
        self.required = required;
        self
    }

    pub fn with_doc(mut self, doc: String) -> Self {
        self.doc = Some(doc);
        self
    }

    /// Assigns new ids from the `next_id` function to this field and all
    /// recursively nested fields.
    pub fn with_fresh_ids<F>(mut self, next_id: &mut F) -> Self
    where
        F: FnMut() -> i32
    {
        self.id = next_id();
        self.r#type = self.r#type.with_fresh_ids(next_id);
        self
    }

    /// Returns an iterator on all recursively nested fields inside this field,
    /// including `self`.
    pub fn all_fields(&self) -> Box<dyn Iterator<Item = &Self> + '_> {
        let iterator = std::iter::once(self);

        match self.schema_type() {
            SchemaType::Primitive(_) => {
                Box::new(iterator)
            },
            SchemaType::Struct(s) => {
                Box::new(iterator.chain(
                    s.fields()
                        .iter()
                        .flat_map(|field| field.all_fields())
                ))
            },
            SchemaType::List(l) => {
                Box::new(iterator.chain(
                    l.field().all_fields()
                ))
            },
            SchemaType::Map(m) => {
                Box::new(iterator.chain(
                    m.key().all_fields()
                ).chain(
                    m.value().all_fields()
                ))
            }
        }
    }
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone)]
#[serde(from = "ListTypeModel", into = "ListTypeModel")]
/// A field type that represents a list of identical elements.
pub struct ListType {
    field: Box<Field>
}

impl ListType {
    pub fn new(element_id: i32, element_required: bool, element: SchemaType) -> Self {
        Self {
            field: Box::new(Field::new(
                element_id,
                "element",
                element_required,
                element
            ))
        }
    }

    pub fn of_field(field: Field) -> Self {
        Self::new(field.id, field.required, field.r#type)
    }

    /// Returns a reference to the nested field element
    pub fn field(&self) -> &Field {
        &self.field
    }
}

/// Serializable `ListType`
#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone)]
#[serde(rename_all = "kebab-case")]
struct ListTypeModel {
    /// Always set to "list".
    r#type: Cow<'static, str>,
    /// Unique identifier for the element
    element_id: i32,
    /// If the element is mandatory.
    element_required: bool,
    /// The type of the element.
    element: SchemaType,
}

impl From<ListType> for ListTypeModel {
    fn from(l: ListType) -> Self {
        Self {
            r#type: Cow::Borrowed(LIST_TAG),
            element_id: l.field.id,
            element_required: l.field.required,
            element: l.field.r#type
        }
    }
}

impl From<ListTypeModel> for ListType {
    fn from(l: ListTypeModel) -> Self {
        Self::new(
            l.element_id,
            l.element_required,
            l.element
        )
    }
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone)]
#[serde(from = "MapTypeModel", into = "MapTypeModel")]
/// A complex field type that contains key-value pairs.
///
/// A map is a collection of key-value pairs with a key type and a value type.
/// Both the key field and value field each have an integer id that is unique
/// in the table schema. Map keys are required and map values can be either
/// optional or required. Both map keys and map values may be any type,
/// including nested types.
pub struct MapType {
    key: Box<Field>,
    value: Box<Field>,
}

impl MapType {
    pub fn new(
        key_id: i32,
        key_type: SchemaType,
        value_id: i32,
        value_required: bool,
        value_type: SchemaType
    ) -> Self {
        Self {
            key: Box::new(Field::new(
                key_id,
                "key",
                true,
                key_type
            )),
            value: Box::new(Field::new(
                value_id,
                "value",
                value_required,
                value_type
            ))
        }
    }

    pub fn key(&self) -> &Field {
        &self.key
    }

    pub fn value(&self) -> &Field {
        &self.value
    }
}

/// Serializable `MapType`
#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone)]
#[serde(rename_all = "kebab-case")]
struct MapTypeModel {
    /// Always set to "map".
    r#type: Cow<'static, str>,
    /// Unique key field id
    key_id: i32,
    /// Type of the map key
    key: SchemaType,
    /// Unique key for the value id
    value_id: i32,
    /// Indicates if the value is required.
    value_required: bool,
    /// Type of the value.
    value: SchemaType,
}

impl From<MapType> for MapTypeModel {
    fn from(m: MapType) -> Self {
        Self {
            r#type: Cow::Borrowed(MAP_TAG),
            key_id: m.key.id,
            key: m.key.r#type,
            value_id: m.value.id,
            value_required: m.value.required,
            value: m.value.r#type
        }
    }
}

impl From<MapTypeModel> for MapType {
    fn from(m: MapTypeModel) -> Self {
        Self::new(m.key_id, m.key, m.value_id, m.value_required, m.value)
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
    pub fn is_primitive(&self) -> bool {
        matches!(self, Self::Primitive(_))
    }

    pub fn is_struct(&self) -> bool {
        matches!(self, Self::Struct(_))
    }

    pub fn is_list(&self) -> bool {
        matches!(self, Self::List(_))
    }

    pub fn is_map(&self) -> bool {
        matches!(self, Self::Map(_))
    }

    /// Assigns new ids from the `next_id` function to this field and all
    /// recursively nested fields.
    ///
    /// Do not use directly. Use `Field::with_fresh_ids()` instead.
    fn with_fresh_ids<F>(self, next_id: &mut F) -> Self
    where
        F: FnMut() -> i32
    {
        match self {
            SchemaType::Primitive(p) => {
                SchemaType::Primitive(p)
            },
            SchemaType::Struct(s) => {
                SchemaType::Struct(StructType::new(
                    s.fields
                        .into_iter()
                        .map(|field| field.with_fresh_ids(next_id))
                        .collect()
                ))
            },
            SchemaType::List(l) => {
                SchemaType::List(ListType::new(
                    next_id(),
                    l.field.required,
                    l.field.r#type.with_fresh_ids(next_id)
                ))
            },
            SchemaType::Map(m) => {
                SchemaType::Map(MapType::new(
                    next_id(),
                    m.key.r#type.with_fresh_ids(next_id),
                    next_id(),
                    m.value.required,
                    m.value.r#type.with_fresh_ids(next_id)
                ))
            },
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
    /// Must be of variant `SchemaType::Struct`
    #[serde(flatten)]
    schema: SchemaType,
}

impl Schema {
    pub fn new(
        schema_id: i32,
        fields: Vec<Field>
    ) -> Self {
        Self {
            schema_id: schema_id,
            identifier_field_ids: None,
            schema: SchemaType::Struct(StructType::new(fields)),
        }
    }

    pub fn id(&self) -> i32 { self.schema_id }

    pub fn max_field_id(&self) -> i32 {
        self.all_fields().max_by_key(|f| f.id()).unwrap().id()
    }

    fn struct_type(&self) -> &StructType {
        match &self.schema {
            SchemaType::Struct(s) => s,
            _ => panic!("unexpected schema type")
        }
    }

    /// Returns a shared slice of the top-level fields in the schema.
    pub fn fields(&self) -> &[Field] {
        &self.struct_type().fields
    }

    /// Returns an iterator on all recursively nested fields in the schema in a
    /// depth-first order.
    ///
    /// A reference is returned for each primitive field,
    /// list element, nested struct field and map key value element.
    pub fn all_fields(&self) -> impl Iterator<Item = &Field> {
        self.struct_type().fields.iter()
            .flat_map(|field| field.all_fields())
    }

    /// Finds a top-level schema field by its name.
    pub fn get_field_by_name(&self, name: &str) -> Option<&Field> {
        self.fields().iter()
            .find(|field| field.name == name)
    }

    pub fn encode(&self) -> IcebergResult<String> {
        serde_json::to_string(self).map_err(|e| IcebergError::SchemaError {
            message: format!("error serializing table schema to json: {e}")
        })
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
    fields: Vec<Field>,
}

impl SchemaBuilder {
    pub fn new(schema_id: i32) -> Self {
        Self {
            schema_id: schema_id,
            next_field_id: RefCell::new(1),
            fields: Vec::new(),
        }
    }

    pub fn add_field(&mut self, field: Field) {
        self.fields.push(field);
    }

    pub fn add_fields(&mut self, fields: Vec<Field>) {
        self.fields.extend(fields);
    }

    fn next_field_id(&self) -> i32 {
        self.next_field_id.replace_with(|&mut old| old + 1)
    }

    pub fn new_primitive_field(
        &self,
        name: &str,
        r#type: PrimitiveType
    ) -> Field {
        Field::new(
            self.next_field_id(),
            name,
            false,
            SchemaType::Primitive(r#type)
        )
    }

    pub fn new_timestamp_field(&self, name: &str) -> Field {
        self.new_primitive_field(name, PrimitiveType::Timestamp)
    }

    pub fn new_string_field(&self, name: &str) -> Field {
        self.new_primitive_field(name, PrimitiveType::String)
    }

    pub fn new_int_field(&self, name: &str) -> Field {
        self.new_primitive_field(name, PrimitiveType::Int)
    }

    pub fn new_long_field(&self, name: &str) -> Field {
        self.new_primitive_field(name, PrimitiveType::Long)
    }

    pub fn new_boolean_field(&self, name: &str) -> Field {
        self.new_primitive_field(name, PrimitiveType::Boolean)
    }

    pub fn new_double_field(&self, name: &str) -> Field {
        self.new_primitive_field(name, PrimitiveType::Double)
    }

    pub fn new_list_field(&self, name: &str, subtype: SchemaType) -> Field {
        Field::new(
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
    ) -> Field {
        Field::new(
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

    pub fn new_struct_field(&self, name: &str, subfields: Vec<Field>) -> Field {
        Field::new(
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

#[cfg(test)]
mod tests {
    use super::*;

    fn create_schema(schema_id: i32) -> Schema {
        Schema::new(schema_id, vec![
            Field::new(
                1,
                "id",
                true,
                SchemaType::Primitive(PrimitiveType::Long)
            ),
            Field::new(
                2,
                "ts",
                false,
                SchemaType::Primitive(PrimitiveType::Timestamp)
            ),
            Field::new(
                3,
                "users",
                false,
                SchemaType::List(ListType::new(
                    4,
                    false,
                    SchemaType::Struct(StructType::new(vec![
                        Field::new(
                            5,
                            "first_name",
                            true,
                            SchemaType::Primitive(PrimitiveType::String)
                        ),
                        Field::new(
                            6,
                            "last_name",
                            true,
                            SchemaType::Primitive(PrimitiveType::String)
                        )
                    ]))
                ))
            ),
            Field::new(
                7,
                "props",
                false,
                SchemaType::Map(MapType::new(
                    8,
                    SchemaType::Primitive(PrimitiveType::String),
                    9,
                    false,
                    SchemaType::Primitive(PrimitiveType::Double)
                ))
            )
        ])
    }

    #[test]
    fn serialize_to_json() {
        let schema = create_schema(0);

        let expected = serde_json::json!({
            "schema-id": 0,
            "type": "struct",
            "fields": [
                {
                    "id": 1,
                    "name": "id",
                    "required": true,
                    "type": "long"
                },
                {
                    "id": 2,
                    "name": "ts",
                    "required": false,
                    "type": "timestamp"
                },
                {
                    "id": 3,
                    "name": "users",
                    "required": false,
                    "type": {
                        "type": "list",
                        "element-id": 4,
                        "element-required": false,
                        "element": {
                            "type": "struct",
                            "fields": [
                                {
                                    "id": 5,
                                    "name": "first_name",
                                    "required": true,
                                    "type": "string"
                                },
                                {
                                    "id": 6,
                                    "name": "last_name",
                                    "required": true,
                                    "type": "string"
                                }
                            ]
                        }
                    }
                },
                {
                    "id": 7,
                    "name": "props",
                    "required": false,
                    "type": {
                        "type": "map",
                        "key-id": 8,
                        "key": "string",
                        "value-id": 9,
                        "value-required": false,
                        "value": "double"
                    }
                }
            ]
        });

        assert_eq!(serde_json::to_value(schema).unwrap(), expected);
    }

    #[test]
    fn deserialize_from_json() {
        let schema = serde_json::from_value::<Schema>(serde_json::json!({
            "schema-id": 0,
            "type": "struct",
            "fields": [
                {
                    "id": 1,
                    "name": "id",
                    "required": true,
                    "type": "long"
                },
                {
                    "id": 2,
                    "name": "ts",
                    "required": false,
                    "type": "timestamp"
                },
                {
                    "id": 3,
                    "name": "users",
                    "required": false,
                    "type": {
                        "type": "list",
                        "element-id": 4,
                        "element-required": false,
                        "element": {
                            "type": "struct",
                            "fields": [
                                {
                                    "id": 5,
                                    "name": "first_name",
                                    "required": true,
                                    "type": "string"
                                },
                                {
                                    "id": 6,
                                    "name": "last_name",
                                    "required": true,
                                    "type": "string"
                                }
                            ]
                        }
                    }
                },
                {
                    "id": 7,
                    "name": "props",
                    "required": false,
                    "type": {
                        "type": "map",
                        "key-id": 8,
                        "key": "string",
                        "value-id": 9,
                        "value-required": false,
                        "value": "double"
                    }
                }
            ]
        })).unwrap();

        let expected = create_schema(0);

        assert_eq!(schema, expected);
    }

    #[test]
    fn max_field_id() {
        assert_eq!(create_schema(0).max_field_id(), 9)
    }

    #[test]
    fn all_fields() {
        let schema = create_schema(0);
        let all_fields = schema.all_fields().collect::<Vec<_>>();

        assert_eq!(all_fields[0].id(), 1);
        assert_eq!(all_fields[0].name(), "id");
        assert!(matches!(all_fields[0].schema_type(), SchemaType::Primitive(_)));

        assert_eq!(all_fields[3].id(), 4);
        assert_eq!(all_fields[3].name(), "element");
        assert!(matches!(all_fields[3].schema_type(), SchemaType::Struct(_)));

        assert_eq!(all_fields[4].id(), 5);
        assert_eq!(all_fields[4].name(), "first_name");
        assert!(matches!(all_fields[4].schema_type(), SchemaType::Primitive(_)));
    }

    #[test]
    fn fresh_ids() {
        let field = Field::new(
            1,
            "users",
            false,
            SchemaType::List(ListType::new(
                2,
                false,
                SchemaType::Struct(StructType::new(vec![
                    Field::new(
                        3,
                        "first_name",
                        true,
                        SchemaType::Primitive(PrimitiveType::String)
                    ),
                    Field::new(
                        4,
                        "last_name",
                        true,
                        SchemaType::Primitive(PrimitiveType::String)
                    )
                ]))
            ))
        );

        let mut next_id = 10;
        let field = field.with_fresh_ids(&mut || { next_id += 1; next_id });
        assert_eq!(field.id, 11);

        let list_type = match field.r#type {
            SchemaType::List(l) => l,
            _ => panic!(),
        };
        assert_eq!(list_type.field().id(), 12);

        let struct_type = match list_type.field().schema_type() {
            SchemaType::Struct(s) => s,
            _ => panic!(),
        };

        assert_eq!(struct_type.fields[0].id, 13);
        assert_eq!(struct_type.fields[1].id, 14);
    }
}
