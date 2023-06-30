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
mod schema;
mod update;

pub use self::schema::{
    Schema, SchemaBuilder, SchemaField, StructField,
    SchemaType, PrimitiveType, StructType, ListType, MapType
};

pub use self::update::{
    SchemaUpdate
};
