//! Iceberg table schema implementation.
//!
//! This module provides [`Schema`] which represents the schema of an Iceberg table. Each
//! schema has a `schema_id` and a vector of fields represented by [`Field`].
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
//! use icelake::schema::{Schema, Field, SchemaType, PrimitiveType};
//!
//! let schema_id = 0;
//! let schema = Schema::new(schema_id, vec![
//!     Field::new(
//!         0,
//!         "id",
//!         true,
//!         SchemaType::Primitive(PrimitiveType::Long)
//!     ),
//!     Field::new(
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
//! use icelake::schema::{Schema, Field, SchemaType, StructType, PrimitiveType};
//!
//! let schema_id = 0;
//! let schema = Schema::new(schema_id, vec![
//!     Field::new(
//!         0,
//!         "user",
//!         false,
//!         SchemaType::Struct(StructType::new(vec![
//!             Field::new(
//!                 1,
//!                 "first_name",
//!                 true,
//!                 SchemaType::Primitive(PrimitiveType::String)
//!             ),
//!             Field::new(
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
pub mod update;
pub mod arrow;

pub use self::schema::{
    Schema, SchemaBuilder, Field,
    SchemaType, PrimitiveType, StructType, ListType, MapType
};
