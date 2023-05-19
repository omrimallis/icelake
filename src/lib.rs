use thiserror;
use object_store;
use serde_json;

mod utils;

pub mod iceberg;
pub mod schema;
pub mod snapshot;
pub mod partition;
pub mod sort;
pub mod manifest;
pub mod storage;
pub mod transaction;

pub use crate::iceberg::{
    IcebergTable, IcebergTableVersion, IcebergTableMetadata,
    IcebergTableLoader
};

pub type IcebergResult<T> = Result<T, IcebergError>;

#[derive(thiserror::Error, Debug)]
pub enum IcebergError {
    #[error("Iceberg error: {message}")]
    CustomError { message: String },

    /// The table's metadata file could not be located. Usually this means the table
    /// does not exist at the specified location.
    #[error("Iceberg table metadata not found at {0}")]
    MetadataNotFound(String),

    #[error("Invalid table location: {0}")]
    InvalidTableLocation(String),

    #[error("Iceberg table not initialized")]
    TableNotInitialized,

    #[error("Can't create table with empty schema")]
    EmptySchema,

    #[error("Schema with schema id {schema_id} not found in schema list")]
    SchemaNotFound { schema_id: i32 },

    #[error("Error serializing table metadata to json: {source}")]
    SerializeMetadataJson {source: serde_json::Error},

    #[error("Error deserializing table metadata from json: {source}")]
    InvalidMetadata {source: serde_json::Error},

    #[error("Error serializing table schema to json: {source}")]
    SerializeSchemaJson { source: serde_json::Error },

    /// Generic JSON serialization error.
    #[error("Error serializing json")]
    SerializeJson { source: serde_json::Error },

    #[error("Error serializing or deserializing Avro: {source}")]
    AvroError {#[from] source: apache_avro::Error},

    #[error("Invalid object store path: {source}")]
    InvalidPath {#[from] source: object_store::path::Error},

    #[error("Object storage error: {source}")]
    ObjectStore {#[from] source: object_store::Error},
}
