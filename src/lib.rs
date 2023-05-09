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
pub mod builder;
pub mod storage;
pub mod transaction;

pub use iceberg::{IcebergTable, IcebergTableVersion, IcebergTableMetadata};
pub use builder::IcebergTableBuilder;

pub type IcebergResult<T> = Result<T, IcebergError>;

#[derive(thiserror::Error, Debug)]
pub enum IcebergError {
    #[error("Iceberg error: {message}")]
    CustomError { message: String },

    #[error("Iceberg table not initialized")]
    TableNotInitialized,

    #[error("Can't create table with empty schema")]
    EmptySchema,

    #[error("Schema with schema id {schema_id} not found in schema list")]
    SchemaNotFound { schema_id: i32 },

    #[error("Error serializing table metadata to json: {source}")]
    SerializeMetadataJson {#[from] source: serde_json::Error},

    #[error("Error serializeing table schema to json: {source}")]
    SerializeSchemaJson { source: serde_json::Error },

    #[error("Error serializing manifest file to Avro: {source}")]
    SerializeManifestFile {#[from] source: apache_avro::Error},

    #[error("Invalid object store path: {source}")]
    InvalidPath {#[from] source: object_store::path::Error},

    #[error("Object storage error: {source}")]
    Storage {#[from] source: object_store::Error},
}
