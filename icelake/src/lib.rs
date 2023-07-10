//! Native Iceberg table implementation in Rust.
//!
//! [Apache Iceberg](https://iceberg.apache.org/) is an open table format for huge
//! analytic tables on data lakes.
//!
//! This crate provides an interface for creating, loading and managing Iceberg
//! tables from native Rust code, with a future Python wrapper being worked at.
//!
//! Loading or creating Iceberg tables is done with the [`IcebergTableLoader`]
//! struct. The returned [`IcebergTable`] is the main interface for managing the table
//! after it has been loaded or created.
//!
//! ## Working with Iceberg tables on S3
//!
//! Load an Iceberg table from S3, or create it with the given schema if it does not
//! exist:
//! ```rust
//! use std::collections::HashMap;
//! use icelake::{IcebergTableLoader, IcebergResult};
//! use icelake::schema::SchemaBuilder;
//!
//! #[tokio::main]
//! async fn main() -> IcebergResult<()> {
//!     let schema_id = 0;
//!     let mut schema_builder = SchemaBuilder::new(schema_id);
//!     schema_builder.add_fields(vec![
//!         schema_builder.new_int_field("id").with_required(true),
//!         schema_builder.new_int_field("user_id"),
//!         schema_builder.new_timestamp_field("ts"),
//!     ]);
//!     let schema = schema_builder.build();
//!
//!     let storage_options = HashMap::from([
//!         ("aws_region".to_string(), "us-east-1".to_string()),
//!         ("aws_bucket_name".to_string(), "icerberg-bucket".to_string()),
//!         ("aws_access_key_id".to_string(), "A...".to_string()),
//!         ("aws_secret_access_key".to_string(), "eH...".to_string())
//!     ]);
//!
//!     let table = IcebergTableLoader::from_url("s3://iceberg-bucket/users")
//!         .with_storage_options(storage_options)
//!         .with_schema(schema)
//!         .load_or_create()
//!         .await;
//!
//!     match table {
//!         Ok(table) => {
//!             println!("Table loaded or created");
//!             let current_snapshot = table.current_snapshot()?;
//!             if let Some(current_snapshot) = current_snapshot {
//!                 println!("Current snapshot id: {}", current_snapshot.snapshot_id);
//!             }
//!         },
//!         Err(..) => println!("Failed loading or creating table"),
//!     }
//!
//!     Ok(())
//! }
//! ```
use thiserror;
use object_store;
use serde_json;
use arrow_schema;

mod utils;

pub mod iceberg;
pub mod schema;
pub mod value;
pub mod snapshot;
pub mod partition;
pub mod sort;
pub mod manifest;
pub mod storage;
pub mod transaction;
pub mod writer;

pub use crate::iceberg::{
    IcebergTable, IcebergTableVersion, IcebergTableMetadata,
    IcebergTableLoader, IcebergFile
};

/// A result type returned by functions in this crate.
pub type IcebergResult<T> = Result<T, IcebergError>;

/// An Iceberg table error.
#[derive(thiserror::Error, Debug)]
pub enum IcebergError {
    #[error("Iceberg error: {message}")]
    CustomError { message: String },

    #[error("unsupported: {0}")]
    Unsupported(String),

    /// The table's metadata file could not be located. Usually this means the table
    /// does not exist at the specified location.
    #[error("Iceberg table metadata not found at {0}")]
    MetadataNotFound(String),

    /// The URL location specified for the table is invalid: It might have an invalid
    /// URL scheme, point to an invalid path or path that is not a directory when using
    /// local file systems.
    #[error("invalid table location: {0}")]
    InvalidTableLocation(String),

    #[error("Iceberg table already exists at: {0}")]
    TableAlreadyExists(String),

    /// An operation has been attempted on an Iceberg table that was not initialized
    /// and therefore has no [IcebergTableMetadata] associated with it.
    #[error("Iceberg table not initialized")]
    TableNotInitialized,

    /// An error with creating an Iceberg table schema.
    #[error("schema error: {message}")]
    SchemaError { message: String },

    #[error("value error: {0}")]
    ValueError(String),

    /// An error with manifest files.
    #[error("manifest error: {0}")]
    ManifestError(String),

    /// Error with Iceberg table partitioning.
    #[error("partition error: {message}")]
    PartitionError { message: String },

    /// Failed serializing the table's metadata to json.
    #[error("error serializing table metadata to json: {source}")]
    SerializeMetadataJson {source: serde_json::Error},

    /// Attempted to parse an invalid metadata file.
    #[error("error deserializing table metadata from json: {source}")]
    InvalidMetadata {source: serde_json::Error},

    /// Failed serializing the table's schema to json.
    #[error("error serializing table schema to json: {source}")]
    SerializeSchemaJson { source: serde_json::Error },

    /// Generic JSON serialization error.
    #[error("error serializing json")]
    SerializeJson { source: serde_json::Error },

    /// Failed serializing or deserializing an Avro file
    /// (manifest or manifest list files).
    #[error("error serializing or deserializing Avro")]
    AvroError {#[from] source: apache_avro::Error},

    /// A path to an object that is not in the table's location was ecnountered.
    #[error("invalid object store path")]
    InvalidPath {#[from] source: object_store::path::Error},

    /// An error from the underlying object storage.
    #[error("object store error")]
    ObjectStore {#[from] source: object_store::Error},

    /// An error related to the Parquet file format.
    #[error("Parquet error")]
    ParquetError {#[from] source: parquet::errors::ParquetError},

    /// A system I/O error
    #[error("I/O error")]
    IoError {#[from] source: std::io::Error},

    /// Apache Arrow error
    #[error("Arrow error")]
    ArrowError{#[from] source: arrow_schema::ArrowError},
}
