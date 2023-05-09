use std::sync::Arc;

use bytes::Bytes;
use object_store;

use crate::{IcebergResult, IcebergError};

/// Represents a storage engine for an Iceberg table.
pub struct IcebergObjectStore {
    // URI containing the location of the table, e.g.
    // /path/to/table or s3://bucket-name/path/to/table
    location: String,
    // Object storage backend, e.g. a cloud object store like S3 or an abstraction
    // over the local file system.
    storage: Arc<dyn object_store::ObjectStore>,
}

impl IcebergObjectStore {
    pub fn new(
        storage: Arc<dyn object_store::ObjectStore>,
        location: String
    ) -> Self {
        Self {
            storage: storage,
            location: location
        }
    }

    pub fn from_uri(location: String) -> IcebergResult<Self> {
        // TODO: Infer storage system from URI (e.g. file, S3, etc.)
        // TODO: Extra validations, see implementation in DeltaTableBuilder
        // TODO: Create folder if not exists
        // TODO: Need to ensure that `location` matches the prefix defined in `storage`.
        
        let storage = object_store::local::LocalFileSystem::new_with_prefix(&location)?;
        Ok(Self {
            storage: Arc::new(storage),
            location: location
        })
    }

    pub fn location(&self) -> &str {
        self.location.as_ref()
    }

    pub async fn put(
        &self,
        path: &IcebergPath,
        bytes: Bytes
    ) -> IcebergResult<()> {
        self.storage
            .put(&path.inner, bytes) 
            .await
            .map_err(|e| IcebergError::Storage { source: e })
    }

    pub async fn get(&self, path: &IcebergPath) -> IcebergResult<Bytes> {
        let res = self.storage
            .get(&path.inner)
            .await?;

        let bytes = res.bytes().await?;

        Ok(bytes)
    }

    /// Creates a path to a new metadata file in the table backed by this storage.
    pub fn create_metadata_path(&self, relative_path: &str) -> IcebergPath {
        IcebergPath {
            inner: object_store::path::Path::from_iter([
                // TODO: Remove the hardcoded "iceberg_rs" path
                "iceberg_rs", "metadata", relative_path
            ])
        }
    }

    /// Creates a path to a new data file in the table backed by this storage.
    pub fn create_data_path(&self, relative_path: &str) -> IcebergPath {
        IcebergPath {
            inner: object_store::path::Path::from_iter([
                // TODO: Remove the hardcoded "iceberg_rs" path
                "iceberg_rs", "data", relative_path
            ])
        }
    }

    pub fn to_uri(&self, path: &IcebergPath) -> String {
        format!(
            "{}/{}",
            (&self.location[..]).trim_end_matches('/'),
            path.as_ref()
        )
    }
}

/// Represents a path to a data or metadata file in an Iceberg table.
pub struct IcebergPath {
    inner: object_store::path::Path,
}

impl IcebergPath {
    pub fn to_string(&self) -> String {
        self.as_ref().to_string()
    }

    pub fn as_ref(&self) -> &str {
        self.inner.as_ref()
    }

    pub fn from_url(url: &str) -> IcebergResult<Self> {
        Ok(Self {
            inner: object_store::path::Path::from_url_path(url)?
        })
    }
}

impl From<&str> for IcebergPath {
    fn from(value: &str) -> Self {
        Self { inner: object_store::path::Path::from(value) }
    }
}

impl From<String> for IcebergPath {
    fn from(value: String) -> Self {
        Self { inner: object_store::path::Path::from(value) }
    }
}
