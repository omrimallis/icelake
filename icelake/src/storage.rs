//! Backing storage for Iceberg tables.
use std::sync::Arc;
use std::collections::HashMap;

use url::Url;
use bytes::Bytes;
use object_store::{
    ObjectStore,
    Error as ObjectStoreError,
    local::LocalFileSystem,
    aws::AmazonS3Builder,
    path::{Path, PathPart},
    path::Error as PathError
};
use chrono;
use futures::StreamExt;

use crate::{IcebergResult, IcebergError};

enum IcebergStorageType {
    Local,
    S3
}

/// Represents a storage engine for an Iceberg table.
pub struct IcebergStorage {
    // URI containing the location of the table, e.g.
    // file:///path/to/table or s3://bucket-name/path/to/table
    location: Url,
    // Object storage backend, e.g. a cloud object store like S3 or an abstraction
    // over the local file system.
    object_store: Arc<dyn ObjectStore>,
    storage_type: IcebergStorageType,
}

impl IcebergStorage {
    /// Ensures a url to a local directory is valid, normalizes it
    /// and creates missing directories.
    fn setup_local_path(location: Url) -> IcebergResult<Url> {
        let path = location.to_file_path().map_err(|_| {
            IcebergError::InvalidTableLocation(format!(
                "Invalid local table location: {}", location
            ))
        })?;

        if path.exists() {
            if !path.is_dir() {
                return Err(IcebergError::InvalidTableLocation(format!(
                    "Table location exists, but is not a directory: {}", location
                )));
            }
        } else {
            std::fs::create_dir_all(&path).map_err(|_| {
                IcebergError::InvalidTableLocation(format!(
                    "Could not create local directory: {}", path.display()
                ))
            })?;
        }

        let path = std::fs::canonicalize(path).map_err(|_| {
            IcebergError::InvalidTableLocation(format!(
                "Failed to canonicalize location: {}", location
            ))
        })?;

        let url = Url::from_directory_path(path).map_err(|_| {
            IcebergError::InvalidTableLocation(format!(
                "Directory path must be absolute: {}", location
            ))
        })?;

        Ok(url)
    }

    /// Initiailizes a new IcebergStorage from a URL and storage options.
    ///
    /// The URL determines the type of the backing object store. For example,
    /// `s3://bucket/table/` will create an S3-backed Iceberg table and
    /// `file:///path/to/table` will create it on the local filesystem.
    ///
    /// `storage_options` may include specific options like access credentials.
    /// Valid keys are:
    /// * S3 tables - `"aws_access_key_id"`, `"aws_secret_access_key"`, `"aws_region"`.
    pub fn from_url(
        location: &str,
        storage_options: HashMap<String, String>
    ) -> IcebergResult<Self> {
        // Always store the URL as a directory, needed when
        // calculcating relative paths using Url::make_relative().
        let mut location = location.to_string();
        if !location.ends_with("/") {
            location.push_str("/");
        }

        let url = Url::parse(&location).map_err(|e| {
            IcebergError::InvalidTableLocation(format!(
                "Invalid table url {}: {}", location, e
            ))
        })?;

        if url.scheme() == "file" {
            let url = IcebergStorage::setup_local_path(url)?;
            let object_store = LocalFileSystem::new_with_prefix(url.path())?;

            Ok(IcebergStorage {
                location: url,
                object_store: Arc::new(object_store),
                storage_type: IcebergStorageType::Local
            })
        } else if url.scheme() == "s3" {
            let bucket = url.host_str()
                .ok_or_else(|| {
                    IcebergError::InvalidTableLocation(format!(
                        "Missing S3 bucket name: {}", location
                    ))
                })?;

            let object_store = AmazonS3Builder::new()
                .with_bucket_name(bucket)
                .try_with_options(storage_options)?
                .build()?;

            Ok(IcebergStorage {
                location: url,
                object_store: Arc::new(object_store),
                storage_type: IcebergStorageType::S3
            })
        } else {
            Err(IcebergError::InvalidTableLocation(format!(
                "URL scheme {} not supported for Iceberg tables",
                url.scheme()
            )))
        }
    }

    pub fn location(&self) -> &str {
        self.location.as_str().trim_end_matches("/")
    }

    /// Returns a reference to the underlying object store.
    pub fn object_store(&self) -> Arc<dyn object_store::ObjectStore> {
        self.object_store.clone()
    }

    /// Converts an Iceberg table path to the object store path.
    ///
    /// May return `None` only when `None` is given, i.e. the path is empty.
    ///
    /// # Arguments
    ///
    /// * `path` - A path relative to the table's location.
    fn to_object_store_path(&self, path: Option<&IcebergPath>) -> Option<Path> {
        match self.storage_type {
            IcebergStorageType::Local => {
                // On local storage we use the path relative to the table's location.
                path.map(|p| p.inner.clone())
            },
            IcebergStorageType::S3 => {
                // On S3 storage we have to use the full path inside the bucket.
                let prefix = self.location.path().trim_end_matches("/");
                match path {
                    Some(path) => {
                        Some(Path::from(format!(
                            "{}/{}", prefix, path.as_ref()
                        )))
                    },
                    None => {
                        Some(Path::from(prefix))
                    }
                }
            }
        }
    }

    /// Converts an object store path to an Iceberg table path.
    /// May fail if the path is not under the table's location.
    fn to_iceberg_path(&self, path: Path) -> IcebergResult<IcebergPath> {
        match self.storage_type {
            IcebergStorageType::Local => {
                Ok(IcebergPath { inner: path })
            },
            IcebergStorageType::S3 => {
                // On S3, the object store path is relative to the bucket, not to the
                // table's location. Manually remove the table's path from each object.
                let prefix = self.location.path();

                path.prefix_match(&Path::from(prefix))
                    .map(|parts| {
                        IcebergPath { inner: Path::from_iter(parts) }
                    })
                    .ok_or_else(|| {
                        IcebergError::InvalidPath {
                            source: PathError::PrefixMismatch {
                                path: path.to_string(),
                                prefix: prefix.to_string()
                            }
                        }
                    })
            }
        }
    }

    /// Wraps the put() method of the underlying object store.
    pub async fn put(&self, path: &IcebergPath, bytes: Bytes) -> IcebergResult<()> {
        self.object_store
            .put(&self.to_object_store_path(Some(path)).unwrap(), bytes)
            .await?;

        Ok(())
    }

    /// Wraps the get() method of the underlying object store.
    pub async fn get(&self, path: &IcebergPath) -> IcebergResult<Bytes> {
        let res = self.object_store
            .get(&self.to_object_store_path(Some(path)).unwrap())
            .await?;

        let bytes = res.bytes().await?;

        Ok(bytes)
    }

    /// Wraps the `delete()` method of the underlying object store.
    pub async fn delete(&self, path: &IcebergPath) -> IcebergResult<()> {
        self.object_store.delete(
            &self.to_object_store_path(Some(path)).unwrap()
        ).await?;

        Ok(())
    }

    pub async fn list(
        &self,
        path: Option<&IcebergPath>
    ) -> IcebergResult<Vec<IcebergObjectMeta>> {
        let mut stream = self.object_store
            .list(self.to_object_store_path(path).as_ref())
            .await?;

        let mut objects: Vec<IcebergObjectMeta> = Vec::new();

        while let Some(obj_meta) = stream.next().await {
            // Exit early if any objects can't be listed.
            // We exclude the special case of a not found error on some of the list entities.
            // This error mainly occurs for local stores when a temporary file has been deleted by
            // concurrent writers or if the table is vacuumed by another client.
            let obj_meta = match obj_meta {
                Ok(meta) => Ok(meta),
                Err(ObjectStoreError::NotFound { .. }) => continue,
                Err(err) => Err(err),
            }?;

            objects.push(IcebergObjectMeta {
                location: self.to_iceberg_path(obj_meta.location)?,
                last_modified: obj_meta.last_modified,
                size: obj_meta.size
            });
        }

        Ok(objects)
    }

    /// Creates an IcebergPath with the relative path to a table object from
    /// its full URL.
    pub fn create_path_from_url(&self, url: &str) -> IcebergResult<IcebergPath> {
        let url = Url::parse(url).map_err(|_| {
            IcebergError::InvalidTableLocation(format!(
                "Invalid absolute object path: {}", url
            ))
        })?;

        let relative_path = self.location.make_relative(&url).ok_or(
            IcebergError::InvalidTableLocation(format!(
                "Invalid Object path {}: can't be made relative to table's location",
                url
            ))
        )?;

        Ok(IcebergPath { inner: Path::parse(relative_path)? })
    }

    pub fn to_uri(&self, path: &IcebergPath) -> String {
        format!(
            "{}/{}",
            (&self.location.as_str()[..]).trim_end_matches('/'),
            path.as_ref()
        )
    }
}

/// Represents a path to a data or metadata file in an Iceberg table.
///
/// This struct wraps object_store::path::Path to handle complexities arising from
/// differences in how object_store handles local filesystems and object stores.
///
/// An IcebergPath is always relative to the location of the table.
#[derive(Clone)]
pub struct IcebergPath {
    inner: object_store::path::Path,
}

impl IcebergPath {
    pub fn to_string(&self) -> String {
        self.as_ref().to_string()
    }

    pub fn from_url(url: &str) -> IcebergResult<Self> {
        Ok(Self {
            inner: object_store::path::Path::from_url_path(url)?
        })
    }

    pub fn filename(&self) -> Option<&str> {
        self.inner.filename()
    }
}

impl AsRef<str> for IcebergPath {
    fn as_ref(&self) -> &str {
        self.inner.as_ref()
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

impl<I> FromIterator<I> for IcebergPath
where
    I: Into<String>
{
    fn from_iter<T: IntoIterator<Item = I>>(iter: T) -> Self {
        Self {
            inner: Path::from_iter(
               T::into_iter(iter).map(|s| PathPart::from(s.into()))
            )
        }
    }
}

impl std::fmt::Display for IcebergPath {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.inner.fmt(f)
    }
}

/// Contains metadata about an object in an Iceberg table.
pub struct IcebergObjectMeta {
    /// Location of the object relative to the table's location.
    pub location: IcebergPath,
    /// Last modification time.
    pub last_modified: chrono::DateTime<chrono::offset::Utc>,
    /// Size of the object in bytes.
    pub size: usize,
}
