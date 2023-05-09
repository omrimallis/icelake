use std::sync::Arc;
use std::collections::HashMap;

use crate::IcebergResult;
use crate::iceberg::IcebergTable;
use crate::storage::IcebergObjectStore;

use object_store::ObjectStore;

pub struct IcebergTableBuilder {
    table_uri: String,
    storage_backend: Option<Arc<dyn ObjectStore>>,
    storage_options: Option<HashMap<String, String>>,
}

impl IcebergTableBuilder {
    pub fn from_uri(table_uri: &str) -> Self {
        Self {
            table_uri: String::from(table_uri),
            storage_backend: None,
            storage_options: None,
        }
    }

    pub fn with_storage_options(mut self, storage_options: HashMap<String, String>) -> Self {
        self.storage_options = Some(storage_options);
        self
    }

    pub fn with_storage_backend(mut self, storage_backend: Arc<dyn ObjectStore>) -> Self {
        self.storage_backend = Some(storage_backend);
        self
    }

    pub fn build_storage(self) -> IcebergResult<IcebergObjectStore> {
        match self.storage_backend {
            Some(storage) => {
                Ok(IcebergObjectStore::new(storage, self.table_uri))
            },
            None => {
                IcebergObjectStore::from_uri(self.table_uri)
            }
        }
    }

    pub fn build(self) -> IcebergResult<IcebergTable> {
        Ok(IcebergTable::new(self.build_storage()?))
    }
}
