//! Tests for basic transaction operations.
use uuid::Uuid;

use icelake::{IcebergTable, IcebergTableLoader};
use icelake::schema::{Schema, Field, PrimitiveType};
use icelake::transaction::{AppendFilesOperation, OverwriteFilesOperation};
use icelake::manifest::{
    Manifest, ManifestFile, ManifestReader,
    DataFile, DataFileContent, DataFileFormat
};

/// Wraps IcebergTable with temporary directory creation and deletion and a few
/// helper functions.
struct TestTable {
    path: std::path::PathBuf,
    table: IcebergTable
}

impl TestTable {
    fn schema() -> Schema {
        Schema::new(0, vec![
            Field::new_primitive(1, "id", true, PrimitiveType::Long),
            Field::new_primitive(2, "ts", false, PrimitiveType::Timestamp),
            Field::new_primitive(3, "name", false, PrimitiveType::String),
        ])
    }

    fn new_datafile(
        &self,
        record_count: i64,
        file_size_in_bytes: i64,
    ) -> DataFile {
        let mut path = self.path.clone();
        path.push("metadata");
        path.push(format!("{}.parquet", Uuid::new_v4()));

        DataFile::builder(
            DataFileContent::Data,
            path.to_str().unwrap(),
            DataFileFormat::Parquet,
            record_count,
            file_size_in_bytes
        ).build()
    }

    async fn new() -> Self {
        let mut path = std::env::temp_dir();
        path.push("icelake");
        path.push(format!("tbl-{}", Uuid::new_v4().to_string()));

        let table = IcebergTableLoader::from_url(
            &format!("file://{}", path.to_str().unwrap())
        ).with_schema(Self::schema())
            .create()
            .await
            .unwrap();

        Self { path, table }
    }

    async fn read_manifest(&self, manifest_file: &ManifestFile) -> Manifest {
        let storage = self.table.storage();
        let path = storage.create_path_from_url(&manifest_file.manifest_path).unwrap();
        let bytes = storage.get(&path).await.unwrap();
        ManifestReader::new().read(&bytes).unwrap()
    }
}

impl Drop for TestTable {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir(&self.path);
    }
}

impl std::ops::Deref for TestTable {
    type Target = IcebergTable;

    fn deref(&self) -> &Self::Target {
        &self.table
    }
}

impl std::ops::DerefMut for TestTable {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.table
    }
}

#[tokio::test]
async fn append_operation() {
    let mut table = TestTable::new().await;

    let datafile1 = table.new_datafile(1111, 10111);
    let datafile2 = table.new_datafile(2222, 20222);

    // Append two fake files in two operations, to create two manifests.
    let mut transaction = table.new_transaction();
    let mut op = AppendFilesOperation::new();
    op.append_file(datafile1);
    transaction.add_operation(Box::new(op));
    transaction.commit().await.unwrap();

    let mut transaction = table.new_transaction();
    let mut op = AppendFilesOperation::new();
    op.append_file(datafile2);
    transaction.add_operation(Box::new(op));
    transaction.commit().await.unwrap();

    let snapshot = table.current_snapshot().unwrap().unwrap();
    let manifest_list = table.read_manifest_list(&snapshot).await.unwrap();
    let manifest_files = manifest_list.manifest_files();

    assert_eq!(manifest_files.len(), 2);

    assert_eq!(manifest_files[0].sequence_number, 1);
    assert_eq!(manifest_files[0].added_data_files_count, 1);
    assert_eq!(manifest_files[0].added_rows_count, 1111);
    assert_eq!(manifest_files[0].existing_data_files_count, 0);

    assert_eq!(manifest_files[1].sequence_number, 2);
    assert_eq!(manifest_files[1].added_data_files_count, 1);
    assert_eq!(manifest_files[1].added_rows_count, 2222);
    assert_eq!(manifest_files[1].existing_data_files_count, 0);
}

#[tokio::test]
async fn overwrite_operation() {
    let mut table = TestTable::new().await;

    let datafile1 = table.new_datafile(1111, 10111);
    let datafile2 = table.new_datafile(2222, 20222);
    let datafile3 = table.new_datafile(3333, 30333);

    // Append two fake files in two operations, to create two manifests.
    let mut transaction = table.new_transaction();
    let mut op = AppendFilesOperation::new();
    op.append_file(datafile1.clone());
    transaction.add_operation(Box::new(op));
    transaction.commit().await.unwrap();

    let mut transaction = table.new_transaction();
    let mut op = AppendFilesOperation::new();
    op.append_file(datafile2.clone());
    transaction.add_operation(Box::new(op));
    transaction.commit().await.unwrap();

    // Use an overwrite operation to replace the first data file
    let mut transaction = table.new_transaction();
    let mut op = OverwriteFilesOperation::new();
    op.delete_file(&datafile1.file_path);
    op.append_file(datafile3.clone());
    transaction.add_operation(Box::new(op));
    transaction.commit().await.unwrap();
    let snapshot = table.current_snapshot().unwrap().unwrap();
    let manifest_list = table.read_manifest_list(&snapshot).await.unwrap();
    let manifest_files = manifest_list.manifest_files();

    // The overwrite operation is expected to merge the first two manifests
    // into a new single manifest.
    assert_eq!(manifest_files.len(), 1);
    assert_eq!(manifest_files[0].sequence_number, 3);
    assert_eq!(manifest_files[0].added_data_files_count, 1);
    assert_eq!(manifest_files[0].added_rows_count, 3333);
    assert_eq!(manifest_files[0].existing_data_files_count, 1);
    assert_eq!(manifest_files[0].existing_rows_count, 2222);
    assert_eq!(manifest_files[0].deleted_data_files_count, 1);
    assert_eq!(manifest_files[0].deleted_rows_count, 1111);

    let manifest = table.read_manifest(&manifest_files[0]).await;
    assert_eq!(manifest.entries().len(), 3);

    let entry = manifest.entries().iter().find(|entry| entry.deleted()).unwrap();
    assert_eq!(entry.data_file(), &datafile1);

    let entry = manifest.entries().iter().find(|entry| entry.existing()).unwrap();
    assert_eq!(entry.data_file(), &datafile2);

    let entry = manifest.entries().iter().find(|entry| entry.added()).unwrap();
    assert_eq!(entry.data_file(), &datafile3);
}
