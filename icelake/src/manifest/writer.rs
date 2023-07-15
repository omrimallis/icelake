use bytes::Bytes;

use crate::IcebergResult;
use super::avro::serialize_manifest;
use super::manifest::{Manifest, ManifestContentType, ManifestFile, ManifestFileType};

/// Encodes [`Manifest`]s to binary avro format.
pub struct ManifestWriter {
    sequence_number: i64,
    snapshot_id: i64,
}

impl ManifestWriter {
    /// Initializes a new writer that encodes manifests for the given snapshot id
    /// and sequence number.
    pub fn new(sequence_number: i64, snapshot_id: i64) -> Self {
        Self { sequence_number, snapshot_id }
    }

    /// Encodes the manifest into Avro binary format.
    ///
    /// Returns the serialized bytes alongside with a corresponding [`ManifestFile`]
    /// describing the manifest.  The `ManifestFile` can the be added to a
    /// [`ManifestList`](super::manifest::ManifestList).
    /// The function encodes the manifest but does not actually write it to storage.
    pub fn write(
        &self,
        manifest_path: &str,
        manifest: &Manifest
    ) -> IcebergResult<(Bytes, ManifestFile)> {
        let encoded = serialize_manifest(&manifest)?;

        let manifest_file = ManifestFile {
            manifest_path: manifest_path.to_string(),
            manifest_length: encoded.len().try_into().unwrap(),
            partition_spec_id: manifest.partition_spec().spec_id(),
            content: match manifest.content_type() {
                ManifestContentType::Data => ManifestFileType::Data,
                ManifestContentType::Deletes => ManifestFileType::Delete
            },
            sequence_number: self.sequence_number,
            min_sequence_number: manifest.min_sequence_number()
                .unwrap_or(self.sequence_number),
            added_snapshot_id: self.snapshot_id,
            added_data_files_count: manifest.added_data_files_count(),
            existing_data_files_count: manifest.existing_data_files_count(),
            deleted_data_files_count: manifest.deleted_data_files_count(),
            added_rows_count: manifest.added_rows_count(),
            existing_rows_count: manifest.existing_rows_count(),
            deleted_rows_count: manifest.deleted_rows_count(),
            partitions: None,
        };
        
        Ok((Bytes::from(encoded), manifest_file))
    }
}
