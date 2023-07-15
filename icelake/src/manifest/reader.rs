use crate::IcebergResult;
use super::avro::deserialize_manifest;
use super::manifest::{Manifest, ManifestFile};

/// Decodes [`Manifest`]s from binary avro format.
pub struct ManifestReader {
    sequence_number: i64,
    snapshot_id: i64,
}

impl ManifestReader {
    /// Initializes a new reader for the given snapshot id and sequence number.
    ///
    /// When a `Manifest` is decoded, it will inherit the snapshot id and
    /// sequence number if they are not explicitly encoded in the manifest.
    pub fn new(sequence_number: i64, snapshot_id: i64) -> Self {
        Self {
            sequence_number,
            snapshot_id
        }
    }

    /// Initializes a new reader to read the manifest associated with the given
    /// `ManifestFile`.
    ///
    /// This is similar to calling [`ManifestReader::new()`], with the sequence number
    /// and snapshot id derived from the `ManifestFile`.
    pub fn for_manifest_file(manifest_file: &ManifestFile) -> Self {
        Self {
            sequence_number: manifest_file.sequence_number,
            snapshot_id: manifest_file.added_snapshot_id
        }
    }

    /// Decodes the manifest from Avro binary format, filling the inherited metadata if
    /// needed.
    pub fn read(&self, bytes: &[u8]) -> IcebergResult<Manifest> {
        let mut manifest = deserialize_manifest(bytes)?;


        // Inherit snapshot id and sequence numbers if not present.
        for mut entry in manifest.entries_mut().iter_mut() {
            if entry.snapshot_id.is_none() {
                entry.snapshot_id = Some(self.snapshot_id);
            }

            if entry.sequence_number.is_none() && entry.added() {
                entry.sequence_number = Some(self.sequence_number);
            }

            if entry.file_sequence_number.is_none() && entry.added() {
                entry.file_sequence_number = Some(self.sequence_number);
            }
        }

        Ok(manifest)
    }
}
