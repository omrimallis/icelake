use crate::IcebergResult;
use super::avro::deserialize_manifest;
use super::manifest::Manifest;

pub struct ManifestReader;

impl ManifestReader {
    pub fn new() -> Self { Self {} }

    pub fn read(&self, bytes: &[u8]) -> IcebergResult<Manifest> {
        deserialize_manifest(bytes)
    }
}
