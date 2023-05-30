//! Inteface to Iceberg table snapshots.
use std::collections::HashMap;

use serde::{Serialize, Deserialize};

// Parts of this module were taken from
// https://github.com/oliverdaff/iceberg-rs/

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone)]
#[serde(rename_all = "lowercase")]
/// The type of operations included in the snapshot, this allows
/// certain snapshots to be skipped during operation.
pub enum SnapshotOperation {
    /// Only data files were added and no files were removed.
    Append,
    /// Data and delete files were added and removed without changing
    /// table data; i.e., compaction, changing the data file format,
    /// or relocating data files.
    Replace,
    /// Data and delete files were added and removed in a logical
    /// overwrite operation.
    Overwrite,
    /// Data files were removed and their contents logically deleted
    /// and/or delete files were added to delete rows.
    Delete,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone, Default)]
/// Summarises the changes in the snapshot.
pub struct SnapshotSummary {
    /// The type of operation in the snapshot
    pub operation: Option<SnapshotOperation>,
    /// Other summary data.
    #[serde(flatten)]
    pub stats: HashMap<String, String>,
}

impl SnapshotSummary {
    pub fn builder() -> SnapshotSummaryBuilder {
        SnapshotSummaryBuilder::new()
    }
}

pub struct SnapshotSummaryBuilder {
    pub operation: Option<SnapshotOperation>,
    // In the SnapshotSummary, stats should be encoded as strings.
    // However, in practice, they are all integers.
    pub stats: HashMap<String, i64>,
}

impl SnapshotSummaryBuilder {
    pub fn new() -> Self {
        Self { operation: None, stats: HashMap::new() }
    }

    pub fn operation<'a>(&'a mut self, operation: SnapshotOperation) -> &'a mut Self {
        self.operation = Some(operation);
        self
    }

    pub fn copy_totals<'a>(&'a mut self, summary: &SnapshotSummary) -> &'a mut Self {
        let keys = [
            "total-records", "total-files-size", "total-data-files",
            "total-delete-files", "total-position-deletes",
            "total-equality-deletes"
        ];
        for k in keys {
            if let Some(value) = summary.stats.get(k) {
                if let Ok(value) = value.parse::<i64>() {
                    self.stats.insert(k.to_string(), value);
                }
            }
        }
        self
    }

    fn add_to_stat<'a>(&'a mut self, stat_name: &str, count: i64) {
        self.stats.entry(stat_name.to_string())
            .and_modify(|v| { *v += count })
            .or_insert(count);
    }

    pub fn add_data_files<'a>(&'a mut self, count: i64) -> &'a mut Self {
        self.add_to_stat("added-data-files", count);
        self.add_to_stat("total-data-files", count);
        self
    }

    pub fn add_records<'a>(&'a mut self, count: i64) -> &'a mut Self {
        self.add_to_stat("added-records", count);
        self.add_to_stat("total-records", count);
        self
    }

    pub fn add_file_sizes<'a>(&'a mut self, count: i64) -> &'a mut Self {
        self.add_to_stat("added-files-size", count);
        self.add_to_stat("total-files-size", count);
        self
    }

    pub fn build(&self) -> SnapshotSummary {
        SnapshotSummary {
            operation: self.operation.clone(),
            stats: self.stats.iter().map(|(k, v)| {
                (k.to_string(), v.to_string())
            }).collect()
        }
    }
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone)]
#[serde(rename_all = "kebab-case")]
/// A V2 compliant snapshot.
pub struct Snapshot {
    /// A unique long ID
    pub snapshot_id: i64,
    /// The snapshot ID of the snapshot’s parent.
    /// Omitted for any snapshot with no parent
    #[serde(skip_serializing_if = "Option::is_none")]
    pub parent_snapshot_id: Option<i64>,
    /// A monotonically increasing long that tracks the order of
    /// changes to a table.
    pub sequence_number: i64,
    /// A timestamp when the snapshot was created, used for garbage
    /// collection and table inspection
    pub timestamp_ms: i64,
    /// The location of a manifest list for this snapshot that
    /// tracks manifest files with additional metadata.
    pub manifest_list: String,
    /// A string map that summarizes the snapshot changes, including operation.
    pub summary: SnapshotSummary,
    /// ID of the table’s current schema when the snapshot was created.
    pub schema_id: Option<i32>,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone)]
#[serde(rename_all = "kebab-case")]
/// A log of when each snapshot was made.
pub struct SnapshotLog {
    /// Id of the snapshot.
    pub snapshot_id: i64,
    /// Last updated timestamp
    pub timestamp_ms: i64,
}

impl SnapshotLog {
    pub fn new(snapshot_id: i64, timestamp_ms: i64) -> Self {
        Self { snapshot_id, timestamp_ms }
    }
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone)]
#[serde(rename_all = "kebab-case")]
pub struct SnapshotReference {
    /// A reference’s snapshot ID. The tagged snapshot or latest snapshot of a branch.
    pub snapshot_id: i64,
    /// Type of the reference, tag or branch.
    #[serde(flatten)]
    pub r#type: SnapshotReferenceType,
    /// For snapshot references except the main branch, a positive number for the max
    /// age of the snapshot reference to keep while expiring snapshots. Defaults to
    /// table property history.expire.max-ref-age-ms. The main branch never expires.
    #[serde(skip_serializing_if = "Option::is_none")]
    max_ref_age_ms: Option<i64>,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone)]
#[serde(rename_all = "lowercase", tag = "type")]
/// Retention policy field, which differ based on it it
/// is a Branch or Tag Reference
pub enum SnapshotReferenceType {
    #[serde(rename_all = "kebab-case")]
    /// A branch reference
    Branch {
        /// A positive number for the minimum number of snapshots to keep in a
        /// branch while expiring snapshots.
        #[serde(skip_serializing_if = "Option::is_none")]
        min_snapshots_to_keep: Option<i32>,
        /// A positive number for the max age of snapshots to keep when expiring,
        /// including the latest snapshot.
        #[serde(skip_serializing_if = "Option::is_none")]
        max_snapshot_age_ms: Option<i64>,
    },
    /// A tag reference.
    Tag,
}
