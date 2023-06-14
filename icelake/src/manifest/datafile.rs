use std::collections::HashMap;

use crate::partition::PartitionValues;

#[derive(Debug, Clone)]
pub enum DataFileContent {
    Data,
    PositionDelete,
    EqualityDelete,
}

#[derive(Debug, Clone)]
pub enum DataFileFormat {
    Avro,
    ORC,
    Parquet
}

/// Points to a file containing table data, and stores partition values and statistics
/// for the data.
#[derive(Debug, Clone)]
pub struct DataFile {
    /// Type of content stored by the data file: data, equality deletes, or position
    /// deletes.
    pub content: DataFileContent,
    /// Full URI for the file with FS scheme.
    pub file_path: String,
    /// String file format name: avro, orc or parquet.
    pub file_format: DataFileFormat,
    /// Partition data tuple.
    pub partition: PartitionValues,
    /// Number of records in this file.
    pub record_count: i64,
    /// Total file size in bytes.
    pub file_size_in_bytes: i64,
    /// Map from column id to the total size on disk of all regions that store the
    /// column. Does not include bytes necessary to read other columns, like footers.
    /// Leave null for row-oriented formats (Avro).
    pub column_sizes: Option<HashMap<i32, i64>>,
    /// Map from column id to number of values in the column (including null and NaN
    /// values).
    pub value_counts: Option<HashMap<i32, i64>>,
    /// Map from column id to number of null values in the column.
    pub null_value_counts: Option<HashMap<i32, i64>>,
    /// Map from column id to number of NaN values in the column.
    pub nan_value_counts: Option<HashMap<i32, i64>>,
    /// Map from column id to number of distinct values in the column; distinct counts
    /// must be derived using values in the file by counting or using sketches, but not
    /// using methods like merging existing distinct counts.
    pub distinct_counts: Option<HashMap<i32, i64>>,
    /// Map from column id to lower bound in the column serialized as binary. Each value
    /// must be less than or equal to all non-null, non-NaN values in the column for
    /// the file.
    pub lower_bounds: Option<HashMap<i32, Vec<u8>>>,
    /// Map from column id to upper bound in the column serialized as binary. Each value
    /// must be greater than or equal to all non-null, non-Nan values in the column for
    /// the file.
    pub upper_bounds: Option<HashMap<i32, Vec<u8>>>,
    /// Implementation-specific key metadata for encryption.
    pub key_metadata: Option<Vec<u8>>,
    /// Split offsets for the data file. For example, all row group offsets in a Parquet
    /// file. Must be sorted ascending.
    pub split_offsets: Option<Vec<i64>>,
    /// Field ids used to determine row equality in equality delete files. Required
    /// when content=2 and should be null otherwise. Fields with ids listed in this
    /// column must be present in the delete file.
    pub equality_ids: Option<Vec<i32>>,
    /// ID representing sort order for this file.
    pub sort_order_id: Option<i32>,
}

impl DataFile {
    pub fn builder(
        content: DataFileContent,
        file_path: &str,
        file_format: DataFileFormat,
        record_count: i64,
        file_size_in_bytes: i64,
    ) -> DataFileBuilder {
        DataFileBuilder::new(
            content,
            file_path,
            file_format,
            record_count,
            file_size_in_bytes
        )
    }
}

pub struct DataFileBuilder {
    data_file: DataFile
}

impl DataFileBuilder {
    pub fn new(
        content: DataFileContent,
        file_path: &str,
        file_format: DataFileFormat,
        record_count: i64,
        file_size_in_bytes: i64,
    ) -> Self {
        Self {
            data_file: DataFile {
                content: content,
                file_path: String::from(file_path),
                file_format: file_format,
                partition: PartitionValues::default(),
                record_count: record_count,
                file_size_in_bytes: file_size_in_bytes,
                column_sizes: None,
                value_counts: None,
                null_value_counts: None,
                nan_value_counts: None,
                distinct_counts: None,
                lower_bounds: None,
                upper_bounds: None,
                key_metadata: None,
                split_offsets: None,
                equality_ids: None,
                sort_order_id: None,
            }
        }
    }

    /// Add partition values to the `DataFile`.
    ///
    /// `partitions` should contain a map of the partition field name to its value. All
    /// records in the file pointed by the `DataFile` must have these partition values.
    pub fn with_partition_values(
        mut self,
        partition_values: PartitionValues
    ) -> Self {
        self.data_file.partition = partition_values;
        self
    }

    pub fn build(self) -> DataFile {
        self.data_file
    }
}
