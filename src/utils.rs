use std::time::SystemTime;

use crate::{IcebergError, IcebergResult};

/// Returns the current time as milliseconds since Unix Epoch, as needed for
/// saving in the table metadata and snapshots.
pub fn current_time_ms() -> IcebergResult<i64> {
    let now = SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(|_| IcebergError::CustomError {
            message: "system clock before Unix Epoch time".to_string()
        })?;

    // Attempt tp convert u128 to i64
    let now = i64::try_from(now.as_millis())
        .map_err(|_| IcebergError::CustomError {
            message: "system clock does not fit in long".to_string()
        })?;

    Ok(now)
}
