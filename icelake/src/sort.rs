//! Interface to Iceberg table ordering.
use serde::{Deserialize, Serialize};

use crate::partition::PartitionTransform;

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone)]
/// Defines the sort order for a field.
pub enum SortDirection {
    /// Sort the field ascending.
    #[serde(rename = "asc")]
    Ascending,
    /// Sort the field descending.
    #[serde(rename = "desc")]
    Descending,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone)]
/// Defines the sort order for nulls in a field.
pub enum NullOrder {
    #[serde(rename = "nulls-first")]
    /// Place the nulls first in the search.
    First,
    #[serde(rename = "nulls-last")]
    /// Place the nulls last in the search.
    Last,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone)]
#[serde(rename_all = "kebab-case")]
/// Definition of a how a field should be used within a sort.
pub struct SortField {
    /// A source column id from the table’s schema
    pub source_id: i32,
    /// A transform that is used to produce values to be sorted on from the source column.
    pub transform: PartitionTransform,
    /// A sort direction, that can only be either asc or desc
    pub direction: SortDirection,
    /// A null order that describes the order of null values when sorted.
    pub null_order: NullOrder,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone)]
#[serde(rename_all = "kebab-case")]
/// A sort order is defined by an sort order id and a list of sort fields.
/// The order of the sort fields within the list defines the order in
/// which the sort is applied to the data.
pub struct SortOrder {
    /// Identifier for SortOrder, order_id `0` is no sort order.
    pub order_id: i32,
    /// Details of the sort
    pub fields: Vec<SortField>,
}

impl SortOrder {
    /// Create a new, empty sort order.
    pub fn new() -> Self { Self { order_id: 0, fields: Vec::new() } }
}
