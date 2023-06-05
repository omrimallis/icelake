use std::sync::Arc;

use rand::Rng;

use arrow::error::ArrowError;
use arrow::array::{Int32Array, Int64Array, TimestampMicrosecondArray, Float64Array};
use arrow::datatypes::{SchemaRef as ArrowSchemaRef};
use arrow::record_batch::RecordBatch;

use icelake::{IcebergResult, IcebergTableLoader};
use icelake::schema::SchemaBuilder;
use icelake::writer::RecordBatchWriter;
use icelake::arrow_schema::iceberg_to_arrow_schema;

static DEFAULT_TABLE_URL: &str = "file:///tmp/iceberg/events";

/// Generate a `RecordBatch` with a bunch of random data.
fn generate_record_batch(
    schema: ArrowSchemaRef,
    start_entry: i32,
    entries: i32
) -> Result<RecordBatch, ArrowError> {
    let end_entry = start_entry + entries;

    let id: Vec<i64> = (start_entry..end_entry).into_iter()
        .map(|i| i64::from(i))
        .collect();

    let now: i64 = chrono::Utc::now().timestamp_micros();
    let ts: Vec<i64> = (start_entry..end_entry).into_iter()
        .map(|i| (now + i64::from(i) * 1_000_000))
        .collect();

    let product_id: Vec<i32> = rand::thread_rng()
        .sample_iter(rand::distributions::Uniform::from(1..=1_000))
        .take(entries.try_into().unwrap())
        .collect();

    let quantity: Vec<i32> = rand::thread_rng()
        .sample_iter(rand::distributions::Uniform::from(1..=10_000))
        .take(entries.try_into().unwrap())
        .collect();

    let price: Vec<f64> = rand::thread_rng()
        .sample_iter(rand::distributions::Standard)
        .take(entries.try_into().unwrap())
        .collect();

    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(id)),
            Arc::new(TimestampMicrosecondArray::from(ts)),
            Arc::new(Int32Array::from(product_id)),
            Arc::new(Int32Array::from(quantity)),
            Arc::new(Float64Array::from(price)),
        ]
    )
}

#[tokio::main]
async fn main() -> IcebergResult<()> {
    let mut schema_builder = SchemaBuilder::new(0);

    schema_builder.add_fields(vec![
        schema_builder.new_long_field("id").with_required(true),
        schema_builder.new_timestamp_field("ts").with_required(true),
        schema_builder.new_int_field("product_id"),
        schema_builder.new_int_field("quantity"),
        schema_builder.new_double_field("price"),
    ]);

    let schema = schema_builder.build();
    let arrow_schema = Arc::new(iceberg_to_arrow_schema(&schema)?);

    let table_url = std::env::args().nth(1).unwrap_or(DEFAULT_TABLE_URL.to_string());
    let mut table = IcebergTableLoader::from_url(&table_url)
        .with_env_options()
        .load_or_create(schema)
        .await?;

    let mut writer = RecordBatchWriter::new(arrow_schema.clone());

    // Write multiple batches of data.
    let batch_size: i32 = 1_000_000;
    let record_batch = generate_record_batch(
        arrow_schema.clone(), 1, batch_size
    )?;

    writer.write(&record_batch)?;
    writer.commit(&mut table).await?;

    println!(
        "Commited {} records to the Iceberg table at {}",
        batch_size,
        table_url
    );

    Ok(())
}
