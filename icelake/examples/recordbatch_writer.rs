use std::sync::Arc;
use std::collections::HashMap;

use rand::Rng;

use arrow::error::ArrowError;
use arrow::array::{Int32Array, Int64Array, TimestampMicrosecondArray, Float64Array};
use arrow::datatypes::{SchemaRef as ArrowSchemaRef};
use arrow::record_batch::RecordBatch;

use icelake::{IcebergResult, IcebergTableLoader};
use icelake::schema::SchemaBuilder;
use icelake::value::Value;
use icelake::partition::PartitionSpec;
use icelake::writer::RecordBatchWriter;

static DEFAULT_TABLE_URL: &str = "file:///tmp/iceberg/events";

/// Generate a `RecordBatch` with a bunch of random data.
fn generate_record_batch(
    schema: ArrowSchemaRef,
    product_id: i32,
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

    let product_id: Vec<i32> = vec![product_id; entries.try_into().unwrap()];

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

async fn run() -> IcebergResult<()> {
    let table_url = std::env::args().nth(1).unwrap_or(DEFAULT_TABLE_URL.to_string());

    let mut schema_builder = SchemaBuilder::new(0);
    schema_builder.add_fields(vec![
        schema_builder.new_long_field("id").with_required(true),
        schema_builder.new_timestamp_field("ts").with_required(true),
        schema_builder.new_int_field("product_id"),
        schema_builder.new_int_field("quantity"),
        schema_builder.new_double_field("price"),
    ]);
    let schema = schema_builder.build();

    // Partition the table by product_id.
    let partition_spec = PartitionSpec::builder(0, schema.clone())
        .add_identity_field("product_id")?
        .build();

    // Load an existing table or create a new one.
    let mut table = IcebergTableLoader::from_url(&table_url)
        .with_env_options()
        .with_schema(schema.clone())
        .with_partition_spec(partition_spec)
        .load_or_create()
        .await?;

    let mut writer = RecordBatchWriter::for_table(&table)?;

    // Write multiple batches of data, each to a separate partition.
    for product_id in 0..10 {
        // Generate 1m entries for each partition
        let record_batch = generate_record_batch(
            writer.arrow_schema(), product_id, 1, 1_000_000
        )?;

        let mut partition = HashMap::new();
        partition.insert(
            schema.get_field_by_name("product_id").unwrap().id,
            Some(Value::Int(product_id))
        );

        writer.write_partition(partition, &record_batch)?;
    }

    // Commit all partitions at once.
    writer.commit(&mut table).await?;

    println!(
        "Commited 10 files in 10 partitions to the Iceberg table at {}",
        table_url
    );

    Ok(())
}

#[tokio::main]
async fn main() {
    let result = run().await;

    match result {
        Ok(()) => {
            println!("Done");
        },
        Err(err) => {
            println!("Failed with error: {err}");
        }
    }
}
