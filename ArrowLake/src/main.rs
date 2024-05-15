use datafusion::prelude::*;
use datafusion::arrow::util::pretty::print_batches;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::execution::context::SessionContext;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    // Create a DataFusion context
    let ctx = SessionContext::new();

    // Register a Parquet file as a table
    ctx.register_parquet("example", "/Users/thomasmcgeehan/VDS/veloce/ArrowLake/data/flights.parquet", ParquetReadOptions::default()).await?;

    // Create a DataFrame
    let df = ctx.sql("SELECT example.\"DEP_DELAY\", COUNT(*) as CNT FROM example GROUP BY example.\"DEP_DELAY\"").await?;

    // Collect the results into a Vec<RecordBatch>
    let results: Vec<RecordBatch> = df.collect().await?;

    // Print the results
    print_batches(&results)?;

    Ok(())
}
