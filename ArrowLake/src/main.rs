use arrowlake::config::Config;
use arrowlake::data_ingestion::{gcs::load_from_gcs, bigquery::load_from_bigquery, iceberg::load_from_iceberg};
use arrowlake::query_engine::datafusion::execute_datafusion_query;
use arrowlake::utils::logging::init_logging;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    init_logging();
    
    let config = Config::from_file("config.toml")?;

    load_from_gcs(&config.gcs_bucket).await?;
    load_from_bigquery(&config.bigquery_dataset).await?;
    load_from_iceberg(&config.iceberg_table).await?;

    execute_datafusion_query("SELECT * FROM some_table").await?;

    Ok(())
}
