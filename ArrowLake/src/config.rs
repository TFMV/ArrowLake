use serde::Deserialize;
use std::fs;

#[derive(Deserialize)]
pub struct Config {
    pub gcs_bucket: String,
    pub bigquery_dataset: String,
    pub iceberg_table: String,
}

impl Config {
    pub fn from_file(filename: &str) -> Result<Self, Box<dyn std::error::Error>> {
        let config_contents = fs::read_to_string(filename)?;
        let config: Config = toml::from_str(&config_contents)?;
        Ok(config)
    }
}
