package join

import (
	"context"
	"database/sql"
	"fmt"
	"os"

	_ "github.com/marcboeker/go-duckdb"
	"gopkg.in/yaml.v2"
)

type Config struct {
	Postgres struct {
		ConnectionString string `yaml:"connection_string"`
	} `yaml:"postgres"`
	Parquet struct {
		FilePath string `yaml:"file_path"`
	} `yaml:"parquet"`
	Query struct {
		ParquetTableName  string   `yaml:"parquet_table_name"`
		PostgresTableName string   `yaml:"postgres_table_name"`
		JoinColumn        string   `yaml:"join_column"`
		SelectColumns     []string `yaml:"select_columns"`
		Query             string   `yaml:"query"`
	} `yaml:"query"`
}

func LoadConfig(configPath string) (*Config, error) {
	data, err := os.ReadFile(configPath)
	if err != nil {
		return nil, fmt.Errorf("unable to read config file: %v", err)
	}

	var config Config
	err = yaml.Unmarshal(data, &config)
	if err != nil {
		return nil, fmt.Errorf("unable to unmarshal config file: %v", err)
	}

	return &config, nil
}

func JoinParquetWithPostgres(ctx context.Context, config *Config) error {
	db, err := sql.Open("duckdb", "")
	if err != nil {
		return fmt.Errorf("failed to open DuckDB: %w", err)
	}
	defer db.Close()

	// Load the Parquet file into DuckDB
	_, err = db.Exec(fmt.Sprintf(`CREATE TABLE %s AS SELECT * FROM read_parquet('%s')`, config.Query.ParquetTableName, config.Parquet.FilePath))
	if err != nil {
		return fmt.Errorf("failed to create Parquet table: %w", err)
	}

	// Enable DuckDB extensions
	_, err = db.Exec(`INSTALL postgres; LOAD postgres;`)
	if err != nil {
		return fmt.Errorf("failed to install and load PostgreSQL extension: %w", err)
	}

	// Attach the PostgreSQL database
	attachCmd := fmt.Sprintf(`ATTACH '%s' AS postgres_db (TYPE POSTGRES);`, config.Postgres.ConnectionString)
	_, err = db.Exec(attachCmd)
	if err != nil {
		return fmt.Errorf("failed to attach PostgreSQL database: %w", err)
	}

	// Execute the query from the config file
	rows, err := db.Query(config.Query.Query)
	if err != nil {
		return fmt.Errorf("failed to execute join query: %w", err)
	}
	defer rows.Close()

	// Process the results
	var count int
	if rows.Next() {
		err := rows.Scan(&count)
		if err != nil {
			return fmt.Errorf("failed to scan row: %w", err)
		}
	}

	fmt.Printf("Count of rows: %d\n", count)

	return nil
}
