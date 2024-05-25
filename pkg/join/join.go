package join

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

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
		JoinColumns       []string `yaml:"join_columns"`
		SelectColumns     []string `yaml:"select_columns"`
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

// JoinParquetWithPostgres joins a Parquet file with a PostgreSQL table.
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
	_, err = db.Exec(`INSTALL postgres_scanner; LOAD postgres_scanner;`)
	if err != nil {
		return fmt.Errorf("failed to install and load PostgreSQL scanner extension: %w", err)
	}

	// Attach the PostgreSQL database
	_, err = db.Exec(fmt.Sprintf(`CALL postgres_attach('postgres_db', true, '', true, '', '%s')`, config.Postgres.ConnectionString))
	if err != nil {
		return fmt.Errorf("failed to attach PostgreSQL database: %w", err)
	}

	// Construct the join query
	joinConditions := make([]string, len(config.Query.JoinColumns))
	for i, col := range config.Query.JoinColumns {
		joinConditions[i] = fmt.Sprintf("p.%s = pg.%s", col, col)
	}
	joinConditionStr := strings.Join(joinConditions, " AND ")
	selectColumnsStr := strings.Join(config.Query.SelectColumns, ", ")

	query := fmt.Sprintf(`
		SELECT p.*, %s
		FROM %s p
		JOIN postgres_db.%s pg ON %s
		LIMIT 10;
	`, selectColumnsStr, config.Query.ParquetTableName, config.Query.PostgresTableName, joinConditionStr)

	// Execute the join query
	rows, err := db.Query(query)
	if err != nil {
		return fmt.Errorf("failed to execute join query: %w", err)
	}
	defer rows.Close()

	// Process the results
	columns, err := rows.Columns()
	if err != nil {
		return fmt.Errorf("failed to get columns: %w", err)
	}

	values := make([]sql.NullString, len(columns))
	valuePtrs := make([]interface{}, len(columns))
	for i := range values {
		valuePtrs[i] = &values[i]
	}

	for rows.Next() {
		err = rows.Scan(valuePtrs...)
		if err != nil {
			return fmt.Errorf("failed to scan row: %w", err)
		}

		for i, col := range values {
			fmt.Printf("%s: %v\n", columns[i], col.String)
		}
	}

	return nil
}
