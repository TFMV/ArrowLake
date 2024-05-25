package join

import (
	"database/sql"
	"fmt"
	"testing"

	_ "github.com/marcboeker/go-duckdb"
)

func setupTestEnvironment(db *sql.DB) error {
	// Create a mock Parquet table in DuckDB
	_, err := db.Exec(`
		CREATE TABLE parquet_table (
			id INTEGER,
			parquet_col1 VARCHAR,
			parquet_col2 VARCHAR
		);
		INSERT INTO parquet_table VALUES
			(1, 'parquet_value1', 'parquet_value2'),
			(2, 'parquet_value3', 'parquet_value4');
	`)
	if err != nil {
		return fmt.Errorf("failed to create and populate parquet_table: %v", err)
	}

	// Create a mock PostgreSQL table in DuckDB
	_, err = db.Exec(`
		CREATE TABLE postgres_table (
			id INTEGER,
			postgres_col1 VARCHAR,
			postgres_col2 VARCHAR
		);
		INSERT INTO postgres_table VALUES
			(1, 'postgres_value1', 'postgres_value2'),
			(3, 'postgres_value3', 'postgres_value4');
	`)
	if err != nil {
		return fmt.Errorf("failed to create and populate postgres_table: %v", err)
	}

	return nil
}

func TestJoinParquetWithPostgres(t *testing.T) {
	// Connect to DuckDB
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatalf("failed to connect to DuckDB: %v", err)
	}
	defer db.Close()

	// Setup test environment
	err = setupTestEnvironment(db)
	if err != nil {
		t.Fatalf("failed to setup test environment: %v", err)
	}

	// Define a custom query to perform the join
	query := `
		SELECT p.id, p.parquet_col1, p.parquet_col2, pg.postgres_col1, pg.postgres_col2
		FROM parquet_table p
		JOIN postgres_table pg ON p.id = pg.id
	`
	rows, err := db.Query(query)
	if err != nil {
		t.Fatalf("failed to execute join query: %v", err)
	}
	defer rows.Close()

	var results []struct {
		ID           int
		ParquetCol1  string
		ParquetCol2  string
		PostgresCol1 string
		PostgresCol2 string
	}

	for rows.Next() {
		var (
			id           int
			parquetCol1  string
			parquetCol2  string
			postgresCol1 string
			postgresCol2 string
		)
		if err := rows.Scan(&id, &parquetCol1, &parquetCol2, &postgresCol1, &postgresCol2); err != nil {
			t.Fatalf("failed to scan row: %v", err)
		}
		results = append(results, struct {
			ID           int
			ParquetCol1  string
			ParquetCol2  string
			PostgresCol1 string
			PostgresCol2 string
		}{
			ID:           id,
			ParquetCol1:  parquetCol1,
			ParquetCol2:  parquetCol2,
			PostgresCol1: postgresCol1,
			PostgresCol2: postgresCol2,
		})
	}

	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}

	expected := struct {
		ID           int
		ParquetCol1  string
		ParquetCol2  string
		PostgresCol1 string
		PostgresCol2 string
	}{
		ID:           1,
		ParquetCol1:  "parquet_value1",
		ParquetCol2:  "parquet_value2",
		PostgresCol1: "postgres_value1",
		PostgresCol2: "postgres_value2",
	}

	if results[0] != expected {
		t.Fatalf("expected %+v, got %+v", expected, results[0])
	}
}
