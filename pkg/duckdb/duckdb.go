package duckdb

import (
	"database/sql"
)

var (
	db *sql.DB
)

// InitDuckDB initializes the DuckDB connection
func InitDuckDB() error {
	var err error
	db, err = sql.Open("duckdb", "")
	if err != nil {
		return err
	}
	return nil
}

// QueryArrowTable runs a SQL query on an Arrow table using DuckDB
func QueryArrowTable(query string) (*sql.Rows, error) {
	rows, err := db.Query(query)
	if err != nil {
		return nil, err
	}
	return rows, nil
}
