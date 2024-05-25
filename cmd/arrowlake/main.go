package main

import (
	"log"

	"arrowlake/pkg/duckdb"

	"github.com/TFMV/arrowlake/pkg/arrow"
)

func main() {
	// Initialize Arrow and DuckDB
	err := arrow.InitArrow()
	if err != nil {
		log.Fatalf("Failed to initialize Arrow: %v", err)
	}

	err = duckdb.InitDuckDB()
	if err != nil {
		log.Fatalf("Failed to initialize DuckDB: %v", err)
	}

	// Example use case: Run a SQL query on Arrow table using DuckDB
	query := "SELECT * FROM my_arrow_table WHERE value > 10"
	result, err := duckdb.QueryArrowTable(query)
	if err != nil {
		log.Fatalf("Failed to query Arrow table: %v", err)
	}

	log.Printf("Query Result: %v", result)
}
