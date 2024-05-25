package main

import (
	"context"
	"log"

	"github.com/TFMV/arrowlake/pkg/join"
)

func main() {
	ctx := context.Background()

	// Load configuration
	configPath := "/Users/thomasmcgeehan/ArrowLake/arrowlake/config.yaml"
	config, err := join.LoadConfig(configPath)
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}

	// Join Parquet file with PostgreSQL table
	err = join.JoinParquetWithPostgres(ctx, config)
	if err != nil {
		log.Fatalf("Failed to join Parquet with Postgres: %v", err)
	}
}
