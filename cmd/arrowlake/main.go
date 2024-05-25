package main

import (
	"context"
	"log"

	"github.com/TFMV/arrowlake/pkg/join"
)

func main() {
	ctx := context.Background()

	// Load the configuration file
	config, err := join.LoadConfig("/Users/thomasmcgeehan/ArrowLake/arrowlake/config.yaml")
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}

	// Join data sources
	err = join.JoinDataSources(ctx, config)
	if err != nil {
		log.Fatalf("Failed to join data sources: %v", err)
	}
}
