package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
	"sync"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const MaxVectorDimensions = 16000 // Adjusted as per pgvector's limit

type Vector struct {
	Name      string
	Embedding []float32
}

var (
	dbURL       = flag.String("dburl", "postgresql://postgres:foo@localhost:5432/pagila_c", "Database URL")
	filePath    = flag.String("file", "/Users/thomasmcgeehan/VDS/veloce/go/ingest/vectors.txt", "Path to vector file")
	batchSize   = flag.Int("batchsize", 1000, "Batch size for processing")
	threadCount = flag.Int("threads", 4, "Number of threads for parallel processing")
)

func ParseVector(line string) (Vector, error) {
	parts := strings.Split(line, ",")
	if len(parts)-1 > MaxVectorDimensions {
		return Vector{}, fmt.Errorf("vector exceeds maximum dimensions: got %d, max %d", len(parts)-1, MaxVectorDimensions)
	}

	name := parts[0]
	vec := make([]float32, len(parts)-1)
	for i, part := range parts[1:] {
		var val float32
		_, err := fmt.Sscanf(part, "%f", &val)
		if err != nil {
			return Vector{}, fmt.Errorf("error parsing vector: %w", err)
		}
		vec[i] = val
	}
	return Vector{Name: name, Embedding: vec}, nil
}

func readVectorData(filePath string) ([]Vector, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, fmt.Errorf("error opening file: %w", err)
	}
	defer file.Close()

	var vectors []Vector
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		vector, err := ParseVector(line)
		if err != nil {
			log.Printf("Skipping line due to error: %s\n", err)
			continue
		}
		vectors = append(vectors, vector)
	}

	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("error reading from file: %w", err)
	}
	return vectors, nil
}

func vectorToString(v Vector) string {
	var sb strings.Builder
	sb.WriteString("[")
	for i, val := range v.Embedding {
		sb.WriteString(fmt.Sprintf("%.1f", val))
		if i < len(v.Embedding)-1 {
			sb.WriteString(", ")
		}
	}
	sb.WriteString("]")
	return sb.String()
}

func convertVectorsToPgvectorFormat(vectors []Vector) [][]interface{} {
	pgvectorData := make([][]interface{}, len(vectors))
	for i, v := range vectors {
		pgvectorData[i] = []interface{}{v.Name, vectorToString(v)}
	}
	return pgvectorData
}

func processVectorBatch(pool *pgxpool.Pool, vectorChannel <-chan []Vector, wg *sync.WaitGroup) {
	defer wg.Done()

	for batch := range vectorChannel {
		pgvectorData := convertVectorsToPgvectorFormat(batch)

		tableName := pgx.Identifier{"items"}
		columnNames := []string{"name", "embedding"}

		tx, err := pool.Begin(context.Background())
		if err != nil {
			log.Printf("Failed to begin transaction: %v\n", err)
			continue
		}

		_, err = tx.CopyFrom(context.Background(), tableName, columnNames, pgx.CopyFromRows(pgvectorData))
		if err != nil {
			log.Printf("Error inserting vectors into pgvector: %v\n", err)
			if rollbackErr := tx.Rollback(context.Background()); rollbackErr != nil {
				log.Printf("Failed to rollback transaction: %v\n", rollbackErr)
			}
			continue
		}

		if err := tx.Commit(context.Background()); err != nil {
			log.Printf("Failed to commit transaction: %v\n", err)
		}
	}
}

func main() {
	flag.Parse()

	vectors, err := readVectorData(*filePath)
	if err != nil {
		log.Fatalf("Error reading vector data: %v\n", err)
	}

	pool, err := pgxpool.New(context.Background(), *dbURL)
	if err != nil {
		log.Fatalf("Failed to connect to database: %v\n", err)
	}
	defer pool.Close()

	vectorChannel := make(chan []Vector)
	var wg sync.WaitGroup

	go func() {
		defer close(vectorChannel)
		for i := 0; i < len(vectors); i += *batchSize {
			end := i + *batchSize
			if end > len(vectors) {
				end = len(vectors)
			}
			vectorChannel <- vectors[i:end]
		}
	}()

	for i := 0; i < *threadCount; i++ {
		wg.Add(1)
		go processVectorBatch(pool, vectorChannel, &wg)
	}

	wg.Wait()
	fmt.Println("Vector data import completed.")
}