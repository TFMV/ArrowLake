package main

import (
	"bufio"
	"context"
	"fmt"
	"log"
	"os"
	"strings"
	"sync"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const VectorDimensions = 6 // Update this based on expected vector dimensions

type Vector struct {
	vec []float32
}

func NewVector(vec []float32) Vector {
	return Vector{vec: vec}
}

func ParseVector(line string) (Vector, error) {
	parts := strings.Split(line, ",")
	if len(parts) != VectorDimensions {
		return Vector{}, fmt.Errorf("unexpected number of dimensions: got %d, want %d", len(parts), VectorDimensions)
	}

	vec := make([]float32, len(parts))
	for i, part := range parts {
		var val float32
		_, err := fmt.Sscanf(strings.TrimSpace(part), "%f", &val)
		if err != nil {
			return Vector{}, fmt.Errorf("error parsing vector: %w", err)
		}
		vec[i] = val
	}
	return NewVector(vec), nil
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
	sb.WriteString("{")
	for i, val := range v.vec {
		sb.WriteString(fmt.Sprintf("%.1f", val))
		if i < len(v.vec)-1 {
			sb.WriteString(", ")
		}
	}
	sb.WriteString("}")
	return sb.String()
}

func convertVectorsToPgvectorFormat(vectors []Vector) [][]interface{} {
	pgvectorData := make([][]interface{}, len(vectors))
	for i, v := range vectors {
		pgvectorData[i] = []interface{}{vectorToString(v)}
	}
	return pgvectorData
}

func processVectorBatch(pool *pgxpool.Pool, vectorChannel <-chan []Vector, wg *sync.WaitGroup) {
	defer wg.Done()

	for batch := range vectorChannel {
		pgvectorData := convertVectorsToPgvectorFormat(batch)

		tableName := pgx.Identifier{"items"}
		columnNames := []string{"embedding"}

		_, err := pool.CopyFrom(context.Background(), tableName, columnNames, pgx.CopyFromRows(pgvectorData))
		if err != nil {
			log.Printf("Error inserting vectors into pgvector: %v\n", err)
			continue
		}
	}
}

func main() {
	dbURL := "postgresql://postgres:foo@localhost:5432/pagila_c"

	filePath := "/Users/thomasmcgeehan/VDS/veloce/python/vllm/ingest/vectors.txt"

	vectors, err := readVectorData(filePath)
	if err != nil {
		log.Fatalf("Error reading vector data: %v\n", err)
	}

	pool, err := pgxpool.New(context.Background(), dbURL)
	if err != nil {
		log.Fatalf("Failed to connect to database: %v\n", err)
	}
	defer pool.Close()

	vectorChannel := make(chan []Vector)
	var wg sync.WaitGroup

	go func() {
		defer close(vectorChannel)
		batchSize := 1000
		for i := 0; i < len(vectors); i += batchSize {
			end := i + batchSize
			if end > len(vectors) {
				end = len(vectors)
			}
			vectorChannel <- vectors[i:end]
		}
	}()

	threadCount := 4
	for i := 0; i < threadCount; i++ {
		wg.Add(1)
		go processVectorBatch(pool, vectorChannel, &wg)
	}

	wg.Wait()
	fmt.Println("Vector data import completed.")
}
