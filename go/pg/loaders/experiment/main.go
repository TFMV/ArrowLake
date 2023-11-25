package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"cloud.google.com/go/storage"
	_ "github.com/pgx/v5/pgxpool"
)

const (
	projectID     = "your-gcs-project-id"
	bucket        = "your-gcs-bucket-name"
	objectPrefix  = "data/"
	numWorkers    = 8
	maxConcurrent = numWorkers * 2
)

type Data struct {
	Id   int    `json:"id"`
	Name string `json:"name"`
}

func main() {
	// Initialize GCS client
	ctx := context.Background()
	client, err := storage.NewClient(ctx)
	if err != nil {
		panic(err)
	}
	defer client.Close()

	// Connect to PostgreSQL
	connStr := fmt.Sprintf("user=username password=password host=%s port=%d dbname=%s sslmode=disable", "localhost", 5432, "mydb")
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		panic(err)
	}
	defer db.Close()

	// Prepare statement for inserting data
	insertStmt, err := db.Prepare("INSERT INTO mytable (id, name) VALUES ($1, $2)")
	if err != nil {
		panic(err)
	}
	defer insertStmt.Close()

	// Start workers
	workChan := make(chan []byte, maxConcurrent)
	doneChan := make(chan bool, numWorkers)
	for i := 0; i < numWorkers; i++ {
		go worker(workChan, doneChan, insertStmt)
	}

	// Iterate over objects in GCS bucket
	it := client.Bucket(bucket).Objects(ctx, &storage.Query{Prefix: objectPrefix})
	for {
		objAttrs, err := it.Next()
		if err == iterator.Done {
			break
		} else if err != nil {
			panic(err)
		}

		// Read object content
		reader, err := client.Object(bucket, objAttrs.Name).NewReader(ctx)
		if err != nil {
			panic(err)
		}
		defer reader.Close()

		// Decode JSON data
		var data []*Data
		decoder := json.NewDecoder(reader)
		if err := decoder.Decode(&data); err != nil {
			panic(err)
		}

		// Send data to workers
		for _, d := range data {
			workChan <- []byte(fmt.Sprintf("%d,%s\n", d.Id, d.Name))
		}
	}

	close(workChan)
	<-doneChan
}

func worker(workChan chan []byte, doneChan chan bool, insertStmt *sql.Stmt) {
	for work := range workChan {
		parts := strings.SplitN(string(work), ",", 2)
		id, _ := strconv.Atoi(parts[0])
		name := parts[1]

		// Execute INSERT query
		result, err := insertStmt.Exec(id, name)
		if err != nil {
			panic(err)
		}

		rowsAffected, err := result.RowsAffected()
		if err != nil {
			panic(err)
		}

		fmt.Printf("Inserted %d rows\n", rowsAffected)
	}

	doneChan <- true
}
