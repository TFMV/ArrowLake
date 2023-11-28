package main

import (
	"context"
	"log"

	"cloud.google.com/go/storage"
	"github.com/apache/arrow/go/arrow"
	"github.com/apache/arrow/go/arrow/ipc"
	"github.com/apache/arrow/go/arrow/memory"
)

func main() {
	// Create a context
	ctx := context.Background()

	// Create a GCS client
	gcsClient, err := storage.NewClient(ctx)
	if err != nil {
		log.Fatalf("Failed to create GCS client: %v", err)
	}

	// Get the bucket and object
	bucket := gcsClient.Bucket("my-bucket")
	object := bucket.Object("my-arrow-ipc-file.arrow")

	// Get a reader for the object
	reader, err := object.NewReader(ctx)
	if err != nil {
		log.Fatalf("Failed to create object reader: %v", err)
	}
	defer reader.Close()

	// Create an Arrow memory pool
	mem := memory.NewGoAllocator()

	// Create an Arrow IPC reader
	ipcReader, err := ipc.NewReader(reader, ipc.WithAllocator(mem))
	if err != nil {
		log.Fatalf("Failed to create IPC reader: %v", err)
	}

	// Iterate over all record batches in the IPC file
	for {
		recordBatch, err := ipcReader.Read()
		if err != nil {
			log.Fatalf("Failed to read record batch: %v", err)
		}
		if recordBatch == nil {
			// No more batches
			break
		}

		// Process the record batch
		processRecordBatch(recordBatch)

		// Release memory used by this batch
		recordBatch.Release()
	}
}

func processRecordBatch(recordBatch arrow.RecordBatch) {
	// Get the schema
	schema := recordBatch.Schema()

	// Iterate over all columns
	for i, column := range recordBatch.Columns() {
		// Get the column name
		name := schema.Field(i).Name

		// Get the column data
		data := column.Data()

		// Process the column data
		processColumn(name, data)

		// Release memory used by this column
		data.Release()
	}
}
