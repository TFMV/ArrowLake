package main

import (
	"context"
	"fmt"
	"log"

	"cloud.google.com/go/storage"
	"github.com/apache/iceberg-go"
)

func convertParquetToIceberg(bucketName, parquetFilePath, icebergFilePath string) error {
	ctx := context.Background()

	// Initialize GCS client
	client, err := storage.NewClient(ctx)
	if err != nil {
		return fmt.Errorf("storage.NewClient: %v", err)
	}
	defer client.Close()

	// Read Parquet file from GCS
	reader, err := client.Bucket(bucketName).Object(parquetFilePath).NewReader(ctx)
	if err != nil {
		return fmt.Errorf("Object(%q).NewReader: %v", parquetFilePath, err)
	}
	defer reader.Close()

	parquetFileReader, err := parquet.NewFileReader(reader)
	if err != nil {
		return fmt.Errorf("parquet.NewFileReader: %v", err)
	}
	defer parquetFileReader.Close()

	// Create a new Iceberg table writer
	icebergWriter, err := iceberg.NewWriter(ctx, icebergFilePath)
	if err != nil {
		return fmt.Errorf("iceberg.NewWriter: %v", err)
	}
	defer icebergWriter.Close()

	// Convert and write to Iceberg
	for parquetFileReader.Next() {
		record := parquetFileReader.Record()
		if err := icebergWriter.Write(record); err != nil {
			return fmt.Errorf("icebergWriter.Write: %v", err)
		}
	}

	if err := icebergWriter.Commit(); err != nil {
		return fmt.Errorf("icebergWriter.Commit: %v", err)
	}

	return nil
}

func main() {
	bucketName := "your-gcs-bucket"
	parquetFilePath := "path/to/parquet/file"
	icebergFilePath := "path/to/iceberg/output"

	if err := convertParquetToIceberg(bucketName, parquetFilePath, icebergFilePath); err != nil {
		log.Fatalf("Failed to convert Parquet to Iceberg: %v", err)
	}

	log.Println("Conversion completed successfully")
}
