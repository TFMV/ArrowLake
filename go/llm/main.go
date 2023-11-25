package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log"
	"strings"
	"sync"

	"cloud.google.com/go/storage"
	"google.golang.org/api/iterator"
)

func walkGCSBucket(bucketName string) error {
	ctx := context.Background()
	client, err := storage.NewClient(ctx)
	if err != nil {
		return fmt.Errorf("storage.NewClient: %v", err)
	}
	defer client.Close()

	it := client.Bucket(bucketName).Objects(ctx, nil)
	for {
		attrs, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return fmt.Errorf("Bucket(%q).Objects: %v", bucketName, err)
		}

		fileType := determineFileType(client.Bucket(bucketName).Object(attrs.Name))
		if fileType == "parquet" {
			fmt.Println(attrs.Name)
		}

	}
	return nil
}

func determineFileType(obj *storage.ObjectHandle) string {
	ctx := context.Background()

	// Get the object attributes to inspect the file name
	attrs, err := obj.Attrs(ctx)
	if err != nil {
		return "unknown"
	}

	fileName := attrs.Name

	// Check file extension
	if strings.HasSuffix(fileName, "csv.gz") {
		return "gz.csv"
	} else if strings.HasSuffix(fileName, ".avro") {
		return "avro"
	} else if strings.HasSuffix(fileName, ".parquet") {
		return "parquet"
	} else if strings.HasSuffix(fileName, ".csv") {
		return "csv"
	} else if strings.HasSuffix(fileName, ".json") {
		return "json"
	}

	reader, err := obj.NewReader(ctx)
	if err != nil {
		return "unknown"
	}
	defer reader.Close()

	buf := make([]byte, 512)
	if _, err := io.ReadFull(reader, buf); err != nil {
		return "unknown"
	}

	if bytes.HasPrefix(buf, []byte{0x50, 0x41, 0x52, 0x31}) {
		return "parquet"
	} else if bytes.HasPrefix(buf, []byte{0x50, 0x4b}) {
		return "zip"
	} else if bytes.HasPrefix(buf, []byte{0x1f, 0x8b}) {
		return "gzip"
	} else if bytes.HasPrefix(buf, []byte{0x7b}) {
		return "json"
	} else if bytes.HasPrefix(buf, []byte{0x5b}) {
		return "json"
	} else if bytes.HasPrefix(buf, []byte{0x41, 0x56, 0x52, 0x4f}) {
		return "avro"
	} else if bytes.HasPrefix(buf, []byte{0x49, 0x44, 0x33}) {
		return "mp3"
	} else if bytes.HasPrefix(buf, []byte{0x46, 0x4c, 0x49, 0x46}) {
		return "flif"
	} else if bytes.HasPrefix(buf, []byte{0x46, 0x4f, 0x52, 0x4d}) {
		return "aiff"
	} else if bytes.HasPrefix(buf, []byte{0x52, 0x49, 0x46, 0x46}) {
		return "wav"
	} else if bytes.HasPrefix(buf, []byte{0x00, 0x00, 0x01, 0xba}) {
		return "mpeg"
	} else {
		return "unknown"
	}
}

func readFile(obj *storage.ObjectHandle) {
	ctx := context.Background()
	reader, err := obj.NewReader(ctx)
	if err != nil {
		log.Printf("Failed to create reader: %v", err)
		return
	}
	defer reader.Close()

	// Determine file type (this is a simplified approach)
	fileType := determineFileType(obj) // Implement this function based on object attributes

	switch fileType {
	case "parquet":
		// readParquet(reader)
	case "csv":
		// readCSV(reader)
	// Add cases for other file types
	default:
		log.Println("Unsupported file type")
	}
}

func main() {
	// Setup GCS client and context
	ctx := context.Background()
	client, err := storage.NewClient(ctx)
	if err != nil {
		log.Fatalf("Failed to create client: %v", err)
	}

	bucket := client.Bucket("tfmv")
	it := bucket.Objects(ctx, nil)
	var wg sync.WaitGroup

	for {
		attrs, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			log.Printf("Error listing objects: %v", err)
			break
		}

		obj := bucket.Object(attrs.Name)

		wg.Add(1)
		go func(obj *storage.ObjectHandle) {
			defer wg.Done()
			readFile(obj)
		}(obj)
	}

	wg.Wait()
}
