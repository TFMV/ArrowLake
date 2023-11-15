package main

import (
	"context"
	"fmt"
	"path/filepath"

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

		fileType := filepath.Ext(attrs.Name)
		fmt.Printf("File: %v, Type: %v\n", attrs.Name, fileType)
		// Further processing based on fileType
	}
	return nil
}

func main() {
	bucketName := "tfmv"
	err := walkGCSBucket(bucketName)
	if err != nil {
		fmt.Printf("Failed to walk GCS bucket: %v\n", err)
	}
}
