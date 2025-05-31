// File: vfs/vfs_test.go
package vfs

import (
	"os"
	"strings"
	"testing"
)

func TestParseS3Path(t *testing.T) {
	bucket, prefix, err := parseS3Path("s3://my-bucket/path/to/folder/")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if bucket != "my-bucket" {
		t.Errorf("expected bucket 'my-bucket', got '%s'", bucket)
	}
	if prefix != "path/to/folder/" {
		t.Errorf("expected prefix 'path/to/folder/', got '%s'", prefix)
	}
}

func TestCalculateChunkSize(t *testing.T) {
	prefix := strings.Repeat("a", 100) + "/"
	size := calculateChunkSize(prefix)
	if size <= 0 {
		t.Errorf("expected positive chunk size, got %d", size)
	}
	if size > 768 {
		t.Errorf("expected chunk size <= 768, got %d", size)
	}
}

func TestGetConcurrency_Default(t *testing.T) {
	os.Unsetenv("S3_CONCURRENCY")
	con := getConcurrency()
	if con != defaultConcurrency {
		t.Errorf("expected default concurrency %d, got %d", defaultConcurrency, con)
	}
}

func TestGetConcurrency_Valid(t *testing.T) {
	os.Setenv("S3_CONCURRENCY", "5")
	defer os.Unsetenv("S3_CONCURRENCY")
	con := getConcurrency()
	if con != 5 {
		t.Errorf("expected concurrency 5, got %d", con)
	}
}

func TestGetConcurrency_Invalid(t *testing.T) {
	os.Setenv("S3_CONCURRENCY", "notanint")
	defer os.Unsetenv("S3_CONCURRENCY")
	con := getConcurrency()
	if con != defaultConcurrency {
		t.Errorf("expected default concurrency on invalid input, got %d", con)
	}
}

