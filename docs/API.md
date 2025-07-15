# VFS API Documentation

This document provides detailed API documentation for the VFS Go package.

## Package Overview

The `vfs` package provides a Virtual File System implementation that stores files as base64-encoded chunks in Amazon S3 object keys.

```go
import "github.com/vjeffz/vfs/vfs"
```

## Types

### VFS

The main VFS client structure for interacting with S3-based virtual file system.

```go
type VFS struct {
    // contains filtered or unexported fields
}
```

## Functions

### New

```go
func New() (*VFS, error)
```

`New` creates a new VFS client instance with default AWS configuration.

**Returns:**
- `*VFS`: Configured VFS client
- `error`: Configuration error if AWS setup fails

**Example:**
```go
vfsClient, err := vfs.New()
if err != nil {
    log.Fatal("Failed to initialize VFS:", err)
}
```

**AWS Configuration:**
The function uses AWS SDK's default configuration loading, which supports:
- Environment variables (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_REGION`)
- AWS credential files (`~/.aws/credentials`, `~/.aws/config`) 
- IAM roles (EC2, ECS, Lambda)
- AWS SSO profiles

## Methods

### Encode

```go
func (v *VFS) Encode(inputPath, s3URI string, force bool) error
```

Encodes a local file into base64 chunks stored as S3 object keys.

**Parameters:**
- `inputPath` (string): Path to the local file to encode
- `s3URI` (string): S3 destination URI in format `s3://bucket/prefix/`
- `force` (bool): If true, overwrites existing data without confirmation

**Returns:**
- `error`: Operation error or nil on success

**Behavior:**
1. Checks if target S3 prefix already contains data
2. Prompts for confirmation unless `force` is true
3. Calculates optimal chunk size based on prefix length
4. Reads file in chunks and base64-encodes each chunk
5. Uploads chunks in parallel as S3 object keys
6. Progress is displayed during upload

**Example:**
```go
// Basic encoding
err := vfsClient.Encode("document.pdf", "s3://my-bucket/docs/", false)

// Force overwrite without confirmation
err := vfsClient.Encode("photo.jpg", "s3://my-bucket/images/", true)
```

**Error Conditions:**
- File doesn't exist or can't be read
- Invalid S3 URI format
- AWS permissions issues
- Calculated chunk size too small for S3 key constraints

### Restore

```go
func (v *VFS) Restore(s3URI, outputPath string) error
```

Restores a file from base64 chunks stored in S3 object keys.

**Parameters:**
- `s3URI` (string): S3 source URI in format `s3://bucket/prefix/`
- `outputPath` (string): Local path where restored file will be written

**Returns:**
- `error`: Operation error or nil on success

**Behavior:**
1. Lists all objects under the specified S3 prefix
2. Parses object keys to extract chunk index and base64 data
3. Sorts chunks by index to ensure correct order
4. Downloads and decodes chunks in parallel
5. Writes assembled data to output file
6. Creates parent directories if they don't exist

**Example:**
```go
// Restore to specific location
err := vfsClient.Restore("s3://my-bucket/docs/", "restored-document.pdf")

// Restore to different directory
err := vfsClient.Restore("s3://my-bucket/images/", "/tmp/photo.jpg")
```

**Error Conditions:**
- No chunks found at S3 prefix
- Invalid S3 URI format
- Can't create output directory or file
- Base64 decoding failures
- AWS permissions issues

### Delete

```go
func (v *VFS) Delete(s3URI string) error
```

Deletes all objects under the specified S3 prefix.

**Parameters:**
- `s3URI` (string): S3 prefix URI in format `s3://bucket/prefix/`

**Returns:**
- `error`: Operation error or nil on success

**Behavior:**
1. Lists all objects under the specified prefix
2. Deletes objects in batches using S3 batch delete operations
3. Displays progress during deletion
4. Continues until all objects are removed

**Example:**
```go
// Delete all chunks under prefix
err := vfsClient.Delete("s3://my-bucket/docs/")

// Clean up specific file prefix
err := vfsClient.Delete("s3://my-bucket/images/photo.jpg")
```

**Error Conditions:**
- Invalid S3 URI format
- AWS permissions issues
- S3 service errors

## Configuration

### Environment Variables

#### S3_CONCURRENCY

Controls the number of parallel operations for upload/download.

- **Type**: Integer
- **Default**: 8
- **Range**: 1-unlimited (recommended: 4-32)

```bash
export S3_CONCURRENCY=16
```

Higher values increase throughput but also increase memory usage and API request rate.

### AWS Configuration

VFS uses AWS SDK for Go v2 configuration. Set up credentials using any supported method:

#### Environment Variables
```bash
export AWS_ACCESS_KEY_ID=your-access-key
export AWS_SECRET_ACCESS_KEY=your-secret-key
export AWS_REGION=us-east-1
```

#### AWS Profile
```bash
export AWS_PROFILE=your-profile-name
```

#### IAM Roles
When running on AWS infrastructure (EC2, ECS, Lambda), IAM roles are automatically used.

## Internal Functions

The following functions are used internally by the VFS package:

### parseS3Path

```go
func parseS3Path(s3Path string) (string, string, error)
```

Parses an S3 URI into bucket and prefix components.

### calculateChunkSize

```go
func calculateChunkSize(prefix string) int
```

Calculates optimal chunk size based on S3 key length constraints and prefix length.

### getConcurrency

```go
func getConcurrency() int
```

Retrieves concurrency setting from environment or returns default.

## Constants

```go
const (
    s3MaxKeyLengthBytes = 1024  // Maximum S3 object key length
    maxIndexLen         = 6     // Maximum digits for chunk index
    defaultConcurrency  = 8     // Default parallel operation count
)
```

## Error Handling

All VFS methods return standard Go errors. Common error patterns:

```go
// Check for specific error types
if err != nil {
    switch {
    case strings.Contains(err.Error(), "NoSuchBucket"):
        log.Fatal("S3 bucket does not exist")
    case strings.Contains(err.Error(), "AccessDenied"):
        log.Fatal("Insufficient S3 permissions")
    default:
        log.Fatal("Operation failed:", err)
    }
}
```

## Thread Safety

The VFS client is safe for concurrent use across multiple goroutines. Each method handles its own synchronization internally.

## Memory Considerations

- Chunk data is processed in memory during encode/restore operations
- Memory usage scales with concurrency level and chunk size
- Large files are processed incrementally to minimize memory footprint

## Performance Tips

1. **Tune Concurrency**: Adjust `S3_CONCURRENCY` based on network bandwidth and CPU capacity
2. **Prefix Organization**: Use shorter prefixes to allow larger chunk sizes
3. **Network Proximity**: Use S3 regions close to your infrastructure
4. **Batch Operations**: Group multiple file operations when possible