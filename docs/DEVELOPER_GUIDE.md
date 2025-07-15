# VFS Developer Guide

This guide covers the technical architecture, development workflow, and contribution guidelines for the VFS project.

## Table of Contents

1. [Architecture Overview](#architecture-overview)
2. [Code Structure](#code-structure)
3. [Development Setup](#development-setup)
4. [Testing](#testing)
5. [Contributing](#contributing)
6. [Design Decisions](#design-decisions)
7. [Performance Considerations](#performance-considerations)
8. [Future Enhancements](#future-enhancements)

## Architecture Overview

### Core Concept

VFS implements a novel approach to file storage by encoding file contents as base64 strings and storing them directly in S3 object keys rather than object bodies. This creates a lightweight, metadata-driven storage system.

### Key Components

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   CLI Layer     │    │  Library Layer  │    │   S3 Backend    │
│                 │    │                 │    │                 │
│  • Command      │───▶│  • VFS Client   │───▶│  • Object Keys  │
│    Parsing      │    │  • Chunking     │    │  • Empty Bodies │
│  • User I/O     │    │  • Encoding     │    │  • Metadata     │
│  • Error        │    │  • Concurrency  │    │                 │
│    Handling     │    │                 │    │                 │
└─────────────────┘    └─────────────────┘    └─────────────────┘
```

### Data Flow

#### Encode Operation
```
Local File → Read Chunks → Base64 Encode → S3 Key Names → Empty S3 Objects
```

#### Restore Operation  
```
S3 List Objects → Extract Key Names → Base64 Decode → Reassemble → Local File
```

### S3 Key Structure

```
s3://bucket/prefix/1-SGVsbG8gV29ybGQ
s3://bucket/prefix/2-VEhpcyBpcyBjaHVuaw
s3://bucket/prefix/3-ZmluYWwgY2h1bms
                    │   │
                    │   └── Base64 encoded chunk data
                    └── Chunk index (1-based)
```

## Code Structure

### Directory Layout

```
vfs/
├── cmd/                    # CLI application
│   └── main.go            # Entry point and command parsing
├── vfs/                   # Core library package
│   ├── vfs.go            # Main VFS implementation
│   └── vfs_test.go       # Unit tests
├── docs/                 # Documentation
│   ├── API.md
│   ├── USER_GUIDE.md
│   └── DEVELOPER_GUIDE.md
├── Makefile              # Build automation
├── go.mod                # Go module definition
├── go.sum                # Dependency checksums
├── LICENSE               # Public domain license
└── README.md             # Project overview
```

### Package Dependencies

```go
// External dependencies
github.com/aws/aws-sdk-go-v2/config     // AWS configuration
github.com/aws/aws-sdk-go-v2/service/s3 // S3 client operations

// Standard library
encoding/base64    // Chunk encoding/decoding  
context           // Request contexts
sync              // Concurrency primitives
bufio             // User input handling
path              // S3 key path manipulation
```

### Key Data Structures

```go
// Main VFS client
type VFS struct {
    client      *s3.Client  // AWS S3 client
    concurrency int         // Parallel operation limit
}

// Internal chunk representation
type chunk struct {
    index   int    // 1-based chunk number
    encoded string // Base64 encoded data
}
```

## Development Setup

### Prerequisites

- Go 1.23+ installed
- Git for version control
- AWS CLI configured (for testing)
- Make utility

### Local Environment

```bash
# Clone and setup
git clone https://github.com/vjeffz/vfs.git
cd vfs

# Install dependencies
go mod download

# Build project
make build

# Run tests
make test

# Install development tools
go install golang.org/x/tools/cmd/goimports@latest
go install github.com/golangci/golangci-lint/cmd/golangci-lint@latest
```

### Code Style

Follow standard Go conventions:

```bash
# Format code
go fmt ./...

# Import organization
goimports -w .

# Lint code
golangci-lint run
```

### Build System

The Makefile provides standard targets:

```makefile
build:    # Build CLI binary
run:      # Run with go run
install:  # Install globally
clean:    # Remove build artifacts
test:     # Run unit tests
```

## Testing

### Unit Tests

```bash
# Run all tests
go test ./...

# Run with coverage
go test -cover ./vfs/

# Verbose output
go test -v ./vfs/

# Run specific test
go test -run TestParseS3Path ./vfs/
```

### Test Structure

```go
func TestFunctionName(t *testing.T) {
    // Setup test data
    
    // Execute function
    result, err := functionUnderTest(input)
    
    // Verify results
    if err != nil {
        t.Fatalf("unexpected error: %v", err)
    }
    if result != expected {
        t.Errorf("expected %v, got %v", expected, result)
    }
}
```

### Integration Testing

For integration tests with real S3:

```bash
# Set test environment
export AWS_PROFILE=test-profile
export VFS_TEST_BUCKET=vfs-integration-tests

# Run integration tests (not included in standard test suite)
go test -tags=integration ./...
```

### Mock Testing

Consider adding S3 mocks for comprehensive testing:

```go
// Example mock interface
type S3Client interface {
    PutObject(ctx context.Context, input *s3.PutObjectInput, opts ...func(*s3.Options)) (*s3.PutObjectOutput, error)
    ListObjectsV2(ctx context.Context, input *s3.ListObjectsV2Input, opts ...func(*s3.Options)) (*s3.ListObjectsV2Output, error)
    DeleteObjects(ctx context.Context, input *s3.DeleteObjectsInput, opts ...func(*s3.Options)) (*s3.DeleteObjectsOutput, error)
}
```

## Contributing

### Development Workflow

1. **Fork the repository**
2. **Create feature branch**: `git checkout -b feature/new-feature`
3. **Make changes** following code style guidelines
4. **Add tests** for new functionality
5. **Update documentation** as needed
6. **Commit changes**: `git commit -m "Add new feature"`
7. **Push branch**: `git push origin feature/new-feature`
8. **Submit pull request**

### Commit Message Guidelines

```
type(scope): brief description

Longer description explaining the change motivation
and implementation details.

Fixes #123
```

Types: `feat`, `fix`, `docs`, `style`, `refactor`, `test`, `chore`

### Code Review Checklist

- [ ] Code follows Go conventions
- [ ] Tests added for new functionality
- [ ] Documentation updated
- [ ] No breaking changes (or properly documented)
- [ ] Error handling appropriate
- [ ] Performance considerations addressed

### Release Process

1. Update version in documentation
2. Create git tag: `git tag v1.x.x`
3. Push tag: `git push origin v1.x.x`
4. GitHub Actions creates release (if configured)

## Design Decisions

### Why S3 Object Keys?

**Advantages:**
- **Cost Effective**: No storage charges for empty objects
- **Metadata Rich**: File data accessible via list operations
- **Simple Recovery**: Easy to browse and recover partial data
- **No Size Limits**: Virtually unlimited file size through chunking

**Trade-offs:**
- **Key Length Limit**: 1024 bytes maximum per key
- **API Call Overhead**: More requests for large files
- **Encoding Overhead**: Base64 increases size by ~33%

### Chunking Strategy

```go
func calculateChunkSize(prefix string) int {
    available := s3MaxKeyLengthBytes - len(prefix) - maxIndexLen
    return (available * 3) / 4  // Account for base64 expansion
}
```

This ensures:
- Maximum utilization of S3 key space
- Predictable chunk sizes
- Prefix length awareness

### Concurrency Model

```go
// Semaphore pattern for controlled concurrency
sem := make(chan struct{}, v.concurrency)
var wg sync.WaitGroup

for i, chunk := range chunks {
    sem <- struct{}{}  // Acquire semaphore
    wg.Add(1)
    go func(i int, data []byte) {
        defer wg.Done()
        defer func() { <-sem }()  // Release semaphore
        // Process chunk
    }(i, chunk)
}
wg.Wait()
```

Benefits:
- **Resource Control**: Limits memory and network usage
- **Error Handling**: First error stops processing
- **Progress Tracking**: Real-time operation feedback

### Error Handling Philosophy

- **Fail Fast**: Return immediately on critical errors
- **User Friendly**: Provide actionable error messages
- **Graceful Degradation**: Continue when possible
- **Resource Cleanup**: Always clean up on errors

## Performance Considerations

### Memory Usage

Current implementation loads entire chunks in memory:
- **Memory per chunk**: ~(chunk_size * concurrency)
- **Peak memory**: During concurrent operations
- **Optimization**: Consider streaming for very large files

### Network Efficiency

- **Parallel uploads/downloads**: Configurable concurrency
- **Batch operations**: S3 batch delete for cleanup
- **Request minimization**: Single list operation per restore

### Scalability Limits

- **Chunk count**: No practical limit
- **File size**: Limited by available memory and time
- **Concurrency**: Limited by AWS API rate limits

### Optimization Opportunities

1. **Streaming chunks**: Reduce memory usage
2. **Compression**: Optional chunk compression
3. **Caching**: Local chunk cache for repeated access
4. **Resumable operations**: Resume interrupted transfers

## Future Enhancements

### Planned Features

1. **Compression Support**
   ```go
   type VFS struct {
       client      *s3.Client
       concurrency int
       compression bool  // Enable chunk compression
   }
   ```

2. **Metadata Storage**
   ```go
   type FileMetadata struct {
       OriginalSize int64     `json:"original_size"`
       ChunkCount   int       `json:"chunk_count"`
       Checksum     string    `json:"checksum"`
       Compressed   bool      `json:"compressed"`
   }
   ```

3. **Streaming Operations**
   ```go
   func (v *VFS) EncodeStream(reader io.Reader, s3URI string) error
   func (v *VFS) RestoreStream(s3URI string, writer io.Writer) error
   ```

4. **Progress Callbacks**
   ```go
   type ProgressFunc func(completed, total int64)
   
   func (v *VFS) EncodeWithProgress(inputPath, s3URI string, progress ProgressFunc) error
   ```

### Architecture Evolution

#### Plugin System
```go
type Encoder interface {
    Encode(data []byte) ([]byte, error)
    Decode(data []byte) ([]byte, error)
}

type VFS struct {
    client   *s3.Client
    encoder  Encoder  // Pluggable encoding
}
```

#### Multi-Backend Support
```go
type Backend interface {
    Store(key string, data []byte) error
    Retrieve(prefix string) ([]Chunk, error)
    Delete(prefix string) error
}

// Implementations: S3Backend, GCSBackend, AzureBackend
```

### Testing Infrastructure

1. **Integration test suite** with real S3
2. **Benchmark tests** for performance regression
3. **Fuzz testing** for edge cases
4. **Cross-platform CI/CD**

### Documentation Improvements

1. **API documentation** with examples
2. **Video tutorials** for common use cases
3. **Performance tuning guide**
4. **Troubleshooting runbook**

## Development Tools

### Recommended IDE Setup

**VS Code extensions:**
- Go extension
- AWS Toolkit
- GitLens
- Better Comments

**GoLand/IntelliJ:**
- AWS Toolkit plugin
- Go plugin

### Debugging

```bash
# Debug with Delve
dlv debug cmd/main.go -- encode file.txt s3://bucket/prefix/

# Add debug prints
log.Printf("DEBUG: Processing chunk %d, size %d", index, len(chunk))

# Profile performance
go tool pprof cpu.prof
```

### Continuous Integration

Example GitHub Actions workflow:

```yaml
name: CI
on: [push, pull_request]
jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v2
      - uses: actions/setup-go@v2
        with:
          go-version: 1.23
      - run: make test
      - run: golangci-lint run
```

This developer guide provides the foundation for contributing to VFS and understanding its technical implementation.