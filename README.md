# VFS (Virtual File Splitter)

[![Go Version](https://img.shields.io/badge/go-1.23+-blue.svg)](https://golang.org/dl/)
[![License](https://img.shields.io/badge/license-Unlicense-green.svg)](LICENSE)

VFS is a Go library and CLI tool that implements a novel approach to file storage using Amazon S3. Instead of storing file contents as S3 object bodies, VFS encodes files as base64-encoded chunks and stores them directly in S3 object keys, creating a lightweight, validation-free storage system.

## 🏗️ Architecture

VFS uses an innovative storage approach:

1. **File Chunking**: Large files are split into chunks sized to fit within S3 key length limits (1024 bytes)
2. **Base64 Encoding**: Each chunk is base64-encoded for safe storage in S3 object keys  
3. **Key-Based Storage**: File data is stored in the object key name itself, not the object body
4. **Parallel Processing**: Upload/download operations are parallelized for performance
5. **Prefix Organization**: Files are organized under S3 prefixes for easy management

### Key Benefits

- **Lightweight**: No actual S3 object storage costs (empty objects)
- **Fast Metadata Operations**: File data accessible through S3 list operations
- **Parallel Processing**: Configurable concurrency for optimal performance
- **Simple Recovery**: Restore files even with partial chunk loss
- **Validation-Free**: No complex integrity checking required

## ✨ Features

- ✅ **Chunked File Encoding**: Split files into optimal chunks for S3 key storage
- ✅ **Safe File Restoration**: Rebuild files from S3 key-based chunks
- ✅ **Parallel Operations**: Configurable concurrency via `S3_CONCURRENCY` 
- ✅ **Prefix Management**: Clean deletion of all objects under a prefix
- ✅ **CLI Interface**: Simple command-line tool for common operations
- ✅ **Go Library**: Full programmatic API for integration
- ✅ **Force Mode**: Override existing data with `--force` flag
- ✅ **Smart Chunk Sizing**: Automatic calculation based on prefix length

## 📦 Installation

### From Source

```bash
# Clone the repository
git clone https://github.com/vjeffz/vfs.git
cd vfs

# Build the CLI tool
make build

# Run the tool
./bin/vfs
```

### As a Go Module

```bash
go get github.com/vjeffz/vfs/vfs
```

## 🚀 Quick Start

### CLI Usage

```bash
# Encode a file to S3
vfs encode myfile.txt s3://my-bucket/files/

# Restore a file from S3  
vfs restore s3://my-bucket/files/ restored.txt

# Delete all chunks under a prefix
vfs delete s3://my-bucket/files/
```

### Library Usage

```go
package main

import (
    "log"
    "github.com/vjeffz/vfs/vfs"
)

func main() {
    // Initialize VFS client
    vfsClient, err := vfs.New()
    if err != nil {
        log.Fatal(err)
    }
    
    // Encode a file
    err = vfsClient.Encode("document.pdf", "s3://my-bucket/docs/", false)
    if err != nil {
        log.Fatal(err)
    }
    
    // Restore the file
    err = vfsClient.Restore("s3://my-bucket/docs/", "restored-document.pdf")
    if err != nil {
        log.Fatal(err)
    }
    
    // Clean up
    err = vfsClient.Delete("s3://my-bucket/docs/")
    if err != nil {
        log.Fatal(err)
    }
}
```

## 🛠️ Configuration

### AWS Credentials

VFS uses the AWS SDK for Go v2, which supports multiple credential sources:

```bash
# Environment variables
export AWS_ACCESS_KEY_ID=your-access-key
export AWS_SECRET_ACCESS_KEY=your-secret-key
export AWS_REGION=us-east-1

# Or use AWS profiles
export AWS_PROFILE=your-profile

# Or use IAM roles, ECS task roles, etc.
```

### Concurrency Control

Control parallel operations with the `S3_CONCURRENCY` environment variable:

```bash
# Set concurrency level (default: 8)
export S3_CONCURRENCY=16
vfs encode largefile.bin s3://my-bucket/large/
```

## 📋 CLI Commands

### `encode` - Store a file in S3

```bash
vfs encode <input-file> s3://bucket/prefix/ [--force]
```

**Options:**
- `--force`: Overwrite existing data without confirmation

**Examples:**
```bash
# Basic encoding
vfs encode photo.jpg s3://photos/vacation/

# Force overwrite existing data
vfs encode document.pdf s3://docs/legal/ --force
```

### `restore` - Restore a file from S3

```bash
vfs restore s3://bucket/prefix/ <output-file>
```

**Examples:**
```bash
# Restore to specific file
vfs restore s3://photos/vacation/ recovered-photo.jpg

# Restore to different location
vfs restore s3://docs/legal/ /tmp/document.pdf
```

### `delete` - Remove all chunks under a prefix

```bash
vfs delete s3://bucket/prefix/
```

**Examples:**
```bash
# Delete all files under prefix
vfs delete s3://photos/vacation/

# Clean up specific document
vfs delete s3://docs/legal/contract.pdf
```

## 🔧 Development

### Building

```bash
# Build CLI binary
make build

# Run tests
make test

# Install globally
make install

# Clean build artifacts
make clean
```

### Testing

```bash
# Run unit tests
go test ./vfs/...

# Run with verbose output
go test -v ./vfs/...

# Run with coverage
go test -cover ./vfs/...
```

### Project Structure

```
├── cmd/           # CLI application
│   └── main.go    # Command-line interface
├── vfs/           # Core library
│   ├── vfs.go     # Main VFS implementation  
│   └── vfs_test.go # Unit tests
├── Makefile       # Build configuration
├── go.mod         # Go module definition
└── README.md      # Project documentation
```

## 🤝 Contributing

This project is released into the public domain under the Unlicense. Contributions are welcome!

1. Fork the repository
2. Create a feature branch
3. Add tests for new functionality  
4. Ensure all tests pass
5. Submit a pull request

## ⚠️ Considerations

### Storage Limits

- **S3 Key Length**: Maximum 1024 bytes per key
- **Chunk Size**: Calculated dynamically based on prefix length
- **File Size**: No theoretical limit (chunked storage)

### Performance

- **Concurrency**: Tune `S3_CONCURRENCY` for your use case
- **Network**: Performance depends on S3 connectivity
- **Chunk Count**: More chunks = more S3 API calls

### Use Cases

**Ideal for:**
- Lightweight file storage with minimal S3 costs
- Temporary file sharing and distribution
- File backup with simple recovery
- Development and testing environments

**Not ideal for:**
- Large-scale production file storage
- Files requiring complex access controls
- Applications needing atomic file operations

## 📄 License

This project is released into the public domain under the [Unlicense](LICENSE). You can do anything you want with this software.

## 🔗 Links

- [AWS SDK for Go v2](https://aws.github.io/aws-sdk-go-v2/)
- [S3 Object Key Naming Guidelines](https://docs.aws.amazon.com/AmazonS3/latest/userguide/object-keys.html)

