# VFS User Guide

This guide provides step-by-step instructions for using VFS (Virtual File Splitter) to store and manage files using Amazon S3.

## Table of Contents

1. [Prerequisites](#prerequisites)
2. [Installation](#installation)
3. [AWS Setup](#aws-setup)
4. [Getting Started](#getting-started)
5. [CLI Usage](#cli-usage)
6. [Library Usage](#library-usage)
7. [Advanced Usage](#advanced-usage)
8. [Troubleshooting](#troubleshooting)
9. [Best Practices](#best-practices)

## Prerequisites

- **Go 1.23+** installed on your system
- **AWS Account** with S3 access
- **AWS CLI** configured (recommended) or AWS credentials
- **Git** for cloning the repository

## Installation

### Option 1: Install from Source

```bash
# Clone the repository
git clone https://github.com/vjeffz/vfs.git
cd vfs

# Build the CLI tool
make build

# Verify installation
./bin/vfs
```

### Option 2: Install as Go Module

```bash
# Install globally
go install github.com/vjeffz/vfs/cmd@latest

# Use in your project
go get github.com/vjeffz/vfs/vfs
```

## AWS Setup

### Step 1: Create AWS Credentials

Choose one of the following methods:

#### Method A: Environment Variables
```bash
export AWS_ACCESS_KEY_ID="your-access-key"
export AWS_SECRET_ACCESS_KEY="your-secret-key"  
export AWS_REGION="us-east-1"
```

#### Method B: AWS Credentials File
```bash
# Configure using AWS CLI
aws configure

# Or manually create ~/.aws/credentials
[default]
aws_access_key_id = your-access-key
aws_secret_access_key = your-secret-key
region = us-east-1
```

#### Method C: IAM Roles (for AWS infrastructure)
When running on EC2, ECS, or Lambda, IAM roles are automatically used.

### Step 2: Create S3 Bucket

```bash
# Using AWS CLI
aws s3 mb s3://your-vfs-bucket

# Or use the AWS Console
```

### Step 3: Verify Permissions

Ensure your AWS credentials have the following S3 permissions:
- `s3:ListBucket`
- `s3:PutObject` 
- `s3:DeleteObject`

## Getting Started

### Your First VFS Operation

Let's store and retrieve a simple text file:

```bash
# Create a test file
echo "Hello, VFS!" > test.txt

# Store the file in S3
./bin/vfs encode test.txt s3://your-vfs-bucket/demo/

# Restore the file
./bin/vfs restore s3://your-vfs-bucket/demo/ restored.txt

# Verify the content
cat restored.txt
# Output: Hello, VFS!

# Clean up
./bin/vfs delete s3://your-vfs-bucket/demo/
```

## CLI Usage

### Command Syntax

```bash
vfs <command> [arguments] [options]
```

### Encode Command

Store a local file in S3 as encoded chunks.

```bash
vfs encode <input-file> s3://bucket/prefix/ [--force]
```

**Examples:**

```bash
# Basic file storage
vfs encode document.pdf s3://my-bucket/docs/

# Store with custom prefix
vfs encode photo.jpg s3://my-bucket/photos/vacation/2024/

# Overwrite existing data
vfs encode updated.txt s3://my-bucket/files/ --force
```

**Interactive Overwrite Protection:**
```bash
$ vfs encode file.txt s3://bucket/existing-prefix/
⚠️  S3 path s3://bucket/existing-prefix/ already contains data. Overwrite? [y/N]: n
✋ Upload canceled.
```

### Restore Command

Retrieve a file from S3 encoded chunks.

```bash
vfs restore s3://bucket/prefix/ <output-file>
```

**Examples:**

```bash
# Basic file restoration
vfs restore s3://my-bucket/docs/ document.pdf

# Restore to different name
vfs restore s3://my-bucket/photos/vacation/2024/ my-photo.jpg

# Restore to specific directory
vfs restore s3://my-bucket/files/ /tmp/restored-file.txt
```

### Delete Command

Remove all chunks under an S3 prefix.

```bash
vfs delete s3://bucket/prefix/
```

**Examples:**

```bash
# Delete specific file prefix
vfs delete s3://my-bucket/docs/document.pdf

# Delete entire directory
vfs delete s3://my-bucket/photos/vacation/

# Clean up root prefix
vfs delete s3://my-bucket/
```

## Library Usage

### Basic Integration

```go
package main

import (
    "fmt"
    "log"
    
    "github.com/vjeffz/vfs/vfs"
)

func main() {
    // Initialize VFS client
    client, err := vfs.New()
    if err != nil {
        log.Fatal("VFS initialization failed:", err)
    }
    
    // Store a file
    err = client.Encode("local-file.txt", "s3://my-bucket/files/", false)
    if err != nil {
        log.Fatal("Encode failed:", err)
    }
    fmt.Println("File stored successfully!")
    
    // Retrieve the file
    err = client.Restore("s3://my-bucket/files/", "restored-file.txt")
    if err != nil {
        log.Fatal("Restore failed:", err)
    }
    fmt.Println("File restored successfully!")
}
```

### Error Handling

```go
func handleVFSError(err error) {
    if err == nil {
        return
    }
    
    switch {
    case strings.Contains(err.Error(), "NoSuchBucket"):
        log.Fatal("S3 bucket not found - check bucket name and permissions")
    case strings.Contains(err.Error(), "AccessDenied"):
        log.Fatal("Access denied - check AWS credentials and S3 permissions")
    case strings.Contains(err.Error(), "no such file"):
        log.Fatal("Input file not found - check file path")
    default:
        log.Fatal("Operation failed:", err)
    }
}
```

### Advanced Library Usage

```go
package main

import (
    "fmt"
    "log"
    "os"
    
    "github.com/vjeffz/vfs/vfs"
)

func backupDirectory(sourceDir, s3Prefix string) error {
    client, err := vfs.New()
    if err != nil {
        return err
    }
    
    files, err := os.ReadDir(sourceDir)
    if err != nil {
        return err
    }
    
    for _, file := range files {
        if file.IsDir() {
            continue
        }
        
        localPath := filepath.Join(sourceDir, file.Name())
        s3Path := fmt.Sprintf("%s%s", s3Prefix, file.Name())
        
        fmt.Printf("Backing up %s...\n", file.Name())
        err := client.Encode(localPath, s3Path, true) // Force overwrite
        if err != nil {
            return fmt.Errorf("failed to backup %s: %w", file.Name(), err)
        }
    }
    
    return nil
}

func main() {
    err := backupDirectory("/home/user/documents", "s3://backup-bucket/docs/")
    if err != nil {
        log.Fatal(err)
    }
    fmt.Println("Backup completed successfully!")
}
```

## Advanced Usage

### Performance Tuning

#### Concurrency Control

```bash
# Default concurrency (8 parallel operations)
vfs encode large-file.zip s3://bucket/files/

# Increased concurrency for faster uploads
export S3_CONCURRENCY=16
vfs encode large-file.zip s3://bucket/files/

# Reduced concurrency for limited bandwidth
export S3_CONCURRENCY=4
vfs encode large-file.zip s3://bucket/files/
```

#### Optimal Prefix Design

Choose shorter prefixes to allow larger chunks:

```bash
# Good: Short prefix allows large chunks
vfs encode file.txt s3://bucket/f/

# Less optimal: Long prefix reduces chunk size
vfs encode file.txt s3://bucket/very/long/nested/prefix/structure/
```

### Batch Operations

Process multiple files efficiently:

```bash
#!/bin/bash
# Backup script
export S3_CONCURRENCY=12

for file in /path/to/files/*; do
    if [ -f "$file" ]; then
        echo "Backing up: $file"
        vfs encode "$file" "s3://backup-bucket/$(basename "$file")/" --force
    fi
done
```

### Integration with Other Tools

#### With Cron Jobs

```bash
# Add to crontab for daily backups
0 2 * * * /usr/local/bin/vfs encode /important/data.db s3://backups/daily/ --force
```

#### With Docker

```dockerfile
FROM golang:1.23-alpine AS builder
WORKDIR /app
COPY . .
RUN make build

FROM alpine:latest
RUN apk add --no-cache ca-certificates
COPY --from=builder /app/bin/vfs /usr/local/bin/
ENTRYPOINT ["vfs"]
```

## Troubleshooting

### Common Issues

#### 1. AWS Credentials Not Found

**Error:** `Unable to load AWS config`

**Solutions:**
- Set environment variables: `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`
- Configure AWS CLI: `aws configure`
- Check IAM role permissions on AWS infrastructure

#### 2. Access Denied

**Error:** `AccessDenied: Access Denied`

**Solutions:**
- Verify S3 bucket exists and you have access
- Check IAM permissions for S3 operations
- Ensure correct AWS region configuration

#### 3. File Not Found

**Error:** `no such file or directory`

**Solutions:**
- Verify input file path is correct
- Check file permissions
- Ensure file is not a directory

#### 4. Invalid S3 URI

**Error:** `must start with s3://`

**Solutions:**
- Use correct format: `s3://bucket-name/prefix/`
- Include trailing slash for prefixes
- Check bucket name follows S3 naming rules

### Debug Mode

Enable verbose logging:

```bash
# Set log level for debugging
export AWS_LOG_LEVEL=debug
vfs encode file.txt s3://bucket/prefix/
```

### Performance Issues

#### Slow Uploads/Downloads

1. **Check network connectivity to S3**
2. **Increase concurrency**: `export S3_CONCURRENCY=16`
3. **Use appropriate S3 region**
4. **Monitor AWS service status**

#### High Memory Usage

1. **Reduce concurrency**: `export S3_CONCURRENCY=4`
2. **Use shorter S3 prefixes** to enable larger chunks
3. **Process files in smaller batches**

## Best Practices

### File Organization

```bash
# Organize by date
s3://bucket/backups/2024/01/15/
s3://bucket/backups/2024/01/16/

# Organize by type
s3://bucket/documents/
s3://bucket/images/
s3://bucket/archives/

# Use descriptive prefixes
s3://bucket/user-documents/john-doe/
s3://bucket/project-assets/website-redesign/
```

### Security

1. **Use IAM roles** instead of access keys when possible
2. **Apply least-privilege permissions**
3. **Enable S3 bucket versioning** for additional protection
4. **Use S3 bucket policies** to restrict access
5. **Monitor AWS CloudTrail** for access logs

### Backup Strategy

```bash
# Daily backups with date prefixes
DATE=$(date +%Y/%m/%d)
vfs encode important-data.db "s3://backups/daily/$DATE/" --force

# Keep multiple versions
vfs encode config.json "s3://backups/config/$(date +%Y%m%d-%H%M%S)/" --force
```

### Monitoring

Track VFS operations:

```bash
# Log all operations
{
    echo "$(date): Starting backup"
    vfs encode data.txt s3://bucket/backup/ --force
    echo "$(date): Backup completed with exit code $?"
} >> /var/log/vfs-backup.log
```

### Cost Optimization

1. **Use shorter prefixes** to reduce number of chunks
2. **Delete old backups** regularly
3. **Use S3 lifecycle policies** for automatic cleanup
4. **Monitor S3 request charges** in AWS billing