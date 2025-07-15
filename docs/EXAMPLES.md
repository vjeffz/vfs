# VFS Examples

This document provides practical examples and use cases for the VFS (Virtual File Splitter) tool and library.

## Table of Contents

1. [Basic CLI Examples](#basic-cli-examples)
2. [Library Integration Examples](#library-integration-examples)
3. [Advanced Use Cases](#advanced-use-cases)
4. [Automation Scripts](#automation-scripts)
5. [Integration Patterns](#integration-patterns)

## Basic CLI Examples

### Simple File Operations

```bash
# Store a document
vfs encode report.pdf s3://company-docs/reports/

# Retrieve the document
vfs restore s3://company-docs/reports/ report-copy.pdf

# Verify the files are identical
diff report.pdf report-copy.pdf

# Clean up
vfs delete s3://company-docs/reports/
```

### Working with Different File Types

```bash
# Image files
vfs encode photo.jpg s3://photos/vacation/2024/
vfs encode video.mp4 s3://photos/vacation/2024/

# Configuration files
vfs encode config.json s3://backups/configs/app/
vfs encode .env s3://backups/configs/app/

# Archives
vfs encode backup.tar.gz s3://archives/daily/
vfs encode database.sql s3://archives/db/
```

### Batch Processing

```bash
# Process multiple files
for file in *.txt; do
    echo "Storing: $file"
    vfs encode "$file" "s3://text-files/$(basename "$file")/" --force
done

# Restore multiple files
for prefix in documents/ images/ configs/; do
    vfs restore "s3://backups/$prefix" "restored-${prefix%/}"
done
```

### Force Mode Usage

```bash
# Initial upload
vfs encode data.csv s3://datasets/sales/

# Update with new version (overwrites without prompt)
vfs encode data-updated.csv s3://datasets/sales/ --force

# Automated scripts (no user interaction)
vfs encode nightly-backup.tar s3://backups/nightly/ --force
```

## Library Integration Examples

### Basic Library Usage

```go
package main

import (
    "fmt"
    "log"
    
    "github.com/vjeffz/vfs/vfs"
)

func main() {
    // Initialize client
    client, err := vfs.New()
    if err != nil {
        log.Fatal("Failed to create VFS client:", err)
    }
    
    // Store a file
    fmt.Println("Storing file...")
    err = client.Encode("data.txt", "s3://my-bucket/files/", false)
    if err != nil {
        log.Fatal("Encode failed:", err)
    }
    
    // Retrieve the file
    fmt.Println("Retrieving file...")
    err = client.Restore("s3://my-bucket/files/", "data-restored.txt")
    if err != nil {
        log.Fatal("Restore failed:", err)
    }
    
    fmt.Println("Operation completed successfully!")
}
```

### Error Handling

```go
package main

import (
    "fmt"
    "log"
    "strings"
    
    "github.com/vjeffz/vfs/vfs"
)

func handleVFSOperation(client *vfs.VFS, operation string, args ...string) error {
    var err error
    
    switch operation {
    case "encode":
        err = client.Encode(args[0], args[1], false)
    case "restore":
        err = client.Restore(args[0], args[1])
    case "delete":
        err = client.Delete(args[0])
    default:
        return fmt.Errorf("unknown operation: %s", operation)
    }
    
    if err != nil {
        // Handle specific error types
        switch {
        case strings.Contains(err.Error(), "NoSuchBucket"):
            return fmt.Errorf("S3 bucket not found: %w", err)
        case strings.Contains(err.Error(), "AccessDenied"):
            return fmt.Errorf("insufficient permissions: %w", err)
        case strings.Contains(err.Error(), "no such file"):
            return fmt.Errorf("file not found: %w", err)
        default:
            return fmt.Errorf("operation failed: %w", err)
        }
    }
    
    return nil
}

func main() {
    client, err := vfs.New()
    if err != nil {
        log.Fatal(err)
    }
    
    err = handleVFSOperation(client, "encode", "test.txt", "s3://bucket/files/")
    if err != nil {
        log.Printf("Error: %v", err)
    }
}
```

### Concurrent Operations

```go
package main

import (
    "fmt"
    "log"
    "sync"
    
    "github.com/vjeffz/vfs/vfs"
)

func processFiles(files []string, s3Prefix string) error {
    client, err := vfs.New()
    if err != nil {
        return err
    }
    
    var wg sync.WaitGroup
    errChan := make(chan error, len(files))
    
    for i, file := range files {
        wg.Add(1)
        go func(filename string, index int) {
            defer wg.Done()
            
            s3Path := fmt.Sprintf("%sfile-%d/", s3Prefix, index)
            err := client.Encode(filename, s3Path, true)
            if err != nil {
                errChan <- fmt.Errorf("failed to encode %s: %w", filename, err)
            }
        }(file, i)
    }
    
    wg.Wait()
    close(errChan)
    
    // Check for errors
    for err := range errChan {
        return err
    }
    
    return nil
}

func main() {
    files := []string{"doc1.txt", "doc2.txt", "doc3.txt"}
    err := processFiles(files, "s3://documents/batch/")
    if err != nil {
        log.Fatal(err)
    }
    fmt.Println("All files processed successfully!")
}
```

## Advanced Use Cases

### Backup System

```go
package main

import (
    "fmt"
    "log"
    "os"
    "path/filepath"
    "strings"
    "time"
    
    "github.com/vjeffz/vfs/vfs"
)

type BackupManager struct {
    client   *vfs.VFS
    s3Prefix string
}

func NewBackupManager(s3Prefix string) (*BackupManager, error) {
    client, err := vfs.New()
    if err != nil {
        return nil, err
    }
    
    return &BackupManager{
        client:   client,
        s3Prefix: s3Prefix,
    }, nil
}

func (bm *BackupManager) BackupDirectory(sourceDir string) error {
    return filepath.Walk(sourceDir, func(path string, info os.FileInfo, err error) error {
        if err != nil {
            return err
        }
        
        if info.IsDir() {
            return nil
        }
        
        // Create relative path
        relPath, err := filepath.Rel(sourceDir, path)
        if err != nil {
            return err
        }
        
        // Generate timestamped S3 path
        timestamp := time.Now().Format("2006-01-02")
        s3Path := fmt.Sprintf("%s%s/%s/", bm.s3Prefix, timestamp, 
                             strings.ReplaceAll(relPath, string(os.PathSeparator), "/"))
        
        fmt.Printf("Backing up: %s -> %s\n", path, s3Path)
        return bm.client.Encode(path, s3Path, true)
    })
}

func (bm *BackupManager) RestoreBackup(backupDate, targetDir string) error {
    // Implementation for restore would list all objects under the date prefix
    // and restore each file to the target directory
    return nil
}

func main() {
    backup, err := NewBackupManager("s3://backups/daily/")
    if err != nil {
        log.Fatal(err)
    }
    
    err = backup.BackupDirectory("/home/user/documents")
    if err != nil {
        log.Fatal(err)
    }
    
    fmt.Println("Backup completed successfully!")
}
```

### Configuration Manager

```go
package main

import (
    "encoding/json"
    "fmt"
    "log"
    "os"
    "time"
    
    "github.com/vjeffz/vfs/vfs"
)

type ConfigManager struct {
    client *vfs.VFS
    bucket string
}

type AppConfig struct {
    Version     string            `json:"version"`
    Settings    map[string]string `json:"settings"`
    UpdatedAt   time.Time         `json:"updated_at"`
    Environment string            `json:"environment"`
}

func NewConfigManager(bucket string) (*ConfigManager, error) {
    client, err := vfs.New()
    if err != nil {
        return nil, err
    }
    
    return &ConfigManager{
        client: client,
        bucket: bucket,
    }, nil
}

func (cm *ConfigManager) SaveConfig(env string, config *AppConfig) error {
    config.UpdatedAt = time.Now()
    config.Environment = env
    
    // Marshal to JSON
    data, err := json.MarshalIndent(config, "", "  ")
    if err != nil {
        return err
    }
    
    // Write to temporary file
    tmpFile := fmt.Sprintf("/tmp/config-%s.json", env)
    err = os.WriteFile(tmpFile, data, 0644)
    if err != nil {
        return err
    }
    defer os.Remove(tmpFile)
    
    // Store in VFS
    s3Path := fmt.Sprintf("s3://%s/configs/%s/", cm.bucket, env)
    return cm.client.Encode(tmpFile, s3Path, true)
}

func (cm *ConfigManager) LoadConfig(env string) (*AppConfig, error) {
    // Restore config from VFS
    tmpFile := fmt.Sprintf("/tmp/config-%s-restored.json", env)
    s3Path := fmt.Sprintf("s3://%s/configs/%s/", cm.bucket, env)
    
    err := cm.client.Restore(s3Path, tmpFile)
    if err != nil {
        return nil, err
    }
    defer os.Remove(tmpFile)
    
    // Read and unmarshal
    data, err := os.ReadFile(tmpFile)
    if err != nil {
        return nil, err
    }
    
    var config AppConfig
    err = json.Unmarshal(data, &config)
    return &config, err
}

func main() {
    cm, err := NewConfigManager("my-app-configs")
    if err != nil {
        log.Fatal(err)
    }
    
    // Save configuration
    config := &AppConfig{
        Version: "1.0.0",
        Settings: map[string]string{
            "debug":    "true",
            "log_level": "info",
            "timeout":   "30s",
        },
    }
    
    err = cm.SaveConfig("production", config)
    if err != nil {
        log.Fatal(err)
    }
    
    // Load configuration
    loadedConfig, err := cm.LoadConfig("production")
    if err != nil {
        log.Fatal(err)
    }
    
    fmt.Printf("Loaded config: %+v\n", loadedConfig)
}
```

## Automation Scripts

### Daily Backup Script

```bash
#!/bin/bash
# daily-backup.sh

set -e

# Configuration
BACKUP_SOURCE="/home/user/important"
S3_BUCKET="daily-backups"
DATE=$(date +%Y-%m-%d)
LOG_FILE="/var/log/vfs-backup.log"

# Set up logging
exec 1> >(tee -a "$LOG_FILE")
exec 2>&1

echo "$(date): Starting daily backup"

# Set concurrency for large files
export S3_CONCURRENCY=12

# Create manifest of files being backed up
find "$BACKUP_SOURCE" -type f > "/tmp/backup-manifest-$DATE.txt"
echo "Found $(wc -l < "/tmp/backup-manifest-$DATE.txt") files to backup"

# Store manifest
vfs encode "/tmp/backup-manifest-$DATE.txt" "s3://$S3_BUCKET/manifests/$DATE/" --force

# Backup each file
while IFS= read -r file; do
    if [ -f "$file" ]; then
        rel_path=$(realpath --relative-to="$BACKUP_SOURCE" "$file")
        s3_path="s3://$S3_BUCKET/files/$DATE/$rel_path/"
        
        echo "Backing up: $file"
        if ! vfs encode "$file" "$s3_path" --force; then
            echo "ERROR: Failed to backup $file"
            exit 1
        fi
    fi
done < "/tmp/backup-manifest-$DATE.txt"

# Clean up
rm "/tmp/backup-manifest-$DATE.txt"

echo "$(date): Backup completed successfully"

# Optional: Clean up old backups (keep last 7 days)
CUTOFF_DATE=$(date -d '7 days ago' +%Y-%m-%d)
echo "Cleaning up backups older than $CUTOFF_DATE"
# Note: This would require additional logic to list and delete old prefixes
```

### Sync Script

```bash
#!/bin/bash
# sync-to-vfs.sh - Sync local directory to VFS

LOCAL_DIR="$1"
S3_PREFIX="$2"

if [ -z "$LOCAL_DIR" ] || [ -z "$S3_PREFIX" ]; then
    echo "Usage: $0 <local-dir> <s3-prefix>"
    exit 1
fi

# Create sync manifest
SYNC_LOG="/tmp/vfs-sync-$(date +%s).log"

echo "Syncing $LOCAL_DIR to $S3_PREFIX"
echo "Sync started at $(date)" > "$SYNC_LOG"

# Track operations
SUCCESS_COUNT=0
ERROR_COUNT=0

for file in "$LOCAL_DIR"/*; do
    if [ -f "$file" ]; then
        filename=$(basename "$file")
        s3_path="${S3_PREFIX}${filename}/"
        
        echo "Syncing: $filename"
        if vfs encode "$file" "$s3_path" --force 2>>"$SYNC_LOG"; then
            ((SUCCESS_COUNT++))
            echo "✓ $filename" >> "$SYNC_LOG"
        else
            ((ERROR_COUNT++))
            echo "✗ $filename" >> "$SYNC_LOG"
        fi
    fi
done

echo "Sync completed: $SUCCESS_COUNT successful, $ERROR_COUNT failed"
echo "Sync completed at $(date)" >> "$SYNC_LOG"
echo "Results: $SUCCESS_COUNT successful, $ERROR_COUNT failed" >> "$SYNC_LOG"

if [ $ERROR_COUNT -gt 0 ]; then
    echo "Errors occurred. Check log: $SYNC_LOG"
    exit 1
fi
```

### Recovery Script

```bash
#!/bin/bash
# recover-from-vfs.sh - Recover files from VFS backup

S3_PREFIX="$1"
RECOVERY_DIR="$2"
DATE_FILTER="$3"

if [ -z "$S3_PREFIX" ] || [ -z "$RECOVERY_DIR" ]; then
    echo "Usage: $0 <s3-prefix> <recovery-dir> [date-filter]"
    echo "Example: $0 s3://backups/daily/ /tmp/recovery 2024-01-15"
    exit 1
fi

mkdir -p "$RECOVERY_DIR"

echo "Starting recovery from $S3_PREFIX"
echo "Recovery directory: $RECOVERY_DIR"

# If date filter specified, include it in the prefix
if [ -n "$DATE_FILTER" ]; then
    FULL_PREFIX="${S3_PREFIX}${DATE_FILTER}/"
else
    FULL_PREFIX="$S3_PREFIX"
fi

echo "Using prefix: $FULL_PREFIX"

# First, try to recover manifest if it exists
MANIFEST_PREFIX="${S3_PREFIX}manifests/"
if [ -n "$DATE_FILTER" ]; then
    MANIFEST_PREFIX="${MANIFEST_PREFIX}${DATE_FILTER}/"
fi

echo "Attempting to recover manifest..."
if vfs restore "$MANIFEST_PREFIX" "$RECOVERY_DIR/manifest.txt" 2>/dev/null; then
    echo "✓ Manifest recovered"
    cat "$RECOVERY_DIR/manifest.txt"
else
    echo "⚠ No manifest found, proceeding without it"
fi

# Note: Full recovery would require AWS CLI to list all objects under prefix
# since VFS doesn't have a list-only command
echo "Recovery preparation completed"
echo "To complete recovery, you'll need to:"
echo "1. List all S3 objects under $FULL_PREFIX"
echo "2. For each object prefix, run: vfs restore <prefix> <local-file>"
```

## Integration Patterns

### Web Application Integration

```go
package main

import (
    "fmt"
    "io"
    "net/http"
    "os"
    "path/filepath"
    
    "github.com/vjeffz/vfs/vfs"
)

type FileService struct {
    vfsClient *vfs.VFS
    s3Bucket  string
}

func NewFileService(bucket string) (*FileService, error) {
    client, err := vfs.New()
    if err != nil {
        return nil, err
    }
    
    return &FileService{
        vfsClient: client,
        s3Bucket:  bucket,
    }, nil
}

func (fs *FileService) UploadHandler(w http.ResponseWriter, r *http.Request) {
    if r.Method != http.MethodPost {
        http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
        return
    }
    
    // Parse multipart form
    err := r.ParseMultipartForm(32 << 20) // 32MB max
    if err != nil {
        http.Error(w, "Failed to parse form", http.StatusBadRequest)
        return
    }
    
    file, header, err := r.FormFile("file")
    if err != nil {
        http.Error(w, "Failed to get file", http.StatusBadRequest)
        return
    }
    defer file.Close()
    
    // Save to temporary file
    tmpPath := filepath.Join("/tmp", header.Filename)
    tmpFile, err := os.Create(tmpPath)
    if err != nil {
        http.Error(w, "Failed to create temp file", http.StatusInternalServerError)
        return
    }
    defer func() {
        tmpFile.Close()
        os.Remove(tmpPath)
    }()
    
    _, err = io.Copy(tmpFile, file)
    if err != nil {
        http.Error(w, "Failed to save file", http.StatusInternalServerError)
        return
    }
    tmpFile.Close()
    
    // Store in VFS
    s3Path := fmt.Sprintf("s3://%s/uploads/%s/", fs.s3Bucket, header.Filename)
    err = fs.vfsClient.Encode(tmpPath, s3Path, true)
    if err != nil {
        http.Error(w, "Failed to store file", http.StatusInternalServerError)
        return
    }
    
    w.WriteHeader(http.StatusOK)
    fmt.Fprintf(w, "File uploaded successfully: %s", header.Filename)
}

func (fs *FileService) DownloadHandler(w http.ResponseWriter, r *http.Request) {
    filename := r.URL.Query().Get("file")
    if filename == "" {
        http.Error(w, "File parameter required", http.StatusBadRequest)
        return
    }
    
    // Restore from VFS to temporary file
    tmpPath := filepath.Join("/tmp", filename)
    s3Path := fmt.Sprintf("s3://%s/uploads/%s/", fs.s3Bucket, filename)
    
    err := fs.vfsClient.Restore(s3Path, tmpPath)
    if err != nil {
        http.Error(w, "File not found", http.StatusNotFound)
        return
    }
    defer os.Remove(tmpPath)
    
    // Serve file
    http.ServeFile(w, r, tmpPath)
}

func main() {
    fs, err := NewFileService("file-uploads")
    if err != nil {
        panic(err)
    }
    
    http.HandleFunc("/upload", fs.UploadHandler)
    http.HandleFunc("/download", fs.DownloadHandler)
    
    fmt.Println("Server starting on :8080")
    panic(http.ListenAndServe(":8080", nil))
}
```

### CI/CD Pipeline Integration

```yaml
# .github/workflows/backup-artifacts.yml
name: Backup Build Artifacts

on:
  push:
    branches: [main]
  release:
    types: [published]

jobs:
  backup-artifacts:
    runs-on: ubuntu-latest
    
    steps:
      - uses: actions/checkout@v3
      
      - name: Set up Go
        uses: actions/setup-go@v3
        with:
          go-version: 1.23
          
      - name: Build project
        run: make build
        
      - name: Install VFS
        run: |
          git clone https://github.com/vjeffz/vfs.git /tmp/vfs
          cd /tmp/vfs && make build
          sudo cp bin/vfs /usr/local/bin/
          
      - name: Configure AWS credentials
        uses: aws-actions/configure-aws-credentials@v2
        with:
          aws-access-key-id: ${{ secrets.AWS_ACCESS_KEY_ID }}
          aws-secret-access-key: ${{ secrets.AWS_SECRET_ACCESS_KEY }}
          aws-region: us-east-1
          
      - name: Backup build artifacts
        run: |
          DATE=$(date +%Y-%m-%d)
          COMMIT_SHA=${GITHUB_SHA::8}
          
          # Backup binary
          vfs encode bin/vfs "s3://ci-artifacts/builds/$DATE/$COMMIT_SHA/" --force
          
          # Backup source archive
          tar czf source.tar.gz --exclude='.git' .
          vfs encode source.tar.gz "s3://ci-artifacts/source/$DATE/$COMMIT_SHA/" --force
          
      - name: Backup on release
        if: github.event_name == 'release'
        run: |
          TAG_NAME=${GITHUB_REF#refs/tags/}
          vfs encode bin/vfs "s3://ci-artifacts/releases/$TAG_NAME/" --force
```

These examples demonstrate various ways to integrate VFS into different types of applications and workflows, from simple CLI usage to complex automated systems.