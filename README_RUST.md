# VFS (Virtual File Splitter) - Rust Edition

VFS is a Rust library and CLI tool that allows you to:

- Encode a file into base64-encoded chunks stored as S3 object keys
- Restore a file by decoding these chunks
- Delete all objects under a given S3 prefix

Perfect for lightweight S3-based storage, validation-less chunking, or environments with limited API support.

---

## ✨ Features

- ✅ Chunked file encoding to S3 (key names only)
- ✅ Safe restoration from S3 keys
- ✅ Parallel uploads/downloads (configurable via `S3_CONCURRENCY`)
- ✅ Prefix-safe key name sizing
- ✅ Clean command-line interface and Rust API
- ✅ Async/await for better performance
- ✅ Memory-efficient streaming

---

## 📦 Install

### Prerequisites

- Rust 1.70+ (install from [rustup.rs](https://rustup.rs/))
- AWS credentials configured (via AWS CLI, environment variables, or IAM roles)

### Build from source

```bash
cargo build --release
```

The binary will be available at `target/release/vfs`

### Install globally

```bash
cargo install --path .
```

Or from crates.io (when published):

```bash
cargo install vfs
```

---

## 🛠 Usage

### CLI Commands

```bash
# Encode a file
vfs encode <inputfile> s3://bucket/prefix/

# Encode with force flag (no confirmation prompt)
vfs encode <inputfile> s3://bucket/prefix/ --force

# Restore a file
vfs restore s3://bucket/prefix/ <outputfile>

# Delete all objects under a prefix
vfs delete s3://bucket/prefix/
```

### Environment Variables

Set concurrency level:

```bash
export S3_CONCURRENCY=10
```

### Library Usage

Add to your `Cargo.toml`:

```toml
[dependencies]
vfs = { path = "." }
tokio = { version = "1.0", features = ["full"] }
```

Use in your code:

```rust
use vfs::VFS;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let vfs = VFS::new().await?;
    
    // Encode a file
    vfs.encode("file.txt", "s3://my-bucket/path/", false).await?;
    
    // Restore a file
    vfs.restore("s3://my-bucket/path/", "restored_file.txt").await?;
    
    // Delete prefix
    vfs.delete("s3://my-bucket/path/").await?;
    
    Ok(())
}
```

---

## 🧪 Run Tests

```bash
cargo test
```

---

## 🚀 Performance Improvements in Rust Version

- **Async I/O**: All S3 operations are truly asynchronous
- **Memory efficiency**: Streaming file I/O instead of loading entire file
- **Better concurrency**: Uses semaphores for controlled parallelism
- **Zero-copy operations**: Reduced memory allocations where possible
- **Type safety**: Compile-time guarantees prevent many runtime errors

---

## 📋 AWS Credentials

The tool uses the AWS SDK's default credential chain:

1. Environment variables (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`)
2. AWS credentials file (`~/.aws/credentials`)
3. IAM instance profile (when running on EC2)
4. AWS SSO

---

## 🔄 Migration from Go Version

The Rust version maintains API compatibility with the Go version:

- Same command-line interface
- Same S3 object key format
- Same environment variables
- Files encoded with Go version can be restored with Rust version and vice versa

### Key Differences

- Better error messages with context
- More responsive progress indicators
- Lower memory usage for large files
- Native async/await instead of goroutines

---

## 📝 License

MIT License - see [LICENSE](LICENSE) file for details.