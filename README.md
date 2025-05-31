# VFS (Virtual File Splitter)

VFS is a Go library and CLI tool that allows you to:

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
- ✅ Clean command-line interface and Go API

---

## 📦 Install

```bash
make build
./bin/vfs
```

Or use as a library

```
import "github.com/vjeffz/vfs/vfs"

vfs := vfs.New()
vfs.Encode("file.txt", "s3://my-bucket/path/")
vfs.Restore("s3://my-bucket/path/", "file.txt")
vfs.Delete("s3://my-bucket/path/")
```

## 🛠 Usage

```
vfs encode <inputfile> s3://bucket/prefix/
vfs restore s3://bucket/prefix/ <outputfile>
vfs delete s3://bucket/prefix/
```

Set concurrency with:

```
export S3_CONCURRENCY=10
```

🧪 Run Tests

```
make test
```

