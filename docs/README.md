# VFS Documentation

Welcome to the comprehensive documentation for VFS (Virtual File Splitter) - a Go library and CLI tool for storing files as base64-encoded chunks in Amazon S3 object keys.

## 📚 Documentation Overview

This documentation provides complete coverage of VFS functionality, from basic usage to advanced integration patterns. Choose the appropriate guide based on your needs:

### 🚀 Getting Started

- **[README.md](../README.md)** - Project overview, quick start, and basic examples
- **[USER_GUIDE.md](USER_GUIDE.md)** - Step-by-step instructions for CLI and library usage

### 📖 Reference Documentation

- **[API.md](API.md)** - Complete Go package API documentation
- **[EXAMPLES.md](EXAMPLES.md)** - Practical examples and use cases

### 🔧 Development

- **[DEVELOPER_GUIDE.md](DEVELOPER_GUIDE.md)** - Architecture, contributing, and technical details

## 🎯 Quick Navigation

### For End Users

| I want to... | Documentation |
|--------------|---------------|
| Understand what VFS is | [README.md](../README.md) |
| Install and set up VFS | [USER_GUIDE.md](USER_GUIDE.md#installation) |
| Store my first file | [USER_GUIDE.md](USER_GUIDE.md#getting-started) |
| Use VFS in automation scripts | [EXAMPLES.md](EXAMPLES.md#automation-scripts) |
| Integrate VFS into my application | [EXAMPLES.md](EXAMPLES.md#library-integration-examples) |
| Troubleshoot issues | [USER_GUIDE.md](USER_GUIDE.md#troubleshooting) |

### For Developers

| I want to... | Documentation |
|--------------|---------------|
| Understand the architecture | [DEVELOPER_GUIDE.md](DEVELOPER_GUIDE.md#architecture-overview) |
| Contribute to the project | [DEVELOPER_GUIDE.md](DEVELOPER_GUIDE.md#contributing) |
| Review the API | [API.md](API.md) |
| See advanced examples | [EXAMPLES.md](EXAMPLES.md#advanced-use-cases) |
| Understand design decisions | [DEVELOPER_GUIDE.md](DEVELOPER_GUIDE.md#design-decisions) |

## 📋 Documentation Structure

```
docs/
├── README.md           # This overview document
├── API.md             # Complete Go package API reference
├── USER_GUIDE.md      # Step-by-step user instructions
├── EXAMPLES.md        # Practical examples and use cases
└── DEVELOPER_GUIDE.md # Architecture and contribution guide
```

## 🔗 External Links

- **[Repository](https://github.com/vjeffz/vfs)** - Main VFS repository
- **[Issues](https://github.com/vjeffz/vfs/issues)** - Bug reports and feature requests
- **[AWS SDK for Go v2](https://aws.github.io/aws-sdk-go-v2/)** - Underlying AWS SDK documentation
- **[S3 Documentation](https://docs.aws.amazon.com/s3/)** - Amazon S3 service documentation

## 🏗️ Architecture Quick Overview

VFS uses an innovative approach to file storage:

```
Local File → Split into chunks → Base64 encode → Store as S3 object keys → Empty S3 objects
```

**Key Benefits:**
- 💰 **Cost-effective**: No S3 storage charges (empty objects)
- ⚡ **Fast**: Metadata-driven operations via S3 list APIs
- 🔧 **Simple**: No complex integrity checking required
- 📈 **Scalable**: Unlimited file size through chunking

## 🛠️ Common Use Cases

### Backup and Archival
- Daily backups with automated scripts
- Configuration file versioning
- Document archival systems

### Development Workflows
- CI/CD artifact storage
- Build output archival
- Test data management

### Application Integration
- File upload/download services
- Temporary file sharing
- Cache storage systems

## 📊 Performance Guidelines

| Scenario | Recommended Settings | Notes |
|----------|---------------------|-------|
| Small files (<1MB) | Default concurrency (8) | Standard settings work well |
| Large files (>10MB) | `S3_CONCURRENCY=16` | Increase parallelism |
| Limited bandwidth | `S3_CONCURRENCY=4` | Reduce concurrent operations |
| Batch operations | `S3_CONCURRENCY=12` | Balance throughput and resources |

## ⚠️ Important Considerations

### Storage Limits
- **S3 Key Length**: Maximum 1024 bytes per key
- **Chunk Size**: Automatically calculated based on prefix length
- **File Size**: No theoretical limit (uses chunking)

### Best Practices
1. **Use short prefixes** to maximize chunk size
2. **Set appropriate concurrency** for your use case  
3. **Organize files logically** using prefix structure
4. **Monitor S3 API usage** for cost optimization

## 🤝 Getting Help

- **Questions**: Check the [USER_GUIDE.md](USER_GUIDE.md#troubleshooting) troubleshooting section
- **Issues**: Report bugs via [GitHub Issues](https://github.com/vjeffz/vfs/issues)
- **Examples**: Browse [EXAMPLES.md](EXAMPLES.md) for practical use cases
- **API Reference**: Consult [API.md](API.md) for detailed function documentation

## 📄 License

VFS is released into the public domain under the [Unlicense](../LICENSE). You can do anything you want with this software.

---

**Ready to get started?** Begin with the [USER_GUIDE.md](USER_GUIDE.md) for step-by-step instructions!