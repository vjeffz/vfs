package vfs

import (
	"context"
	"encoding/base64"
	"fmt"
	"io"
	"os"
	"net/url"
	"path"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
)

const (
	s3MaxKeyLengthBytes = 1024
	maxIndexLen         = 6
	defaultConcurrency  = 8
)

type VFS struct {
	client      *s3.Client
	concurrency int
}

func New() (*VFS, error) {
	cfg, err := config.LoadDefaultConfig(context.TODO())
	if err != nil {
		return nil, err
	}
	return &VFS{
		client:      s3.NewFromConfig(cfg),
		concurrency: getConcurrency(),
	}, nil
}

func (v *VFS) Encode(inputPath, s3URI string) error {
	bucket, prefix, err := parseS3Path(s3URI)
	if err != nil {
		return err
	}

	chunkSize := calculateChunkSize(prefix)
	if chunkSize < 1 {
		return fmt.Errorf("calculated chunk size is too small for S3 key constraint")
	}

	file, err := os.Open(inputPath)
	if err != nil {
		return err
	}
	defer file.Close()

	stat, _ := file.Stat()
	totalChunks := int(stat.Size()) / chunkSize
	if stat.Size()%int64(chunkSize) != 0 {
		totalChunks++
	}

	var chunks [][]byte
	buf := make([]byte, chunkSize)
	for {
		n, err := file.Read(buf)
		if n > 0 {
			copyBuf := make([]byte, n)
			copy(copyBuf, buf[:n])
			chunks = append(chunks, copyBuf)
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
	}

	fmt.Printf("Uploading %d chunks...\n", len(chunks))
	var wg sync.WaitGroup
	sem := make(chan struct{}, v.concurrency)
	var errMu sync.Mutex
	var firstErr error

	for i, chunk := range chunks {
		sem <- struct{}{}
		wg.Add(1)
		go func(index int, data []byte) {
			defer wg.Done()
			defer func() { <-sem }()
			encoded := base64.RawURLEncoding.EncodeToString(data)
			key := path.Join(prefix, fmt.Sprintf("%d-%s", index+1, encoded))
			_, err := v.client.PutObject(context.TODO(), &s3.PutObjectInput{
				Bucket: &bucket,
				Key:    &key,
				Body:   nil,
			})
			if err != nil {
				errMu.Lock()
				if firstErr == nil {
					firstErr = err
				}
				errMu.Unlock()
				return
			}
			fmt.Printf("\rUploaded: %d/%d", index+1, len(chunks))
		}(i, chunk)
	}

	wg.Wait()
	fmt.Println("\n✅ Upload complete.")
	return firstErr
}

func (v *VFS) Restore(s3URI, outputPath string) error {
	bucket, prefix, err := parseS3Path(s3URI)
	if err != nil {
		return err
	}

	var chunks []struct {
		index   int
		encoded string
	}

	p := s3.NewListObjectsV2Paginator(v.client, &s3.ListObjectsV2Input{
		Bucket: &bucket,
		Prefix: &prefix,
	})

	for p.HasMorePages() {
		page, err := p.NextPage(context.TODO())
		if err != nil {
			return err
		}
		for _, obj := range page.Contents {
			name := strings.TrimPrefix(*obj.Key, prefix)
			name = strings.TrimPrefix(name, "/")
			parts := strings.SplitN(name, "-", 2)
			if len(parts) != 2 {
				continue
			}
			index, err := strconv.Atoi(parts[0])
			if err != nil {
				continue
			}
			chunks = append(chunks, struct {
				index   int
				encoded string
			}{index, parts[1]})
		}
	}

	sort.Slice(chunks, func(i, j int) bool {
		return chunks[i].index < chunks[j].index
	})

	if err := os.MkdirAll(path.Dir(outputPath), 0755); err != nil {
		return err
	}

	out, err := os.Create(outputPath)
	if err != nil {
		return err
	}
	defer out.Close()

	var wg sync.WaitGroup
	sem := make(chan struct{}, v.concurrency)
	results := make([][]byte, len(chunks))
	var errMu sync.Mutex
	var firstErr error

	fmt.Printf("Downloading %d chunks...\n", len(chunks))

	for i, chunk := range chunks {
		sem <- struct{}{}
		wg.Add(1)
		go func(i int, encoded string) {
			defer wg.Done()
			defer func() { <-sem }()
			data, err := base64.RawURLEncoding.DecodeString(encoded)
			if err != nil {
				errMu.Lock()
				if firstErr == nil {
					firstErr = err
				}
				errMu.Unlock()
				return
			}
			results[i] = data
			fmt.Printf("\rDownloaded: %d/%d", i+1, len(chunks))
		}(i, chunk.encoded)
	}

	wg.Wait()
	fmt.Println("\n✅ Download complete.")
	if firstErr != nil {
		return firstErr
	}

	for _, data := range results {
		if _, err := out.Write(data); err != nil {
			return err
		}
	}
	fmt.Printf("Restored file written to: %s\n", outputPath)
	return nil
}

func (v *VFS) Delete(s3URI string) error {
	bucket, prefix, err := parseS3Path(s3URI)
	if err != nil {
		return err
	}

	p := s3.NewListObjectsV2Paginator(v.client, &s3.ListObjectsV2Input{
		Bucket: &bucket,
		Prefix: &prefix,
	})

	deleted := 0
	for p.HasMorePages() {
		page, err := p.NextPage(context.TODO())
		if err != nil {
			return err
		}
		var toDelete []s3types.ObjectIdentifier
		for _, obj := range page.Contents {
			toDelete = append(toDelete, s3types.ObjectIdentifier{Key: obj.Key})
		}
		if len(toDelete) == 0 {
			break
		}
		_, err = v.client.DeleteObjects(context.TODO(), &s3.DeleteObjectsInput{
			Bucket: &bucket,
			Delete: &s3types.Delete{Objects: toDelete},
		})
		if err != nil {
			return err
		}
		deleted += len(toDelete)
		fmt.Printf("\rDeleted: %d", deleted)
	}
	fmt.Println("\n✅ Delete complete.")
	return nil
}

func parseS3Path(s3Path string) (string, string, error) {
	if !strings.HasPrefix(s3Path, "s3://") {
		return "", "", fmt.Errorf("must start with s3://")
	}
	parsed, err := url.Parse(s3Path)
	if err != nil {
		return "", "", err
	}
	bucket := parsed.Host
	prefix := strings.TrimLeft(parsed.Path, "/")
	if prefix != "" && !strings.HasSuffix(prefix, "/") {
		prefix += "/"
	}
	return bucket, prefix, nil
}

func calculateChunkSize(prefix string) int {
	available := s3MaxKeyLengthBytes - len(prefix) - maxIndexLen
	if available <= 0 {
		return 0
	}
	return (available * 3) / 4
}

func getConcurrency() int {
	val := os.Getenv("S3_CONCURRENCY")
	n, err := strconv.Atoi(val)
	if err != nil || n <= 0 {
		return defaultConcurrency
	}
	return n
}

