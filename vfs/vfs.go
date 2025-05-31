// File: vfs/vfs.go
package vfs

import (
	"context"
	"encoding/base64"
	"fmt"
	"io"
	"net/url"
	"os"
	"path"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
)

const (
	defaultConcurrency   = 8
	s3MaxKeyLengthBytes  = 1024
	maxIndexLen          = 6
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
		return fmt.Errorf("calculated chunk size is too small")
	}
	inputFile, err := os.Open(inputPath)
	if err != nil {
		return err
	}
	defer inputFile.Close()

	stat, _ := inputFile.Stat()
	totalSize := stat.Size()
	totalChunks := int(totalSize / int64(chunkSize))
	if totalSize%int64(chunkSize) != 0 {
		totalChunks++
	}
	chunks := make([][]byte, 0, totalChunks)
	buf := make([]byte, chunkSize)
	for {
		n, err := inputFile.Read(buf)
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

	sem := make(chan struct{}, v.concurrency)
	var wg sync.WaitGroup
	var errMu sync.Mutex
	var firstErr error

	for i, chunk := range chunks {
		sem <- struct{}{}
		wg.Add(1)
		go func(i int, data []byte) {
			defer wg.Done()
			defer func() { <-sem }()
			encoded := base64.StdEncoding.EncodeToString(data)
			key := path.Join(prefix, fmt.Sprintf("%d-%s", i+1, encoded))
			_, err := v.client.PutObject(context.TODO(), &s3.PutObjectInput{
				Bucket: aws.String(bucket),
				Key:    aws.String(key),
				Body:   nil,
			})
			if err != nil {
				errMu.Lock()
				if firstErr == nil {
					firstErr = err
				}
				errMu.Unlock()
			}
			fmt.Printf("\rUploaded: %d/%d", i+1, len(chunks))
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
	paginator := s3.NewListObjectsV2Paginator(v.client, &s3.ListObjectsV2Input{
		Bucket: aws.String(bucket),
		Prefix: aws.String(prefix),
	})
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(context.TODO())
		if err != nil {
			return err
		}
		for _, obj := range page.Contents {
			key := strings.TrimPrefix(*obj.Key, prefix)
			key = strings.TrimPrefix(key, "/")
			parts := strings.SplitN(key, "-", 2)
			if len(parts) != 2 {
				continue
			}
			idx, err := strconv.Atoi(parts[0])
			if err != nil {
				continue
			}
			chunks = append(chunks, struct {
				index   int
				encoded string
			}{idx, parts[1]})
		}
	}
	sort.Slice(chunks, func(i, j int) bool {
		return chunks[i].index < chunks[j].index
	})
	os.MkdirAll(path.Dir(outputPath), 0755)
	out, err := os.Create(outputPath)
	if err != nil {
		return err
	}
	defer out.Close()

	results := make([][]byte, len(chunks))
	sem := make(chan struct{}, v.concurrency)
	var wg sync.WaitGroup
	var errMu sync.Mutex
	var firstErr error
	for i, c := range chunks {
		sem <- struct{}{}
		wg.Add(1)
		go func(i int, enc string) {
			defer wg.Done()
			defer func() { <-sem }()
			decoded, err := base64.StdEncoding.DecodeString(enc)
			if err != nil {
				errMu.Lock()
				if firstErr == nil {
					firstErr = err
				}
				errMu.Unlock()
				return
			}
			results[i] = decoded
			fmt.Printf("\rDownloaded: %d/%d", i+1, len(chunks))
		}(i, c.encoded)
	}
	wg.Wait()
	fmt.Println("\n✅ Download complete.")
	if firstErr != nil {
		return firstErr
	}
	for _, data := range results {
		out.Write(data)
	}
	return nil
}

func (v *VFS) Delete(s3URI string) error {
	bucket, prefix, err := parseS3Path(s3URI)
	if err != nil {
		return err
	}
	paginator := s3.NewListObjectsV2Paginator(v.client, &s3.ListObjectsV2Input{
		Bucket: aws.String(bucket),
		Prefix: aws.String(prefix),
	})
	deleted := 0
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(context.TODO())
		if err != nil {
			return err
		}
		var objs []s3types.ObjectIdentifier
		for _, obj := range page.Contents {
			objs = append(objs, s3types.ObjectIdentifier{Key: obj.Key})
		}
		if len(objs) == 0 {
			break
		}
		_, err = v.client.DeleteObjects(context.TODO(), &s3.DeleteObjectsInput{
			Bucket: aws.String(bucket),
			Delete: &s3types.Delete{Objects: objs},
		})
		if err != nil {
			return err
		}
		deleted += len(objs)
		fmt.Printf("\rDeleted: %d", deleted)
	}
	fmt.Println("\n✅ Delete complete.")
	return nil
}

func parseS3Path(s3Path string) (string, string, error) {
	if !strings.HasPrefix(s3Path, "s3://") {
		return "", "", fmt.Errorf("must start with s3://")
	}
	u, err := url.Parse(s3Path)
	if err != nil {
		return "", "", err
	}
	bucket := u.Host
	prefix := strings.TrimLeft(u.Path, "/")
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
	if val == "" {
		return defaultConcurrency
	}
	n, err := strconv.Atoi(val)
	if err != nil || n <= 0 {
		return defaultConcurrency
	}
	return n
}

