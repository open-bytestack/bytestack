// Package bytestack S3 backend — writes stack files to S3-compatible object storage.
//
// Credentials are resolved via the standard AWS SDK credential chain
// (environment variables, ~/.aws/config, IAM roles, etc.).
//
// Environment variables:
//   - AWS_REGION / AWS_DEFAULT_REGION   S3 region (default: us-east-1)
//   - BYTESTACK_S3_ENDPOINT             Custom endpoint for MinIO / compatible stores
//   - BYTESTACK_S3_TIMEOUT              HTTP timeout (default: 30m, format: Go duration)
//
// Each call to Writer.Put() writes data into an in-memory buffer.
// On Writer.Close() all three stack files are uploaded to S3 via PutObject.
// Large timeouts are configured by default to accommodate slow connections.
package bytestack

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"os"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

const (
	defaultS3Region  = "us-east-1"
	defaultS3Timeout = 30 * time.Minute
)

// ---------------------------------------------------------------------------
// s3 writer factory
// ---------------------------------------------------------------------------

type s3WriterFactory struct {
	client *s3.Client
	bucket string
	prefix string
}

// newS3WriterFactory creates an S3-backed writer factory.
//
// AWS credentials are loaded from the default credential chain.
// Set BYTESTACK_S3_ENDPOINT to use a MinIO-compatible server.
func newS3WriterFactory(bucket, prefix string) (*s3WriterFactory, error) {
	ctx := context.Background()

	region := os.Getenv("AWS_REGION")
	if region == "" {
		region = os.Getenv("AWS_DEFAULT_REGION")
	}
	if region == "" {
		region = defaultS3Region
	}

	cfg, err := config.LoadDefaultConfig(ctx,
		config.WithRegion(region),
	)
	if err != nil {
		return nil, fmt.Errorf("load aws config: %w", err)
	}

	// Honour custom endpoint (e.g. MinIO).
	endpoint := os.Getenv("BYTESTACK_S3_ENDPOINT")

	// Parse custom timeout.
	timeout := defaultS3Timeout
	if ts := os.Getenv("BYTESTACK_S3_TIMEOUT"); ts != "" {
		if d, err := time.ParseDuration(ts); err == nil && d > 0 {
			timeout = d
		}
	}

	client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		if endpoint != "" {
			o.BaseEndpoint = aws.String(endpoint)
			o.UsePathStyle = true // MinIO requires path-style addressing
		}
		o.HTTPClient = &http.Client{
			Timeout: timeout,
		}
	})

	return &s3WriterFactory{
		client: client,
		bucket: bucket,
		prefix: prefix,
	}, nil
}

func (f *s3WriterFactory) CreateWriter(filename string) (stackFileWriter, error) {
	key := filename
	if f.prefix != "" {
		key = f.prefix + "/" + filename
	}
	return &s3FileWriter{
		client: f.client,
		bucket: f.bucket,
		key:    key,
		buf:    new(bytes.Buffer),
	}, nil
}

// ---------------------------------------------------------------------------
// s3FileWriter
// ---------------------------------------------------------------------------

type s3FileWriter struct {
	client *s3.Client
	bucket string
	key    string
	buf    *bytes.Buffer
	closed bool
}

func (w *s3FileWriter) Write(p []byte) (int, error) {
	if w.closed {
		return 0, fmt.Errorf("s3 writer %s/%s: already closed", w.bucket, w.key)
	}
	return w.buf.Write(p)
}

func (w *s3FileWriter) Sync() error {
	return nil
}

func (w *s3FileWriter) Close() error {
	if w.closed {
		return nil
	}
	w.closed = true

	ctx, cancel := context.WithTimeout(context.Background(), defaultS3Timeout)
	defer cancel()

	_, err := w.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(w.bucket),
		Key:    aws.String(w.key),
		Body:   bytes.NewReader(w.buf.Bytes()),
	})
	if err != nil {
		return fmt.Errorf("s3 PutObject %s/%s: %w", w.bucket, w.key, err)
	}
	return nil
}
