package storage

import (
	"context"
	"fmt"
	"net/http"
	"path"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/google/uuid"
)

const defaultPresignExpiration = 10 * time.Minute

type s3Storage struct {
	client     *s3.S3
	bucketName string
	prefix     []string

	bucketMu    sync.Mutex
	bucketReady bool
}

func NewS3Storage(client *s3.S3, bucketName string, prefix ...string) (BlobStorageBacked, error) {
	if client == nil {
		return nil, fmt.Errorf("storage: s3 client must not be nil")
	}

	bucketName = strings.TrimSpace(bucketName)
	if bucketName == "" {
		bucketName = fmt.Sprintf("omni-cache-%s", uuid.NewString())
	}
	bucketName = strings.ToLower(bucketName)

	normalizedPrefix := make([]string, 0, len(prefix))
	for _, segment := range prefix {
		segment = strings.Trim(segment, "/")
		if segment != "" {
			normalizedPrefix = append(normalizedPrefix, segment)
		}
	}

	return &s3Storage{
		client:     client,
		bucketName: bucketName,
		prefix:     normalizedPrefix,
	}, nil
}

func (s *s3Storage) ensureBucketExists(ctx context.Context) error {
	s.bucketMu.Lock()
	defer s.bucketMu.Unlock()

	if s.bucketReady {
		return nil
	}

	headInput := &s3.HeadBucketInput{Bucket: aws.String(s.bucketName)}
	if _, err := s.client.HeadBucketWithContext(ctx, headInput); err == nil {
		s.bucketReady = true
		return nil
	}

	createInput := &s3.CreateBucketInput{
		Bucket: aws.String(s.bucketName),
	}
	_, err := s.client.CreateBucketWithContext(ctx, createInput)
	if err != nil {
		if awsErr, ok := err.(awserr.Error); ok {
			switch awsErr.Code() {
			case s3.ErrCodeBucketAlreadyOwnedByYou, s3.ErrCodeBucketAlreadyExists:
				s.bucketReady = true
				return nil
			}
		}
		return err
	}

	if waitErr := s.client.WaitUntilBucketExistsWithContext(ctx, headInput); waitErr != nil {
		return waitErr
	}

	s.bucketReady = true
	return nil
}

func (s *s3Storage) objectKey(key string) string {
	key = strings.TrimPrefix(key, "/")
	if len(s.prefix) == 0 {
		return key
	}

	parts := make([]string, 0, len(s.prefix)+1)
	parts = append(parts, s.prefix...)
	parts = append(parts, key)
	return path.Join(parts...)
}

func (s *s3Storage) DownloadURLs(ctx context.Context, key string) ([]*URLInfo, error) {
	if err := s.ensureBucketExists(ctx); err != nil {
		return nil, err
	}

	objectKey := s.objectKey(key)
	headInput := &s3.HeadObjectInput{
		Bucket: aws.String(s.bucketName),
		Key:    aws.String(objectKey),
	}

	if _, err := s.client.HeadObjectWithContext(ctx, headInput); err != nil {
		return nil, err
	}

	urls := make([]*URLInfo, 0, 2)

	getInfo, err := s.presignGet(ctx, objectKey)
	if err != nil {
		return nil, err
	}
	urls = append(urls, getInfo)

	if headInfo, err := s.presignHead(ctx, objectKey); err == nil {
		urls = append(urls, headInfo)
	}

	return urls, nil
}

func (s *s3Storage) UploadURL(ctx context.Context, key string, metadata map[string]string) (*URLInfo, error) {
	if err := s.ensureBucketExists(ctx); err != nil {
		return nil, err
	}

	objectKey := s.objectKey(key)

	var objectMetadata map[string]*string
	if len(metadata) > 0 {
		objectMetadata = make(map[string]*string, len(metadata))
		for k, v := range metadata {
			if k == "" {
				continue
			}
			objectMetadata[strings.ToLower(k)] = aws.String(v)
		}
	}

	putInput := &s3.PutObjectInput{
		Bucket:   aws.String(s.bucketName),
		Key:      aws.String(objectKey),
		Metadata: objectMetadata,
	}

	req, _ := s.client.PutObjectRequest(putInput)
	req.SetContext(ctx)
	req.HTTPRequest.Header.Set("Content-Type", "application/octet-stream")
	req.HTTPRequest.Header.Set("X-Amz-Content-Sha256", "UNSIGNED-PAYLOAD")

	info, err := s.presignRequest(ctx, req)
	if err != nil {
		return nil, err
	}

	if info.ExtraHeaders == nil {
		info.ExtraHeaders = make(map[string]string)
	}

	// Ensure callers propagate the headers that were part of the signature.
	info.ExtraHeaders["Content-Type"] = "application/octet-stream"
	info.ExtraHeaders["X-Amz-Content-Sha256"] = "UNSIGNED-PAYLOAD"

	for k, v := range metadata {
		if k == "" {
			continue
		}
		headerKey := fmt.Sprintf("x-amz-meta-%s", strings.ToLower(k))
		info.ExtraHeaders[headerKey] = v
	}

	return info, nil
}

func (s *s3Storage) presignGet(ctx context.Context, objectKey string) (*URLInfo, error) {
	req, _ := s.client.GetObjectRequest(&s3.GetObjectInput{
		Bucket: aws.String(s.bucketName),
		Key:    aws.String(objectKey),
	})
	req.SetContext(ctx)
	return s.presignRequest(ctx, req)
}

func (s *s3Storage) presignHead(ctx context.Context, objectKey string) (*URLInfo, error) {
	req, _ := s.client.HeadObjectRequest(&s3.HeadObjectInput{
		Bucket: aws.String(s.bucketName),
		Key:    aws.String(objectKey),
	})
	req.SetContext(ctx)
	return s.presignRequest(ctx, req)
}

func (s *s3Storage) presignRequest(ctx context.Context, req *request.Request) (*URLInfo, error) {
	req.SetContext(ctx)

	urlStr, err := req.Presign(defaultPresignExpiration)
	if err != nil {
		return nil, err
	}

	extraHeaders := extractRelevantHeaders(req.HTTPRequest.Header)

	return &URLInfo{
		URL:          urlStr,
		ExtraHeaders: extraHeaders,
	}, nil
}

func extractRelevantHeaders(headers http.Header) map[string]string {
	extra := make(map[string]string)

	for key, values := range headers {
		if len(values) == 0 {
			continue
		}

		lowerKey := strings.ToLower(key)
		if lowerKey == "content-type" || strings.HasPrefix(lowerKey, "x-amz-") {
			extra[key] = values[len(values)-1]
		}
	}

	if len(extra) == 0 {
		return nil
	}

	return extra
}
