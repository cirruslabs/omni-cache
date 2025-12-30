package storage

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"path"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
	smithyhttp "github.com/aws/smithy-go/transport/http"
	"github.com/google/uuid"
)

const (
	defaultPresignExpiration = 10 * time.Minute
	bucketWaitTimeout        = 1 * time.Minute
)

type s3Storage struct {
	client        *s3.Client
	presignClient *s3.PresignClient
	bucketName    string
	prefix        []string

	bucketMu    sync.Mutex
	bucketReady bool
}

func NewS3Storage(ctx context.Context, client *s3.Client, bucketName string, prefix ...string) (MultipartBlobStorageBackend, error) {
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

	result := &s3Storage{
		client:        client,
		presignClient: s3.NewPresignClient(client),
		bucketName:    bucketName,
		prefix:        normalizedPrefix,
	}

	if err := result.ensureBucketExists(ctx); err != nil {
		return result, err
	}
	return result, nil
}

func (s *s3Storage) ensureBucketExists(ctx context.Context) error {
	s.bucketMu.Lock()
	defer s.bucketMu.Unlock()

	if s.bucketReady {
		return nil
	}

	headInput := &s3.HeadBucketInput{Bucket: aws.String(s.bucketName)}
	if _, err := s.client.HeadBucket(ctx, headInput); err == nil {
		s.bucketReady = true
		return nil
	}

	createInput := &s3.CreateBucketInput{
		Bucket: aws.String(s.bucketName),
	}
	if _, err := s.client.CreateBucket(ctx, createInput); err != nil {
		var alreadyOwned *types.BucketAlreadyOwnedByYou
		var alreadyExists *types.BucketAlreadyExists

		if errors.As(err, &alreadyOwned) || errors.As(err, &alreadyExists) {
			s.bucketReady = true
			return nil
		}
		return err
	}

	waiter := s3.NewBucketExistsWaiter(s.client)
	if waitErr := waiter.Wait(ctx, headInput, bucketWaitTimeout); waitErr != nil {
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

func (s *s3Storage) CacheInfo(ctx context.Context, key string) (*BlobInfo, error) {
	objectKey := s.objectKey(key)
	headInput := &s3.HeadObjectInput{
		Bucket: aws.String(s.bucketName),
		Key:    aws.String(objectKey),
	}

	result, err := s.client.HeadObject(ctx, headInput)
	if err != nil {
		if isObjectNotFound(err) {
			return nil, nil
		}
		return nil, err
	}

	info := &BlobInfo{
		ExtraHeaders: result.Metadata,
	}
	info.ExtraHeaders["Content-Type"] = "application/octet-stream"

	if result.ContentLength != nil {
		info.SizeInBytes = uint64(*result.ContentLength)
	}

	return info, nil
}

func (s *s3Storage) DownloadURLs(ctx context.Context, key string) ([]*URLInfo, error) {
	objectKey := s.objectKey(key)
	headInput := &s3.HeadObjectInput{
		Bucket: aws.String(s.bucketName),
		Key:    aws.String(objectKey),
	}

	if _, err := s.client.HeadObject(ctx, headInput); err != nil {
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

func (s *s3Storage) DeleteCache(ctx context.Context, key string) error {
	objectKey := s.objectKey(key)
	deleteInput := &s3.DeleteObjectInput{
		Bucket: aws.String(s.bucketName),
		Key:    aws.String(objectKey),
	}

	if _, err := s.client.DeleteObject(ctx, deleteInput); err != nil {
		if isObjectNotFound(err) {
			return nil
		}
		return err
	}

	return nil
}

func (s *s3Storage) UploadURL(ctx context.Context, key string, metadata map[string]string) (*URLInfo, error) {
	objectKey := s.objectKey(key)

	var objectMetadata map[string]string
	if len(metadata) > 0 {
		objectMetadata = make(map[string]string, len(metadata))
		for k, v := range metadata {
			if k == "" {
				continue
			}
			objectMetadata[strings.ToLower(k)] = v
		}
	}

	putInput := &s3.PutObjectInput{
		Bucket:      aws.String(s.bucketName),
		Key:         aws.String(objectKey),
		Metadata:    objectMetadata,
		ContentType: aws.String("application/octet-stream"),
	}

	presigned, err := s.presignClient.PresignPutObject(ctx, putInput, s3.WithPresignExpires(defaultPresignExpiration))
	if err != nil {
		return nil, err
	}

	info := buildURLInfo(presigned)

	if info.ExtraHeaders == nil {
		info.ExtraHeaders = make(map[string]string)
	}

	// Ensure callers propagate the headers that were part of the signature.
	info.ExtraHeaders["Content-Type"] = "application/octet-stream"

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
	presigned, err := s.presignClient.PresignGetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(s.bucketName),
		Key:    aws.String(objectKey),
	}, s3.WithPresignExpires(defaultPresignExpiration))
	if err != nil {
		return nil, err
	}

	return buildURLInfo(presigned), nil
}

func (s *s3Storage) presignHead(ctx context.Context, objectKey string) (*URLInfo, error) {
	presigned, err := s.presignClient.PresignHeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(s.bucketName),
		Key:    aws.String(objectKey),
	}, s3.WithPresignExpires(defaultPresignExpiration))
	if err != nil {
		return nil, err
	}

	return buildURLInfo(presigned), nil
}

func buildURLInfo(presigned *v4.PresignedHTTPRequest) *URLInfo {
	extraHeaders := extractRelevantHeaders(presigned.SignedHeader)

	return &URLInfo{
		URL:          presigned.URL,
		ExtraHeaders: extraHeaders,
	}
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

func isObjectNotFound(err error) bool {
	var apiErr smithy.APIError
	if errors.As(err, &apiErr) {
		switch apiErr.ErrorCode() {
		case "NotFound", "NoSuchKey":
			return true
		default:
			return false
		}
	}

	var respErr *smithyhttp.ResponseError
	if errors.As(err, &respErr) {
		return respErr.HTTPStatusCode() == http.StatusNotFound
	}

	return false
}

func (s *s3Storage) CreateMultipartUpload(ctx context.Context, key string, metadata map[string]string) (string, error) {
	objectKey := s.objectKey(key)

	createInput := &s3.CreateMultipartUploadInput{
		Bucket:      aws.String(s.bucketName),
		Key:         aws.String(objectKey),
		Metadata:    metadata,
		ContentType: aws.String("application/octet-stream"),
	}

	result, err := s.client.CreateMultipartUpload(ctx, createInput)
	if err != nil {
		return "", err
	}

	return *result.UploadId, nil
}

func (s *s3Storage) UploadPartURL(ctx context.Context, key string, uploadID string, partNumber uint32, contentLength uint64) (*URLInfo, error) {
	objectKey := s.objectKey(key)

	uploadPartInput := &s3.UploadPartInput{
		Bucket:        aws.String(s.bucketName),
		Key:           aws.String(objectKey),
		UploadId:      aws.String(uploadID),
		PartNumber:    aws.Int32(int32(partNumber)),
		ContentLength: aws.Int64(int64(contentLength)),
	}

	presigned, err := s.presignClient.PresignUploadPart(ctx, uploadPartInput, s3.WithPresignExpires(defaultPresignExpiration))
	if err != nil {
		return nil, err
	}

	return buildURLInfo(presigned), nil
}

func (s *s3Storage) CommitMultipartUpload(ctx context.Context, key string, uploadID string, parts []MultipartUploadPart) error {
	objectKey := s.objectKey(key)

	completedParts := make([]types.CompletedPart, len(parts))
	for i, part := range parts {
		completedParts[i] = types.CompletedPart{
			PartNumber: aws.Int32(int32(part.PartNumber)),
			ETag:       aws.String(part.ETag),
		}
	}

	completeInput := &s3.CompleteMultipartUploadInput{
		Bucket:   aws.String(s.bucketName),
		Key:      aws.String(objectKey),
		UploadId: aws.String(uploadID),
		MultipartUpload: &types.CompletedMultipartUpload{
			Parts: completedParts,
		},
	}

	_, err := s.client.CompleteMultipartUpload(ctx, completeInput)
	return err
}
