package storage

import "github.com/aws/aws-sdk-go/service/s3"

type s3Storage struct {
	BlobStorageBacked
	client *s3.S3
}

func NewS3Storage(client *s3.S3) BlobStorageBacked {
	return &s3Storage{client: client}
}
