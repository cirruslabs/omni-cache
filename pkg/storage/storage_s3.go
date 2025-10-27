package storage

import "github.com/aws/aws-sdk-go/service/s3"

type s3Storage struct {
	BlobStorageBacked
	client     *s3.S3
	bucketName string
	prefix     []string
}

func NewS3Storage(client *s3.S3, bucketName string, prefix ...string) BlobStorageBacked {
	return &s3Storage{client: client, bucketName: bucketName, prefix: prefix}
}
