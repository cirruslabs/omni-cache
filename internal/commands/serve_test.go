package commands

import (
	"context"
	"net/url"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stretchr/testify/require"
)

// TestNewS3ClientCustomEndpointPresignOmitsChecksumMode ensures presigned GETs
// for custom endpoints carry no x-amz-checksum-mode param, which S3-compatible
// stores like SeaweedFS reject with 403. Presigning is offline; no backend.
func TestNewS3ClientCustomEndpointPresignOmitsChecksumMode(t *testing.T) {
	// Mirror config.LoadDefaultConfig, which sets ResponseChecksumValidation to
	// WhenSupported; that default is what adds x-amz-checksum-mode and is what
	// newS3Client must override for custom endpoints.
	cfg := aws.Config{
		Region:                     "us-east-1",
		Credentials:                credentials.NewStaticCredentialsProvider("id", "secret", ""),
		ResponseChecksumValidation: aws.ResponseChecksumValidationWhenSupported,
	}

	client, err := newS3Client(cfg, "http://127.0.0.1:8333")
	require.NoError(t, err)

	presigned, err := s3.NewPresignClient(client).PresignGetObject(context.Background(), &s3.GetObjectInput{
		Bucket: aws.String("bucket"),
		Key:    aws.String("key"),
	})
	require.NoError(t, err)

	parsed, err := url.Parse(presigned.URL)
	require.NoError(t, err)
	require.NotContains(t, parsed.Query(), "X-Amz-Checksum-Mode",
		"presigned GET must not carry x-amz-checksum-mode; S3-compatible stores reject the extra signed parameter with 403")
}
