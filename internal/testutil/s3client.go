package testutil

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/cirruslabs/omni-cache/pkg/storage"
	"github.com/docker/go-connections/nat"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

func NewStorage(t *testing.T) storage.BlobStorageBacked {
	bucketName := fmt.Sprintf("omni-cache-test-%s", strings.ReplaceAll(uuid.NewString(), "-", ""))
	stor, err := storage.NewS3Storage(S3Client(t), bucketName)
	require.NoError(t, err)
	return stor
}

func S3Client(t *testing.T) *s3.S3 {
	t.Helper()

	ctx := context.Background()

	localstackContainer, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        "localstack/localstack",
			WaitingFor:   wait.ForHTTP("/_localstack/health").WithPort("4566/tcp"),
			ExposedPorts: []string{"4566/tcp"},
		},
		Started: true,
	})
	require.NoError(t, err)

	exposedPort, err := nat.NewPort("tcp", "4566")
	require.NoError(t, err)

	mappedPort, err := localstackContainer.MappedPort(ctx, exposedPort)
	require.NoError(t, err)

	session, err := session.NewSession(&aws.Config{
		Endpoint:    aws.String(fmt.Sprintf("http://test.s3.localhost.localstack.cloud:%d/", mappedPort.Int())),
		Region:      aws.String("us-east-1"),
		Credentials: credentials.NewStaticCredentials("id", "secret", ""),
	})
	require.NoError(t, err)

	return s3.New(session)
}
