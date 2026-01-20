package commands

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/cirruslabs/omni-cache/pkg/storage"
	"github.com/spf13/cobra"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

const (
	defaultDevBucketName       = "omni-cache-dev"
	defaultLocalstackImage     = "localstack/localstack"
	localstackAccessKey        = "id"
	localstackSecretKey        = "secret"
	localstackRegion           = "us-east-1"
	localstackHealthPath       = "/_localstack/health"
	localstackPort             = "4566/tcp"
	localstackTerminateTimeout = 10 * time.Second
)

type devOptions struct {
	bucketName      string
	prefix          string
	localstackImage string
}

func newDevCmd() *cobra.Command {
	opts := &devOptions{
		bucketName:      envOrFirst(bucketEnv, bucketEnvAlt),
		prefix:          envOrFirst(prefixEnv, prefixEnvAlt),
		localstackImage: defaultLocalstackImage,
	}
	if opts.bucketName == "" {
		opts.bucketName = defaultDevBucketName
	}

	cmd := &cobra.Command{
		Use:   "dev",
		Short: "Start the cache daemon with a LocalStack S3 backend",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			// https://github.com/spf13/cobra/issues/340#issuecomment-374617413
			cmd.SilenceUsage = true

			return runDev(cmd.Context(), opts)
		},
	}

	cmd.Flags().StringVar(&opts.bucketName, "bucket", opts.bucketName, "S3 bucket name")
	cmd.Flags().StringVar(&opts.prefix, "prefix", opts.prefix, "S3 object key prefix")
	cmd.Flags().StringVar(&opts.localstackImage, "localstack-image", opts.localstackImage, "LocalStack container image")

	return cmd
}

func runDev(ctx context.Context, opts *devOptions) error {
	if opts == nil {
		return fmt.Errorf("dev options are nil")
	}

	listenAddr, err := resolveListenAddr()
	if err != nil {
		return err
	}

	bucketName := strings.TrimSpace(opts.bucketName)
	if bucketName == "" {
		bucketName = defaultDevBucketName
	}
	prefixValue := strings.TrimSpace(opts.prefix)

	image := strings.TrimSpace(opts.localstackImage)
	if image == "" {
		image = defaultLocalstackImage
	}

	slog.InfoContext(ctx, "starting localstack for dev", "image", image)
	container, endpoint, err := startLocalstack(ctx, image)
	if err != nil {
		return err
	}
	defer terminateLocalstack(container)

	slog.InfoContext(ctx, "localstack ready", "endpoint", endpoint, "bucket", bucketName)

	client, err := newLocalstackS3Client(ctx, endpoint)
	if err != nil {
		return err
	}

	backend, err := newS3BackendForClient(ctx, client, bucketName, prefixValue)
	if err != nil {
		return err
	}

	return runServer(ctx, listenAddr, bucketName, backend)
}

func startLocalstack(ctx context.Context, image string) (testcontainers.Container, string, error) {
	image = strings.TrimSpace(image)
	if image == "" {
		return nil, "", fmt.Errorf("localstack image is empty")
	}

	request := testcontainers.ContainerRequest{
		Image:        image,
		ExposedPorts: []string{localstackPort},
		Env: map[string]string{
			"DEFAULT_REGION": localstackRegion,
			"SERVICES":       "s3",
		},
		WaitingFor: wait.ForHTTP(localstackHealthPath).WithPort(localstackPort),
	}

	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: request,
		Started:          true,
	})
	if err != nil {
		return nil, "", err
	}

	host, err := container.Host(ctx)
	if err != nil {
		terminateLocalstack(container)
		return nil, "", err
	}

	port, err := container.MappedPort(ctx, localstackPort)
	if err != nil {
		terminateLocalstack(container)
		return nil, "", err
	}

	endpoint := fmt.Sprintf("http://%s:%s", host, port.Port())
	return container, endpoint, nil
}

func newLocalstackS3Client(ctx context.Context, endpoint string) (*s3.Client, error) {
	endpoint = strings.TrimSpace(endpoint)
	if endpoint == "" {
		return nil, fmt.Errorf("localstack endpoint is empty")
	}

	cfg, err := config.LoadDefaultConfig(ctx,
		config.WithRegion(localstackRegion),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(localstackAccessKey, localstackSecretKey, "")),
	)
	if err != nil {
		return nil, fmt.Errorf("load aws config: %w", err)
	}

	return s3.NewFromConfig(cfg, func(options *s3.Options) {
		options.BaseEndpoint = aws.String(endpoint)
		options.UsePathStyle = true
	}), nil
}

func newS3BackendForClient(ctx context.Context, client *s3.Client, bucketName, prefix string) (storage.MultipartBlobStorageBackend, error) {
	if client == nil {
		return nil, fmt.Errorf("s3 client is nil")
	}

	prefix = strings.TrimSpace(prefix)
	if prefix == "" {
		return storage.NewS3Storage(ctx, client, bucketName)
	}
	return storage.NewS3Storage(ctx, client, bucketName, prefix)
}

func terminateLocalstack(container testcontainers.Container) {
	if container == nil {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), localstackTerminateTimeout)
	defer cancel()

	if err := container.Terminate(ctx); err != nil {
		slog.Warn("failed to terminate localstack", "err", err)
	}
}
