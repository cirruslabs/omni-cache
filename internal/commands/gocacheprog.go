package commands

import (
	"context"
	"fmt"
	"os"
	"runtime"
	"strings"

	"github.com/cirruslabs/omni-cache/internal/gocacheprog"
	"github.com/cirruslabs/omni-cache/pkg/server"
	"github.com/spf13/cobra"
)

type gocacheprogOptions struct {
	socketPath string
	cacheHost  string
	cacheDir   string
	strict     bool
}

func newGoCacheProgCmd() *cobra.Command {
	opts := &gocacheprogOptions{}
	if runtime.GOOS != "windows" {
		if socketPath, err := server.DefaultSocketPath(); err == nil {
			opts.socketPath = socketPath
		}
	}

	cmd := &cobra.Command{
		Use:   "gocacheprog",
		Short: "Run as a GOCACHEPROG helper",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			cmd.SilenceUsage = true
			return runGoCacheProg(cmd.Context(), opts)
		},
	}

	cmd.Flags().StringVar(&opts.socketPath, "socket", opts.socketPath, "Unix socket path for the omni-cache daemon")
	cmd.Flags().StringVar(&opts.cacheHost, "host", opts.cacheHost, "TCP host:port for the omni-cache daemon (overrides --socket)")
	cmd.Flags().StringVar(&opts.cacheDir, "cache-dir", opts.cacheDir, "Directory for local cache files")
	cmd.Flags().BoolVar(&opts.strict, "strict", opts.strict, "Fail requests on remote cache errors")

	return cmd
}

func runGoCacheProg(ctx context.Context, opts *gocacheprogOptions) error {
	if opts == nil {
		return fmt.Errorf("gocacheprog options are nil")
	}

	cacheDir := strings.TrimSpace(opts.cacheDir)
	cacheHost := strings.TrimSpace(opts.cacheHost)
	socketPath := strings.TrimSpace(opts.socketPath)

	var cacheClient gocacheprog.CacheClient
	switch {
	case cacheHost != "":
		baseURL, err := normalizeCacheHost(cacheHost)
		if err != nil {
			return err
		}
		cacheClient, err = gocacheprog.NewHTTPCacheClient(baseURL, nil)
		if err != nil {
			return err
		}
	case runtime.GOOS == "windows":
		return fmt.Errorf("missing required --host on windows")
	default:
		if socketPath == "" {
			return fmt.Errorf("missing unix socket path")
		}
		var err error
		cacheClient, err = gocacheprog.NewUnixSocketCacheClient(socketPath)
		if err != nil {
			return err
		}
	}

	handler, err := gocacheprog.NewHandler(gocacheprog.Config{
		CacheClient: cacheClient,
		CacheDir:    cacheDir,
		Strict:      opts.strict,
	})
	if err != nil {
		return err
	}

	return handler.Serve(ctx, os.Stdin, os.Stdout)
}

func normalizeCacheHost(host string) (string, error) {
	host = strings.TrimSpace(host)
	if host == "" {
		return "", fmt.Errorf("cache host is empty")
	}
	if strings.HasPrefix(host, "http://") || strings.HasPrefix(host, "https://") {
		return host, nil
	}
	return "http://" + host, nil
}
