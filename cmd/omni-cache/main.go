package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/cirruslabs/omni-cache/internal/commands"
	"github.com/spf13/pflag"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := commands.NewRootCmd().ExecuteContext(ctx); err != nil {
		if !errors.Is(err, pflag.ErrHelp) {
			fmt.Fprintln(os.Stderr, err)
		}
		os.Exit(1)
	}
}
