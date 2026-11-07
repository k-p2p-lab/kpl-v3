package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/k-p2p-lab/kpl-v3/internal/master"
)

var version = "dev"

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	err := master.NewApp().Execute(ctx, os.Args[1:], version)
	stop()
	isRuntime := master.IsRuntimeInvocation(os.Args[1:])
	if err != nil && !isRuntime {
		fmt.Fprintln(os.Stderr, "kpl:", err)
	}
	os.Exit(master.ExitCode(err))
}
