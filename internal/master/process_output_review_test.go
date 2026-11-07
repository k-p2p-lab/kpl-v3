package master

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"testing"
	"time"
)

func TestCommandOutputRejectsOversizedMetadataAndStopsProducer(t *testing.T) {
	if os.Getenv("KPL_OUTPUT_LIMIT_CHILD") == "1" {
		_, _ = os.Stdout.Write(bytes.Repeat([]byte("x"), 9<<20))
		time.Sleep(2 * time.Second)
		os.Exit(0)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	output, err := (ExecRunner{}).Output(ctx, Command{
		Name: executable, Args: []string{"-test.run=^TestCommandOutputRejectsOversizedMetadataAndStopsProducer$"},
		Env: append(os.Environ(), "KPL_OUTPUT_LIMIT_CHILD=1"), Stderr: io.Discard,
	})
	if !errors.Is(err, errCapturedCommandOutputLimit) || len(output) != 0 {
		t.Fatalf("oversized metadata was accepted or returned as valid partial output: bytes=%d err=%v", len(output), err)
	}
	if ctx.Err() != nil {
		t.Fatal("output limit did not stop its producer before the caller deadline")
	}
}
