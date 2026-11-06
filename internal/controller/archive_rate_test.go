package controller

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"math"
	"os"
	"testing"
	"time"
)

func TestArchiveTransferDelay(t *testing.T) {
	for _, test := range []struct {
		name          string
		bytes         int
		rate          uint64
		elapsed, want time.Duration
	}{
		{"default", 1 << 20, 128, 0, 7812500 * time.Nanosecond},
		{"previous limit", 1 << 20, 32, 0, 31250 * time.Microsecond},
		{"partial chunk", 1 << 19, 128, time.Millisecond, 2906250 * time.Nanosecond},
		{"slow NAS", 1 << 20, 128, time.Second, 0},
		{"unlimited", 1 << 20, 0, 0, 0},
		{"maximum rate", 1 << 20, math.MaxUint64, 0, 0},
		{"empty", 0, 1, 0, 0},
	} {
		t.Run(test.name, func(t *testing.T) {
			if got := archiveTransferDelay(test.bytes, test.rate, test.elapsed); got != test.want {
				t.Fatalf("delay=%s want=%s", got, test.want)
			}
		})
	}
}

func TestArchiveRateDefaultsAndExplicitUnlimited(t *testing.T) {
	zero, custom := uint64(0), uint64(512)
	for _, rate := range []*uint64{nil, &zero, &custom} {
		s := New(ServerConfig{DataDir: t.TempDir(), ArchiveMiBPerSecond: rate}, nil)
		want := DefaultArchiveMiBPerSecond
		if rate != nil {
			want = *rate
			*rate = 99
		}
		if got := archiveOverviewForTest(t, s).ArchiveMiBPerSecond; got != want {
			t.Fatalf("effective rate=%d want=%d", got, want)
		}
	}
}

func TestArchiveCopyUsesRateWithoutChangingBytes(t *testing.T) {
	for _, rate := range []uint64{0, 1, 128} {
		s := New(ServerConfig{DataDir: t.TempDir(), ArchiveMiBPerSecond: &rate}, nil)
		local, err := os.OpenRoot(t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		defer local.Close()
		remote, err := os.OpenRoot(t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		defer remote.Close()
		payload := bytes.Repeat([]byte("a"), 128<<10)
		if err := local.WriteFile("source", payload, 0600); err != nil {
			t.Fatal(err)
		}
		source, err := openResultFile(local, "source")
		if err != nil {
			t.Fatal(err)
		}
		started := time.Now()
		stored, err := s.copyArchiveFile(t.Context(), remote, "target", source)
		source.close()
		if err != nil {
			t.Fatal(err)
		}
		if rate == 1 && time.Since(started) < 200*time.Millisecond {
			t.Fatal("copy and verification ignored the configured limit")
		}
		sum := sha256.Sum256(payload)
		if stored.Size != int64(len(payload)) || stored.SHA256 != hex.EncodeToString(sum[:]) {
			t.Fatal("copy lost integrity evidence")
		}
		got, err := remote.ReadFile("target")
		if err != nil || !bytes.Equal(got, payload) {
			t.Fatalf("changed archive bytes: %v", err)
		}
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		if _, err := s.hashArchiveFile(ctx, resultFile{}); err == nil {
			t.Fatal("rate setting bypassed cancellation")
		}
	}
}
