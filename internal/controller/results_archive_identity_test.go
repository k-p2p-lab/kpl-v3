package controller

import (
	"bytes"
	"crypto/sha256"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestResultArchiveSizeCacheDetectsSameSizeRewriteWithRestoredMtime(t *testing.T) {
	s := New(ServerConfig{DataDir: t.TempDir()}, nil)
	const id = "same-size-export"
	resultFixture(t, s, id, "completed", time.Now().Add(-time.Minute).UTC())
	path := filepath.Join(s.config.DataDir, currentRunsDirectory, id, "scenario.yaml")
	base := "version: 3\nname: original scenario\nphases:\n  - action: wait\n    duration: 1ms\n#"
	before := []byte(base + strings.Repeat("a", 8192) + "\n")
	var comment strings.Builder
	for i := range 128 {
		digest := sha256.Sum256(fmt.Appendf(nil, "replacement-comment-%d", i))
		fmt.Fprintf(&comment, "%x", digest)
	}
	after := []byte(base + comment.String() + "\n")
	if len(before) != len(after) {
		t.Fatal("fixture changed the source length")
	}
	if err := os.WriteFile(path, before, 0600); err != nil {
		t.Fatal(err)
	}
	original, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	url := "/api/v1/experiments/" + id + "/download"
	first := resultRequest(s, http.MethodHead, url)
	if first.Code != http.StatusOK {
		t.Fatalf("initial HEAD failed: %d %s", first.Code, first.Body)
	}
	if err := os.WriteFile(path, after, 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Chtimes(path, original.ModTime(), original.ModTime()); err != nil {
		t.Fatal(err)
	}
	rewritten, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if !os.SameFile(original, rewritten) || original.Size() != rewritten.Size() || !original.ModTime().Equal(rewritten.ModTime()) {
		t.Fatal("fixture did not preserve inode, size and mtime")
	}
	if sourceFileIdentity(original.Sys()) == sourceFileIdentity(rewritten.Sys()) {
		t.Skip("filesystem does not expose source change identity")
	}
	snapshot, err := s.captureResultFilesContext(t.Context(), id, false)
	if err != nil {
		t.Fatal(err)
	}
	defer snapshot.close()
	var actual bytes.Buffer
	if err := snapshot.writeZIP(t.Context(), &actual); err != nil {
		t.Fatal(err)
	}
	second := resultRequest(s, http.MethodHead, url)
	want := strconv.Itoa(actual.Len())
	if second.Code != http.StatusOK || second.Header().Get("Content-Length") != want {
		t.Fatalf("HEAD reused stale ZIP size after source rewrite: status=%d before=%s after=%s actual=%s", second.Code, first.Header().Get("Content-Length"), second.Header().Get("Content-Length"), want)
	}
	download := resultRequest(s, http.MethodGet, url)
	if download.Code != http.StatusOK || download.Header().Get("Content-Length") != strconv.Itoa(download.Body.Len()) {
		t.Fatalf("download length mismatch: status=%d advertised=%s actual=%d", download.Code, download.Header().Get("Content-Length"), download.Body.Len())
	}
	if got := decodeResultZIP(t, download.Body.Bytes())["scenario.yaml"]; !bytes.Equal(got, after) {
		t.Fatal("download did not preserve the rewritten source bytes")
	}
}
