package model

import (
	"bufio"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"testing"
)

// A build can replace bin/kpl while a previous invocation still runs. The
// reported executable hash must describe that process, not the new pathname.
func TestBuildDetailsHashesRunningExecutableAfterReplacement(t *testing.T) {
	if os.Getenv("KPL_VERSION_IDENTITY_CHILD") == "1" {
		_, _ = os.Stdout.WriteString("ready\n")
		_, _ = bufio.NewReader(os.Stdin).ReadString('\n')
		_ = json.NewEncoder(os.Stdout).Encode(BuildDetails())
		os.Exit(0)
	}
	if runtime.GOOS != "linux" {
		t.Skip("Linux running-executable identity regression")
	}
	source, err := os.Open("/proc/self/exe")
	if err != nil {
		t.Skipf("running executable unavailable: %v", err)
	}
	defer source.Close()
	dir := t.TempDir()
	path := filepath.Join(dir, "kpl-version-test")
	copy, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0700)
	if err != nil {
		t.Fatal(err)
	}
	hash := sha256.New()
	_, copyErr := io.Copy(io.MultiWriter(copy, hash), source)
	closeErr := copy.Close()
	if copyErr != nil || closeErr != nil {
		t.Fatalf("copy executable: %v / %v", copyErr, closeErr)
	}
	expected := "sha256:" + hex.EncodeToString(hash.Sum(nil))
	child := exec.CommandContext(t.Context(), path, "-test.run=^TestBuildDetailsHashesRunningExecutableAfterReplacement$")
	child.Env = append(os.Environ(), "KPL_VERSION_IDENTITY_CHILD=1")
	child.Stderr = os.Stderr
	input, err := child.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	output, err := child.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := child.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = child.Process.Kill() })
	reader := bufio.NewReader(output)
	if line, err := reader.ReadString('\n'); err != nil || line != "ready\n" {
		t.Fatalf("child readiness: %q, %v", line, err)
	}
	replacement := filepath.Join(dir, "replacement")
	if err := os.WriteFile(replacement, []byte("different executable content"), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(replacement, path); err != nil {
		t.Fatal(err)
	}
	if _, err := input.Write([]byte("measure\n")); err != nil {
		t.Fatal(err)
	}
	_ = input.Close()
	var actual BuildMetadata
	if err := json.NewDecoder(reader).Decode(&actual); err != nil {
		t.Fatal(err)
	}
	if err := child.Wait(); err != nil {
		t.Fatal(err)
	}
	if actual.BinaryHash != expected {
		t.Fatalf("hash described a replacement file, not the running executable: got %s want %s", actual.BinaryHash, expected)
	}
}
