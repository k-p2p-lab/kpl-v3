package controller

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"io"
	"reflect"
	"sort"
	"strings"
)

var analysisSourceNames = []string{"scenario.yaml", "experiment.json", "events.jsonl", "observations.jsonl"}

const analysisSourceHashPrefix = "sampled-sha256-v1:"
const sourceSampleSize int64 = 512
const sourceSampleCount int64 = 128

// Stable pseudorandom positions let separate snapshots sample the same bytes.
// Include both ends and the logical length. This is a cache fingerprint, not
// proof of full-file equality: same-size edits outside the samples can be missed.
func sourceSampleOffsets(name string, size int64) []int64 {
	if size <= sourceSampleSize*sourceSampleCount {
		var offsets []int64
		for offset := int64(0); offset < size; offset += sourceSampleSize {
			offsets = append(offsets, offset)
		}
		return offsets
	}
	positions := map[int64]bool{0: true, size - sourceSampleSize: true}
	for i := 0; len(positions) < int(sourceSampleCount); i++ {
		seed := sha256.Sum256([]byte(fmt.Sprintf("kpl-source-samples-v1:%s:%d:%d", name, size, i)))
		positions[int64(binary.LittleEndian.Uint64(seed[:8])%uint64(size-sourceSampleSize+1))] = true
	}
	offsets := make([]int64, 0, len(positions))
	for offset := range positions {
		offsets = append(offsets, offset)
	}
	sort.Slice(offsets, func(i, j int) bool { return offsets[i] < offsets[j] })
	return offsets
}

func analysisSourceSampleBytes(files []resultFile) int64 {
	var total int64
	for _, name := range analysisSourceNames {
		var size int64
		for _, file := range files {
			if file.name == name {
				size += file.size
			}
		}
		total += min(size, sourceSampleSize*sourceSampleCount)
	}
	return total
}

func currentSourceHash(hash string) bool { return strings.HasPrefix(hash, analysisSourceHashPrefix) }

// Existing full hashes remain usable while their cheap source revision is
// unchanged. On a later source change, a new sampled baseline requires analysis;
// the old full digest cannot be converted into a sample digest retroactively.
func reusableSourceHash(hash string) bool {
	return currentSourceHash(hash) || strings.HasPrefix(hash, "sha256:")
}

// Sample logical streams using ReaderAt, so large NAS logs are not read in full.
// Positions are independent of local paths, mtimes and archive segment boundaries.
func analysisSourceHash(ctx context.Context, files []resultFile, progress func(int64)) (string, error) {
	h := sha256.New()
	_, _ = io.WriteString(h, analysisSourceHashPrefix)
	buffer := make([]byte, sourceSampleSize)
	for _, name := range analysisSourceNames {
		content := sha256.New()
		var size int64
		var parts []resultFile
		for _, file := range files {
			if file.name != name || file.size == 0 {
				continue
			}
			parts = append(parts, file)
			size += file.size
		}
		reader := joinedResultReader{parts: parts}
		for _, offset := range sourceSampleOffsets(name, size) {
			if err := ctx.Err(); err != nil {
				return "", err
			}
			length := min(sourceSampleSize, size-offset)
			n, err := reader.ReadAt(buffer[:length], offset)
			if err != nil && err != io.EOF {
				return "", fmt.Errorf("sample %s: %w", name, err)
			}
			if int64(n) != length {
				return "", fmt.Errorf("sample %s: %w", name, io.ErrUnexpectedEOF)
			}
			fmt.Fprintf(content, "%d:", offset)
			_, _ = content.Write(buffer[:n])
			if progress != nil {
				progress(int64(n))
			}
		}
		fmt.Fprintf(h, "%s:%d:%x;", name, size, content.Sum(nil))
	}
	if err := ctx.Err(); err != nil {
		return "", err
	}
	return analysisSourceHashPrefix + hex.EncodeToString(h.Sum(nil)), nil
}

// A cheap verification hint, not the content hash. Include change time and file
// identity so same-size rewrites with restored mtime still request hash checking.
// Inspect only stable fields; access time changes merely from reading the logs.
func sourceFileIdentity(info any) string {
	v := reflect.ValueOf(info)
	if v.Kind() == reflect.Pointer {
		if v.IsNil() {
			return ""
		}
		v = v.Elem()
	}
	if !v.IsValid() || v.Kind() != reflect.Struct {
		return ""
	}
	identity := ""
	for _, name := range []string{"Dev", "Ino", "Ctim", "Ctimespec", "CreationTime", "LastWriteTime"} {
		field := v.FieldByName(name)
		if field.IsValid() && field.CanInterface() {
			identity += fmt.Sprintf("%s=%v;", name, field.Interface())
		}
	}
	return identity
}
