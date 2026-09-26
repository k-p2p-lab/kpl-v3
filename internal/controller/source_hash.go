package controller

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"reflect"
)

var analysisSourceNames = []string{"scenario.yaml", "experiment.json", "events.jsonl", "observations.jsonl"}

// Hash the logical source streams, independent of local paths, mtimes and NAS
// segment boundaries. Run only in a bounded analysis worker, never under the
// Controller's persistence/analysis locks or while listing saved results.
func analysisSourceHash(ctx context.Context, files []resultFile, progress func(int64)) (string, error) {
	h := sha256.New()
	_, _ = io.WriteString(h, "kpl-analysis-source-v1\n")
	buffer := make([]byte, 128<<10)
	for _, name := range analysisSourceNames {
		content := sha256.New()
		var size int64
		for _, file := range files {
			if file.name != name || file.size == 0 {
				continue
			}
			reader := file.reader()
			var read int64
			for {
				if err := ctx.Err(); err != nil {
					return "", err
				}
				n, err := reader.Read(buffer)
				if n > 0 {
					_, _ = content.Write(buffer[:n])
					read += int64(n)
					if progress != nil {
						progress(int64(n))
					}
				}
				if err == io.EOF {
					break
				}
				if err != nil {
					return "", fmt.Errorf("hash %s: %w", name, err)
				}
			}
			if read != file.size {
				return "", fmt.Errorf("hash %s: %w", name, io.ErrUnexpectedEOF)
			}
			size += read
		}
		fmt.Fprintf(h, "%s:%d:%x;", name, size, content.Sum(nil))
	}
	if err := ctx.Err(); err != nil {
		return "", err
	}
	return "sha256:" + hex.EncodeToString(h.Sum(nil)), nil
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
