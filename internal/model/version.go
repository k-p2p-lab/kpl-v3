package model

import (
	"crypto/sha256"
	"encoding/hex"
	"io"
	"os"
	"runtime/debug"
	"sync"
)

var versionOnce sync.Once
var binaryVersion string

func BuildVersion() string {
	versionOnce.Do(func() {
		binaryVersion = "unknown"
		if info, ok := debug.ReadBuildInfo(); ok {
			revision, dirty := "development", ""
			for _, setting := range info.Settings {
				if setting.Key == "vcs.revision" {
					revision = setting.Value
				}
				if setting.Key == "vcs.modified" && setting.Value == "true" {
					dirty = "+modified"
				}
			}
			binaryVersion = revision + dirty + "/" + info.GoVersion
		}
		if executable, err := os.Executable(); err == nil {
			if file, err := os.Open(executable); err == nil {
				defer file.Close()
				hash := sha256.New()
				if _, err = io.Copy(hash, file); err == nil {
					binaryVersion += "/sha256:" + hex.EncodeToString(hash.Sum(nil))
				}
			}
		}
	})
	return binaryVersion
}
