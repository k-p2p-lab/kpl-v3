package model

import (
	"crypto/sha256"
	"encoding/hex"
	"io"
	"os"
	"runtime"
	"runtime/debug"
	"sync"
)

var versionOnce sync.Once
var binaryVersion string

// These values may be stamped by release builds. SourceRevision is especially
// useful in container builds, where the Git directory is outside the context.
var ReleaseVersion = "dev"
var SourceRevision string
var SourceModified string
var BuildTime string

type BuildMetadata struct {
	Version    string `json:"version"`
	Revision   string `json:"revision"`
	Modified   bool   `json:"modified"`
	GoVersion  string `json:"goVersion"`
	BinaryHash string `json:"binaryHash"`
	BuildTime  string `json:"buildTime,omitempty"`
}

var buildMetadata BuildMetadata

// BuildDetails describes this executable, independently of its container image.
// A binary hash is never used as an OCI image digest.
func BuildDetails() BuildMetadata {
	BuildVersion()
	return buildMetadata
}

func BuildVersion() string {
	versionOnce.Do(func() {
		buildMetadata = BuildMetadata{Version: ReleaseVersion, Revision: SourceRevision, GoVersion: runtime.Version(), BuildTime: BuildTime, Modified: SourceModified == "true"}
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
			if SourceRevision != "" {
				revision = SourceRevision
			}
			if SourceModified == "true" {
				dirty = "+modified"
			}
			buildMetadata.Revision = revision
			buildMetadata.Modified = dirty != ""
			buildMetadata.GoVersion = info.GoVersion
			if (buildMetadata.Version == "" || buildMetadata.Version == "dev") && info.Main.Version != "" && info.Main.Version != "(devel)" {
				buildMetadata.Version = info.Main.Version
			}
			binaryVersion = revision + dirty + "/" + info.GoVersion
		}
		// On Linux the executable pathname can be replaced while this process
		// still runs the previous inode (for example, by kpl dev build). The
		// kernel's executable handle identifies the code actually running.
		executable := "/proc/self/exe"
		if runtime.GOOS != "linux" {
			executable, _ = os.Executable()
		}
		if executable != "" {
			if file, err := os.Open(executable); err == nil {
				defer file.Close()
				hash := sha256.New()
				if _, err = io.Copy(hash, file); err == nil {
					buildMetadata.BinaryHash = "sha256:" + hex.EncodeToString(hash.Sum(nil))
					binaryVersion += "/" + buildMetadata.BinaryHash
				}
			}
		}
		if buildMetadata.Revision == "" {
			buildMetadata.Revision = "unknown"
		}
		if (buildMetadata.Version == "" || buildMetadata.Version == "dev") && buildMetadata.Revision != "unknown" && buildMetadata.Revision != "development" {
			buildMetadata.Version = buildMetadata.Revision[:min(12, len(buildMetadata.Revision))]
			if buildMetadata.Modified {
				buildMetadata.Version += "+modified"
			}
		}
	})
	return binaryVersion
}
