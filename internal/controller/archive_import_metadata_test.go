package controller

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func archiveImportMetadataFixture(t *testing.T, legacy bool) (*Server, *os.Root, func([]byte)) {
	t.Helper()
	s := New(ServerConfig{DataDir: t.TempDir()}, nil)
	archive, err := s.openArchiveStore()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { archive.Close() })
	const id = "import-identity"
	if err := archive.Mkdir(id, 0700); err != nil {
		t.Fatal(err)
	}
	remote, err := openResultDirectory(archive, id)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { remote.Close() })
	write := func(metadata []byte) {
		t.Helper()
		files := map[string][]byte{"scenario.yaml": []byte("version: 3\nname: archive import\nphases:\n  - action: wait\n    duration: 1ms\n")}
		if metadata != nil {
			files["experiment.json"] = metadata
		} else if err := remote.Remove("experiment.json"); err != nil && !os.IsNotExist(err) {
			t.Fatal(err)
		}
		manifest := runArchiveManifestData{Version: 1, RunID: id, Files: map[string]storedRunFile{}, Logs: map[string][]storedRunFile{}}
		for name, data := range files {
			if err := remote.WriteFile(name, data, 0600); err != nil {
				t.Fatal(err)
			}
			digest := sha256.Sum256(data)
			manifest.Files[name] = storedRunFile{Object: name, Size: int64(len(data)), SHA256: hex.EncodeToString(digest[:])}
		}
		if !legacy {
			if err := writeRunArchive(remote, manifest); err != nil {
				t.Fatal(err)
			}
		}
	}
	return s, remote, write
}

func TestArchiveImportRejectsInvalidMetadataBeforeLocalPublication(t *testing.T) {
	for _, legacy := range []bool{false, true} {
		for _, invalid := range []struct {
			name string
			data []byte
		}{
			{"foreign ID", []byte(`{"id":"foreign-run","state":"completed"}`)},
			{"malformed", []byte(`{"id":"import-identity","state":`)},
			{"trailing value", []byte(`{"id":"import-identity","state":"completed"} {}`)},
			{"missing", nil},
			{"empty state", []byte(`{"id":"import-identity","state":""}`)},
		} {
			t.Run(fmt.Sprintf("legacy=%t/%s", legacy, invalid.name), func(t *testing.T) {
				s, _, write := archiveImportMetadataFixture(t, legacy)
				write(invalid.data)
				firstErr := s.importArchivedRuns(t.Context())
				localPath := filepath.Join(s.config.DataDir, currentRunsDirectory, "import-identity")
				_, localErr := os.Stat(localPath)
				// A valid repair must be discoverable on the next scan, including
				// after a restart. Rejected bytes must never become a local cache.
				valid := []byte(`{"id":"import-identity","state":"completed","futureField":true}`)
				write(valid)
				restarted := New(s.config, nil)
				secondErr := restarted.importArchivedRuns(t.Context())
				local, readErr := os.ReadFile(filepath.Join(localPath, "experiment.json"))
				if firstErr == nil || !os.IsNotExist(localErr) || secondErr != nil || readErr != nil || !bytes.Equal(local, valid) {
					t.Fatalf("invalid metadata became an authoritative index: initial=%v local=%v repair=%v read=%v bytes=%s", firstErr, localErr, secondErr, readErr, local)
				}
			})
		}
	}
}

func TestArchiveImportPreservesIntermediateMetadataAndLocalAuthority(t *testing.T) {
	for _, legacy := range []bool{false, true} {
		for _, state := range []string{"queued", "running", "completed", "interrupted", "cleanup-pending"} {
			t.Run(fmt.Sprintf("legacy=%t/state=%s", legacy, state), func(t *testing.T) {
				s, _, write := archiveImportMetadataFixture(t, legacy)
				valid := fmt.Appendf(nil, `{"id":"import-identity","state":%q,"futureField":{"preserved":true}}`, state)
				write(valid)
				if err := s.importArchivedRuns(t.Context()); err != nil {
					t.Fatal(err)
				}
				// An indexed local record remains authoritative even when a later
				// remote record is invalid. Discovery must not replace local edits.
				write([]byte(`{"id":"foreign-run","state":"completed"}`))
				if err := s.importArchivedRuns(t.Context()); err != nil {
					t.Fatal(err)
				}
				local, err := os.ReadFile(filepath.Join(s.config.DataDir, currentRunsDirectory, "import-identity", "experiment.json"))
				if err != nil || !bytes.Equal(local, valid) {
					t.Fatalf("local metadata changed: bytes=%s err=%v", local, err)
				}
			})
		}
	}
}

func TestArchiveImportRetriesAfterPartialFilesystemPublication(t *testing.T) {
	for _, legacy := range []bool{false, true} {
		t.Run(fmt.Sprint("legacy=", legacy), func(t *testing.T) {
			s, _, write := archiveImportMetadataFixture(t, legacy)
			valid := []byte(`{"id":"import-identity","state":"completed"}`)
			write(valid)
			path := filepath.Join(s.config.DataDir, currentRunsDirectory, "import-identity")
			if err := os.MkdirAll(path, 0700); err != nil {
				t.Fatal(err)
			}
			// A non-file destination lets the import publish its intent and
			// experiment metadata, then prevents it from committing the index.
			blocked := filepath.Join(path, "scenario.yaml")
			if err := os.Mkdir(blocked, 0700); err != nil {
				t.Fatal(err)
			}
			firstErr := s.importArchivedRuns(t.Context())
			_, intentErr := os.Lstat(filepath.Join(path, runArchiveImportFile))
			_, metadataErr := os.Lstat(filepath.Join(path, "experiment.json"))
			if err := os.Remove(blocked); err != nil {
				t.Fatal(err)
			}
			secondErr := New(s.config, nil).importArchivedRuns(t.Context())
			local, readErr := os.ReadFile(filepath.Join(path, "experiment.json"))
			_, markerErr := os.Lstat(filepath.Join(path, runArchiveImportFile))
			if firstErr == nil || intentErr != nil || metadataErr != nil || secondErr != nil || readErr != nil || !bytes.Equal(local, valid) || !os.IsNotExist(markerErr) {
				t.Fatalf("partial import failed to recover: initial=%v intent=%v metadata=%v retry=%v read=%v marker=%v bytes=%s", firstErr, intentErr, metadataErr, secondErr, readErr, markerErr, local)
			}
		})
	}
}

func writeArchiveImportOptionalMetadata(t *testing.T, remote *os.Root, legacy bool, name string, data []byte) {
	t.Helper()
	if err := remote.WriteFile(name, data, 0600); err != nil {
		t.Fatal(err)
	}
	if legacy {
		return
	}
	manifest, err := readRunArchive(remote, "import-identity")
	if err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(data)
	manifest.Files[name] = storedRunFile{Object: name, Size: int64(len(data)), SHA256: hex.EncodeToString(digest[:])}
	if err := writeRunArchive(remote, manifest); err != nil {
		t.Fatal(err)
	}
}

func TestArchiveImportRejectsInvalidOptionalMetadataBeforePublication(t *testing.T) {
	for _, legacy := range []bool{false, true} {
		for _, name := range []string{resultNoteFile, analysisJobFile} {
			for _, damage := range []string{"foreign identity", "malformed", "trailing value", "note too large"} {
				if damage == "note too large" && name != resultNoteFile {
					continue
				}
				t.Run(fmt.Sprintf("legacy=%t/%s/%s", legacy, name, damage), func(t *testing.T) {
					s, remote, write := archiveImportMetadataFixture(t, legacy)
					write([]byte(`{"id":"import-identity","state":"completed"}`))
					valid := []byte(`{"version":1,"id":"saved-job","runId":"import-identity","state":"completed","futureField":true}`)
					if name == resultNoteFile {
						valid = []byte(`{"runId":"import-identity","text":"보관 메모","revision":"saved-revision","updatedAt":"2026-01-01T00:00:00Z","futureField":true}`)
					}
					invalid := bytes.Clone(valid)
					switch damage {
					case "foreign identity":
						invalid = bytes.Replace(invalid, []byte("import-identity"), []byte("foreign-run"), 1)
					case "malformed":
						invalid = invalid[:len(invalid)-1]
					case "trailing value":
						invalid = append(invalid, []byte(" {}")...)
					case "note too large":
						invalid = append(invalid, []byte(strings.Repeat(" ", resultNoteFileLimit))...)
					}
					writeArchiveImportOptionalMetadata(t, remote, legacy, name, invalid)
					firstErr := s.importArchivedRuns(t.Context())
					path := filepath.Join(s.config.DataDir, currentRunsDirectory, "import-identity")
					_, localErr := os.Stat(path)
					writeArchiveImportOptionalMetadata(t, remote, legacy, name, valid)
					restarted := New(s.config, nil)
					repairErr := restarted.importArchivedRuns(t.Context())
					api := "/api/v1/analysis-jobs/import-identity"
					if name == resultNoteFile {
						api = "/api/v1/results/import-identity/note"
					}
					response := resultRequest(restarted, http.MethodGet, api)
					local, readErr := os.ReadFile(filepath.Join(path, name))
					if firstErr == nil || !os.IsNotExist(localErr) || repairErr != nil || response.Code != http.StatusOK || readErr != nil || !bytes.Equal(local, valid) {
						t.Fatalf("invalid optional metadata became local authority: initial=%v local=%v repair=%v status=%d read=%v bytes=%d", firstErr, localErr, repairErr, response.Code, readErr, len(local))
					}
				})
			}
		}
	}
}

func TestArchiveImportKeepsOptionalPendingJobBytesAndReaderSemantics(t *testing.T) {
	for _, legacy := range []bool{false, true} {
		for _, state := range []string{"queued", "running"} {
			t.Run(fmt.Sprintf("legacy=%t/%s", legacy, state), func(t *testing.T) {
				s, remote, write := archiveImportMetadataFixture(t, legacy)
				write([]byte(`{"id":"import-identity","state":"completed"}`))
				job := analysisJobStatus{Version: 1, AnalysisVersion: currentAnalysisVersion, RunID: "import-identity", ID: "pending-job", State: state, StartedAt: time.Unix(100, 0)}
				data, err := json.Marshal(job)
				if err != nil {
					t.Fatal(err)
				}
				writeArchiveImportOptionalMetadata(t, remote, legacy, analysisJobFile, data)
				if err := s.importArchivedRuns(t.Context()); err != nil {
					t.Fatal(err)
				}
				local, err := os.ReadFile(filepath.Join(s.config.DataDir, currentRunsDirectory, "import-identity", analysisJobFile))
				if err != nil || !bytes.Equal(local, data) {
					t.Fatalf("pending source metadata was rewritten: %v %s", err, local)
				}
				loaded, err := s.analysisJobStatus("import-identity")
				if err != nil || loaded.State != "interrupted" || loaded.ID != job.ID || !loaded.StartedAt.Equal(job.StartedAt) {
					t.Fatalf("reader restart semantics changed: %+v %v", loaded, err)
				}
			})
		}
	}
}
