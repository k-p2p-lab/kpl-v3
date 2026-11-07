package controller

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"testing"
	"time"
)

type archiveArtifactRecoveryFixture struct {
	server                  *Server
	id, name, local, remote string
	stored                  storedRunFile
	content                 []byte
}

// Two complete publications allow the local index to lag behind an already
// published replacement. This is the crash window after the NAS manifest
// commit and before the local manifest commit/unlink.
func archiveArtifactRecovery(t *testing.T, name string) archiveArtifactRecoveryFixture {
	t.Helper()
	s := New(ServerConfig{DataDir: t.TempDir()}, nil)
	id := "artifact-crash"
	resultFixture(t, s, id, "completed", time.Now().UTC())
	localDir := filepath.Join(s.config.DataDir, currentRunsDirectory, id)
	local := filepath.Join(localDir, name)
	root, err := s.analysisDirectory(id)
	if err != nil {
		t.Fatal(err)
	}
	slot := 0
	if name == analysisArtifactFile(analysisResultFile, 1) || name == analysisArtifactFile(analysisSummaryFile, 1) {
		slot = 1
	}
	status := analysisJobStatus{Version: 1, AnalysisVersion: currentAnalysisVersion, ID: "artifact-job", RunID: id, State: "completed", ArtifactSlot: slot}
	err = writeAnalysisJSON(root, analysisJobFile, status)
	root.Close()
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(local, []byte("{\"analysis\":\"older\"}\n"), 0600); err != nil {
		t.Fatal(err)
	}
	archiveTestRun(t, s, id)
	before, err := os.ReadFile(filepath.Join(localDir, runArchiveManifest))
	if err != nil {
		t.Fatal(err)
	}
	content := []byte("{\"analysis\":\"newer\"}\n")
	if err := os.WriteFile(local, content, 0600); err != nil {
		t.Fatal(err)
	}
	archiveTestRun(t, s, id)
	stored := archiveReuseManifest(t, s, id).Files[name]
	if stored.Object == "" {
		t.Fatal("missing committed artifact")
	}
	if err := os.WriteFile(filepath.Join(localDir, runArchiveManifest), before, 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(local, content, 0600); err != nil {
		t.Fatal(err)
	}
	return archiveArtifactRecoveryFixture{
		server: New(s.config, nil), id: id, name: name, local: local,
		remote: filepath.Join(s.config.DataDir, archivedRunsDirectory, id, stored.Object),
		stored: stored, content: content,
	}
}

func archiveObjectNames(t *testing.T, f archiveArtifactRecoveryFixture) []string {
	t.Helper()
	entries, err := os.ReadDir(filepath.Dir(f.remote))
	if err != nil {
		t.Fatal(err)
	}
	names := make([]string, 0, len(entries))
	for _, entry := range entries {
		names = append(names, entry.Name())
	}
	return names
}

func TestArchiveRecoveryReusesPublishedArtifactBeforeLocalManifestCommit(t *testing.T) {
	for _, name := range []string{analysisResultFile, analysisSummaryFile,
		analysisArtifactFile(analysisResultFile, 1), analysisArtifactFile(analysisSummaryFile, 1)} {
		t.Run(name, func(t *testing.T) {
			f := archiveArtifactRecovery(t, name)
			objects := archiveObjectNames(t, f)
			archiveTestRun(t, f.server, f.id)
			if got := archiveReuseManifest(t, f.server, f.id).Files[name].Object; got != f.stored.Object {
				t.Fatalf("recovery uploaded identical committed artifact again: %s != %s", got, f.stored.Object)
			}
			if got := archiveObjectNames(t, f); !reflect.DeepEqual(got, objects) {
				t.Fatalf("recovery created redundant archive objects: before=%v after=%v", objects, got)
			}
			if _, err := os.Stat(f.local); !os.IsNotExist(err) {
				t.Fatalf("verified remote duplicate retained local payload: %v", err)
			}
			kind := "result"
			if name == analysisSummaryFile || name == analysisArtifactFile(analysisSummaryFile, 1) {
				kind = "summary"
			}
			response := resultRequest(f.server, "GET", "/api/v1/analysis-jobs/"+f.id+"/"+kind+"?jobId=artifact-job")
			if response.Code != 200 || !bytes.Equal(response.Body.Bytes(), f.content) {
				t.Fatalf("recovery did not retain the exact downloadable artifact: %d %s", response.Code, response.Body.String())
			}
		})
	}
}

func TestArchiveRecoveryReusesPublishedArtifactMissingFromLocalIndex(t *testing.T) {
	f := archiveArtifactRecovery(t, analysisResultFile)
	root, err := f.server.analysisDirectory(f.id)
	if err != nil {
		t.Fatal(err)
	}
	manifest, err := readRunArchive(root, f.id)
	if err == nil {
		delete(manifest.Files, f.name)
		err = writeRunArchive(root, manifest)
	}
	root.Close()
	if err != nil {
		t.Fatal(err)
	}
	objects := archiveObjectNames(t, f)
	archiveTestRun(t, f.server, f.id)
	if got := archiveReuseManifest(t, f.server, f.id).Files[f.name].Object; got != f.stored.Object {
		t.Fatalf("recovery ignored a published artifact absent from the local index: %s != %s", got, f.stored.Object)
	}
	if got := archiveObjectNames(t, f); !reflect.DeepEqual(got, objects) {
		t.Fatal("missing local reference caused a redundant upload")
	}
	if _, err := os.Stat(f.local); !os.IsNotExist(err) {
		t.Fatalf("verified artifact retained local payload: %v", err)
	}
}

func TestArchiveRecoveryPreservesArtifactWhenNewRemoteManifestObjectIsDamaged(t *testing.T) {
	for _, damage := range []string{"missing", "truncated", "same-size-corruption"} {
		t.Run(damage, func(t *testing.T) {
			f := archiveArtifactRecovery(t, analysisResultFile)
			switch damage {
			case "missing":
				if err := os.Remove(f.remote); err != nil {
					t.Fatal(err)
				}
			case "truncated":
				if err := os.WriteFile(f.remote, f.content[:len(f.content)/2], 0600); err != nil {
					t.Fatal(err)
				}
			default:
				corrupt := bytes.Clone(f.content)
				corrupt[0] ^= 1
				if err := os.WriteFile(f.remote, corrupt, 0600); err != nil {
					t.Fatal(err)
				}
			}
			objects := archiveObjectNames(t, f)
			if err := f.server.archiveRun(context.Background(), f.id, 0); err == nil {
				t.Error("recovery accepted a damaged object already published in the remote manifest")
			}
			if current, err := os.ReadFile(f.local); err != nil || !bytes.Equal(current, f.content) {
				t.Fatalf("recovery lost the valid local artifact: %v", err)
			}
			if got := archiveObjectNames(t, f); !reflect.DeepEqual(got, objects) {
				t.Fatalf("failed verification still copied more archive objects: before=%v after=%v", objects, got)
			}
			if err := os.WriteFile(f.remote, f.content, 0600); err != nil {
				t.Fatal(err)
			}
			restoredObjects := archiveObjectNames(t, f)
			archiveTestRun(t, f.server, f.id)
			if got := archiveReuseManifest(t, f.server, f.id).Files[f.name].Object; got != f.stored.Object {
				t.Fatalf("restored archive was uploaded again instead of reused: %s != %s", got, f.stored.Object)
			}
			if got := archiveObjectNames(t, f); !reflect.DeepEqual(got, restoredObjects) {
				t.Fatal("retry after restoring the exact committed object created redundant objects")
			}
			if _, err := os.Stat(f.local); !os.IsNotExist(err) {
				t.Fatalf("restored verified archive did not release the local artifact: %v", err)
			}
			response := resultRequest(f.server, "GET", "/api/v1/analysis-jobs/"+f.id+"/result?jobId=artifact-job")
			if response.Code != 200 || !bytes.Equal(response.Body.Bytes(), f.content) {
				t.Fatalf("restored archive artifact download changed: %d %s", response.Code, response.Body.String())
			}
		})
	}
}

func TestArchiveRecoveryDoesNotReusePublishedArtifactWithDifferentLocalContent(t *testing.T) {
	f := archiveArtifactRecovery(t, analysisResultFile)
	changed := []byte("{\"analysis\":\"other\"}\n")
	if len(changed) != len(f.content) {
		t.Fatal("fixture needs equal-length distinct content")
	}
	if err := os.WriteFile(f.local, changed, 0600); err != nil {
		t.Fatal(err)
	}
	archiveTestRun(t, f.server, f.id)
	stored := archiveReuseManifest(t, f.server, f.id).Files[f.name]
	if stored.Object == f.stored.Object {
		t.Fatal("different local bytes were treated as the already published artifact")
	}
	actual, err := os.ReadFile(filepath.Join(filepath.Dir(f.remote), stored.Object))
	if err != nil || !bytes.Equal(actual, changed) {
		t.Fatalf("new local artifact was not archived intact: %v", err)
	}
	previous, err := os.ReadFile(f.remote)
	if err != nil || !bytes.Equal(previous, f.content) {
		t.Fatalf("new publication damaged the earlier immutable object: %v", err)
	}
	if _, err := os.Stat(f.local); !os.IsNotExist(err) {
		t.Fatalf("new verified artifact retained redundant local bytes: %v", err)
	}
}

func TestArchiveRecoveryPreservesNewArtifactPublishedDuringVerification(t *testing.T) {
	f := archiveArtifactRecovery(t, analysisResultFile)
	before, err := os.Stat(f.local)
	if err != nil {
		t.Fatal(err)
	}
	changed := []byte(`{"analysis":"other"}`)
	var once sync.Once
	var replaceErr error
	f.server.archiveIOCheck = func() {
		once.Do(func() {
			// Analysis publication replaces the pathname atomically. The
			// archiver still holds the original captured artifact inode.
			f.server.state.persistMu.Lock()
			defer f.server.state.persistMu.Unlock()
			root, err := f.server.analysisDirectory(f.id)
			if err != nil {
				replaceErr = err
				return
			}
			defer root.Close()
			replaceErr = writeAnalysisJSON(root, f.name, map[string]string{"analysis": "other"})
			if replaceErr == nil {
				// Even equal size/mtime must not authorize removing a newly
				// published inode while cleaning the previous captured copy.
				replaceErr = os.Chtimes(f.local, before.ModTime(), before.ModTime())
			}
			if replaceErr == nil {
				replaceErr = writeAnalysisJSON(root, analysisJobFile, analysisJobStatus{
					Version: 1, AnalysisVersion: currentAnalysisVersion, ID: "new-artifact-job", RunID: f.id, State: "completed",
				})
			}
			f.server.state.markRunArchiveDirty(f.id)
		})
	}
	archiveTestRun(t, f.server, f.id)
	if replaceErr != nil {
		t.Fatal(replaceErr)
	}
	// JSON publication uses the same final newline as the initial fixture.
	changed = append(changed, '\n')
	actual, err := os.ReadFile(f.local)
	if err != nil || !bytes.Equal(actual, changed) {
		t.Fatalf("archive cleanup discarded a newly published local artifact: %v", err)
	}
	previous, err := os.ReadFile(f.remote)
	if err != nil || !bytes.Equal(previous, f.content) {
		t.Fatalf("archive recovery modified the earlier immutable artifact: %v", err)
	}
	response := resultRequest(f.server, "GET", "/api/v1/analysis-jobs/"+f.id+"/result?jobId=new-artifact-job")
	if response.Code != 200 || !bytes.Equal(response.Body.Bytes(), changed) {
		t.Fatalf("newly published artifact became unavailable: %d %s", response.Code, response.Body.String())
	}
}
